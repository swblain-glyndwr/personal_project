import json
import sys
from dataclasses import replace
from types import ModuleType
from types import SimpleNamespace

import numpy as np
import pytest

from jobs.features.nextads import smoke_product_embedding_runtime as smoke_module
from jobs.features.nextads.smoke_product_embedding_runtime import (
    _runtime_matches,
    run_smoke,
    validate_cpu_torch,
    validate_embedding_vector,
    validate_model_metadata,
    validate_model_version_provenance,
    validate_runtime_environment,
    validate_safe_model_artifacts,
)
from next_ads.features.embedding_contract import (
    load_approved_dev_smoke_binding,
    load_product_embedding_definition,
)


def _write_safe_model_artifact(model_data_path):
    model_data_path.mkdir(parents=True, exist_ok=True)
    (model_data_path / "1_Pooling").mkdir()
    (model_data_path / "2_Normalize").mkdir()
    (model_data_path / "model.safetensors").write_bytes(b"safe")
    (model_data_path / "modules.json").write_text(
        json.dumps(smoke_module.EXPECTED_SENTENCE_TRANSFORMER_MODULES)
    )


def _fake_mlflow_artifact(binding, artifact_root, flavors):
    class FakeClient:
        @staticmethod
        def get_model_version(*, name, version):
            return SimpleNamespace(
                run_id=binding.source_run_id,
                name=name,
                version=version,
            )

    return SimpleNamespace(
        MlflowClient=lambda: FakeClient(),
        artifacts=SimpleNamespace(
            download_artifacts=lambda **kwargs: str(artifact_root)
        ),
        models=SimpleNamespace(
            Model=SimpleNamespace(
                load=lambda path: SimpleNamespace(
                    metadata={
                        "source_model_name": (
                            "sentence-transformers/all-MiniLM-L12-v2"
                        ),
                        "normalised_embeddings": True,
                    },
                    flavors=flavors,
                )
            )
        ),
    )


def test_runtime_release_accepts_exact_bundle_and_runtime_tag_forms():
    expected = "15.4.x-scala2.12"

    assert _runtime_matches("15.4.x-scala2.12", expected)
    assert _runtime_matches("15.4", expected)
    assert _runtime_matches("15.4.12", expected)
    assert not _runtime_matches("15.3", expected)
    assert not _runtime_matches("16.4.x-scala2.12", expected)


def test_runtime_validation_requires_every_exact_dependency_pin():
    definition = load_product_embedding_definition()
    packages = dict(definition.runtime_profile.package_versions)

    validate_runtime_environment(
        definition,
        runtime_version="15.4.x-scala2.12",
        python_version="3.11",
        package_versions=packages,
    )

    packages["torch"] = "2.4.0"
    with pytest.raises(ValueError, match=r"torch: expected 2.10.0\+cpu"):
        validate_runtime_environment(
            definition,
            runtime_version="15.4.x-scala2.12",
            python_version="3.11",
            package_versions=packages,
        )


def test_vector_validation_accepts_finite_normalised_384_vector():
    definition = load_product_embedding_definition()
    vector = np.full(384, 1.0 / np.sqrt(384), dtype="float64")

    dimension, norm = validate_embedding_vector(vector, definition)

    assert dimension == 384
    assert norm == pytest.approx(1.0)


@pytest.mark.parametrize(
    "vector, message",
    [
        (np.ones(383), "dimension must be 384"),
        (np.append(np.ones(383), np.nan), "non-finite"),
        (np.ones(384), "L2-normalised"),
    ],
)
def test_vector_validation_rejects_invalid_output(vector, message):
    definition = load_product_embedding_definition()

    with pytest.raises(ValueError, match=message):
        validate_embedding_vector(vector, definition)


def test_runtime_validation_rejects_wrong_dbr_or_python():
    definition = load_product_embedding_definition()
    packages = dict(definition.runtime_profile.package_versions)

    with pytest.raises(ValueError, match="must use 15.4"):
        validate_runtime_environment(
            definition,
            runtime_version="16.4.x-scala2.12",
            python_version="3.11",
            package_versions=packages,
        )
    with pytest.raises(ValueError, match="must use Python 3.11"):
        validate_runtime_environment(
            definition,
            runtime_version="15.4.x-scala2.12",
            python_version="3.10",
            package_versions=packages,
        )


def test_cpu_validation_rejects_cuda_build_or_available_device():
    cpu_torch = SimpleNamespace(
        version=SimpleNamespace(cuda=None),
        cuda=SimpleNamespace(is_available=lambda: False),
    )
    assert validate_cpu_torch(cpu_torch)["observed_accelerator"] == "CPU"

    cuda_torch = SimpleNamespace(
        version=SimpleNamespace(cuda="12.1"),
        cuda=SimpleNamespace(is_available=lambda: False),
    )
    with pytest.raises(ValueError, match="CPU-only Torch"):
        validate_cpu_torch(cuda_torch)


def test_model_metadata_requires_exact_encoder_and_normalisation():
    definition = load_product_embedding_definition()

    evidence = validate_model_metadata(
        {
            "source_model_name": definition.source_model_name,
            "normalised_embeddings": "true",
        },
        definition,
    )
    assert evidence["model_metadata_normalised_embeddings"] is True

    with pytest.raises(ValueError, match="source_model_name"):
        validate_model_metadata(
            {
                "source_model_name": (
                    "sentence-transformers/all-MiniLM-L6-v2"
                ),
                "normalised_embeddings": "true",
            },
            definition,
        )


def test_model_provenance_requires_approved_run_name_and_version():
    binding = load_approved_dev_smoke_binding()
    valid = SimpleNamespace(
        run_id=binding.source_run_id,
        name=binding.model.registered_model_name,
        version=str(binding.model.registered_model_version),
    )

    validate_model_version_provenance(valid, binding)

    with pytest.raises(ValueError, match="run ID does not match"):
        validate_model_version_provenance(
            SimpleNamespace(
                run_id="0" * 32,
                name=valid.name,
                version=valid.version,
            ),
            binding,
        )


def test_safe_artifact_validation_requires_safetensors_and_no_code(tmp_path):
    _write_safe_model_artifact(tmp_path)
    evidence = validate_safe_model_artifacts(tmp_path)

    assert evidence["safetensor_file_count"] == 1
    assert evidence["module_graph_verified"] is True
    assert len(evidence["artifact_sha256"]) == 64

    (tmp_path / "custom.py").write_text("raise RuntimeError")
    with pytest.raises(ValueError, match="disallowed executable"):
        validate_safe_model_artifacts(tmp_path)


def test_safe_artifact_validation_rejects_custom_module_graph(tmp_path):
    _write_safe_model_artifact(tmp_path)
    modules = list(smoke_module.EXPECTED_SENTENCE_TRANSFORMER_MODULES)
    modules[0] = {
        **modules[0],
        "type": "custom_package.RemoteCode",
    }
    (tmp_path / "modules.json").write_text(json.dumps(modules))

    with pytest.raises(ValueError, match="approved Transformer"):
        validate_safe_model_artifacts(tmp_path)


def test_approved_loader_checks_and_loads_the_exact_safe_artifact(
    tmp_path,
    monkeypatch,
):
    definition = load_product_embedding_definition()
    binding = load_approved_dev_smoke_binding()
    artifact_root = tmp_path / "artifact"
    model_data_path = artifact_root / "model.sentence_transformer"
    _write_safe_model_artifact(model_data_path)
    flavors = {
        "sentence_transformers": {
            "code": None,
            "sentence_transformers_version": "2.4.0",
        },
        "python_function": {
            "code": None,
            "data": "model.sentence_transformer",
            "loader_module": "mlflow.sentence_transformers",
        },
    }
    fake_mlflow = _fake_mlflow_artifact(binding, artifact_root, flavors)
    artifact_sha256 = validate_safe_model_artifacts(model_data_path)[
        "artifact_sha256"
    ]
    binding = replace(binding, artifact_sha256=artifact_sha256)
    constructor_calls = []

    class FakeSentenceTransformer:
        def __init__(self, path, **kwargs):
            constructor_calls.append((path, kwargs))

    sentence_transformers_module = ModuleType("sentence_transformers")
    sentence_transformers_module.SentenceTransformer = (
        FakeSentenceTransformer
    )
    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        sentence_transformers_module,
    )

    model, evidence = smoke_module.load_approved_sentence_transformer(
        fake_mlflow,
        binding,
        definition,
    )

    assert isinstance(model, FakeSentenceTransformer)
    assert constructor_calls == [
        (
            str(model_data_path.resolve()),
            {"device": "cpu", "trust_remote_code": False},
        )
    ]
    assert evidence["module_graph_verified"] is True
    assert evidence["custom_model_code"] is False
    assert evidence["approved_artifact_sha256"] == artifact_sha256


def test_approved_loader_rejects_an_unapproved_artifact_digest(
    tmp_path,
    monkeypatch,
):
    definition = load_product_embedding_definition()
    binding = load_approved_dev_smoke_binding()
    artifact_root = tmp_path / "artifact"
    model_data_path = artifact_root / "model.sentence_transformer"
    _write_safe_model_artifact(model_data_path)
    flavors = {
        "sentence_transformers": {
            "sentence_transformers_version": "2.4.0"
        },
        "python_function": {
            "data": "model.sentence_transformer",
            "loader_module": "mlflow.sentence_transformers",
        },
    }
    fake_mlflow = _fake_mlflow_artifact(binding, artifact_root, flavors)

    class UnexpectedSentenceTransformer:
        def __init__(self, *args, **kwargs):
            raise AssertionError("Unapproved artifact must not be loaded")

    sentence_transformers_module = ModuleType("sentence_transformers")
    sentence_transformers_module.SentenceTransformer = (
        UnexpectedSentenceTransformer
    )
    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        sentence_transformers_module,
    )

    with pytest.raises(ValueError, match="artifact digest does not match"):
        smoke_module.load_approved_sentence_transformer(
            fake_mlflow,
            binding,
            definition,
        )


@pytest.mark.parametrize(
    ("flavors", "expected_message"),
    [
        ({}, "does not contain a SentenceTransformers flavor"),
        (
            {
                "sentence_transformers": {
                    "sentence_transformers_version": "2.4.0"
                },
                "python_function": {
                    "data": "../outside-model",
                    "loader_module": "mlflow.sentence_transformers",
                },
            },
            "python_function data must be model.sentence_transformer",
        ),
        (
            {
                "sentence_transformers": {
                    "sentence_transformers_version": "2.4.0"
                },
                "python_function": {
                    "data": "model.sentence_transformer",
                    "loader_module": "custom.loader",
                },
            },
            "loader_module must be mlflow.sentence_transformers",
        ),
    ],
)
def test_approved_loader_rejects_wrong_flavor_or_path_traversal(
    tmp_path,
    monkeypatch,
    flavors,
    expected_message,
):
    definition = load_product_embedding_definition()
    binding = load_approved_dev_smoke_binding()
    artifact_root = tmp_path / "artifact"
    artifact_root.mkdir()
    fake_mlflow = _fake_mlflow_artifact(binding, artifact_root, flavors)
    sentence_transformers_module = ModuleType("sentence_transformers")
    sentence_transformers_module.SentenceTransformer = object
    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        sentence_transformers_module,
    )

    with pytest.raises(ValueError, match=expected_message):
        smoke_module.load_approved_sentence_transformer(
            fake_mlflow,
            binding,
            definition,
        )


def test_run_smoke_uses_fixed_binding_and_emits_observed_evidence(monkeypatch):
    definition = load_product_embedding_definition()
    package_versions = dict(definition.runtime_profile.package_versions)
    monkeypatch.setattr(
        smoke_module,
        "_runtime_version",
        lambda spark: "15.4.x-scala2.12",
    )
    monkeypatch.setattr(
        smoke_module,
        "_installed_package_versions",
        lambda package_names: package_versions,
    )
    registry_uris = []
    fake_mlflow = SimpleNamespace(
        set_registry_uri=lambda uri: registry_uris.append(uri)
    )
    fake_torch = SimpleNamespace(
        version=SimpleNamespace(cuda=None),
        cuda=SimpleNamespace(is_available=lambda: False),
    )

    class FakeModel:
        @staticmethod
        def get_sentence_embedding_dimension():
            return 384

        @staticmethod
        def encode(*args, **kwargs):
            return [np.full(384, 1.0 / np.sqrt(384))]

    def fake_loader(mlflow_module, binding, loaded_definition):
        assert binding.model.registered_model_version == 11
        assert loaded_definition == definition
        return FakeModel(), {
            "approved_source_run_id": binding.source_run_id,
            "safetensor_file_count": 1,
        }

    manifest = run_smoke(
        object(),
        mlflow_module=fake_mlflow,
        torch_module=fake_torch,
        model_loader=fake_loader,
    )

    assert registry_uris == ["databricks-uc"]
    assert manifest["status"] == "PASS"
    assert manifest["model_uri"].endswith("/11")
    assert manifest["approved_source_run_id"] == (
        "95be978bd9e24783afe4e68def0c9845"
    )
    assert manifest["observed_accelerator"] == "CPU"
    assert manifest["writes_performed"] is False

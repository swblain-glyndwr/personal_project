from copy import deepcopy
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import yaml

from next_ads.features.embedding_contract import (
    EmbeddingModelBinding,
    EmbeddingRuntimeProfile,
    ProductEmbeddingDefinition,
    load_approved_dev_smoke_binding,
    load_product_embedding_definition,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTRACT_PATH = (
    PROJECT_ROOT / "configs" / "features" / "product_embeddings.yaml"
)
REQUIREMENTS_PATH = PROJECT_ROOT / "requirements-feature-store-embeddings.txt"


def _contract_document() -> dict:
    return yaml.safe_load(CONTRACT_PATH.read_text())


def _write_contract(tmp_path: Path, document: dict) -> Path:
    path = tmp_path / "product_embeddings.yaml"
    path.write_text(yaml.safe_dump(document, sort_keys=False))
    return path


def test_product_embedding_definition_pins_supported_runtime_and_model():
    definition = load_product_embedding_definition()

    assert definition.contract_version == 1
    assert (
        definition.source_model_name
        == "sentence-transformers/all-MiniLM-L12-v2"
    )
    assert definition.embedding_dimension == 384
    assert definition.normalize_embeddings is True
    assert definition.allow_runtime_registration is False
    assert definition.runtime_profile.spark_version == "15.4.x-scala2.12"
    assert definition.runtime_profile.runtime_engine == "STANDARD"
    assert definition.runtime_profile.accelerator == "CPU"
    assert definition.runtime_profile.python_version == "3.11"
    assert definition.runtime_profile.requirements_file == (
        "requirements-feature-store-embeddings.txt"
    )
    assert definition.runtime_profile.pip_options == (
        "--extra-index-url https://download.pytorch.org/whl/cpu",
    )
    assert dict(definition.runtime_profile.package_versions) == {
        "mlflow": "3.11.1",
        "numpy": "1.26.4",
        "protobuf": "4.24.1",
        "sentence-transformers": "2.4.0",
        "torch": "2.10.0+cpu",
        "transformers": "4.41.2",
    }


def test_product_embedding_definition_and_runtime_are_immutable():
    definition = load_product_embedding_definition()

    with pytest.raises(FrozenInstanceError):
        definition.embedding_dimension = 768
    with pytest.raises(FrozenInstanceError):
        definition.runtime_profile.spark_version = "18.1.x-scala2.13"


def test_direct_contract_construction_cannot_bypass_validation():
    definition = load_product_embedding_definition()
    runtime = definition.runtime_profile

    with pytest.raises(ValueError, match="spark_version must be"):
        EmbeddingRuntimeProfile(
            spark_version="18.1.x-scala2.13",
            runtime_engine=runtime.runtime_engine,
            accelerator=runtime.accelerator,
            python_version=runtime.python_version,
            requirements_file=runtime.requirements_file,
            pip_options=runtime.pip_options,
            package_versions=runtime.package_versions,
        )
    with pytest.raises(ValueError, match="embedding_dimension must be 384"):
        ProductEmbeddingDefinition(
            contract_version=definition.contract_version,
            source_model_name=definition.source_model_name,
            embedding_dimension=768,
            normalize_embeddings=definition.normalize_embeddings,
            allow_runtime_registration=(definition.allow_runtime_registration),
            runtime_profile=runtime,
        )


def test_embedding_requirements_are_exact_and_match_runtime_profile():
    definition = load_product_embedding_definition()
    requirement_pins = {
        name: version
        for name, version in (
            line.split("==", 1)
            for line in REQUIREMENTS_PATH.read_text().splitlines()
            if line.strip()
            and not line.lstrip().startswith(("#", "--"))
        )
    }

    assert requirement_pins == dict(
        definition.runtime_profile.package_versions
    )
    assert REQUIREMENTS_PATH.read_text().splitlines()[0] == (
        "--extra-index-url https://download.pytorch.org/whl/cpu"
    )


def test_dev_smoke_binding_is_fixed_to_recorded_model_provenance():
    binding = load_approved_dev_smoke_binding()

    assert binding.environment == "DEV"
    assert binding.purpose == "compatibility_smoke_only"
    assert binding.model.registered_model_name == (
        "marketingdata_dev.stephen_blain."
        "nextads_pctr_advert_sentence_transformer"
    )
    assert binding.model.registered_model_version == 11
    assert binding.source_run_id == "95be978bd9e24783afe4e68def0c9845"
    assert binding.artifact_sha256 == (
        "bc4daec2a2647ce42ad35df49181c762187cd4e1fed008915ce4f76ac89ca384"
    )


def test_dev_smoke_binding_rejects_changed_provenance(tmp_path):
    binding_path = tmp_path / "binding.yaml"
    binding_path.write_text(
        "product_embedding_smoke:\n"
        "  environment: DEV\n"
        "  purpose: compatibility_smoke_only\n"
        "  registered_model_name: catalog.schema.model\n"
        "  registered_model_version: 1\n"
        "  model_uri: models:/catalog.schema.model/1\n"
        "  source_run_id: not-a-run-id\n"
        f"  artifact_sha256: {'0' * 64}\n"
    )

    with pytest.raises(ValueError, match="source_run_id"):
        load_approved_dev_smoke_binding(binding_path)


def test_embedding_model_binding_requires_one_exact_numeric_mlflow_version():
    binding = EmbeddingModelBinding(
        registered_model_name=(
            "marketingdata_dev.nextads_feature_store.product_encoder"
        ),
        registered_model_version="11",
        model_uri=(
            "models:/marketingdata_dev.nextads_feature_store."
            "product_encoder/11"
        ),
    )

    assert binding.registered_model_version == 11
    assert binding.model_uri.endswith("/11")
    with pytest.raises(FrozenInstanceError):
        binding.registered_model_version = 12


@pytest.mark.parametrize(
    ("version", "model_uri", "expected_message"),
    [
        ("", "models:/catalog.schema.model/1", "positive numeric version"),
        (
            "champion",
            "models:/catalog.schema.model/1",
            "positive numeric version",
        ),
        (0, "models:/catalog.schema.model/0", "positive numeric version"),
        (1, "", "empty model_uri"),
        (
            1,
            "models:/catalog.schema.model@champion",
            "cannot use an MLflow alias",
        ),
        (
            1,
            "models:/catalog.schema.model/champion",
            "exact numeric MLflow form",
        ),
    ],
)
def test_embedding_model_binding_rejects_alias_blank_and_non_numeric_versions(
    version,
    model_uri,
    expected_message,
):
    with pytest.raises(ValueError, match=expected_message):
        EmbeddingModelBinding(
            registered_model_name="catalog.schema.model",
            registered_model_version=version,
            model_uri=model_uri,
        )


@pytest.mark.parametrize(
    ("registered_name", "registered_version", "model_uri", "expected_message"),
    [
        (
            "catalog.schema.other_model",
            11,
            "models:/catalog.schema.model/11",
            "does not match registered_model_name",
        ),
        (
            "catalog.schema.model",
            12,
            "models:/catalog.schema.model/11",
            "does not match registered_model_version",
        ),
    ],
)
def test_embedding_model_binding_rejects_inconsistent_fields(
    registered_name,
    registered_version,
    model_uri,
    expected_message,
):
    with pytest.raises(ValueError, match=expected_message):
        EmbeddingModelBinding(
            registered_model_name=registered_name,
            registered_model_version=registered_version,
            model_uri=model_uri,
        )


@pytest.mark.parametrize(
    "registered_name",
    [
        "workspace_model",
        "schema.model",
        "catalog..model",
        "catalog.schema.model.extra",
    ],
)
def test_embedding_model_binding_requires_three_part_unity_catalog_name(
    registered_name,
):
    with pytest.raises(ValueError, match="three-part Unity Catalog name"):
        EmbeddingModelBinding(
            registered_model_name=registered_name,
            registered_model_version=11,
            model_uri=f"models:/{registered_name}/11",
        )


@pytest.mark.parametrize(
    ("field_path", "invalid_value", "expected_message"),
    [
        (("source_model_name",), "", "empty source_model_name"),
        (
            ("source_model_name",),
            "sentence-transformers/all-MiniLM-L6-v2",
            "source_model_name must be",
        ),
        (("embedding_dimension",), 768, "embedding_dimension must be 384"),
        (
            ("normalize_embeddings",),
            False,
            "normalize_embeddings must be True",
        ),
        (
            ("allow_runtime_registration",),
            True,
            "allow_runtime_registration must be False",
        ),
        (
            ("runtime_profile", "spark_version"),
            "18.1.x-scala2.13",
            "spark_version must be",
        ),
        (
            ("runtime_profile", "runtime_engine"),
            "PHOTON",
            "runtime_engine must be 'STANDARD'",
        ),
        (
            ("runtime_profile", "accelerator"),
            "GPU",
            "accelerator must be 'CPU'",
        ),
        (
            ("runtime_profile", "python_version"),
            "3.12",
            "python_version must be '3.11'",
        ),
    ],
)
def test_product_embedding_definition_rejects_inconsistent_fields(
    tmp_path,
    field_path,
    invalid_value,
    expected_message,
):
    document = deepcopy(_contract_document())
    target = document["product_embeddings"]
    for field_name in field_path[:-1]:
        target = target[field_name]
    target[field_path[-1]] = invalid_value

    with pytest.raises(ValueError, match=expected_message):
        load_product_embedding_definition(_write_contract(tmp_path, document))


def test_package_config_must_match_exact_requirements_file(tmp_path):
    requirements_path = tmp_path / "requirements.txt"
    requirements_path.write_text("sentence-transformers==2.4.0\n")

    with pytest.raises(ValueError, match="do not match"):
        load_product_embedding_definition(
            requirements_path=requirements_path,
        )


def test_embedding_requirements_reject_non_exact_versions(tmp_path):
    requirements_path = tmp_path / "requirements.txt"
    requirements_path.write_text("sentence-transformers>=2.4.0\n")

    with pytest.raises(ValueError, match="must use exact package pins"):
        load_product_embedding_definition(
            requirements_path=requirements_path,
        )

import hashlib
from datetime import date
import inspect
from types import SimpleNamespace

import pytest

from next_ads.model_development.spark_training import (
    SparkBinaryClassifierTrainer,
    artifact_directory_digest,
    log_spark_model_with_signature,
    temporal_validation_cutoff,
    validate_spark_model_signature,
)
from next_ads.model_development.registry import load_model_definition


def test_artifact_digest_includes_relative_paths_and_file_bytes(tmp_path):
    model = tmp_path / "model"
    data = model / "data"
    data.mkdir(parents=True)
    (model / "MLmodel").write_text("model metadata")
    (data / "part-00000").write_bytes(b"model bytes")

    first = artifact_directory_digest(model)
    second = artifact_directory_digest(model)

    assert first == second
    assert len(first) == hashlib.sha256().digest_size * 2
    (data / "part-00000").write_bytes(b"changed model bytes")
    assert artifact_directory_digest(model) != first


def test_artifact_digest_rejects_empty_or_missing_artifacts(tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()

    with pytest.raises(ValueError, match="empty"):
        artifact_directory_digest(empty)
    with pytest.raises(ValueError, match="does not exist"):
        artifact_directory_digest(tmp_path / "missing")


def test_trainer_uses_unity_catalog_safe_version_tags():
    source = inspect.getsource(SparkBinaryClassifierTrainer.train)

    assert "MODEL_VERSION_TAG_ARTIFACT_DIGEST" in source
    assert "MODEL_VERSION_TAG_BUILD_ID" in source
    assert "MODEL_VERSION_TAG_TRAINING_RECEIPT_ID" in source
    assert "registered_model_artifact_digest(registered_model_uri)" in source
    assert "client.download_artifacts" not in source
    assert '"nextads.' not in source
    assert "definition.training_observation.observation_date_column" in source
    assert (
        "definition.training_observation.observation_timestamp" not in source
    )


def test_generic_build_registers_an_exact_version_without_assigning_an_alias():
    source = inspect.getsource(SparkBinaryClassifierTrainer.train)

    assert "mlflow.register_model(" in source
    assert "set_model_version_tag(" in source
    assert "set_registered_model_alias(" not in source
    assert "dev_candidate" not in source


def test_temporal_validation_holds_out_the_latest_whole_dates():
    dates = tuple(date(2026, 8, day) for day in range(1, 11))

    assert temporal_validation_cutoff(dates, validation_percent=20) == date(
        2026,
        8,
        9,
    )
    with pytest.raises(ValueError, match="at least two dates"):
        temporal_validation_cutoff((date(2026, 8, 1),))


class _NamedSchema:
    def __init__(self, *names):
        self._names = names

    def input_names(self):
        return list(self._names)


class _SelectedFrame:
    def __init__(self, columns):
        self.columns = tuple(columns)

    def select(self, *columns):
        return _SelectedFrame(columns)


def test_log_spark_model_receives_declared_named_signature():
    definition = load_model_definition("shopping_bag_pctr")
    transformed_inputs = []

    class Model:
        def transform(self, frame):
            transformed_inputs.append(frame.columns)
            return _SelectedFrame((*frame.columns, "prediction"))

    signature = SimpleNamespace(
        inputs=_NamedSchema(*definition.model_feature_columns),
        outputs=_NamedSchema("prediction"),
    )
    inferred_frames = []

    def infer_signature(model_input, model_output):
        inferred_frames.append((model_input.columns, model_output.columns))
        return signature

    logged = []
    mlflow = SimpleNamespace(
        spark=SimpleNamespace(
            log_model=lambda model, **kwargs: logged.append((model, kwargs))
        )
    )
    model = Model()

    actual = log_spark_model_with_signature(
        mlflow,
        model,
        definition,
        _SelectedFrame(("audit_only", *definition.model_feature_columns)),
        infer_signature_fn=infer_signature,
    )

    assert actual is signature
    assert transformed_inputs == [definition.model_feature_columns]
    assert inferred_frames == [
        (definition.model_feature_columns, ("prediction",))
    ]
    assert logged == [
        (
            model,
            {
                "artifact_path": "model",
                "signature": signature,
            },
        )
    ]


def test_spark_model_signature_rejects_audit_inputs_or_wrong_output():
    definition = SimpleNamespace(
        model_feature_columns=("location", "advert_theme")
    )

    with pytest.raises(ValueError, match="declared model features"):
        validate_spark_model_signature(
            definition,
            SimpleNamespace(
                inputs=_NamedSchema("location", "treatment", "advert_theme"),
                outputs=_NamedSchema("prediction"),
            ),
        )
    with pytest.raises(ValueError, match="Spark prediction"):
        validate_spark_model_signature(
            definition,
            SimpleNamespace(
                inputs=_NamedSchema(*definition.model_feature_columns),
                outputs=_NamedSchema("probability"),
            ),
        )

from dataclasses import replace
from datetime import date, datetime, timezone

import pytest

from next_ads.model_development.contracts import (
    ModelBuild,
    TrainingFeatureBinding,
    TrainingSetReceipt,
)
from next_ads.model_development.registry import load_model_definition
from next_ads.model_development import runtime


NOW = datetime(2026, 8, 17, tzinfo=timezone.utc)


def _receipt(definition):
    return TrainingSetReceipt(
        receipt_id="receipt",
        model_name="analytics_pctr",
        model_definition_checksum=definition.checksum,
        feature_bindings=(
            TrainingFeatureBinding(
                feature_id="pctr",
                feature_snapshot_id="snapshot",
                feature_snapshot_attempt_id="attempt",
                backing_table="catalog.schema.pctr",
                delta_version=3,
                row_count=10,
                schema_checksum="b" * 64,
                value_checksum="c" * 64,
            ),
        ),
        observation_start=date(2026, 8, 11),
        observation_end=date(2026, 8, 11),
        label_end=date(2026, 8, 11),
        schema_checksum="d" * 64,
        data_checksum="e" * 64,
        code_sha="abc123",
        leakage_status="PASS",
        status="READY",
        created_at=NOW,
        completed_at=NOW,
    )


def _ready_build(definition, receipt):
    return ModelBuild(
        model_build_id=runtime.model_build_id(definition, receipt),
        model_name=definition.model_name,
        training_receipt_id=receipt.receipt_id,
        model_definition_checksum=definition.checksum,
        runtime_profile=definition.runtime_profile,
        status="READY",
        created_at=NOW,
        mlflow_run_id="run",
        registered_model_name="catalog.schema.model",
        registered_model_version=2,
        model_uri="models:/catalog.schema.model/2",
        artifact_digest="f" * 64,
        metrics=(("auc_pr", 0.5),),
        completed_at=NOW,
    )


def test_retry_reuses_ready_build_without_calling_trainer(monkeypatch):
    definition = load_model_definition("analytics_pctr")
    receipt = _receipt(definition)
    ready = _ready_build(definition, receipt)
    monkeypatch.setattr(runtime, "load_ready_model_build", lambda *_a, **_k: ready)
    monkeypatch.setattr(
        runtime,
        "persist_model_build",
        lambda *_a, **_k: pytest.fail("retry must not write a duplicate build"),
    )

    class Trainer:
        def train(self, *_args):
            pytest.fail("retry must not retrain")

    validated = []
    actual, reused = runtime.train_or_reuse_model(
        object(),
        catalog="catalog",
        schema="schema",
        definition=definition,
        receipt=receipt,
        training_frame=object(),
        trainer=Trainer(),
        ready_build_validator=validated.append,
    )

    assert actual == ready
    assert reused is True
    assert validated == [ready]


def test_retry_recovers_registered_version_before_retraining(monkeypatch):
    definition = load_model_definition("analytics_pctr")
    receipt = _receipt(definition)
    ready = _ready_build(definition, receipt)
    persisted = []
    monkeypatch.setattr(runtime, "load_ready_model_build", lambda *_a, **_k: None)
    monkeypatch.setattr(
        runtime,
        "persist_model_build",
        lambda *_a, **kwargs: persisted.append(kwargs["build"]),
    )

    class Trainer:
        def train(self, *_args):
            pytest.fail("a recovered registered version must not be retrained")

    validated = []
    actual, reused = runtime.train_or_reuse_model(
        object(),
        catalog="catalog",
        schema="schema",
        definition=definition,
        receipt=receipt,
        training_frame=object(),
        trainer=Trainer(),
        ready_build_validator=validated.append,
        ready_build_recovery=lambda: ready,
    )

    assert actual == ready
    assert reused is True
    assert persisted == [ready]
    assert validated == [ready]


def test_new_build_records_training_then_ready(monkeypatch):
    definition = load_model_definition("analytics_pctr")
    receipt = _receipt(definition)
    ready = _ready_build(definition, receipt)
    persisted = []
    monkeypatch.setattr(runtime, "load_ready_model_build", lambda *_a, **_k: None)
    monkeypatch.setattr(
        runtime,
        "persist_model_build",
        lambda *_a, **kwargs: persisted.append(kwargs["build"]),
    )

    class Trainer:
        def train(self, *_args):
            return ready

    actual, reused = runtime.train_or_reuse_model(
        object(),
        catalog="catalog",
        schema="schema",
        definition=definition,
        receipt=receipt,
        training_frame=object(),
        trainer=Trainer(),
    )

    assert actual == ready
    assert reused is False
    assert [build.status for build in persisted] == ["TRAINING", "READY"]


def test_training_failure_is_persisted_and_never_ready(monkeypatch):
    definition = load_model_definition("analytics_pctr")
    receipt = _receipt(definition)
    persisted = []
    monkeypatch.setattr(runtime, "load_ready_model_build", lambda *_a, **_k: None)
    monkeypatch.setattr(
        runtime,
        "persist_model_build",
        lambda *_a, **kwargs: persisted.append(kwargs["build"]),
    )

    class Trainer:
        def train(self, *_args):
            raise RuntimeError("training failed")

    with pytest.raises(RuntimeError, match="training failed"):
        runtime.train_or_reuse_model(
            object(),
            catalog="catalog",
            schema="schema",
            definition=definition,
            receipt=receipt,
            training_frame=object(),
            trainer=Trainer(),
        )

    assert [build.status for build in persisted] == ["TRAINING", "FAILED"]
    assert persisted[-1].failure_reason == "RuntimeError: training failed"


def test_trainer_cannot_return_a_build_for_different_inputs(monkeypatch):
    definition = load_model_definition("analytics_pctr")
    receipt = _receipt(definition)
    persisted = []
    monkeypatch.setattr(runtime, "load_ready_model_build", lambda *_a, **_k: None)
    monkeypatch.setattr(
        runtime,
        "persist_model_build",
        lambda *_a, **kwargs: persisted.append(kwargs["build"]),
    )
    wrong = replace(_ready_build(definition, receipt), model_build_id="wrong")

    class Trainer:
        def train(self, *_args):
            return wrong

    with pytest.raises(ValueError, match="different inputs"):
        runtime.train_or_reuse_model(
            object(),
            catalog="catalog",
            schema="schema",
            definition=definition,
            receipt=receipt,
            training_frame=object(),
            trainer=Trainer(),
        )

    assert persisted[-1].status == "FAILED"


def test_training_rejects_receipt_from_a_different_definition(monkeypatch):
    definition = load_model_definition("analytics_pctr")
    receipt = replace(
        _receipt(definition),
        model_definition_checksum="a" * 64,
    )
    monkeypatch.setattr(
        runtime,
        "load_ready_model_build",
        lambda *_a, **_k: pytest.fail("invalid receipt must not be loaded"),
    )

    with pytest.raises(ValueError, match="different model definition"):
        runtime.train_or_reuse_model(
            object(),
            catalog="catalog",
            schema="schema",
            definition=definition,
            receipt=receipt,
            training_frame=object(),
            trainer=object(),
        )

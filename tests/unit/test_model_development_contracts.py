from datetime import date, datetime, timezone

import pytest

from next_ads.model_development import (
    DBR_15_4_SPARK_CPU,
    FeatureLookupSpec,
    ModelBuild,
    ModelDefinition,
    TrainingFeatureBinding,
    TrainingSetReceipt,
)


NOW = datetime(2026, 8, 1, tzinfo=timezone.utc)
DIGEST = "a" * 64


def _lookup():
    return FeatureLookupSpec(
        feature_id="next_uk_nextads_fs_pctr_model_input",
        selected_columns=("advert_ctr", "device_ctr"),
        key_mapping=(
            ("account_number", "account_number"),
            ("advert_id", "advert_id"),
        ),
        observation_timestamp="impression_timestamp",
        defaults=(("device_ctr", 0.0),),
    )


def _definition(**overrides):
    values = {
        "model_name": "shopping_bag_pctr",
        "provider_id": "shopping_bag_pctr",
        "problem_statement": "Rank eligible Shopping Bag adverts",
        "prediction_entity": "account, advert and session",
        "prediction_time": "session start",
        "label": "advert click within the session",
        "observation_keys": (
            "account_number",
            "advert_id",
            "reference_date",
        ),
        "success_metrics": ("log_loss", "calibration"),
        "runtime_profile": DBR_15_4_SPARK_CPU,
        "feature_lookups": (_lookup(),),
        "trainer": "spark_logistic_regression",
        "score_provider": "shopping_bag_pctr_scores",
        "candidate_adapter": "shopping_bag_candidates",
    }
    values.update(overrides)
    return ModelDefinition(**values)


def _feature_binding():
    return TrainingFeatureBinding(
        feature_id="next_uk_nextads_fs_pctr_model_input",
        feature_snapshot_id="analytics_pctr:2026-08-01",
        feature_snapshot_attempt_id="123",
        backing_table=(
            "marketingdata_dev.nextads_feature_store."
            "next_uk_nextads_fs_pctr_model_input"
        ),
        delta_version=42,
        row_count=100,
        schema_checksum=DIGEST,
        value_checksum="b" * 64,
    )


def test_definition_is_stable_and_evaluate_only():
    first = _definition()
    second = _definition()

    assert first.checksum == second.checksum
    assert first.activation_mode == "EVALUATE"
    with pytest.raises(ValueError, match="must remain EVALUATE"):
        _definition(activation_mode="CHAMPION")


def test_definition_rejects_unapproved_runtime():
    with pytest.raises(ValueError, match="Unsupported model runtime"):
        _definition(runtime_profile="dbr_latest")


def test_feature_lookup_requires_observation_timestamp_and_known_defaults():
    with pytest.raises(ValueError, match="observation_timestamp"):
        FeatureLookupSpec(
            feature_id="feature",
            selected_columns=("value",),
            key_mapping=(("account", "account"),),
            observation_timestamp="",
        )
    with pytest.raises(ValueError, match="unselected columns"):
        FeatureLookupSpec(
            feature_id="feature",
            selected_columns=("value",),
            key_mapping=(("account", "account"),),
            observation_timestamp="observed_at",
            defaults=(("other", 0),),
        )


def test_ready_training_receipt_cannot_contain_future_feature_failure():
    with pytest.raises(ValueError, match="must pass leakage"):
        TrainingSetReceipt(
            receipt_id="receipt",
            model_name="shopping_bag_pctr",
            model_definition_checksum=DIGEST,
            feature_bindings=(_feature_binding(),),
            observation_start=date(2026, 1, 1),
            observation_end=date(2026, 6, 30),
            label_end=date(2026, 7, 7),
            schema_checksum="c" * 64,
            data_checksum="d" * 64,
            code_sha="abc123",
            leakage_status="FAIL",
            status="READY",
            created_at=NOW,
            completed_at=NOW,
        )


def test_training_receipt_can_pin_several_dates_of_one_feature():
    first = _feature_binding()
    second = TrainingFeatureBinding(
        **{
            **first.__dict__,
            "feature_snapshot_id": "snapshot-2",
            "feature_snapshot_attempt_id": "attempt-2",
            "delta_version": 4,
        }
    )
    receipt = TrainingSetReceipt(
        receipt_id="receipt",
        model_name="shopping_bag_pctr",
        model_definition_checksum=DIGEST,
        feature_bindings=(first, second),
        observation_start=date(2026, 1, 1),
        observation_end=date(2026, 6, 30),
        label_end=date(2026, 7, 7),
        schema_checksum="c" * 64,
        data_checksum="d" * 64,
        code_sha="abc123",
        leakage_status="PASS",
        status="READY",
        created_at=NOW,
        completed_at=NOW,
    )

    assert len(receipt.feature_bindings) == 2


def test_ready_model_build_requires_exact_registered_artifact():
    definition = _definition()
    with pytest.raises(ValueError, match="exact MLflow artifact"):
        ModelBuild(
            model_build_id="build",
            model_name=definition.model_name,
            training_receipt_id="receipt",
            model_definition_checksum=definition.checksum,
            runtime_profile=definition.runtime_profile,
            status="READY",
            created_at=NOW,
            completed_at=NOW,
        )

    build = ModelBuild(
        model_build_id="build",
        model_name=definition.model_name,
        training_receipt_id="receipt",
        model_definition_checksum=definition.checksum,
        runtime_profile=definition.runtime_profile,
        status="READY",
        created_at=NOW,
        completed_at=NOW,
        mlflow_run_id="run",
        registered_model_name="catalog.schema.shopping_bag_pctr",
        registered_model_version=3,
        model_uri="models:/catalog.schema.shopping_bag_pctr/3",
        artifact_digest="e" * 64,
        metrics=(("log_loss", 0.42),),
    )

    assert build.registered_model_version == 3

from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

import next_ads.model_development.external_outputs as outputs


RUN_DATE = date(2026, 8, 11)
COMPONENTS = (
    outputs.ExternalModelComponent(
        role="popularity_classifier",
        model_uri=(
            "models:/marketingdata_dev.ds_sandbox."
            "nextads_analytics_pctr_popularity_classification_model/2"
        ),
        expected_run_id="4323ac0b3ffa4b099be19b47a90885ba",
    ),
    outputs.ExternalModelComponent(
        role="affinity_regressor",
        model_uri=(
            "models:/marketingdata_dev.ds_sandbox."
            "nextads_analytics_pctr_affinity_regression_model/2"
        ),
        expected_run_id="5063f0209c1140fd9dee917052c171ac",
    ),
)


def test_external_model_components_require_exact_numeric_versions():
    with pytest.raises(ValueError, match="exact numeric model URI"):
        outputs.ExternalModelComponent(
            role="classifier",
            model_uri="models:/catalog.schema.model@champion",
            expected_run_id="run",
        )


def test_external_output_receipt_is_stable_for_the_same_inputs():
    values = {
        "model_name": "analytics_pctr",
        "provider_id": "analytics_pctr",
        "source_table": "catalog.schema.predictions",
        "source_delta_version": 3,
        "run_date": RUN_DATE,
        "producing_run_id": "123",
        "components": COMPONENTS,
    }

    assert outputs._receipt_id(**values) == outputs._receipt_id(**values)
    assert outputs._receipt_id(**values) != outputs._receipt_id(
        **{**values, "source_delta_version": 4}
    )


def test_external_output_receipt_keeps_both_analytics_models():
    receipt = outputs.ExternalScoreOutputReceipt(
        receipt_id="a" * 64,
        model_name="analytics_pctr",
        provider_id="analytics_pctr",
        source_table="catalog.schema.predictions",
        source_delta_version=3,
        run_date=RUN_DATE,
        row_count=100,
        schema_checksum="b" * 64,
        producing_run_id="123",
        components=COMPONENTS,
        created_at=datetime(2026, 8, 17, tzinfo=timezone.utc),
    )

    assert [component.role for component in receipt.components] == [
        "popularity_classifier",
        "affinity_regressor",
    ]


def test_analytics_adapter_uses_the_existing_canonical_contract(monkeypatch):
    captured = {}
    receipt = outputs.ExternalScoreOutputReceipt(
        receipt_id="a" * 64,
        model_name="analytics_pctr",
        provider_id="analytics_pctr",
        source_table="catalog.schema.predictions",
        source_delta_version=3,
        run_date=RUN_DATE,
        row_count=100,
        schema_checksum="b" * 64,
        producing_run_id="123",
        components=COMPONENTS,
        created_at=datetime(2026, 8, 17, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        outputs,
        "adapt_account_entity_scores",
        lambda frame, **kwargs: captured.update(frame=frame, **kwargs)
        or "canonical",
    )

    result = outputs.adapt_external_advert_scores(
        "predictions",
        receipt,
        provider_build_id="build-123",
        account_column="account_number",
        advert_column="UniqueAdID",
        raw_score_column="combined_weighted_score",
        score_column="combined_weighted_score",
    )

    assert result == "canonical"
    assert captured == {
        "frame": "predictions",
        "provider_build_id": "build-123",
        "provider_id": "analytics_pctr",
        "entity_type": "ad",
        "run_date": RUN_DATE,
        "account_column": "account_number",
        "entity_column": "UniqueAdID",
        "raw_score_column": "combined_weighted_score",
        "score_column": "combined_weighted_score",
    }


def test_external_output_bind_requires_a_run_date_column(monkeypatch):
    frame = SimpleNamespace(columns=["account_number"])
    reader = SimpleNamespace(
        option=lambda *_args: SimpleNamespace(table=lambda _table: frame)
    )
    spark = SimpleNamespace(read=reader)

    with pytest.raises(ValueError, match="missing rundate"):
        outputs.bind_external_score_output(
            spark,
            model_name="analytics_pctr",
            provider_id="analytics_pctr",
            source_table="catalog.schema.predictions",
            source_delta_version=3,
            run_date=RUN_DATE,
            producing_run_id="123",
            components=COMPONENTS,
        )


def test_external_latest_output_can_be_bound_without_a_date_column(
    monkeypatch,
):
    class Frame:
        columns = ["account_number", "UniqueAdID"]

        def count(self):
            return 2

    frame = Frame()
    reader = SimpleNamespace(
        option=lambda *_args: SimpleNamespace(table=lambda _table: frame)
    )
    spark = SimpleNamespace(read=reader)
    monkeypatch.setattr(outputs, "schema_checksum", lambda _frame: "c" * 64)

    exact, receipt = outputs.bind_external_score_output(
        spark,
        model_name="analytics_pctr",
        provider_id="analytics_pctr",
        source_table="catalog.schema.predictions_latest",
        source_delta_version=4,
        run_date=RUN_DATE,
        run_date_column=None,
        producing_run_id="123",
        components=COMPONENTS,
    )

    assert exact is frame
    assert receipt.source_delta_version == 4
    assert receipt.row_count == 2


def test_external_components_verify_the_exact_registered_run():
    class Client:
        def get_model_version(self, name, version):
            expected = {
                (COMPONENTS[0].model_uri.split("/")[-2], "2"): (
                    COMPONENTS[0].expected_run_id
                ),
                (COMPONENTS[1].model_uri.split("/")[-2], "2"): (
                    COMPONENTS[1].expected_run_id
                ),
            }
            return SimpleNamespace(run_id=expected[(name, version)])

    outputs.verify_external_model_components(Client(), COMPONENTS)


def test_external_component_rejects_a_changed_registered_run():
    client = SimpleNamespace(
        get_model_version=lambda *_args: SimpleNamespace(run_id="different")
    )

    with pytest.raises(ValueError, match="different MLflow run"):
        outputs.verify_external_model_components(client, COMPONENTS)

from datetime import date, datetime, timezone

import pytest

from next_ads.model_development import (
    AccountAdvertCandidateAdapter,
    ExternalAnalyticsScoreProvider,
    ExternalModelComponent,
    ExternalScoreOutputReceipt,
    ModelPluginRegistry,
    SparkAccountAdvertScoreProvider,
    SparkBinaryClassifierTrainer,
    load_model_definition,
)


def _external_receipt():
    return ExternalScoreOutputReceipt(
        receipt_id="a" * 64,
        model_name="analytics_pctr",
        provider_id="analytics_pctr",
        source_table="catalog.schema.predictions",
        source_delta_version=3,
        run_date=date(2026, 8, 11),
        row_count=10,
        schema_checksum="b" * 64,
        producing_run_id="123",
        components=(
            ExternalModelComponent(
                role="classifier",
                model_uri="models:/catalog.schema.classifier/2",
                expected_run_id="run-1",
            ),
        ),
        created_at=datetime(2026, 8, 17, tzinfo=timezone.utc),
    )


def test_shopping_bag_definition_resolves_without_orchestration_changes():
    definition = load_model_definition("shopping_bag_pctr")
    plugins = ModelPluginRegistry()

    trainer = plugins.trainer(
        definition,
        registered_model_name=(
            "marketingdata_dev.ds_sandbox.nextads_shopping_bag_pctr"
        ),
    )
    scorer = plugins.score_provider(
        definition,
        run_date=date(2026, 8, 11),
    )
    adapter = plugins.candidate_adapter(
        definition,
        scope_filters=definition.evaluation_scope,
    )

    assert isinstance(trainer, SparkBinaryClassifierTrainer)
    assert isinstance(scorer, SparkAccountAdvertScoreProvider)
    assert isinstance(adapter, AccountAdvertCandidateAdapter)
    assert adapter.scope_filters == (
        ("route", ("v1", "v2")),
        ("location", ("SB1", "SB2", "ShoppingBagPage")),
    )


def test_analytics_definition_resolves_its_external_score_adapter():
    definition = load_model_definition("analytics_pctr")
    provider = ModelPluginRegistry().score_provider(
        definition,
        receipt=_external_receipt(),
    )

    assert isinstance(provider, ExternalAnalyticsScoreProvider)


def test_unknown_plugin_is_rejected_at_the_definition_boundary():
    definition = load_model_definition("shopping_bag_pctr")
    registry = ModelPluginRegistry()
    registry._score_providers.clear()

    with pytest.raises(ValueError, match="Unknown score provider"):
        registry.score_provider(definition, run_date=date(2026, 8, 11))


def test_current_candidate_scoring_does_not_require_historical_label_keys(
    monkeypatch,
):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    requested_columns = []

    class FakeColumn:
        def cast(self, _data_type):
            return self

        def desc_nulls_last(self):
            return self

        def over(self, _window):
            return self

        def __eq__(self, _other):
            return self

    class FakeWindow:
        def orderBy(self, *_columns):  # noqa: N802 - mirrors Spark API
            return self

    class FakeFrame:
        columns = [
            "account_number",
            "advert_id",
            "exposure_timestamp",
            "route",
            "scope_type",
            "scope_value",
        ]

        def withColumn(self, _name, _value):  # noqa: N802 - mirrors Spark API
            return self

        def where(self, _condition):
            return self

        def drop(self, _name):
            return self

    def column(name):
        requested_columns.append(name)
        return FakeColumn()

    monkeypatch.setattr(F, "col", column)
    monkeypatch.setattr(F, "lit", lambda _value: FakeColumn())
    monkeypatch.setattr(F, "row_number", lambda: FakeColumn())
    monkeypatch.setattr(
        Window,
        "partitionBy",
        lambda *_columns: FakeWindow(),
    )

    definition = load_model_definition("shopping_bag_pctr")
    provider = SparkAccountAdvertScoreProvider(date(2026, 8, 18))
    result = provider._latest_predictions(
        definition,
        FakeFrame(),
        scope_columns=("route", "scope_type", "scope_value"),
        tie_break_columns=(),
    )

    assert isinstance(result, FakeFrame)
    assert "exposure_id" not in requested_columns
    assert "label_horizon_days" not in requested_columns

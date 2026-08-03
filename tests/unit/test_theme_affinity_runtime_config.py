import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from next_ads.common.config_manager import load_config
from next_ads.ranking.provider_context import ProviderContext
from next_ads.ranking.theme_affinity.data_prep import (
    build_common_params,
    build_sql_entries,
)
from next_ads.ranking.theme_affinity.config import (
    read_runtime_foundation_output,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _provider_context():
    return ProviderContext(
        context_slot="theme_affinity_serving",
        orchestration_run_id=123,
        provider_id="theme_affinity",
        provider_build_id="provider-build",
        provider_build_attempt_id="provider-build:task:0",
        input_snapshot_id="input-snapshot",
        run_date=date(2026, 7, 30),
        model_uri="models:/catalog.schema.model/1",
        bindings_json=json.dumps(
            {
                "foundation": {
                    "scoring_foundation_build_id": "foundation-build",
                    "scoring_foundation_build_attempt_id": (
                        "foundation-build:task:0"
                    ),
                    "outputs": {
                        "ranked": {
                            "table": "catalog.schema.foundation_ranked",
                            "delta_version": 17,
                            "schema_version": "account_theme_ranked/v1",
                        },
                        "complete": {
                            "table": "catalog.schema.foundation_complete",
                            "delta_version": 23,
                            "schema_version": "account_theme_complete/v1",
                        },
                    },
                }
            }
        ),
        capability="account_theme",
        use_case="theme_ranking",
        invocation_checksum="checksum",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
        scoring_foundation_build_id="foundation-build",
        scoring_foundation_build_attempt_id="foundation-build:task:0",
    )


class _VersionedReader:
    def __init__(self):
        self.calls = []
        self.version = None

    def option(self, name, value):
        self.version = (name, value)
        return self

    def table(self, table):
        self.calls.append((table, self.version))
        return table


class _VersionedSpark:
    def __init__(self):
        self.read = _VersionedReader()


def test_theme_affinity_reads_exact_bound_foundation_delta_versions():
    spark = _VersionedSpark()
    runtime = SimpleNamespace(provider_context=_provider_context())

    ranked = read_runtime_foundation_output(spark, runtime, "ranked")
    complete = read_runtime_foundation_output(spark, runtime, "complete")

    assert ranked == "catalog.schema.foundation_ranked"
    assert complete == "catalog.schema.foundation_complete"
    assert spark.read.calls == [
        ("catalog.schema.foundation_ranked", ("versionAsOf", 17)),
        ("catalog.schema.foundation_complete", ("versionAsOf", 23)),
    ]


def test_theme_affinity_foundation_read_has_no_mutable_table_fallback():
    with pytest.raises(ValueError, match="provider context is incomplete"):
        read_runtime_foundation_output(
            _VersionedSpark(),
            SimpleNamespace(provider_context=None),
            "ranked",
        )


def test_prediction_and_cleaning_do_not_read_mutable_foundation_tables():
    prediction = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_affinity/predict.py"
    ).read_text()
    cleaning = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_affinity/clean_output.py"
    ).read_text()

    assert (
        'read_runtime_foundation_output(spark, runtime, "ranked")'
        in prediction
    )
    assert "spark.table(model_tables.predict_input_table)" not in prediction
    assert (
        'read_runtime_foundation_output(spark, runtime, "complete")'
        in cleaning
    )
    assert "spark.table(model_tables.predict_complete)" not in cleaning


def test_theme_affinity_tables_resolve_to_dev_user_schema(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "test_user")

    config = load_config("dev")

    assert (
        config.ranking_model_tables.model_latest
        == "marketingdata_dev.test_user.next_uk_nextads_theme_affinity_model_latest"
    )
    assert (
        config.ranking_model_tables.model_full
        == "marketingdata_dev.test_user.next_uk_nextads_theme_affinity_model_full"
    )
    assert (
        config.ranking_model_tables.inference_log
        == "marketingdata_dev.test_user.next_uk_nextads_theme_affinity_inference_log"
    )
    assert (
        config.ranking_model_tables.predict_input_table
        == "marketingdata_dev.test_user.next_uk_nextads_theme_affinity_predict_ranked"
    )
    assert (
        config.ranking_model_tables.model_train_input_table
        == config.ranking_model_tables.predict_input_table
    )
    assert (
        config.ranking_model_tables.model_train_input_table
        == "marketingdata_dev.test_user.next_uk_nextads_theme_affinity_predict_ranked"
    )
    assert (
        "complete_ranked"
        not in config.ranking_model_tables.model_train_input_table
    )
    assert (
        config.theme_affinity_assignment_sources.champion
        == "marketingdata_dev.test_user.next_uk_nextads_theme_affinity_model_latest"
    )
    assert (
        config.theme_affinity_assignment_sources.challenger
        == "marketingdata_dev.test_user.next_uk_nextads_theme_affinity_model_latest"
    )


def test_theme_affinity_tables_can_resolve_dev_integration_schema(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "nextads_integration")

    config = load_config("dev")

    assert (
        config.ranking_model_tables.model_latest
        == "marketingdata_dev.nextads_integration.next_uk_nextads_theme_affinity_model_latest"
    )


def test_theme_affinity_tables_resolve_to_preprod_schema(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "ignored_user")

    config = load_config("preprod")

    assert (
        config.ranking_model_tables.model_latest
        == "marketingdata_prod.ds_sandbox.next_uk_nextads_theme_affinity_model_latest"
    )
    assert (
        config.theme_affinity_assignment_sources.champion
        == "marketingdata_prod.ds_sandbox.next_uk_nextads_theme_affinity_model_latest"
    )


def test_theme_affinity_tables_resolve_to_prod_schema(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "ignored_user")

    config = load_config("prod")

    assert (
        config.ranking_model_tables.model_latest
        == "marketingdata_prod.warehouse.next_uk_nextads_theme_affinity_model_latest"
    )
    assert (
        config.ranking_model_tables.inference_log
        == "marketingdata_prod.warehouse.next_uk_nextads_theme_affinity_inference_log"
    )


def test_map_theme_scores_uses_the_selected_provider_build():
    script = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_score_mapping.py"
    ).read_text()

    assert "load_provider_theme_scores(" in script
    assert "provider_build_id" in script
    assert "provider_signals_delta_version" in script
    assert "theme_affinity_assignment_sources" not in script
    assert "config.ranking_model_tables.model_latest" not in script
    assert "cfg['tables']['read'][\"hackathon_assignments\"]" not in script
    assert 'cfg["tables"]["read"]["hackathon_assignments"]' not in script

    settings = (
        PROJECT_ROOT / "configs/runtime/tables_settings.yaml"
    ).read_text()
    client_config = (PROJECT_ROOT / "configs/clients/next_uk.yaml").read_text()
    assert "hackathon_assignments" not in settings
    assert "hackathon_assignments" not in client_config


def test_theme_affinity_runtime_uses_new_outputs_for_assignments(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "test_user")

    config = load_config("dev")

    assert (
        "theme_affinity_model_latest"
        in config.ranking_model_tables.model_latest
    )
    assert (
        config.ranking_model_tables.model_latest
        == config.theme_affinity_assignment_sources.champion
    )


def test_theme_affinity_clean_output_writes_inference_log():
    source = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_affinity/clean_output.py"
    ).read_text()

    assert "model_tables.inference_log" in source
    assert "runtime.model_uri" in source
    assert 'F.lit(model_id).alias("model_id")' in source
    assert 'F.col("Score").cast("double").alias("prediction")' in source
    assert 'F.lit(None).cast("int").alias("label")' in source
    assert 'F.lit(None).cast("date").alias("label_observed_until")' in source


def test_theme_affinity_inference_log_label_enrichment_uses_results_outcomes():
    source = (
        PROJECT_ROOT
        / "jobs/nextads_reporting/enrich_theme_affinity_inference_log.py"
    ).read_text()

    assert "df_sessions_master_meta" in source
    assert 'F.col("Revenue") > 0' in source
    assert "F.date_sub(F.lit(max_session_date), label_window_days)" in source
    assert "MERGE INTO {inference_log_table}" in source
    assert "target.label = source.label" in source
    assert (
        "target.label_observed_until = source.label_observed_until" in source
    )


def test_theme_affinity_runtime_tables_are_in_dev_setup_contract(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "test_user")

    config = load_config("dev")

    expected_setup_tables = {
        "theme_affinity_predict_master": config.ranking_model_tables.predict_master,
        "theme_affinity_predict_complete": (
            config.ranking_model_tables.predict_complete
        ),
        "theme_affinity_predict_ranked": (
            config.ranking_model_tables.predict_input_table
        ),
        "theme_affinity_predict_half": (
            config.ranking_model_tables.predict_output_table
        ),
        "theme_affinity_model_latest": config.ranking_model_tables.model_latest,
        "theme_affinity_model_full": config.ranking_model_tables.model_full,
        "theme_affinity_inference_log": config.ranking_model_tables.inference_log,
    }

    for table_ref, expected_path in expected_setup_tables.items():
        assert getattr(config.tables_write, table_ref) == expected_path

    assert (
        PROJECT_ROOT
        / "sql/ranking/theme_affinity/create_table_theme_affinity_inference_log.sql"
    ).exists()


def test_adsv2_write_tables_are_available_under_tables_write(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "test_user")

    config = load_config("dev")

    assert (
        config.tables_write.control_sheet_v2
        == "marketingdata_dev.test_user.next_uk_nextads_control_sheet_v2"
    )
    assert (
        config.tables_write.control_sheet_latest_v2
        == "marketingdata_dev.test_user.next_uk_nextads_control_sheet_latest_v2"
    )
    assert not hasattr(config.ranking_model_tables, "control_sheet_v2")


def test_payload_and_feedback_write_tables_are_available_under_tables_write(
    monkeypatch,
):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "test_user")

    config = load_config("dev")

    assert (
        config.tables_write.nextads_payload
        == "marketingdata_dev.test_user.next_uk_nextads_payload"
    )
    assert (
        config.tables_write.nextads_payload_latest
        == "marketingdata_dev.test_user.next_uk_nextads_payload_latest"
    )
    assert (
        config.tables_write.results_underperforming_ads
        == "marketingdata_dev.test_user.next_uk_nextads_results_underperforming_ads"
    )
    assert not hasattr(config.ranking_model_tables, "nextads_payload")
    assert not hasattr(config.ranking_model_tables, "nextads_payload_latest")
    assert not hasattr(
        config.ranking_model_tables, "results_underperforming_ads"
    )


def test_theme_affinity_reference_date_uses_current_operational_mode():
    params = build_common_params("current", "schema", "prefix")

    assert params["table_prefix"] == "prefix"
    assert len(params["reference_date"].split("-")) == 3


def test_theme_affinity_accepts_explicit_current_operational_date():
    current_date = datetime.today().strftime("%Y-%m-%d")

    params = build_common_params(
        current_date,
        "schema",
        "prefix",
        operational=True,
    )

    assert params["reference_date"] == current_date


def test_theme_affinity_operational_date_allows_repair_after_midnight():
    previous_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    common = build_common_params(
        previous_date,
        "schema",
        "prefix",
        operational=True,
    )
    entries = build_sql_entries(
        previous_date,
        "prefix",
        operational=True,
    )

    assert common["reference_date"] == previous_date
    atbs_entry = next(
        entry for entry in entries[0] if entry["file"] == "0_atbs.sql"
    )
    assert atbs_entry["params"]["end_date_atbs"] == previous_date


def test_theme_affinity_historical_mode_rejects_recent_iso_date():
    previous_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    with pytest.raises(ValueError, match="at least 28 days"):
        build_common_params(previous_date, "schema", "prefix")


def test_theme_affinity_operational_mode_rejects_future_date():
    future_date = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")

    with pytest.raises(ValueError, match="cannot be in the future"):
        build_common_params(
            future_date,
            "schema",
            "prefix",
            operational=True,
        )


def test_theme_affinity_reference_date_rejects_old_widget_sentinel():
    with pytest.raises(ValueError):
        build_common_params("predict", "schema", "prefix")


def test_theme_affinity_reference_date_rejects_empty_value():
    with pytest.raises(ValueError, match="current or YYYY-MM-DD"):
        build_common_params("", "schema", "prefix")


def test_theme_affinity_last_year_windows_are_not_inverted():
    params = build_common_params("2026-05-01", "schema", "prefix")

    assert params["start_date_views_ly"] == "2025-04-01"
    assert params["end_date_views_ly"] == "2025-05-01"
    assert params["start_date_baskets_ly"] == "2025-04-01"
    assert params["end_date_baskets_ly"] == "2025-05-01"

    sql_entries = build_sql_entries("2026-05-01", "prefix")
    baskets_ly_params = next(
        entry["params"]
        for entry in sql_entries[0]
        if entry["file"] == "0_baskets_ly.sql"
    )
    views_ly_params = next(
        entry["params"]
        for entry in sql_entries[0]
        if entry["file"] == "0_views_ly.sql"
    )

    assert baskets_ly_params == {
        "start_date_baskets_ly": "2025-04-01",
        "end_date_baskets_ly": "2025-05-01",
    }
    assert views_ly_params == {
        "start_date_views_ly": "2025-04-01",
        "end_date_views_ly": "2025-05-01",
    }

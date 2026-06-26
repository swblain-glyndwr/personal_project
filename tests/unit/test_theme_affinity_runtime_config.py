from pathlib import Path

import pytest

from next_ads.common.config_manager import load_config
from next_ads.ranking.theme_affinity.data_prep import (
    build_common_params,
    build_sql_entries,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


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
    assert "complete_ranked" not in config.ranking_model_tables.model_train_input_table
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


def test_map_theme_scores_uses_config_led_assignment_sources():
    script = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_score_mapping.py"
    ).read_text()

    assert "theme_affinity_assignment_sources.champion" in script
    assert "theme_affinity_assignment_sources.challenger" in script
    assert "config.ranking_model_tables.model_latest" not in script
    assert 'cfg[\'tables\'][\'read\']["hackathon_assignments"]' not in script
    assert 'cfg["tables"]["read"]["hackathon_assignments"]' not in script

    settings = (PROJECT_ROOT / "configs/runtime/tables_settings.yaml").read_text()
    client_config = (PROJECT_ROOT / "configs/clients/next_uk.json").read_text()
    assert "hackathon_assignments" not in settings
    assert "hackathon_assignments" not in client_config


def test_theme_affinity_runtime_uses_new_outputs_for_assignments(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "test_user")

    config = load_config("dev")

    assert "theme_affinity_model_latest" in config.ranking_model_tables.model_latest
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
    assert 'F.col("ProbAggRebased").cast("double").alias("prediction")' in source
    assert 'F.lit(None).cast("int").alias("label")' in source
    assert 'F.lit(None).cast("date").alias("label_observed_until")' in source


def test_theme_affinity_inference_log_label_enrichment_uses_results_outcomes():
    source = (
        PROJECT_ROOT / "jobs/results/enrich_theme_affinity_inference_log.py"
    ).read_text()

    assert "df_sessions_master_meta" in source
    assert 'F.col("Revenue") > 0' in source
    assert 'F.date_sub(F.lit(max_session_date), label_window_days)' in source
    assert "MERGE INTO {inference_log_table}" in source
    assert "target.label = source.label" in source
    assert "target.label_observed_until = source.label_observed_until" in source


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
    assert not hasattr(config.ranking_model_tables, "results_underperforming_ads")


def test_theme_affinity_reference_date_uses_current_operational_mode():
    params = build_common_params("current", "schema", "prefix")

    assert params["table_prefix"] == "prefix"
    assert len(params["reference_date"].split("-")) == 3


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
        entry["params"] for entry in sql_entries[0] if entry["file"] == "0_baskets_ly.sql"
    )
    views_ly_params = next(
        entry["params"] for entry in sql_entries[0] if entry["file"] == "0_views_ly.sql"
    )

    assert baskets_ly_params == {
        "start_date_baskets_ly": "2025-04-01",
        "end_date_baskets_ly": "2025-05-01",
    }
    assert views_ly_params == {
        "start_date_views_ly": "2025-04-01",
        "end_date_views_ly": "2025-05-01",
    }

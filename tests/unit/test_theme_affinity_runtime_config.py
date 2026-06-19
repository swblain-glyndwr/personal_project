from pathlib import Path

import pytest

from next_ads.common.config_manager import load_config
from next_ads.ranking.theme_affinity.data_prep import build_common_params


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
        config.ranking_model_tables.predict_input_table
        == "marketingdata_dev.test_user.next_uk_nextads_theme_affinity_predict_ranked"
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


def test_theme_affinity_tables_resolve_to_prod_schema(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "ignored_user")

    config = load_config("prod")

    assert (
        config.ranking_model_tables.model_latest
        == "marketingdata_prod.warehouse.next_uk_nextads_theme_affinity_model_latest"
    )


def test_map_theme_scores_uses_config_led_assignment_sources():
    script = (PROJECT_ROOT / "scripts/map_theme_scores_to_ads.py").read_text()

    assert "theme_affinity_assignment_sources.champion" in script
    assert "theme_affinity_assignment_sources.challenger" in script
    assert "config.ranking_model_tables.model_latest" not in script
    assert 'cfg[\'tables\'][\'read\']["hackathon_assignments"]' not in script
    assert 'cfg["tables"]["read"]["hackathon_assignments"]' not in script


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

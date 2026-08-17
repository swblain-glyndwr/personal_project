import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FEATURE_JOB_ROOT = PROJECT_ROOT / "jobs" / "features" / "nextads"
sys.path.insert(0, str(FEATURE_JOB_ROOT))

from jobs.features.nextads import preflight_checks  # noqa: E402
from next_ads.features import load_feature_store_registry  # noqa: E402


EXPECTED_DAILY_PREFLIGHT_TABLES = (
    "next_uk_nextads_fs_account_profile",
    "next_uk_nextads_fs_account_web_activity_90d",
    "next_uk_nextads_fs_item_attributes_latest",
    "next_uk_nextads_fs_advert_core_daily",
    "next_uk_nextads_fs_advert_attribute_profile_daily",
    "next_uk_nextads_fs_account_theme_interactions_daily",
    "next_uk_nextads_fs_account_theme_affinity_daily",
    "next_uk_nextads_fs_theme_popularity_daily",
    "next_uk_nextads_fs_theme_affinity_model_input",
    "next_uk_nextads_fs_labels_clicks",
    "next_uk_nextads_fs_labels_theme_response",
)


def test_central_preflight_contracts_follow_registry_order():
    registry = load_feature_store_registry()

    assert preflight_checks.expected_preflight_table_names(registry) == (
        EXPECTED_DAILY_PREFLIGHT_TABLES
    )


def test_preflight_contracts_exclude_scaffolds_on_demand_and_per_run_features():
    registry = load_feature_store_registry()
    expected = set(preflight_checks.expected_preflight_table_names(registry))

    assert "next_uk_nextads_fs_pctr_model_input" not in expected
    assert "next_uk_nextads_fs_theme_affinity_training_input" not in expected
    assert "next_uk_nextads_fs_feature_quality_events" not in expected
    assert "next_uk_nextads_fs_product_embeddings_latest" not in expected
    assert "next_uk_nextads_fs_advert_product_profile_daily" not in expected
    assert (
        registry.table_spec(
            "next_uk_nextads_fs_advert_product_profile_daily"
        ).preflight_mode
        == "BUILDER"
    )


def test_preflight_frame_validation_returns_registry_order_not_mapping_order():
    registry = load_feature_store_registry()
    planned_frames = {
        table_name: object()
        for table_name in reversed(EXPECTED_DAILY_PREFLIGHT_TABLES)
    }

    assert (
        preflight_checks.validate_planned_feature_frames(
            planned_frames,
            registry,
        )
        == EXPECTED_DAILY_PREFLIGHT_TABLES
    )


def test_preflight_frame_validation_reports_missing_and_unexpected_frames():
    registry = load_feature_store_registry()
    planned_frames = {
        table_name: object()
        for table_name in EXPECTED_DAILY_PREFLIGHT_TABLES
        if table_name != "next_uk_nextads_fs_labels_clicks"
    }
    planned_frames["next_uk_nextads_fs_unregistered"] = object()

    with pytest.raises(ValueError) as exc_info:
        preflight_checks.validate_planned_feature_frames(
            planned_frames,
            registry,
        )

    message = str(exc_info.value)
    assert "missing=next_uk_nextads_fs_labels_clicks" in message
    assert "unexpected=next_uk_nextads_fs_unregistered" in message

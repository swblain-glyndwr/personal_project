from pathlib import Path

import pytest
import yaml

from next_ads.model_development import (
    DBR_15_4_SPARK_CPU,
    load_model_definition,
    load_model_definitions,
)
from next_ads.model_development.spark_training import MODEL_EVALUATION_METRICS


PROJECT_ROOT = Path(__file__).resolve().parents[2]

POPULARITY_FEATURES = {
    "cash_acc",
    "advert_ctr",
    "device_ctr",
    "geo_ctr",
    "gender_ctr",
    "dod_ctr_change",
    "wow_ctr_change",
    "age",
    "number_pages_viewed",
    "prior_30_day_order_value",
    "customer_total_clicks",
    "customer_total_unique_adverts_clicked",
    "customer_advert_previous_click_number",
    "number_clicks_same_algodivision",
    "advert_impressions",
    "device_impressions",
    "geo_impressions",
    "gender_impressions",
    "day_impressions",
    "prior_day_impressions",
}
AFFINITY_FEATURES = {
    "view_theme_score",
    "perc_order_value_cat_affinity",
    "perc_30_day_order_value_cat_affinity",
    "perc_order_qty_cat_affinity",
    "view_highest_catid_weight",
    "view_lift_adjusted",
    "purchase_highest_catid_weight",
    "purchase_lift_adjusted",
    "purchase_theme_affinity",
}


def test_analytics_pctr_is_a_separate_evaluate_model_adopter():
    definition = load_model_definition("analytics_pctr")
    lookup = definition.feature_lookups[0]

    assert definition.provider_id == "analytics_pctr"
    assert definition.activation_mode == "EVALUATE"
    assert definition.runtime_profile == DBR_15_4_SPARK_CPU
    assert definition.trainer == "analytics_pctr_two_stage_xgboost"
    assert definition.label == "ad_clicked"
    assert lookup.feature_id == "next_uk_nextads_fs_pctr_model_input"
    assert set(lookup.selected_columns) == (
        POPULARITY_FEATURES | AFFINITY_FEATURES
    )
    assert lookup.observation_timestamp == "reference_date"


def test_shopping_bag_pctr_uses_feature_store_labels_and_reusable_features():
    definition = load_model_definition("shopping_bag_pctr")

    assert definition.provider_id == "shopping_bag_pctr"
    assert definition.activation_mode == "EVALUATE"
    assert definition.training_observation.feature_id == (
        "next_uk_nextads_fs_shopping_bag_click_labels"
    )
    assert dict(definition.training_observation.filters) == {
        "route": "v1",
        "platform": "WEB",
        "label_horizon_days": 0,
        "label_is_mature": True,
        "impression_count": 1,
    }
    assert definition.observation_keys == (
        "exposure_id",
        "label_horizon_days",
    )
    assert definition.training_observation.observation_timestamp == (
        "exposure_timestamp"
    )
    assert definition.training_observation.label_maturity_column == (
        "label_maturity_date"
    )
    assert definition.evaluation_use_case == "shopping_bag_advert_ranking"
    assert definition.success_metrics == (
        "auc_pr",
        "auc_roc",
        "log_loss",
        "calibration_gap",
        "lift_at_5_percent",
    )
    assert definition.success_metrics == MODEL_EVALUATION_METRICS
    assert dict(definition.evaluation_scope) == {
        "route": ("v1",),
        "location": ("SB1", "SB2"),
    }
    assert dict(definition.training_observation.filters)["route"] == "v1"
    assert dict(definition.training_observation.filters)["platform"] == "WEB"
    assert {lookup.feature_id for lookup in definition.feature_lookups} == {
        "next_uk_nextads_fs_shopping_bag_account_activity_90d",
        "next_uk_nextads_fs_advert_core_daily",
    }
    activity = next(
        lookup
        for lookup in definition.feature_lookups
        if lookup.feature_id
        == "next_uk_nextads_fs_shopping_bag_account_activity_90d"
    )
    assert activity.availability_lag_days == 1
    assert dict(activity.defaults)["browse_session_recency_days"] == 91
    assert dict(activity.defaults)["action_recency_days"] == 91


def test_shopping_bag_model_features_exclude_outcome_and_audit_columns():
    definition = load_model_definition("shopping_bag_pctr")

    assert "clicked" not in definition.model_feature_columns
    assert "label_maturity_date" not in definition.model_feature_columns
    assert "impression_count" not in definition.model_feature_columns
    assert "exposure_timestamp" not in definition.model_feature_columns
    assert "location" in definition.model_feature_columns
    assert {
        "route",
        "platform",
        "placement_rank",
        "device",
        "operating_system",
        "page_surface",
        "treatment",
    }.isdisjoint(definition.model_feature_columns)


def test_shopping_bag_evaluation_provider_is_not_in_a_serving_portfolio():
    settings = yaml.safe_load(
        (PROJECT_ROOT / "configs" / "scoring" / "scoring_settings.yaml")
        .read_text()
    )["default"]["scoring"]
    entries = [
        entry
        for client in settings["client_portfolios"].values()
        for portfolio in client.values()
        for route in portfolio["routes"].values()
        for policy in route["policies"]
        for entry in policy["entries"]
    ]

    assert all(
        entry["provider_id"] != "shopping_bag_pctr" for entry in entries
    )


def test_registry_rejects_duplicate_model_or_provider(tmp_path):
    path = tmp_path / "models.yaml"
    source = (
        PROJECT_ROOT / "configs" / "models" / "nextads_models.yaml"
    ).read_text()
    first_model = source.split("models:\n", maxsplit=1)[1]
    path.write_text("models:\n" + first_model + first_model)

    with pytest.raises(ValueError, match="Duplicate model definitions"):
        load_model_definitions(path)


def test_unknown_model_does_not_fall_back_to_another_definition():
    with pytest.raises(KeyError, match="Unknown model definition"):
        load_model_definition("unknown_pctr")

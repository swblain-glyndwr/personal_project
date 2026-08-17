from pathlib import Path

import pytest

from next_ads.model_development import (
    DBR_15_4_SPARK_CPU,
    load_model_definition,
    load_model_definitions,
)


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
        "next_uk_nextads_fs_labels_clicks"
    )
    assert dict(definition.training_observation.filters) == {
        "label_horizon_days": 7
    }
    assert {lookup.feature_id for lookup in definition.feature_lookups} == {
        "next_uk_nextads_fs_account_profile",
        "next_uk_nextads_fs_account_web_activity_90d",
        "next_uk_nextads_fs_advert_core_daily",
        "next_uk_nextads_fs_account_advert_affinity_daily",
    }


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

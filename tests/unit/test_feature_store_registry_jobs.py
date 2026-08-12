from argparse import Namespace

import pytest

from jobs.features.nextads import _registry_job
from next_ads.features import load_feature_store_registry


def test_registry_selects_implemented_features_by_logical_builder():
    registry = load_feature_store_registry()

    theme_features = registry.features_for_builder(
        "build_theme_affinity_features"
    )

    assert [feature.name for feature in theme_features] == [
        "next_uk_nextads_fs_account_theme_interactions_daily",
        "next_uk_nextads_fs_account_theme_affinity_daily",
        "next_uk_nextads_fs_theme_popularity_daily",
        "next_uk_nextads_fs_labels_theme_response",
    ]
    assert theme_features[-1].source_job == "build_model_inputs"
    assert theme_features[-1].builder == "build_theme_affinity_features"


def test_registry_excludes_scaffolds_from_builder_outputs_by_default():
    registry = load_feature_store_registry()

    assert [
        feature.name
        for feature in registry.features_for_builder("build_advert_features")
    ] == [
        "next_uk_nextads_fs_item_attributes_latest",
        "next_uk_nextads_fs_advert_core_daily",
        "next_uk_nextads_fs_advert_attribute_profile_daily",
    ]
    assert registry.features_for_builder("build_pctr_affinity_features") == ()
    assert {
        feature.name
        for feature in registry.features_for_builder(
            "build_pctr_affinity_features",
            include_scaffolds=True,
        )
    } == {
        "next_uk_nextads_fs_account_advert_affinity_daily",
        "next_uk_nextads_fs_session_context_daily",
    }


def test_builder_output_validation_accepts_the_exact_implemented_set():
    registry = load_feature_store_registry()
    output_names = (
        "next_uk_nextads_fs_account_profile",
        "next_uk_nextads_fs_account_web_activity_90d",
    )

    assert (
        _registry_job.validate_builder_output_tables(
            "build_account_features",
            output_names,
            registry,
        )
        == output_names
    )


def test_builder_output_validation_reports_missing_and_unexpected_tables():
    registry = load_feature_store_registry()

    with pytest.raises(ValueError) as exc_info:
        _registry_job.validate_builder_output_tables(
            "build_account_features",
            (
                "next_uk_nextads_fs_account_profile",
                "next_uk_nextads_fs_unregistered",
            ),
            registry,
        )

    message = str(exc_info.value)
    assert "missing=next_uk_nextads_fs_account_web_activity_90d" in message
    assert "unexpected=next_uk_nextads_fs_unregistered" in message


def test_builder_output_validation_rejects_an_unknown_builder():
    with pytest.raises(ValueError, match="Unknown feature-store builder"):
        _registry_job.validate_builder_output_tables(
            "missing_builder",
            (),
        )


@pytest.mark.parametrize(
    "output_names",
    [
        "next_uk_nextads_fs_account_profile",
        ("next_uk_nextads_fs_account_profile", ""),
        (
            "next_uk_nextads_fs_account_profile",
            "next_uk_nextads_fs_account_profile",
        ),
    ],
)
def test_builder_output_validation_rejects_ambiguous_names(output_names):
    with pytest.raises(ValueError):
        _registry_job.validate_builder_output_tables(
            "build_account_features",
            output_names,
        )


def test_owned_table_logging_uses_logical_builder_and_target_namespace(
    monkeypatch,
):
    registry = load_feature_store_registry()
    monkeypatch.setattr(
        _registry_job,
        "load_feature_store_registry",
        lambda: registry,
    )
    args = Namespace(
        catalog="marketingdata_dev",
        schema="feature_test",
        reference_date="2026-08-12",
        log_level="INFO",
    )

    owned_tables = _registry_job.log_owned_tables(
        "build_theme_affinity_features",
        args,
    )

    assert owned_tables[-1] == (
        "marketingdata_dev.feature_test."
        "next_uk_nextads_fs_labels_theme_response"
    )
    assert not any("pctr" in table_path for table_path in owned_tables)

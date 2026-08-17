from copy import deepcopy
from dataclasses import fields
from pathlib import Path

import pytest
import yaml

from next_ads.features import (
    FeatureTableSpec,
    OfflineFeatureDefinition,
    OfflineFeatureState,
    OfflineStoreBinding,
    load_feature_store_registry,
    normalize_release_id,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = (
    PROJECT_ROOT / "configs" / "features" / "nextads_feature_store.yaml"
)

ACTIVE_FEATURES = {
    "next_uk_nextads_fs_account_profile",
    "next_uk_nextads_fs_account_web_activity_90d",
    "next_uk_nextads_fs_item_attributes_latest",
    "next_uk_nextads_fs_advert_core_daily",
    "next_uk_nextads_fs_advert_attribute_profile_daily",
    "next_uk_nextads_fs_account_theme_interactions_daily",
    "next_uk_nextads_fs_account_theme_affinity_daily",
    "next_uk_nextads_fs_theme_popularity_daily",
    "next_uk_nextads_fs_labels_clicks",
    "next_uk_nextads_fs_labels_theme_response",
    "next_uk_nextads_fs_feature_quality_events",
}
COMPATIBILITY_FEATURES = {
    "next_uk_nextads_fs_theme_affinity_model_input",
    "next_uk_nextads_fs_theme_affinity_training_input",
}
SCAFFOLD_FEATURES = {
    "next_uk_nextads_fs_product_embeddings_latest",
    "next_uk_nextads_fs_advert_semantic_profile_daily",
    "next_uk_nextads_fs_advert_product_profile_daily",
    "next_uk_nextads_fs_seasonal_product_demand_daily",
    "next_uk_nextads_fs_account_advert_affinity_daily",
    "next_uk_nextads_fs_session_context_daily",
    "next_uk_nextads_fs_pctr_model_input",
}
IMPLEMENTED_BUILDERS = {
    "next_uk_nextads_fs_account_profile": "build_account_features",
    "next_uk_nextads_fs_account_web_activity_90d": "build_account_features",
    "next_uk_nextads_fs_item_attributes_latest": "build_advert_features",
    "next_uk_nextads_fs_advert_core_daily": "build_advert_features",
    "next_uk_nextads_fs_advert_attribute_profile_daily": (
        "build_advert_features"
    ),
    "next_uk_nextads_fs_account_theme_interactions_daily": (
        "build_theme_affinity_features"
    ),
    "next_uk_nextads_fs_account_theme_affinity_daily": (
        "build_theme_affinity_features"
    ),
    "next_uk_nextads_fs_theme_popularity_daily": (
        "build_theme_affinity_features"
    ),
    "next_uk_nextads_fs_theme_affinity_model_input": "build_model_inputs",
    "next_uk_nextads_fs_theme_affinity_training_input": (
        "build_theme_affinity_training_input"
    ),
    "next_uk_nextads_fs_labels_clicks": "build_model_inputs",
    "next_uk_nextads_fs_labels_theme_response": (
        "build_theme_affinity_features"
    ),
    "next_uk_nextads_fs_feature_quality_events": "quality_checks",
}


def _registry_config() -> dict:
    return yaml.safe_load(REGISTRY_PATH.read_text())


def _write_registry(tmp_path, raw_registry: dict) -> Path:
    path = tmp_path / "feature_store.yaml"
    path.write_text(yaml.safe_dump(raw_registry, sort_keys=False))
    return path


def test_offline_feature_definitions_have_explicit_delivery_states():
    registry = load_feature_store_registry()
    features_by_state = {
        state: {
            feature.feature_id
            for feature in registry.offline_features
            if feature.state is state
        }
        for state in OfflineFeatureState
    }

    assert len(registry.offline_features) == 20
    assert features_by_state[OfflineFeatureState.ACTIVE] == ACTIVE_FEATURES
    assert (
        features_by_state[OfflineFeatureState.COMPATIBILITY]
        == COMPATIBILITY_FEATURES
    )
    assert features_by_state[OfflineFeatureState.SCAFFOLD] == SCAFFOLD_FEATURES


def test_scaffolds_are_not_implemented_and_explain_missing_contracts():
    registry = load_feature_store_registry()

    assert {
        feature.feature_id for feature in registry.implemented_features
    } == ACTIVE_FEATURES | COMPATIBILITY_FEATURES
    for feature in registry.offline_features:
        if feature.state is OfflineFeatureState.SCAFFOLD:
            assert not feature.implemented
            assert feature.missing_contracts
        else:
            assert feature.implemented
            assert feature.missing_contracts == ()

    theme_labels = registry.table_spec(
        "next_uk_nextads_fs_labels_theme_response"
    )
    assert theme_labels.builder == "build_theme_affinity_features"
    assert theme_labels.source_job == "build_model_inputs"


def test_each_implemented_feature_names_its_actual_builder_task():
    registry = load_feature_store_registry()

    assert {
        feature.feature_id: feature.builder
        for feature in registry.implemented_features
    } == IMPLEMENTED_BUILDERS


def test_logical_definitions_do_not_contain_environment_locations():
    logical_fields = {field.name for field in fields(OfflineFeatureDefinition)}

    assert "catalog" not in logical_fields
    assert "schema" not in logical_fields
    assert "bundle_target" not in logical_fields
    assert FeatureTableSpec is OfflineFeatureDefinition


def test_legacy_feature_table_constructor_signature_remains_compatible():
    feature = FeatureTableSpec(
        "example_feature",
        "account",
        "one row per account",
        ("account_number",),
        "build_example",
        "marketing_data",
        "daily",
        True,
        ("example_model",),
    )

    assert feature.timestamp_key is None
    assert feature.state is OfflineFeatureState.ACTIVE
    assert feature.missing_contracts == ()
    assert feature.builder == "build_example"


def test_offline_store_bindings_resolve_dev_preprod_and_prod_locations():
    registry = load_feature_store_registry()
    bindings = {
        binding.environment: binding for binding in registry.store_bindings
    }

    assert bindings == {
        "DEV": OfflineStoreBinding(
            environment="DEV",
            catalog="marketingdata_dev",
            schema="nextads_feature_store",
            bundle_target="DEV_FEATURE_STORE",
            repository_declared=True,
            table_name_template="{feature_id}",
        ),
        "PREPROD": OfflineStoreBinding(
            environment="PREPROD",
            catalog="marketingdata_prod",
            schema="ds_sandbox",
            bundle_target="PREPROD",
            repository_declared=False,
            table_name_template="{release_id}__{feature_id}",
        ),
        "PROD": OfflineStoreBinding(
            environment="PROD",
            catalog="marketingdata_prod",
            schema="nextads_feature_store",
            bundle_target="PROD",
            repository_declared=False,
            table_name_template="{feature_id}",
        ),
    }
    assert registry.resolved_binding_table_path(
        "next_uk_nextads_fs_account_profile", "DEV"
    ) == (
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_fs_account_profile"
    )
    assert registry.resolved_binding_table_path(
        "next_uk_nextads_fs_account_profile",
        "PREPROD",
        release_id="release/2026.08.12",
    ) == (
        "marketingdata_prod.ds_sandbox."
        f"{normalize_release_id('release/2026.08.12')}__"
        "next_uk_nextads_fs_account_profile"
    )
    assert registry.resolved_binding_table_path(
        "next_uk_nextads_fs_account_profile", "PROD"
    ) == (
        "marketingdata_prod.nextads_feature_store."
        "next_uk_nextads_fs_account_profile"
    )
    assert bindings["DEV"].repository_state == "REPO_DECLARED"
    assert bindings["PROD"].repository_state == "PLANNED"
    with pytest.raises(ValueError, match="requires a release_id"):
        registry.resolved_binding_table_path(
            "next_uk_nextads_fs_account_profile", "PREPROD"
        )


def test_preprod_binding_resolves_different_release_isolated_tables():
    registry = load_feature_store_registry()
    table_name = "next_uk_nextads_fs_account_profile"

    first = registry.resolved_binding_table_path(
        table_name, "PREPROD", release_id="release/2026.08.12"
    )
    second = registry.resolved_binding_table_path(
        table_name, "PREPROD", release_id="release/2026.08.13"
    )

    assert first != second
    assert first.startswith("marketingdata_prod.ds_sandbox.")
    assert second.startswith("marketingdata_prod.ds_sandbox.")


def test_preprod_binding_does_not_collide_for_similarly_normalized_ids():
    registry = load_feature_store_registry()
    table_name = "next_uk_nextads_fs_account_profile"

    hyphenated = registry.resolved_binding_table_path(
        table_name, "PREPROD", release_id="release/foo-bar"
    )
    underscored = registry.resolved_binding_table_path(
        table_name, "PREPROD", release_id="release/foo_bar"
    )

    assert hyphenated != underscored


def test_unknown_feature_states_are_rejected(tmp_path):
    raw_registry = _registry_config()
    raw_registry["feature_store"]["physical_tables"][0]["state"] = "MAYBE"

    with pytest.raises(ValueError, match="unsupported state"):
        load_feature_store_registry(_write_registry(tmp_path, raw_registry))


def test_scaffold_missing_contracts_are_required(tmp_path):
    raw_registry = _registry_config()
    scaffold = next(
        feature
        for feature in raw_registry["feature_store"]["physical_tables"]
        if feature["state"] == "SCAFFOLD"
    )
    scaffold.pop("missing_contracts")

    with pytest.raises(ValueError, match="must declare missing_contracts"):
        load_feature_store_registry(_write_registry(tmp_path, raw_registry))


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "expected_message"),
    [
        ("primary_keys", "account_number", "primary_keys must be a list"),
        ("primary_keys", ["account_number", ""], "contains a blank"),
        (
            "consumers",
            ["pctr", "pctr"],
            "consumers contains duplicates",
        ),
        (
            "missing_contracts",
            "materializer",
            "missing_contracts must be a list",
        ),
    ],
)
def test_feature_contract_lists_reject_scalars_blanks_and_duplicates(
    tmp_path,
    field_name,
    invalid_value,
    expected_message,
):
    raw_registry = _registry_config()
    if field_name == "missing_contracts":
        feature = next(
            item
            for item in raw_registry["feature_store"]["physical_tables"]
            if item["state"] == "SCAFFOLD"
        )
    else:
        feature = raw_registry["feature_store"]["physical_tables"][0]
    feature[field_name] = invalid_value

    with pytest.raises(ValueError, match=expected_message):
        load_feature_store_registry(_write_registry(tmp_path, raw_registry))


def test_duplicate_definitions_and_missing_environment_bindings_are_rejected(
    tmp_path,
):
    duplicate_registry = _registry_config()
    duplicate_registry["feature_store"]["physical_tables"].append(
        deepcopy(duplicate_registry["feature_store"]["physical_tables"][0])
    )
    with pytest.raises(
        ValueError, match="Duplicate feature-store table names"
    ):
        load_feature_store_registry(
            _write_registry(tmp_path, duplicate_registry)
        )

    missing_binding_registry = _registry_config()
    missing_binding_registry["feature_store"]["store_bindings"] = [
        binding
        for binding in missing_binding_registry["feature_store"][
            "store_bindings"
        ]
        if binding["environment"] != "PROD"
    ]
    with pytest.raises(
        ValueError, match="Missing offline store bindings: PROD"
    ):
        load_feature_store_registry(
            _write_registry(tmp_path, missing_binding_registry)
        )


def test_compatibility_views_reference_known_logical_features(tmp_path):
    raw_registry = _registry_config()
    raw_registry["feature_store"]["compatibility_views"][0][
        "source_feature"
    ] = "missing_feature"

    with pytest.raises(ValueError, match="references unknown source feature"):
        load_feature_store_registry(_write_registry(tmp_path, raw_registry))


@pytest.mark.parametrize(
    "missing_field", ["name", "source_job", "source_feature", "consumers"]
)
def test_compatibility_views_require_complete_contracts(
    tmp_path, missing_field
):
    raw_registry = _registry_config()
    raw_registry["feature_store"]["compatibility_views"][0].pop(missing_field)

    with pytest.raises(ValueError, match="missing required fields"):
        load_feature_store_registry(_write_registry(tmp_path, raw_registry))


def test_compatibility_view_names_are_unique_and_do_not_collide(tmp_path):
    duplicate_registry = _registry_config()
    duplicate_registry["feature_store"]["compatibility_views"].append(
        deepcopy(duplicate_registry["feature_store"]["compatibility_views"][0])
    )
    with pytest.raises(
        ValueError, match="Duplicate feature-store compatibility view names"
    ):
        load_feature_store_registry(
            _write_registry(tmp_path, duplicate_registry)
        )

    collision_registry = _registry_config()
    collision_registry["feature_store"]["compatibility_views"][0]["name"] = (
        "next_uk_nextads_fs_account_profile"
    )
    with pytest.raises(ValueError, match="collide with physical tables"):
        load_feature_store_registry(
            _write_registry(tmp_path, collision_registry)
        )


def test_compatibility_views_require_a_sql_contract(tmp_path):
    raw_registry = _registry_config()
    raw_registry["feature_store"]["compatibility_views"][0]["name"] = (
        "next_uk_nextads_missing_view_contract"
    )

    with pytest.raises(ValueError, match="is missing SQL contract"):
        load_feature_store_registry(_write_registry(tmp_path, raw_registry))

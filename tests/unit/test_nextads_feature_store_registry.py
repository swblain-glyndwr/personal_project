import ast
from pathlib import Path

import yaml

from next_ads.features import load_feature_store_registry
from scripts.table_operations.create_tables import extract_create_table_columns


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _sql_columns(table_name):
    registry = load_feature_store_registry()
    sql = registry.sql_contract_path(table_name).read_text()
    return {name for name, _ in extract_create_table_columns(sql)}


def _theme_affinity_model_features():
    config_path = PROJECT_ROOT / "hackathon_model" / "config.py"
    module_ast = ast.parse(config_path.read_text())
    for node in module_ast.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "features":
                    return ast.literal_eval(node.value)
    raise AssertionError("hackathon_model/config.py does not define features")


def test_feature_store_registry_loads_physical_tables_and_views():
    registry = load_feature_store_registry()

    assert registry.name == "nextads_feature_store"
    assert registry.default_catalog == "marketingdata_dev"
    assert registry.default_schema == "ds_sandbox"
    assert len(registry.physical_tables) == 20
    assert {
        view["name"] for view in registry.compatibility_views
    } == {
        "next_uk_nextads_theme_affinity_features_latest",
        "next_uk_nextads_pctr_features_latest",
    }


def test_feature_store_table_names_are_unique_and_have_required_metadata():
    registry = load_feature_store_registry()
    table_names = registry.table_names()

    assert len(table_names) == len(set(table_names))
    for table in registry.physical_tables:
        assert table.entity
        assert table.grain
        assert table.primary_keys
        assert table.source_job
        assert table.owner
        assert table.freshness
        assert table.consumers


def test_every_physical_feature_store_table_has_sql_contract_with_keys():
    registry = load_feature_store_registry()

    for table in registry.physical_tables:
        contract_path = registry.sql_contract_path(table.name)
        assert contract_path.is_file()
        columns = _sql_columns(table.name)
        assert set(table.primary_keys).issubset(columns)
        if table.timestamp_key:
            assert table.timestamp_key in columns


def test_theme_affinity_model_input_preserves_current_feature_columns():
    columns = _sql_columns("next_uk_nextads_fs_theme_affinity_model_input")

    assert set(_theme_affinity_model_features()).issubset(columns)


def test_pctr_model_input_carries_analytics_pctr_compatibility_columns():
    columns = _sql_columns("next_uk_nextads_fs_pctr_model_input")

    assert {
        "account_number",
        "advert_id",
        "location",
        "session_date",
        "reference_date",
        "device_simple",
        "channel_simple",
        "geocountry_simple",
        "all_ctr",
        "device_ctr",
        "channel_ctr",
        "geo_ctr",
        "viewed_latest_advert_catid_affinity",
        "purchased_latest_advert_catid_affinity",
        "customer_advert_impressions_30d",
        "rules_based_pctr",
    }.issubset(columns)


def test_feature_store_views_are_contract_artifacts_not_physical_tables():
    registry = load_feature_store_registry()
    physical_names = set(registry.table_names())

    for view in registry.compatibility_views:
        assert view["name"] not in physical_names
        view_path = (
            PROJECT_ROOT
            / "sql"
            / "features"
            / "nextads"
            / f"create_view_{view['name']}.sql"
        )
        assert view_path.is_file()


def test_feature_store_job_is_development_only_and_unscheduled():
    bundle_config = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text())
    job_config = yaml.safe_load(
        (
            PROJECT_ROOT
            / "resources"
            / "jobs"
            / "mktg_next_uk_nextads_feature_store.yml"
        ).read_text()
    )

    assert (
        "resources/jobs/mktg_next_uk_nextads_feature_store.yml"
        in bundle_config["include"]
    )
    assert (
        bundle_config["variables"]["feature_store_reference_date"]["default"]
        == "1970-01-01"
    )
    assert set(job_config["targets"]) == {"SANDBOX", "DEV", "DEV_INTEGRATION"}

    job = job_config["nextads_feature_store_config"][
        "mktg_next_uk_nextads_feature_store"
    ]
    assert "schedule" not in job
    assert job["tags"]["domain"] == "feature_store"
    assert job["tasks"][0]["task_key"] == "create_feature_store_tables"
    assert any(
        "${var.feature_store_reference_date}"
        in task["spark_python_task"]["parameters"]
        for task in job["tasks"][1:]
    )
    assert all(
        task["spark_python_task"]["python_file"].startswith(
            ("../../jobs/features/nextads/", "../../scripts/table_operations/")
        )
        for task in job["tasks"]
    )

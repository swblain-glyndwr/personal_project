import ast
from pathlib import Path

import pytest
import yaml

from next_ads.features import load_feature_store_registry, normalize_schema_name
from next_ads.features.materialization import validate_required_columns
from next_ads.features.theme_affinity import (
    THEME_AFFINITY_MODEL_FEATURE_COLUMNS,
)
from scripts.table_operations.create_feature_store_tables import (
    create_feature_store_tables,
    create_databricks_feature_table,
    schema_from_contract,
)
from next_ads.features.sql_contracts import extract_create_table_columns


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class _FakeSchemaQuery:
    def filter(self, _condition):
        return self

    def collect(self):
        return [{"databaseName": "feature_schema"}]


class _FakeCatalog:
    def tableExists(self, _table_path):  # noqa: N802 - mirrors Spark API
        return False


class _FakeSpark:
    def __init__(self):
        self.catalog = _FakeCatalog()
        self.sql_calls = []

    def sql(self, query):
        self.sql_calls.append(query)
        if query.startswith("SHOW SCHEMAS"):
            return _FakeSchemaQuery()
        assert query.startswith(
            ("CREATE SCHEMA", "GRANT ", "CREATE OR REPLACE VIEW")
        )
        return None


class _FakeFeatureEngineeringClient:
    def __init__(self):
        self.create_table_calls = []

    def create_table(
        self,
        name,
        primary_keys,
        schema,
        description=None,
        timestamp_keys=None,
        partition_columns=None,
        tags=None,
    ):
        self.create_table_calls.append(
            {
                "name": name,
                "primary_keys": primary_keys,
                "schema": schema,
                "description": description,
                "timestamp_keys": timestamp_keys,
                "partition_columns": partition_columns,
                "tags": tags,
            }
        )


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
    assert registry.default_schema == "nextads_feature_store"
    assert len(registry.physical_tables) == 19
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
    assert "next_uk_nextads_fs_two_tower_training_pairs" not in table_names
    for table in registry.physical_tables:
        assert table.entity
        assert table.grain
        assert table.primary_keys
        assert table.source_job
        assert table.owner
        assert table.freshness
        assert table.consumers
        assert "two_tower" not in table.consumers


def test_feature_store_schema_names_are_normalized_for_dev_user_paths():
    registry = load_feature_store_registry()

    assert normalize_schema_name("Stephen_Blain") == "stephen_blain"
    assert normalize_schema_name("stephen.blain@next.co.uk") == "stephen_blain"
    assert (
        registry.resolved_table_path(
            "next_uk_nextads_fs_account_profile",
            catalog="marketingdata_dev",
            schema="Stephen_Blain",
        )
        == "marketingdata_dev.stephen_blain.next_uk_nextads_fs_account_profile"
    )


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
    assert set(_theme_affinity_model_features()).issubset(
        THEME_AFFINITY_MODEL_FEATURE_COLUMNS
    )


def test_feature_store_write_helpers_validate_required_columns():
    class FakeDataFrame:
        columns = ["account_number"]

    with pytest.raises(ValueError, match="missing required columns"):
        validate_required_columns(
            FakeDataFrame(),
            ["account_number", "reference_date"],
            "feature_table",
        )


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
    assert "customer_ad_product_cosine_similarity" not in columns


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


def test_feature_store_setup_uses_databricks_feature_engineering_client():
    fake_spark = _FakeSpark()
    fake_client = _FakeFeatureEngineeringClient()

    created_tables = create_feature_store_tables(
        fake_spark,
        catalog="marketingdata_dev",
        schema="feature_schema",
        feature_engineering_client=fake_client,
    )

    registry = load_feature_store_registry()
    assert created_tables == [
        registry.resolved_table_path(
            table.name,
            catalog="marketingdata_dev",
            schema="feature_schema",
        )
        for table in registry.physical_tables
    ]
    assert len(fake_client.create_table_calls) == len(registry.physical_tables)
    first_call = fake_client.create_table_calls[0]
    assert first_call["name"] == (
        "marketingdata_dev.feature_schema."
        "next_uk_nextads_fs_account_profile"
    )
    assert first_call["primary_keys"] == ["account_number", "reference_date"]
    assert first_call["timestamp_keys"] == ["reference_date"]
    assert first_call["partition_columns"] == ["reference_date"]
    assert first_call["tags"]["nextads_feature_store"] == (
        "nextads_feature_store"
    )
    assert not any("CREATE TABLE" in query for query in fake_spark.sql_calls)
    assert any(
        query.startswith("CREATE OR REPLACE VIEW")
        for query in fake_spark.sql_calls
    )


def test_feature_engineering_create_table_argument_filter_supports_api_variants():
    captured = {}

    class ClientWithTimeseriesColumn:
        def create_table(
            self,
            name,
            primary_keys,
            schema,
            timeseries_column=None,
            tags=None,
        ):
            captured.update(
                {
                    "name": name,
                    "primary_keys": primary_keys,
                    "schema": schema,
                    "timeseries_column": timeseries_column,
                    "tags": tags,
                }
            )

    schema = schema_from_contract(
        (
            PROJECT_ROOT
            / "sql"
            / "features"
            / "nextads"
            / "create_table_next_uk_nextads_fs_account_profile.sql"
        ).read_text()
    )

    create_databricks_feature_table(
        ClientWithTimeseriesColumn(),
        name="catalog.schema.table",
        primary_keys=("account_number", "reference_date"),
        schema=schema,
        description="test table",
        timestamp_key="reference_date",
        partition_columns=["reference_date"],
        tags={"owner": "marketing_data"},
    )

    assert captured["name"] == "catalog.schema.table"
    assert captured["primary_keys"] == ["account_number", "reference_date"]
    assert captured["timeseries_column"] == "reference_date"
    assert captured["tags"] == {"owner": "marketing_data"}


def test_feature_engineering_argument_filter_ignores_extra_variants_for_kwargs_signature():
    captured = {}

    class ClientWithTimestampKeysAndKwargs:
        def create_table(
            self,
            name,
            primary_keys,
            schema,
            timestamp_keys=None,
            tags=None,
            **kwargs,
        ):
            captured.update(
                {
                    "name": name,
                    "primary_keys": primary_keys,
                    "schema": schema,
                    "timestamp_keys": timestamp_keys,
                    "tags": tags,
                    "kwargs": kwargs,
                }
            )

    schema = schema_from_contract(
        (
            PROJECT_ROOT
            / "sql"
            / "features"
            / "nextads"
            / "create_table_next_uk_nextads_fs_account_profile.sql"
        ).read_text()
    )

    create_databricks_feature_table(
        ClientWithTimestampKeysAndKwargs(),
        name="catalog.schema.table",
        primary_keys=("account_number", "reference_date"),
        schema=schema,
        description="test table",
        timestamp_key="reference_date",
        partition_columns=["reference_date"],
        tags={"owner": "marketing_data"},
    )

    assert captured["name"] == "catalog.schema.table"
    assert captured["primary_keys"] == ["account_number", "reference_date"]
    assert captured["timestamp_keys"] == ["reference_date"]
    assert captured["tags"] == {"owner": "marketing_data"}
    assert captured["kwargs"] == {}


def test_feature_store_job_has_shared_dev_schedule_and_no_prod_targets():
    bundle_config = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text())
    libraries_config = yaml.safe_load(
        (PROJECT_ROOT / "resources" / "variables" / "libraries.yml").read_text()
    )
    clusters_config = yaml.safe_load(
        (PROJECT_ROOT / "resources" / "variables" / "clusters.yml").read_text()
    )
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
        == "predict"
    )
    assert (
        bundle_config["variables"]["feature_store_schema"]["default"]
        == "nextads_feature_store"
    )
    assert (
        bundle_config["variables"]["feature_store_theme_source_catalog"][
            "default"
        ]
        == "marketingdata_prod"
    )
    assert (
        bundle_config["variables"]["feature_store_theme_source_schema"][
            "default"
        ]
        == "ds_sandbox"
    )
    assert (
        bundle_config["variables"]["feature_store_theme_table_prefix"][
            "default"
        ]
        == "next_uk_nextads_theme_affinity_predict"
    )
    assert (
        bundle_config["targets"]["SANDBOX"]["variables"][
            "feature_store_schema"
        ]
        == "${workspace.current_user.short_name}"
    )
    assert (
        bundle_config["targets"]["DEV"]["variables"]["feature_store_schema"]
        == "${var.git_last_commit_user_name}"
    )
    assert (
        bundle_config["targets"]["DEV"]["variables"][
            "theme_affinity_training_input_table"
        ]
        == (
            "marketingdata_dev.nextads_feature_store."
            "next_uk_nextads_fs_theme_affinity_model_input"
        )
    )
    assert (
        bundle_config["targets"]["DEV_INTEGRATION"]["variables"][
            "feature_store_schema"
        ]
        == "nextads_integration"
    )
    assert (
        bundle_config["targets"]["DEV_INTEGRATION"]["variables"][
            "feature_store_theme_source_catalog"
        ]
        == "marketingdata_dev"
    )
    assert (
        bundle_config["targets"]["DEV_INTEGRATION"]["variables"][
            "feature_store_theme_source_schema"
        ]
        == "nextads_integration"
    )
    assert (
        bundle_config["targets"]["DEV_FEATURE_STORE"]["variables"][
            "feature_store_schema"
        ]
        == "nextads_feature_store"
    )
    assert (
        bundle_config["targets"]["DEV_FEATURE_STORE"]["variables"][
            "feature_store_theme_source_catalog"
        ]
        == "marketingdata_dev"
    )
    assert (
        bundle_config["targets"]["DEV_FEATURE_STORE"]["variables"][
            "feature_store_theme_source_schema"
        ]
        == "nextads_integration"
    )
    assert "feature_store_schema" not in bundle_config["targets"]["PREPROD"][
        "variables"
    ]
    assert "feature_store_schema" not in bundle_config["targets"]["PROD"][
        "variables"
    ]
    feature_store_libraries = libraries_config["variables"][
        "feature_store_libraries"
    ]["default"]
    assert all("requirements" not in library for library in feature_store_libraries)
    assert feature_store_libraries[1]["pypi"]["package"] == (
        "databricks-feature-engineering==0.12.1"
    )
    shared_cluster_keys = {
        cluster["job_cluster_key"]
        for cluster in clusters_config["variables"]["job_clusters_config"]["default"]
    }
    assert "next_ads_feature_store_ml_cluster_D4ads_v5_1_1" not in shared_cluster_keys
    feature_store_cluster = clusters_config["variables"][
        "feature_store_job_clusters_config"
    ]["default"][0]
    assert (
        feature_store_cluster["job_cluster_key"]
        == "next_ads_feature_store_ml_cluster_D4ads_v5_1_1"
    )
    assert (
        feature_store_cluster["new_cluster"]["spark_version"]
        == "17.3.x-cpu-ml-scala2.13"
    )
    assert feature_store_cluster["new_cluster"]["runtime_engine"] == "STANDARD"
    assert set(job_config["targets"]) == {
        "SANDBOX",
        "DEV",
        "DEV_INTEGRATION",
        "DEV_FEATURE_STORE",
    }

    job = job_config["nextads_feature_store_config"][
        "mktg_next_uk_nextads_feature_store"
    ]
    assert "schedule" not in job
    scheduled_job = job_config["targets"]["DEV_FEATURE_STORE"]["resources"][
        "jobs"
    ]["mktg_next_uk_nextads_feature_store"]
    assert scheduled_job["schedule"] == {
        "quartz_cron_expression": "0 0 21 * * ?",
        "timezone_id": "Europe/London",
        "pause_status": "UNPAUSED",
    }
    assert job["job_clusters"] == "${var.feature_store_job_clusters_config}"
    assert job["tags"]["domain"] == "feature_store"
    assert job["tasks"][0]["task_key"] == "create_feature_store_tables"
    create_parameters = job["tasks"][0]["spark_python_task"]["parameters"]
    assert "--manage_principal" in create_parameters
    assert "${var.run_as_SPN_name}" in create_parameters
    assert create_parameters.count("--all_privileges_principal") == 2
    assert any(
        "${var.feature_store_reference_date}"
        in task["spark_python_task"]["parameters"]
        for task in job["tasks"][1:]
    )
    pctr_task = next(
        task
        for task in job["tasks"]
        if task["task_key"] == "build_pctr_affinity_features"
    )
    assert "--theme_source_schema" not in pctr_task["spark_python_task"][
        "parameters"
    ]
    for task_key in (
        "build_theme_affinity_features",
        "build_model_inputs",
        "quality_checks",
    ):
        task = next(task for task in job["tasks"] if task["task_key"] == task_key)
        parameters = task["spark_python_task"]["parameters"]
        assert "--theme_source_catalog" in parameters
        assert (
            parameters[parameters.index("--theme_source_catalog") + 1]
            == "${var.feature_store_theme_source_catalog}"
        )
        assert "--theme_source_schema" in parameters
        assert (
            parameters[parameters.index("--theme_source_schema") + 1]
            == "${var.feature_store_theme_source_schema}"
        )
        assert "--theme_table_prefix" in parameters
        assert (
            parameters[parameters.index("--theme_table_prefix") + 1]
            == "${var.feature_store_theme_table_prefix}"
        )
    assert all(
        "${var.feature_store_schema}"
        in task["spark_python_task"]["parameters"]
        for task in job["tasks"]
    )
    assert all(
        task["libraries"] == "${var.feature_store_libraries}"
        for task in job["tasks"]
    )
    assert all(
        task["job_cluster_key"]
        == "next_ads_feature_store_ml_cluster_D4ads_v5_1_1"
        for task in job["tasks"]
    )
    assert all(
        task["spark_python_task"]["python_file"].startswith(
            ("../../jobs/features/nextads/", "../../scripts/table_operations/")
        )
        for task in job["tasks"]
    )

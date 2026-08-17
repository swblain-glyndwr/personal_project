import ast
from pathlib import Path

import pytest
import yaml

from next_ads.features import load_feature_store_registry, normalize_schema_name
from next_ads.features.materialization import validate_required_columns
from next_ads.features.theme_affinity import (
    THEME_AFFINITY_MODEL_FEATURE_COLUMNS,
)
from jobs.table_operations.create_feature_store_tables import (
    create_feature_metadata_tables,
    create_feature_store_tables,
    create_databricks_feature_table,
    migrate_empty_dev_scaffolds,
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
            (
                "CREATE SCHEMA",
                "GRANT ",
                "CREATE OR REPLACE VIEW",
                "CREATE TABLE IF NOT EXISTS",
                "DROP VIEW",
                "DROP TABLE",
            )
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
    config_path = (
        PROJECT_ROOT
        / "experiments"
        / "hackathon_theme_affinity_model"
        / "config.py"
    )
    module_ast = ast.parse(config_path.read_text())
    for node in module_ast.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "features":
                    return ast.literal_eval(node.value)
    raise AssertionError(
        "experiments/hackathon_theme_affinity_model/config.py does not define features"
    )


def test_feature_store_registry_loads_physical_tables_and_views():
    registry = load_feature_store_registry()

    assert registry.name == "nextads_feature_store"
    assert registry.default_catalog == "marketingdata_dev"
    assert registry.default_schema == "nextads_feature_store"
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

    assert registry.table_spec(
        "next_uk_nextads_fs_item_attributes_latest"
    ).write_mode == "overwrite"
    assert registry.table_spec(
        "next_uk_nextads_fs_account_profile"
    ).write_mode == "merge"


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


def test_theme_affinity_training_input_preserves_current_feature_columns():
    columns = _sql_columns("next_uk_nextads_fs_theme_affinity_training_input")

    assert set(_theme_affinity_model_features()).issubset(columns)
    assert {"label", "model_score"}.issubset(columns)


def test_feature_store_write_helpers_validate_required_columns():
    class FakeDataFrame:
        columns = ["account_number"]

    with pytest.raises(ValueError, match="missing required columns"):
        validate_required_columns(
            FakeDataFrame(),
            ["account_number", "reference_date"],
            "feature_table",
        )


def test_feature_store_write_helpers_validate_required_key_values_before_write():
    materialization = (
        PROJECT_ROOT / "src" / "next_ads" / "features" / "materialization.py"
    ).read_text()

    assert "def validate_required_column_values" in materialization
    assert (
        "validate_required_column_values(aligned_df, table.primary_keys, table_name)"
        in materialization
    )
    assert "receipt = replace_scope_by_name(" in (
        materialization.split(
            "validate_required_column_values(aligned_df, table.primary_keys, table_name)",
            maxsplit=1,
        )[1]
    )


def test_pctr_model_input_carries_analytics_pctr_compatibility_columns():
    columns = _sql_columns("next_uk_nextads_fs_pctr_model_input")

    assert {
        "account_number",
        "advert_id",
        "reference_date",
        "ad_clicked",
        "treatment_type",
        "location",
        "age",
        "cash_acc",
        "advert_ctr",
        "device_ctr",
        "geo_ctr",
        "gender_ctr",
        "number_pages_viewed",
        "customer_total_clicks",
        "view_theme_score",
        "view_lift_adjusted",
        "purchase_lift_adjusted",
        "purchase_theme_affinity",
    }.issubset(columns)
    feature = load_feature_store_registry().table_spec(
        "next_uk_nextads_fs_pctr_model_input"
    )
    assert feature.primary_keys == (
        "account_number",
        "advert_id",
        "reference_date",
    )
    assert feature.consumers == ("analytics_pctr",)


def test_pctr_model_input_covers_the_existing_analytics_training_features():
    training_notebook = PROJECT_ROOT / "experiments" / "analytics_pctr" / (
        "train_model.py"
    )
    tree = ast.parse(training_notebook.read_text())
    assignments = {
        target.id: ast.literal_eval(node.value)
        for node in tree.body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
        and target.id
        in {"popularity_feature_columns", "affinity_feature_columns"}
    }
    training_features = set(assignments["popularity_feature_columns"])
    training_features.update(assignments["affinity_feature_columns"])
    training_features.remove("age_imputed")
    training_features.add("age")

    contract_columns = _sql_columns("next_uk_nextads_fs_pctr_model_input")

    assert training_features.issubset(contract_columns)
    assert {"ad_clicked", "treatment_type", "location"}.issubset(
        contract_columns
    )


def test_account_advert_affinity_matches_the_location_independent_source():
    registry = load_feature_store_registry()
    feature = registry.table_spec(
        "next_uk_nextads_fs_account_advert_affinity_daily"
    )
    columns = _sql_columns(feature.name)

    assert feature.primary_keys == (
        "account_number",
        "advert_id",
        "reference_date",
    )
    assert feature.preflight_mode == "BUILDER"
    assert "location" not in columns


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
    assert first_call["partition_columns"] is None
    assert first_call["tags"]["nextads_feature_store"] == (
        "nextads_feature_store"
    )
    metadata_calls = [
        query
        for query in fake_spark.sql_calls
        if query.startswith("CREATE TABLE IF NOT EXISTS")
    ]
    assert len(metadata_calls) == 5
    assert any(
        query.startswith("CREATE OR REPLACE VIEW")
        for query in fake_spark.sql_calls
    )


def test_feature_store_setup_can_recreate_registered_objects():
    fake_spark = _FakeSpark()
    fake_client = _FakeFeatureEngineeringClient()

    create_feature_store_tables(
        fake_spark,
        catalog="marketingdata_dev",
        schema="feature_schema",
        feature_engineering_client=fake_client,
        recreate_tables=True,
    )

    registry = load_feature_store_registry()
    drop_view_calls = [
        query for query in fake_spark.sql_calls if query.startswith("DROP VIEW")
    ]
    drop_table_calls = [
        query for query in fake_spark.sql_calls if query.startswith("DROP TABLE")
    ]
    assert len(drop_view_calls) == len(registry.compatibility_views)
    assert len(drop_table_calls) == len(registry.physical_tables)
    assert drop_table_calls[0] == (
        "DROP TABLE IF EXISTS "
        "marketingdata_dev.feature_schema.next_uk_nextads_fs_account_profile"
    )


def test_feature_metadata_tables_are_created_from_repo_contracts():
    fake_spark = _FakeSpark()

    paths = create_feature_metadata_tables(
        fake_spark,
        "marketingdata_dev",
        "feature_schema",
    )

    assert len(paths) == 5
    assert all(path.startswith("marketingdata_dev.feature_schema.") for path in paths)
    assert all(
        query.startswith("CREATE TABLE IF NOT EXISTS")
        for query in fake_spark.sql_calls
    )


class _ExistingFrame:
    def __init__(self, schema, row_count):
        self.schema = schema
        self.row_count = row_count

    def limit(self, _count):
        return self

    def count(self):
        return self.row_count


class _ExistingCatalog:
    def tableExists(self, _table_path):  # noqa: N802 - Spark API spelling
        return True


class _ExistingSpark:
    def __init__(self, frame):
        self.catalog = _ExistingCatalog()
        self.frame = frame
        self.sql_calls = []

    def table(self, _table_path):
        return self.frame

    def sql(self, query):
        self.sql_calls.append(query)


def test_empty_dev_scaffold_migration_replaces_only_changed_empty_contract():
    from pyspark.sql import types as T

    spark = _ExistingSpark(
        _ExistingFrame(
            T.StructType([T.StructField("item_id", T.StringType())]),
            row_count=0,
        )
    )

    migrated = migrate_empty_dev_scaffolds(
        spark,
        "marketingdata_dev",
        "feature_schema",
        ["next_uk_nextads_fs_product_embeddings_latest"],
    )

    assert migrated == [
        "marketingdata_dev.feature_schema."
        "next_uk_nextads_fs_product_embeddings_latest"
    ]
    assert spark.sql_calls == [
        "DROP TABLE IF EXISTS marketingdata_dev.feature_schema."
        "next_uk_nextads_fs_product_embeddings_latest"
    ]


def test_empty_dev_scaffold_migration_dry_run_does_not_drop_table():
    from pyspark.sql import types as T

    spark = _ExistingSpark(
        _ExistingFrame(
            T.StructType([T.StructField("item_id", T.StringType())]),
            row_count=0,
        )
    )

    migrated = migrate_empty_dev_scaffolds(
        spark,
        "marketingdata_dev",
        "feature_schema",
        ["next_uk_nextads_fs_product_embeddings_latest"],
        dry_run=True,
    )

    assert migrated == [
        "marketingdata_dev.feature_schema."
        "next_uk_nextads_fs_product_embeddings_latest"
    ]
    assert spark.sql_calls == []


def test_account_affinity_and_analytics_pctr_allow_empty_dev_migration():
    from pyspark.sql import types as T

    spark = _ExistingSpark(
        _ExistingFrame(
            T.StructType([T.StructField("account_number", T.StringType())]),
            row_count=0,
        )
    )
    account_affinity = (
        "next_uk_nextads_fs_account_advert_affinity_daily"
    )

    migrated = migrate_empty_dev_scaffolds(
        spark,
        "marketingdata_dev",
        "feature_schema",
        [account_affinity],
        dry_run=True,
    )

    assert migrated == [
        f"marketingdata_dev.feature_schema.{account_affinity}"
    ]
    pctr_model_input = "next_uk_nextads_fs_pctr_model_input"
    assert migrate_empty_dev_scaffolds(
        spark,
        "marketingdata_dev",
        "feature_schema",
        [pctr_model_input],
        dry_run=True,
    ) == [f"marketingdata_dev.feature_schema.{pctr_model_input}"]
    with pytest.raises(ValueError, match="Unsupported empty scaffold"):
        migrate_empty_dev_scaffolds(
            spark,
            "marketingdata_dev",
            "feature_schema",
            ["next_uk_nextads_fs_session_context_daily"],
            dry_run=True,
        )


def test_empty_dev_scaffold_migration_refuses_nonempty_or_prod_tables():
    from pyspark.sql import types as T

    spark = _ExistingSpark(
        _ExistingFrame(
            T.StructType([T.StructField("item_id", T.StringType())]),
            row_count=1,
        )
    )
    table_name = "next_uk_nextads_fs_product_embeddings_latest"

    with pytest.raises(ValueError, match="non-empty DEV scaffold"):
        migrate_empty_dev_scaffolds(
            spark,
            "marketingdata_dev",
            "feature_schema",
            [table_name],
        )
    with pytest.raises(ValueError, match="only in marketingdata_dev"):
        migrate_empty_dev_scaffolds(
            spark,
            "marketingdata_prod",
            "feature_schema",
            [table_name],
        )

    assert not spark.sql_calls


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
        (
            PROJECT_ROOT / "pipelines" / "databricks" / "variables" / "libraries.yml"
        ).read_text()
    )
    clusters_config = yaml.safe_load(
        (
            PROJECT_ROOT / "pipelines" / "databricks" / "variables" / "clusters.yml"
        ).read_text()
    )
    job_config = yaml.safe_load(
        (
            PROJECT_ROOT
            / "pipelines"
            / "databricks"
            / "jobs"
            / "mktg_next_uk_nextads_feature_store.yml"
        ).read_text()
    )

    assert (
        "pipelines/databricks/jobs/mktg_next_uk_nextads_feature_store.yml"
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
        bundle_config["variables"]["feature_store_source_catalog"]["default"]
        == "marketingdata_prod"
    )
    assert (
        bundle_config["variables"]["feature_store_source_schema"]["default"]
        == "warehouse"
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
        == "warehouse"
    )
    assert (
        bundle_config["variables"]["feature_store_theme_table_prefix"][
            "default"
        ]
        == "next_uk_nextads_account_theme_foundation"
    )
    assert (
        bundle_config["variables"]["feature_store_theme_training_reference_date"][
            "default"
        ]
        == "skip"
    )
    assert (
        bundle_config["variables"]["feature_store_theme_training_table_prefix"][
            "default"
        ]
        == "next_uk_nextads_theme_affinity_training"
    )
    assert (
        bundle_config["variables"][
            "feature_store_product_embedding_binding"
        ]["default"]
        == "configs/features/product_embedding_materialization_dev.yaml"
    )
    assert (
        bundle_config["variables"][
            "feature_store_analytics_pctr_source_binding"
        ]["default"]
        == "configs/features/analytics_pctr_source_dev.yaml"
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
            "feature_store_product_embedding_binding"
        ]
        == (
            "configs/features/"
            "product_embedding_materialization_personal_dev.yaml"
        )
    )
    assert (
        bundle_config["targets"]["DEV"]["variables"][
            "feature_store_analytics_pctr_source_binding"
        ]
        == "configs/features/analytics_pctr_source_personal_dev.yaml"
    )
    assert (
        "feature_store_theme_source_catalog"
        not in bundle_config["targets"]["DEV"]["variables"]
    )
    assert (
        "feature_store_theme_source_schema"
        not in bundle_config["targets"]["DEV"]["variables"]
    )
    assert (
        bundle_config["targets"]["DEV"]["variables"][
            "theme_affinity_training_input_table"
        ]
        == (
            "marketingdata_dev.nextads_feature_store."
            "next_uk_nextads_fs_theme_affinity_training_input"
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
            "feature_store_source_catalog"
        ]
        == "marketingdata_prod"
    )
    assert (
        bundle_config["targets"]["DEV_FEATURE_STORE"]["variables"][
            "feature_store_source_schema"
        ]
        == "warehouse"
    )
    assert (
        bundle_config["targets"]["DEV_FEATURE_STORE"]["variables"][
            "feature_store_theme_source_catalog"
        ]
        == "marketingdata_prod"
    )
    assert (
        bundle_config["targets"]["DEV_FEATURE_STORE"]["variables"][
            "feature_store_theme_source_schema"
        ]
        == "warehouse"
    )
    assert (
        bundle_config["targets"]["DEV_FEATURE_STORE"]["variables"][
            "feature_store_analytics_pctr_source_schema"
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
    assert feature_store_libraries[2]["pypi"]["package"] == (
        "dynaconf[yaml]==3.2.12"
    )
    shared_cluster_keys = {
        cluster["job_cluster_key"]
        for cluster in clusters_config["variables"]["job_clusters_config"]["default"]
    }
    assert "feature_store_job_clusters_config" not in clusters_config["variables"]
    assert "next_ads_job_cluster_D32ads_v5_1_4" in shared_cluster_keys
    assert "next_ads_job_cluster_D16ads_v5_2_2" in shared_cluster_keys
    assert set(job_config["targets"]) == {"DEV", "DEV_FEATURE_STORE"}

    job = job_config["nextads_feature_store_config"][
        "mktg_next_uk_nextads_feature_store"
    ]
    assert "schedule" not in job
    assert job["timeout_seconds"] == 43200
    personal_job = job_config["targets"]["DEV"]["resources"]["jobs"][
        "mktg_next_uk_nextads_feature_store"
    ]
    assert "schedule" not in personal_job
    assert personal_job["max_concurrent_runs"] == 1
    scheduled_job = job_config["targets"]["DEV_FEATURE_STORE"]["resources"][
        "jobs"
    ]["mktg_next_uk_nextads_feature_store"]
    assert "mktg_next_uk_nextads_feature_store" not in (
        job_config.get("resources", {}).get("jobs", {})
    )
    assert scheduled_job["schedule"] == {
        "quartz_cron_expression": "0 0 21 * * ?",
        "timezone_id": "Europe/London",
        "pause_status": "UNPAUSED",
    }
    assert job["job_clusters"] == "${var.job_clusters_config}"
    assert job["parameters"] == [
        {"name": "reference_date", "default": "${var.feature_store_reference_date}"},
        {
            "name": "source_catalog",
            "default": "${var.feature_store_source_catalog}",
        },
        {"name": "source_schema", "default": "${var.feature_store_source_schema}"},
        {
            "name": "theme_source_catalog",
            "default": "${var.feature_store_theme_source_catalog}",
        },
        {
            "name": "theme_source_schema",
            "default": "${var.feature_store_theme_source_schema}",
        },
        {
            "name": "theme_table_prefix",
            "default": "${var.feature_store_theme_table_prefix}",
        },
        {
            "name": "theme_training_reference_date",
            "default": "${var.feature_store_theme_training_reference_date}",
        },
        {
            "name": "analytics_pctr_source_binding",
            "default": "${var.feature_store_analytics_pctr_source_binding}",
        },
        {
            "name": "analytics_pctr_source_schema",
            "default": "${var.feature_store_analytics_pctr_source_schema}",
        },
        {"name": "recreate_feature_tables", "default": "false"},
    ]
    assert job["tags"]["domain"] == "feature_store"
    assert job["tasks"][0]["task_key"] == "resolve_feature_store_reference_date"
    assert any(
        task["task_key"] == "build_theme_affinity_training_input"
        for task in job["tasks"]
    )
    assert any(
        task["task_key"] == "preflight_feature_store_sources"
        for task in job["tasks"]
    )
    create_task = next(
        task
        for task in job["tasks"]
        if task["task_key"] == "create_feature_store_tables"
    )
    create_parameters = create_task["spark_python_task"]["parameters"]
    assert create_task["depends_on"] == [
        {"task_key": "resolve_feature_store_reference_date"}
    ]
    assert "--manage_principal" in create_parameters
    assert "${var.run_as_SPN_name}" in create_parameters
    assert create_parameters.count("--all_privileges_principal") == 2
    assert "--recreate_tables" in create_parameters
    assert (
        create_parameters[create_parameters.index("--recreate_tables") + 1]
        == "{{job.parameters.recreate_feature_tables}}"
    )
    migrated_scaffolds = {
        create_parameters[index + 1]
        for index, value in enumerate(create_parameters)
        if value == "--empty_scaffold_migration"
    }
    assert "next_uk_nextads_fs_account_advert_affinity_daily" in (
        migrated_scaffolds
    )
    assert "next_uk_nextads_fs_pctr_model_input" in migrated_scaffolds
    assert "next_uk_nextads_fs_session_context_daily" not in (
        migrated_scaffolds
    )
    preflight_task = next(
        task
        for task in job["tasks"]
        if task["task_key"] == "preflight_feature_store_sources"
    )
    assert preflight_task["depends_on"] == [
        {"task_key": "create_feature_store_tables"},
        {"task_key": "refresh_analytics_pctr_feature_source"},
    ]
    for task_key in ("build_account_features", "build_advert_features"):
        task = next(task for task in job["tasks"] if task["task_key"] == task_key)
        assert task["depends_on"] == [
            {"task_key": "preflight_feature_store_sources"}
        ]
    resolved_reference_date = (
        "{{tasks.resolve_feature_store_reference_date.values.reference_date}}"
    )
    resolver_task = job["tasks"][0]
    resolver_parameters = resolver_task["spark_python_task"]["parameters"]
    assert resolver_parameters[resolver_parameters.index("--reference_date") + 1] == (
        "{{job.parameters.reference_date}}"
    )
    for task in job["tasks"]:
        if (
            "spark_python_task" not in task
            or task["task_key"]
            in {"create_feature_store_tables", "resolve_feature_store_reference_date"}
        ):
            continue
        parameters = task["spark_python_task"]["parameters"]
        assert parameters[parameters.index("--reference_date") + 1] == (
            resolved_reference_date
        )
    for task in job["tasks"]:
        if (
            "spark_python_task" not in task
            or task["task_key"] == "create_feature_store_tables"
        ):
            continue
        parameters = task["spark_python_task"]["parameters"]
        assert "--source_catalog" in parameters
        assert (
            parameters[parameters.index("--source_catalog") + 1]
            == "{{job.parameters.source_catalog}}"
        )
        assert "--source_schema" in parameters
        assert (
            parameters[parameters.index("--source_schema") + 1]
            == "{{job.parameters.source_schema}}"
        )
    for task_key in (
        "preflight_feature_store_sources",
        "build_account_features",
        "build_advert_features",
        "build_theme_affinity_features",
        "build_model_inputs",
        "quality_checks",
    ):
        task = next(task for task in job["tasks"] if task["task_key"] == task_key)
        parameters = task["spark_python_task"]["parameters"]
        assert "--theme_source_catalog" in parameters
        assert (
            parameters[parameters.index("--theme_source_catalog") + 1]
            == "{{job.parameters.theme_source_catalog}}"
        )
        assert "--theme_source_schema" in parameters
        assert (
            parameters[parameters.index("--theme_source_schema") + 1]
            == "{{job.parameters.theme_source_schema}}"
        )
        assert "--theme_table_prefix" in parameters
        assert (
            parameters[parameters.index("--theme_table_prefix") + 1]
            == "{{job.parameters.theme_table_prefix}}"
        )
    training_task = next(
        task
        for task in job["tasks"]
        if task["task_key"] == "build_theme_affinity_training_input"
    )
    training_parameters = training_task["spark_python_task"]["parameters"]
    assert "--job_env" in training_parameters
    assert (
        training_parameters[training_parameters.index("--job_env") + 1]
        == "${var.job_parameter_environment_name}"
    )
    assert "--theme_training_reference_date" in training_parameters
    assert (
        training_parameters[
            training_parameters.index("--theme_training_reference_date") + 1
        ]
        == "{{job.parameters.theme_training_reference_date}}"
    )
    assert "--theme_training_table_prefix" in training_parameters
    assert (
        training_parameters[
            training_parameters.index("--theme_training_table_prefix") + 1
        ]
        == "${var.feature_store_theme_training_table_prefix}"
    )
    quality_task = next(
        task for task in job["tasks"] if task["task_key"] == "quality_checks"
    )
    assert quality_task["depends_on"] == [
        {"task_key": "build_model_inputs"},
        {"task_key": "build_theme_affinity_training_input"},
        {"task_key": "build_advert_product_profile_daily"},
        {"task_key": "build_advert_semantic_profile_daily"},
        {"task_key": "build_seasonal_product_demand_daily"},
    ]
    quality_parameters = quality_task["spark_python_task"]["parameters"]
    assert (
        quality_parameters[
            quality_parameters.index("--theme_training_reference_date") + 1
        ]
        == "{{job.parameters.theme_training_reference_date}}"
    )
    assert all(
        "${var.feature_store_schema}"
        in task["spark_python_task"]["parameters"]
        for task in job["tasks"]
        if "spark_python_task" in task
        if task["task_key"] != "resolve_feature_store_reference_date"
    )
    embedding_task = next(
        task
        for task in job["tasks"]
        if task["task_key"] == "build_product_embeddings_latest"
    )
    assert embedding_task["libraries"] == (
        "${var.feature_store_embedding_libraries}"
    )
    embedding_parameters = embedding_task["spark_python_task"]["parameters"]
    assert (
        embedding_parameters[
            embedding_parameters.index("--product_embedding_binding") + 1
        ]
        == "${var.feature_store_product_embedding_binding}"
    )
    advert_profile_task = next(
        task
        for task in job["tasks"]
        if task["task_key"] == "build_advert_product_profile_daily"
    )
    advert_profile_parameters = advert_profile_task["spark_python_task"][
        "parameters"
    ]
    assert (
        advert_profile_parameters[
            advert_profile_parameters.index("--product_embedding_binding")
            + 1
        ]
        == "${var.feature_store_product_embedding_binding}"
    )
    semantic_task = next(
        task
        for task in job["tasks"]
        if task["task_key"] == "build_advert_semantic_profile_daily"
    )
    seasonal_task = next(
        task
        for task in job["tasks"]
        if task["task_key"] == "build_seasonal_product_demand_daily"
    )
    assert semantic_task["depends_on"] == [
        {"task_key": "build_advert_features"},
        {"task_key": "build_product_embeddings_latest"},
    ]
    assert seasonal_task["depends_on"] == [
        {"task_key": "build_advert_features"},
        {"task_key": "build_product_embeddings_latest"},
    ]
    for task in (semantic_task, seasonal_task):
        parameters = task["spark_python_task"]["parameters"]
        assert (
            parameters[parameters.index("--product_embedding_binding") + 1]
            == "${var.feature_store_product_embedding_binding}"
        )
    embedding_task_keys = {
        "build_product_embeddings_latest",
        "build_advert_semantic_profile_daily",
    }
    assert all(
        task["libraries"] == "${var.feature_store_libraries}"
        for task in job["tasks"]
        if "spark_python_task" in task
        if task["task_key"] not in embedding_task_keys
    )
    assert all(
        task["libraries"] == "${var.feature_store_embedding_libraries}"
        for task in job["tasks"]
        if "spark_python_task" in task
        if task["task_key"] in embedding_task_keys
    )
    assert all(
        task["timeout_seconds"] == 14400
        for task in job["tasks"]
        if task["task_key"] in embedding_task_keys
    )
    assert all(
        task["timeout_seconds"] == 7200
        for task in job["tasks"]
        if "spark_python_task" in task
        if task["task_key"] not in embedding_task_keys
        if task["task_key"] != "resolve_feature_store_reference_date"
    )
    assert resolver_task["timeout_seconds"] == 1800
    assert all(
        task["job_cluster_key"] == "next_ads_job_cluster_D16ads_v5_2_2"
        for task in job["tasks"]
        if task["task_key"] in embedding_task_keys
    )
    assert all(
        task["job_cluster_key"]
        == "next_ads_job_cluster_D32ads_v5_1_4"
        for task in job["tasks"]
        if "spark_python_task" in task
        if task["task_key"] not in embedding_task_keys
    )
    assert all(
        task["spark_python_task"]["python_file"].startswith(
            ("../../../jobs/features/nextads/", "../../../jobs/table_operations/")
        )
        for task in job["tasks"]
        if "spark_python_task" in task
    )

    refresh_task = next(
        task
        for task in job["tasks"]
        if task["task_key"] == "refresh_analytics_pctr_feature_source"
    )
    assert refresh_task["depends_on"] == [
        {"task_key": "create_feature_store_tables"}
    ]
    assert refresh_task["run_job_task"]["job_id"] == (
        "${resources.jobs."
        "mktg_next_uk_nextads_analytics_pctr_feature_source.id}"
    )
    assert refresh_task["run_job_task"]["job_parameters"][
        "reference_date"
    ] == resolved_reference_date
    affinity_task = next(
        task
        for task in job["tasks"]
        if task["task_key"] == "build_pctr_affinity_features"
    )
    affinity_parameters = affinity_task["spark_python_task"]["parameters"]
    assert affinity_parameters[
        affinity_parameters.index(
            "--analytics_pctr_receipt_correlation_id"
        )
        + 1
    ] == "{{job.run_id}}"
    assert refresh_task["run_job_task"]["job_parameters"][
        "receipt_correlation_id"
    ] == "{{job.run_id}}"
    receipt_tasks = [
        task
        for task in job["tasks"]
        if task["task_key"].startswith("build_")
        or task["task_key"] == "quality_checks"
    ]
    for task in receipt_tasks:
        parameters = task["spark_python_task"]["parameters"]
        assert parameters[parameters.index("--feature_build_id") + 1] == (
            "{{job.run_id}}"
        )
        assert parameters[
            parameters.index("--feature_build_attempt_id") + 1
        ] == "{{job.run_id}}"
        assert parameters[parameters.index("--git_commit") + 1] == (
            "${var.git_commit_sha}"
        )


def test_analytics_pctr_feature_source_is_source_only_and_uses_dbr_15_4():
    bundle_config = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text())
    source_path = (
        PROJECT_ROOT
        / "pipelines"
        / "databricks"
        / "jobs"
        / "mktg_next_uk_nextads_analytics_pctr_feature_source.yml"
    )
    source_config = yaml.safe_load(source_path.read_text())

    assert (
        "pipelines/databricks/jobs/"
        "mktg_next_uk_nextads_analytics_pctr_feature_source.yml"
        in bundle_config["include"]
    )
    assert set(source_config["targets"]) == {"DEV", "DEV_FEATURE_STORE"}
    job = source_config["analytics_pctr_feature_source_config"][
        "mktg_next_uk_nextads_analytics_pctr_feature_source"
    ]
    task_keys = [task["task_key"] for task in job["tasks"]]
    assert task_keys == [
        "resolve_reference_date",
        "base_sessions",
        "core_datasets",
        "customer_advert_base",
        "session_aggregation",
        "ctr_metrics",
        "page_views",
        "purchases",
        "customer_ad_exposure",
        "view_advert_affinity",
        "feature_build",
        "validate_source_receipt",
    ]
    assert "predict" not in task_keys
    assert all(
        task["job_cluster_key"] == "next_ads_job_cluster_D32ads_v5_1_4"
        for task in job["tasks"]
    )
    assert job["tasks"][1]["notebook_task"]["base_parameters"][
        "start_date"
    ] == "{{tasks.resolve_reference_date.values.reference_date}}"
    assert job["tasks"][-1]["depends_on"] == [
        {"task_key": "feature_build"}
    ]
    receipt_parameters = job["tasks"][-1]["spark_python_task"]["parameters"]
    assert receipt_parameters[
        receipt_parameters.index("--producing_job_run_id") + 1
    ] == "{{job.run_id}}"
    assert receipt_parameters[
        receipt_parameters.index("--receipt_correlation_id") + 1
    ] == "{{job.parameters.receipt_correlation_id}}"


def test_account_feature_task_uses_theme_affinity_source_outputs():
    source = (
        PROJECT_ROOT
        / "jobs"
        / "features"
        / "nextads"
        / "build_account_features.py"
    ).read_text()
    theme_source = (
        PROJECT_ROOT / "src" / "next_ads" / "features" / "theme_affinity.py"
    ).read_text()

    assert "read_theme_account_source_tables" in source
    assert "build_theme_affinity_account_profile_df" in source
    assert "build_theme_affinity_account_web_activity_df" in source
    assert "build_account_web_activity_df" not in source
    assert '"advanced_features"' in theme_source
    assert '"customer_features"' in theme_source
    assert '"customer_segments"' in theme_source
    assert '"ranked"' in theme_source
    assert "next_uk_nextads_theme_affinity_model_latest" in theme_source
    assert 'table_prefix, "half"' not in theme_source


def test_theme_affinity_feature_builders_filter_null_feature_keys():
    theme_source = (
        PROJECT_ROOT / "src" / "next_ads" / "features" / "theme_affinity.py"
    ).read_text()

    assert "def _filter_required_keys" in theme_source
    assert '_filter_required_keys(popularity_df, "theme_clean")' in theme_source
    assert (
        '_filter_required_keys(ranked_df, "account_number", "theme_clean")'
        in theme_source
    )
    assert "_filter_required_keys(\n        prediction_df," in theme_source


def test_feature_store_creation_avoids_timestamp_partition_conflict():
    materialization = (
        PROJECT_ROOT / "src" / "next_ads" / "features" / "materialization.py"
    ).read_text()
    create_tables = (
        PROJECT_ROOT
        / "jobs"
        / "table_operations"
        / "create_feature_store_tables.py"
    ).read_text()

    assert "receipt = replace_scope_by_name(" in materialization
    assert "receipt = replace_table_by_name(" in materialization
    assert "delete_reference_date_partition" not in materialization
    assert "DeltaTable.forName" not in materialization
    assert "partition_columns = []" in create_tables

    advert_product_contract = (
        PROJECT_ROOT
        / "sql"
        / "features"
        / "nextads"
        / "create_table_next_uk_nextads_fs_advert_product_profile_daily.sql"
    ).read_text()
    assert "PARTITIONED BY" not in advert_product_contract.upper()


def test_theme_affinity_training_input_skip_path_uses_lazy_runtime_imports():
    source = (
        PROJECT_ROOT
        / "jobs"
        / "features"
        / "nextads"
        / "build_theme_affinity_training_input.py"
    ).read_text()

    assert "from next_ads.ranking.theme_affinity.config import resolve_runtime" not in (
        source.split("def main() -> None:", maxsplit=1)[0]
    )
    assert "if _should_skip(args.theme_training_reference_date):" in source
    assert (
        "from next_ads.ranking.theme_affinity.config import resolve_runtime"
        in source.split("if _should_skip(args.theme_training_reference_date):", maxsplit=1)[1]
    )


def test_feature_store_quality_checks_use_single_aggregate_per_table():
    source = (
        PROJECT_ROOT / "jobs" / "features" / "nextads" / "quality_checks.py"
    ).read_text()

    assert "def _quality_counts" in source
    assert "dataframe.agg(" in source
    assert "F.countDistinct(key_struct)" in source
    assert "valid_distinct_key_count" in source
    assert "row_count = dataframe.count()" not in source
    assert "distinct().count()" not in source

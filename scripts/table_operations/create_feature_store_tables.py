"""Create opt-in Next Ads feature tables from registry contracts."""

from __future__ import annotations

import argparse
import inspect
import logging
import sys
from pathlib import Path


try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
except NameError:
    from dsutils.dbc import get_dbutils

    dbutils = get_dbutils()
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent.parent
finally:
    sys.path.insert(0, str(PROJECT_ROOT))


from next_ads.features import load_feature_store_registry
from next_ads.features.feature_store_registry import normalize_schema_name
from next_ads.features.sql_contracts import extract_create_table_columns


LOGGER = logging.getLogger(__name__)


def configure_job_logging(log_level: str) -> None:
    """Configure job logging while keeping dependency internals quiet."""
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)


def ddl_type_to_spark_type(data_type: str):
    """Convert the small repo DDL type subset into Spark SQL types."""
    from pyspark.sql import types as T

    normalized = data_type.upper().replace(" NOT NULL", "").strip()
    if normalized == "STRING":
        return T.StringType()
    if normalized == "DATE":
        return T.DateType()
    if normalized == "INT":
        return T.IntegerType()
    if normalized == "BIGINT":
        return T.LongType()
    if normalized == "DOUBLE":
        return T.DoubleType()
    if normalized == "BOOLEAN":
        return T.BooleanType()
    if normalized == "TIMESTAMP":
        return T.TimestampType()
    if normalized == "ARRAY<DOUBLE>":
        return T.ArrayType(T.DoubleType())
    if normalized == "MAP<STRING, STRING>":
        return T.MapType(T.StringType(), T.StringType())
    if normalized == "MAP<STRING, DOUBLE>":
        return T.MapType(T.StringType(), T.DoubleType())

    raise ValueError(f"Unsupported feature-store DDL type: {data_type}")


def schema_from_contract(contract_sql: str):
    """Build a Spark StructType from a repo SQL feature-table contract."""
    from pyspark.sql import types as T

    fields = []
    for name, data_type in extract_create_table_columns(contract_sql):
        nullable = "NOT NULL" not in data_type.upper()
        fields.append(
            T.StructField(
                name,
                ddl_type_to_spark_type(data_type),
                nullable=nullable,
            )
        )
    return T.StructType(fields)


def validate_schema_exists(spark, catalog: str, schema: str) -> None:
    """Fail fast when the target schema is absent."""
    result = (
        spark.sql(f"SHOW SCHEMAS IN {catalog}")
        .filter(f"`databaseName` = '{schema}'")
        .collect()
    )
    if not result:
        raise ValueError(f"Required schema does not exist: {catalog}.{schema}")


def create_schema_if_missing(
    spark,
    catalog: str,
    schema: str,
    manage_principals: list[str] | None = None,
    all_privileges_principals: list[str] | None = None,
) -> None:
    """Create the target schema and grant shared DEV access when requested."""
    schema_path = f"{catalog}.{schema}"
    LOGGER.info("Ensuring feature-store schema exists: %s", schema_path)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_path}")

    for principal in manage_principals or []:
        if principal:
            spark.sql(f"GRANT MANAGE ON SCHEMA {schema_path} TO `{principal}`")

    for principal in all_privileges_principals or []:
        if principal:
            spark.sql(
                f"GRANT ALL PRIVILEGES ON SCHEMA {schema_path} TO `{principal}`"
            )


def create_feature_engineering_client():
    """Create the Databricks Feature Engineering client at runtime."""
    try:
        from databricks.feature_engineering import FeatureEngineeringClient
    except ImportError as exc:
        raise ImportError(
            "databricks.feature_engineering is required to create Databricks "
            "feature tables. Run this on a Databricks runtime or install the "
            "Databricks Feature Engineering package in the execution "
            "environment. Original import error: "
            f"{exc}"
        ) from exc

    return FeatureEngineeringClient()


def _supported_kwargs(callable_obj, kwargs):
    signature = inspect.signature(callable_obj)
    return {
        key: value
        for key, value in kwargs.items()
        if key in signature.parameters and value is not None
    }


def create_databricks_feature_table(
    feature_engineering_client,
    name: str,
    primary_keys: tuple[str, ...],
    schema,
    description: str,
    timestamp_key: str | None,
    partition_columns: list[str],
    tags: dict[str, str],
) -> None:
    """Create a Databricks Feature Engineering table.

    Databricks has used different names for time-series arguments across
    feature-store APIs. Inspecting the installed client keeps this compatible
    with the Databricks runtime version used by the bundle.
    """
    create_table = feature_engineering_client.create_table
    kwargs = _supported_kwargs(
        create_table,
        {
            "name": name,
            "primary_keys": list(primary_keys),
            "schema": schema,
            "description": description,
            "timestamp_keys": [timestamp_key] if timestamp_key else None,
            "timestamp_key": timestamp_key,
            "timeseries_columns": [timestamp_key] if timestamp_key else None,
            "timeseries_column": timestamp_key,
            "partition_columns": partition_columns or None,
            "tags": tags,
        },
    )
    create_table(**kwargs)


def drop_feature_store_objects(
    spark,
    catalog: str,
    schema: str,
    dry_run: bool = False,
) -> list[str]:
    """Drop registered feature-store objects so they can be recreated cleanly."""
    registry = load_feature_store_registry()
    dropped_objects = []

    for view in registry.compatibility_views:
        view_path = f"{catalog}.{schema}.{view['name']}"
        if dry_run:
            LOGGER.info("Dry run DROP VIEW IF EXISTS %s", view_path)
        else:
            LOGGER.info("Dropping feature-store compatibility view: %s", view_path)
            spark.sql(f"DROP VIEW IF EXISTS {view_path}")
        dropped_objects.append(view_path)

    for table in registry.physical_tables:
        table_path = registry.resolved_table_path(
            table.name,
            catalog=catalog,
            schema=schema,
        )
        if dry_run:
            LOGGER.info("Dry run DROP TABLE IF EXISTS %s", table_path)
        else:
            LOGGER.info("Dropping feature-store table: %s", table_path)
            spark.sql(f"DROP TABLE IF EXISTS {table_path}")
        dropped_objects.append(table_path)

    return dropped_objects


def create_feature_store_tables(
    spark,
    catalog: str | None = None,
    schema: str | None = None,
    dry_run: bool = False,
    feature_engineering_client=None,
    create_schema: bool = True,
    recreate_tables: bool = False,
    manage_principals: list[str] | None = None,
    all_privileges_principals: list[str] | None = None,
) -> list[str]:
    """Create missing physical Databricks feature tables.

    Returns the table paths that were created or would be created in dry-run
    mode. Existing tables are left untouched.
    """
    registry = load_feature_store_registry()
    target_catalog = catalog or registry.default_catalog
    target_schema = normalize_schema_name(schema or registry.default_schema)
    created_tables = []
    fe_client = feature_engineering_client

    if create_schema and not dry_run:
        create_schema_if_missing(
            spark,
            target_catalog,
            target_schema,
            manage_principals=manage_principals,
            all_privileges_principals=all_privileges_principals,
        )
    validate_schema_exists(spark, target_catalog, target_schema)
    if recreate_tables:
        dropped_objects = drop_feature_store_objects(
            spark,
            target_catalog,
            target_schema,
            dry_run=dry_run,
        )
        LOGGER.info("Recreated feature-store objects requested: %s", dropped_objects)
    if not dry_run and fe_client is None:
        fe_client = create_feature_engineering_client()

    for table in registry.physical_tables:
        table_path = registry.resolved_table_path(
            table.name,
            catalog=target_catalog,
            schema=target_schema,
        )
        contract_path = registry.sql_contract_path(table.name)
        contract_sql = contract_path.read_text()

        if spark.catalog.tableExists(table_path):
            LOGGER.info("Feature-store table already exists: %s", table_path)
            continue

        LOGGER.info("Creating feature-store table: %s", table_path)
        feature_schema = schema_from_contract(contract_sql)
        partition_columns = []
        tags = {
            "nextads_feature_store": registry.name,
            "entity": table.entity,
            "source_job": table.source_job,
            "owner": table.owner,
            "freshness": table.freshness,
            "training_safe": str(table.training_safe).lower(),
            "consumers": ",".join(table.consumers),
        }
        if dry_run:
            LOGGER.info(
                "Dry run FeatureEngineeringClient.create_table for %s "
                "keys=%s timestamp_key=%s",
                table_path,
                table.primary_keys,
                table.timestamp_key,
            )
        else:
            create_databricks_feature_table(
                fe_client,
                name=table_path,
                primary_keys=table.primary_keys,
                schema=feature_schema,
                description=table.grain,
                timestamp_key=table.timestamp_key,
                partition_columns=partition_columns,
                tags=tags,
            )
        created_tables.append(table_path)

    create_feature_store_views(
        spark,
        catalog=target_catalog,
        schema=target_schema,
        dry_run=dry_run,
    )
    return created_tables


def create_feature_store_views(
    spark,
    catalog: str | None = None,
    schema: str | None = None,
    dry_run: bool = False,
) -> list[str]:
    """Create or replace feature-store compatibility views."""
    registry = load_feature_store_registry()
    target_catalog = catalog or registry.default_catalog
    target_schema = normalize_schema_name(schema or registry.default_schema)
    created_views = []

    for view in registry.compatibility_views:
        view_name = view["name"]
        view_path = (
            registry.table_root / f"create_view_{view_name}.sql"
        )
        view_sql = view_path.read_text().format(
            catalog=target_catalog,
            schema=target_schema,
        )
        resolved_view = f"{target_catalog}.{target_schema}.{view_name}"
        if dry_run:
            LOGGER.info("Dry run CREATE OR REPLACE VIEW for %s", resolved_view)
        else:
            LOGGER.info("Creating feature-store compatibility view: %s", resolved_view)
            spark.sql(view_sql)
        created_views.append(resolved_view)

    return created_views


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--dry_run", default="False")
    parser.add_argument("--create_schema", default="True")
    parser.add_argument("--recreate_tables", default="False")
    parser.add_argument("--manage_principal", action="append", default=[])
    parser.add_argument("--all_privileges_principal", action="append", default=[])
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_job_logging(args.log_level)

    from dsutils.dbc import configure_spark

    spark = configure_spark()
    created_tables = create_feature_store_tables(
        spark,
        catalog=args.catalog,
        schema=args.schema,
        dry_run=args.dry_run.lower() == "true",
        create_schema=args.create_schema.lower() == "true",
        recreate_tables=args.recreate_tables.lower() == "true",
        manage_principals=args.manage_principal,
        all_privileges_principals=args.all_privileges_principal,
    )
    LOGGER.info("Feature-store table setup complete: %s", created_tables)


if __name__ == "__main__":
    main()

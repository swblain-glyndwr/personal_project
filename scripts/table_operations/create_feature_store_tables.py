"""Create opt-in Next Ads feature-store tables from registry contracts."""

from __future__ import annotations

import argparse
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


LOGGER = logging.getLogger(__name__)


def render_contract(contract_sql: str, catalog: str, schema: str) -> str:
    """Render a feature-store SQL contract for a target catalog/schema."""

    return contract_sql.format(catalog=catalog, schema=schema)


def validate_schema_exists(spark, catalog: str, schema: str) -> None:
    """Fail fast when the target schema is absent."""

    result = (
        spark.sql(f"SHOW SCHEMAS IN {catalog}")
        .filter(f"`databaseName` = '{schema}'")
        .collect()
    )
    if not result:
        raise ValueError(f"Required schema does not exist: {catalog}.{schema}")


def create_feature_store_tables(
    spark,
    catalog: str | None = None,
    schema: str | None = None,
    dry_run: bool = False,
) -> list[str]:
    """Create missing physical feature-store tables.

    Returns the table paths that were created or would be created in dry-run
    mode. Existing tables are left untouched.
    """

    registry = load_feature_store_registry()
    target_catalog = catalog or registry.default_catalog
    target_schema = schema or registry.default_schema
    created_tables = []

    validate_schema_exists(spark, target_catalog, target_schema)

    for table in registry.physical_tables:
        table_path = registry.resolved_table_path(
            table.name,
            catalog=target_catalog,
            schema=target_schema,
        )
        contract_path = registry.sql_contract_path(table.name)
        query = render_contract(
            contract_path.read_text(),
            catalog=target_catalog,
            schema=target_schema,
        )

        if spark.catalog.tableExists(table_path):
            LOGGER.info("Feature-store table already exists: %s", table_path)
            continue

        LOGGER.info("Creating feature-store table: %s", table_path)
        if dry_run:
            LOGGER.info("Dry run SQL for %s:\n%s", table_path, query)
        else:
            spark.sql(query)
        created_tables.append(table_path)

    return created_tables


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--dry_run", default="False")
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    from dsutils.dbc import configure_spark

    spark = configure_spark()
    created_tables = create_feature_store_tables(
        spark,
        catalog=args.catalog,
        schema=args.schema,
        dry_run=args.dry_run.lower() == "true",
    )
    LOGGER.info("Feature-store table setup complete: %s", created_tables)


if __name__ == "__main__":
    main()

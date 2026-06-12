import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    from dsutils.dbc import get_dbutils

    dbutils = get_dbutils()
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

import json
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from next_ads.utils import config_manager
from next_ads.utils import etl


def extract_table_paths(obj, parent_key=""):
    """Recursively extract all table paths from a potentially nested structure.

    Args:
        obj: A dict, string, or other value that may contain table path definitions
        parent_key: The key path for context (used in logging)

    Returns:
        A dict of {table_ref: table_path} where all values are strings (table paths)
    """
    tables = {}

    if isinstance(obj, dict):
        for key, value in obj.items():
            current_key = f"{parent_key}.{key}" if parent_key else key

            if isinstance(value, str):
                # This is a table path (string value)
                tables[current_key] = value
            elif isinstance(value, dict):
                # Recursively extract from nested dict
                nested_tables = extract_table_paths(value, current_key)
                tables.update(nested_tables)
            # Skip other types (lists, None, etc.)

    return tables


def extract_create_table_columns(create_table_sql: str) -> list[tuple[str, str]]:
    """Extract simple column definitions from a CREATE TABLE statement."""
    columns = []
    in_column_block = False

    for raw_line in create_table_sql.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("--"):
            continue

        upper_line = line.upper()
        if not in_column_block:
            if upper_line.startswith("CREATE TABLE"):
                in_column_block = True
            continue

        if (
            upper_line.startswith("CONSTRAINT")
            or upper_line.startswith("PRIMARY KEY")
            or upper_line.startswith("USING ")
            or upper_line.startswith("PARTITIONED ")
            or upper_line.startswith("TBLPROPERTIES")
            or upper_line.startswith(")")
        ):
            break

        parts = line.split()
        if len(parts) < 2:
            continue

        columns.append((parts[0].strip("`"), parts[1]))

    return columns


def build_add_missing_columns_query(
    table: str,
    expected_columns: list[tuple[str, str]],
    actual_columns: list[str],
) -> str | None:
    """Build an additive ALTER TABLE statement for columns absent from target."""
    actual_column_set = set(actual_columns)
    missing_columns = [
        (name, data_type)
        for name, data_type in expected_columns
        if name not in actual_column_set
    ]

    if not missing_columns:
        return None

    columns_sql = ", ".join(
        f"`{name}` {data_type}" for name, data_type in missing_columns
    )
    return f"ALTER TABLE {table} ADD COLUMNS ({columns_sql})"


def main(JOB_ENV, CLIENT, LOG_LEVEL, DROP_TABLES=False, ALTER_TABLES=False):
    configure_logging(
        log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()

    logger.info(f"Running in job environment: {JOB_ENV}")
    if ALTER_TABLES and JOB_ENV.lower() != "dev":
        raise ValueError("--altertables is only supported for dev table setup")

    if not CLIENT:
        assert JOB_ENV.lower() == "dev", (
            f"Client must be specified when running in {JOB_ENV}"
        )
        CLIENT = "next_uk"  # Client can be specified for interactive debugging
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    logger.info(f"Configuring run for client: {CLIENT}")

    # Try to load from Dynaconf first (new approach)
    try:
        config = config_manager.load_config(JOB_ENV)
        logger.info("Loaded configuration from Dynaconf settings")

        # Get table definitions and catalog info from Dynaconf
        tbls_write = config.get("tables_write", {})
        write_catalog = config.get("catalog_write", "marketingdata_dev")
        schema_write = config.get("schema_write", "ds_sandbox")

        if not tbls_write:
            raise ValueError("tables_write not found in Dynaconf config")

        # Recursively extract all table paths from potentially nested structure
        tbls = extract_table_paths(tbls_write)

        if not tbls:
            raise ValueError("No table paths found in tables_write config")

        use_dynaconf = True
        logger.info(
            f"Using Dynaconf tables config with write_catalog={write_catalog}, schema_write={schema_write}"
        )
        logger.info(f"Extracted {len(tbls)} table definitions from config")
    except Exception as e:
        logger.warning(
            f"Failed to load Dynaconf config: {e}. Falling back to legacy JSON config"
        )
        use_dynaconf = False

        # Fallback to legacy JSON config
        with open(PROJECT_ROOT / f"config/{CLIENT}.json") as f:
            cfg = json.load(f)

        tbls = cfg["tables"]["write"]
        SCHEMA = cfg["schema"][JOB_ENV]
        write_catalog = "marketingdata_prod"  # Legacy always used prod catalog
        schema_write = SCHEMA
        logger.info(
            f"Using legacy config with write_catalog={write_catalog}, schema_write={schema_write}"
        )

    # Extract catalog and schema from the first table to validate schema existence
    # This assumes all tables share the same catalog and schema
    first_table_path = list(tbls.values())[0]
    parts = first_table_path.split(".")
    if len(parts) >= 2:
        schema_to_validate = parts[1]
        catalog_to_validate = parts[0]
    else:
        schema_to_validate = schema_write
        catalog_to_validate = write_catalog

    logger.info(
        f"Validating schema existence: {catalog_to_validate}.{schema_to_validate}"
    )

    # Check if schema exists before creating tables
    try:
        result = (
            spark.sql(f"SHOW SCHEMAS IN {catalog_to_validate}")
            .filter(f"`databaseName` = '{schema_to_validate}'")
            .collect()
        )
        if not result:
            error_msg = f"ERROR: Schema does not exist: {catalog_to_validate}.{schema_to_validate}\n"
            logger.error(error_msg)
            raise ValueError(
                f"Required schema does not exist: {catalog_to_validate}.{schema_to_validate}"
            )
        logger.info(
            f"Schema validated: {catalog_to_validate}.{schema_to_validate}"
        )
    except Exception as e:
        logger.error(f"Failed to validate schema: {e}")
        raise

    # Prepare table arguments for template substitution
    if use_dynaconf:
        # When using Dynaconf, tables are already fully resolved
        # but we still need catalog and schema for template substitution in SQL files
        tbl_args = {
            "schema": schema_write,
            "client": CLIENT,
            "catalog": write_catalog,
        }
    else:
        # Legacy: need to substitute placeholders
        tbl_args = {
            "schema": schema_write,
            "client": CLIENT,
            "catalog": write_catalog,
        }

    # Check for missing SQL scripts before proceeding
    missing_scripts = []
    for table_ref in tbls:
        sql_script_path = PROJECT_ROOT / \
            f"sql/create_table_{table_ref.replace('.', '_')}.sql"
        if not sql_script_path.exists():
            missing_scripts.append(str(sql_script_path))

    if missing_scripts:
        raise ValueError(
            f"Missing SQL create scripts: {', '.join(missing_scripts)}")

    for table_ref in tbls:
        if use_dynaconf:
            # Table is already resolved from Dynaconf
            table = tbls[table_ref]
        else:
            # Legacy: use map_tbl to substitute placeholders
            table = etl.map_tbl(tbls[table_ref], **tbl_args)

        if DROP_TABLES and JOB_ENV.lower() == "dev":
            logger.info(f"Dropping table {table} as --droptables is 'True'")
            logger.info(f"Running drop table if exists {table}")
            spark.sql(f"drop table if exists {table}")

        logger.info(f"Checking existence of table {table}")
        # replace . with "_" for nested dynaconf table refs
        with open(PROJECT_ROOT / f"sql/create_table_{table_ref.replace('.', '_')}.sql") as f:
            query = etl.map_tbl("".join(f.readlines()), **tbl_args)

        if spark.catalog.tableExists(table):
            if not ALTER_TABLES:
                logger.debug(f"Table {table} already exists - skipping")
                continue

            logger.info(f"Checking {table} for missing columns")
            expected_columns = extract_create_table_columns(query)
            actual_columns = spark.table(table).columns
            alter_query = build_add_missing_columns_query(
                table,
                expected_columns,
                actual_columns,
            )
            if not alter_query:
                logger.info(f"Table {table} already has all expected columns")
                continue

            logger.info(f"Adding missing columns to {table}")
            logger.info(f"Running: {alter_query}")
            spark.sql(alter_query)
            continue

        logger.info(f"Creating {table_ref} table as: {table}")
        logger.info(f"Running: {query}")
        spark.sql(query)

    logger.info("Run complete")


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    JOB_ENV = jobparser.get_arg("--job_env")
    CLIENT = jobparser.get_arg("--client")
    LOG_LEVEL = jobparser.get_arg("--log_level")
    DROP_TABLES = jobparser.get_typed_arg("--droptables", bool)
    ALTER_TABLES = jobparser.get_typed_arg("--altertables", bool)
    main(JOB_ENV, CLIENT, LOG_LEVEL, DROP_TABLES, ALTER_TABLES)

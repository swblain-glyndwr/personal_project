import re
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
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger

from next_ads.utils import config_manager


TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+[.][A-Za-z0-9_]+[.][A-Za-z0-9_]+$")


def extract_table_paths(obj):
    """Return fully qualified table-like strings from a nested config object."""
    if hasattr(obj, "to_dict"):
        obj = obj.to_dict()

    if isinstance(obj, dict):
        tables = []
        for value in obj.values():
            tables.extend(extract_table_paths(value))
        return tables

    if isinstance(obj, str) and TABLE_NAME_RE.match(obj):
        return [obj]

    return []


def schema_exists(spark, catalog, schema):
    result = (
        spark.sql(f"SHOW SCHEMAS IN {catalog}")
        .filter(f"`databaseName` = '{schema}'")
        .limit(1)
        .collect()
    )
    return bool(result)


def assert_table_exists(spark, table):
    if not spark.catalog.tableExists(table):
        raise AssertionError(f"Required table does not exist: {table}")


def sample_table_read(spark, table):
    spark.table(table).limit(1).collect()


def validate_preprod_route(job_env, config):
    if job_env.lower() != "preprod":
        raise ValueError("This smoke check must run with job_env=preprod")

    if config.catalog_write != "marketingdata_prod":
        raise ValueError("PREPROD catalog_write must resolve to marketingdata_prod")

    if config.schema_write != "ds_sandbox":
        raise ValueError("PREPROD schema_write must resolve to ds_sandbox")


def validate_schemas(spark, config):
    if not schema_exists(spark, config.catalog_write, config.schema_write):
        raise AssertionError(
            f"PREPROD output schema does not exist: "
            f"{config.catalog_write}.{config.schema_write}"
        )

    if not schema_exists(spark, config.catalog_read, config.schema_read):
        raise AssertionError(
            f"PREPROD input schema does not exist: "
            f"{config.catalog_read}.{config.schema_read}"
        )


def collect_table_failures(spark, tables, logger, sample_read_count=0):
    failures = []
    for table in tables:
        logger.info(f"Checking metadata for {table}")
        try:
            assert_table_exists(spark, table)
        except Exception as exc:
            failures.append(f"{table}: {exc}")

    if sample_read_count <= 0:
        logger.info("Skipping sample reads; metadata-only smoke requested")
        return failures

    for table in tables[:sample_read_count]:
        logger.info(f"Sample-reading one row from {table}")
        try:
            sample_table_read(spark, table)
        except Exception as exc:
            failures.append(f"{table}: sample read failed: {exc}")

    return failures


def main(job_env, sample_read_count=0):
    logger = get_logger(__name__)
    spark = configure_spark()
    config = config_manager.load_config(job_env)

    logger.info("Running metadata-only PREPROD dependency smoke")
    logger.info(f"Resolved job_env: {job_env}")
    logger.info(f"catalog_read: {config.catalog_read}")
    logger.info(f"schema_read: {config.schema_read}")
    logger.info(f"catalog_write: {config.catalog_write}")
    logger.info(f"schema_write: {config.schema_write}")

    validate_preprod_route(job_env, config)
    validate_schemas(spark, config)

    read_tables = sorted(set(extract_table_paths(config.get("tables_read", {}))))
    logger.info(f"Checking {len(read_tables)} configured read table dependencies")

    failures = collect_table_failures(
        spark=spark,
        tables=read_tables,
        logger=logger,
        sample_read_count=sample_read_count,
    )

    if failures:
        raise AssertionError(
            "PREPROD dependency smoke failed:\n" + "\n".join(failures)
        )

    logger.info("PREPROD dependency smoke passed without altering tables")


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    LOG_LEVEL = jobparser.get_arg("--log_level")
    configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()

    JOB_ENV = jobparser.get_arg("--job_env")
    SAMPLE_READ_COUNT = int(jobparser.get_arg("--sample_read_count") or 0)

    main(
        job_env=JOB_ENV,
        sample_read_count=SAMPLE_READ_COUNT,
    )

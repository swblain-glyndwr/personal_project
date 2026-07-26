import re
import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
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
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger

from next_ads.common import config_manager
from next_ads.common.paths import resolve_sql_contract_path
from jobs.table_operations.create_tables import (
    compare_table_schema,
    extract_create_table_columns,
    extract_table_paths,
    parse_column_specs,
    spark_schema_column_specs,
)


TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+[.][A-Za-z0-9_]+[.][A-Za-z0-9_]+$")


def _as_dict(obj):
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    return obj


def validate_prod_route(job_env, config):
    if job_env.lower() != "prod":
        raise ValueError("This smoke check must run with job_env=prod")

    if config.catalog_write != "marketingdata_prod":
        raise ValueError(
            "PROD catalog_write must resolve to marketingdata_prod"
        )

    if config.schema_write != "warehouse":
        raise ValueError("PROD schema_write must resolve to warehouse")


def describe_schema_drift(drift, allow_extra_columns=False):
    details = []
    if drift.missing_columns:
        details.append(
            "missing columns: "
            + ", ".join(column.name for column in drift.missing_columns)
        )
    if drift.extra_columns and not allow_extra_columns:
        details.append("unexpected columns: " + ", ".join(drift.extra_columns))
    if drift.order_drift:
        details.append("column order does not match SQL contract")
    if drift.type_drift:
        details.append("type drift: " + "; ".join(drift.type_drift))
    if drift.nullability_drift:
        details.append(
            "nullability drift: " + "; ".join(drift.nullability_drift)
        )
    return details


def collect_contract_failures(
    spark,
    table_contracts,
    logger,
    allow_extra_columns=False,
):
    failures = []
    for table_ref, table in table_contracts.items():
        if not TABLE_NAME_RE.match(table):
            failures.append(
                f"{table_ref}: expected fully qualified table, found {table}"
            )
            continue

        sql_script_path = resolve_sql_contract_path(table_ref)
        if not sql_script_path.exists():
            failures.append(
                f"{table_ref}: missing SQL contract {sql_script_path}"
            )
            continue

        logger.info(f"Checking table contract for {table_ref}: {table}")
        if not spark.catalog.tableExists(table):
            failures.append(f"{table_ref}: missing table {table}")
            continue

        expected_columns = parse_column_specs(
            extract_create_table_columns(sql_script_path.read_text())
        )
        actual_columns = spark_schema_column_specs(spark.table(table).schema)
        drift = compare_table_schema(expected_columns, actual_columns)
        drift_details = describe_schema_drift(
            drift,
            allow_extra_columns=allow_extra_columns,
        )
        if drift_details:
            failures.append(f"{table_ref}: {table} has schema drift")
            failures.extend(f"  - {detail}" for detail in drift_details)

    return failures


def main(job_env, client, allow_extra_columns=False):
    logger = get_logger(__name__)
    spark = configure_spark()
    config = config_manager.load_config(job_env, client=client)

    logger.info("Running read-only PROD table contract smoke")
    logger.info(f"Resolved job_env: {job_env}")
    logger.info(f"Resolved client: {client}")
    logger.info(f"catalog_write: {config.catalog_write}")
    logger.info(f"schema_write: {config.schema_write}")
    logger.info(f"allow_extra_columns: {allow_extra_columns}")

    validate_prod_route(job_env, config)

    table_contracts = extract_table_paths(
        _as_dict(config.get("tables_write", {}))
    )
    logger.info(
        f"Checking {len(table_contracts)} configured write table contracts"
    )

    failures = collect_contract_failures(
        spark=spark,
        table_contracts=table_contracts,
        logger=logger,
        allow_extra_columns=allow_extra_columns,
    )

    if failures:
        raise AssertionError(
            "PROD table contract smoke failed:\n" + "\n".join(failures)
        )

    logger.info("PROD table contract smoke passed without altering tables")


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    LOG_LEVEL = jobparser.get_arg("--log_level")
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()

    JOB_ENV = jobparser.get_arg("--job_env")
    CLIENT = jobparser.get_arg("--client") or "next_uk"
    ALLOW_EXTRA_COLUMNS = jobparser.get_typed_arg(
        "--allow_extra_columns", bool
    )

    main(
        job_env=JOB_ENV,
        client=CLIENT,
        allow_extra_columns=ALLOW_EXTRA_COLUMNS,
    )

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
from scripts.table_operations.create_tables import (
    extract_create_table_columns,
    extract_table_paths,
)


TABLE_NAME_RE = re.compile(r"^[A-Za-z0-9_]+[.][A-Za-z0-9_]+[.][A-Za-z0-9_]+$")
COMPLEX_TYPE_PREFIXES = ("array<", "map<", "struct<")
SPARK_TYPE_ALIASES = {
    "bigint": "bigint",
    "boolean": "boolean",
    "date": "date",
    "double": "double",
    "float": "float",
    "int": "int",
    "integer": "int",
    "long": "bigint",
    "string": "string",
    "timestamp": "timestamp",
}


def _as_dict(obj):
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    return obj


def validate_prod_route(job_env, config):
    if job_env.lower() != "prod":
        raise ValueError("This smoke check must run with job_env=prod")

    if config.catalog_write != "marketingdata_prod":
        raise ValueError("PROD catalog_write must resolve to marketingdata_prod")

    if config.schema_write != "warehouse":
        raise ValueError("PROD schema_write must resolve to warehouse")


def normalize_type(type_name):
    """Return comparable simple scalar type, or None for complex types."""
    cleaned = " ".join(type_name.lower().replace("`", "").split())
    cleaned = cleaned.replace(" not null", "")

    if cleaned.startswith(COMPLEX_TYPE_PREFIXES):
        return None

    base_type = cleaned.split("(", maxsplit=1)[0].split(maxsplit=1)[0]
    return SPARK_TYPE_ALIASES.get(base_type)


def compare_expected_columns(expected_columns, actual_fields, allow_extra_columns=False):
    expected_names = {name for name, _ in expected_columns}
    actual_types_by_name = {name: normalize_type(data_type) for name, data_type in actual_fields}
    actual_names = set(actual_types_by_name)

    missing_columns = []
    extra_columns = sorted(actual_names - expected_names)
    type_mismatches = []
    for name, expected_type in expected_columns:
        if name not in actual_names:
            missing_columns.append(name)
            continue

        normalized_expected = normalize_type(expected_type)
        normalized_actual = actual_types_by_name[name]
        if (
            normalized_expected is not None
            and normalized_actual is not None
            and normalized_expected != normalized_actual
        ):
            type_mismatches.append(
                f"{name}: expected {normalized_expected}, found {normalized_actual}"
            )

    if allow_extra_columns:
        extra_columns = []

    return missing_columns, extra_columns, type_mismatches


def get_actual_fields(spark, table):
    return [
        (field.name, field.dataType.simpleString())
        for field in spark.table(table).schema.fields
    ]


def collect_contract_failures(
    spark,
    table_contracts,
    logger,
    allow_extra_columns=False,
):
    failures = []
    for table_ref, table in table_contracts.items():
        if not TABLE_NAME_RE.match(table):
            failures.append(f"{table_ref}: expected fully qualified table, found {table}")
            continue

        sql_script_path = (
            PROJECT_ROOT / f"sql/create_table_{table_ref.replace('.', '_')}.sql"
        )
        if not sql_script_path.exists():
            failures.append(f"{table_ref}: missing SQL contract {sql_script_path}")
            continue

        logger.info(f"Checking table contract for {table_ref}: {table}")
        if not spark.catalog.tableExists(table):
            failures.append(f"{table_ref}: missing table {table}")
            continue

        expected_columns = extract_create_table_columns(sql_script_path.read_text())
        actual_fields = get_actual_fields(spark, table)
        missing_columns, extra_columns, type_mismatches = compare_expected_columns(
            expected_columns,
            actual_fields,
            allow_extra_columns=allow_extra_columns,
        )

        if missing_columns:
            failures.append(
                f"{table_ref}: {table} missing columns {', '.join(missing_columns)}"
            )
        if extra_columns:
            failures.append(
                f"{table_ref}: {table} has unexpected columns "
                + ", ".join(extra_columns)
            )
        if type_mismatches:
            failures.append(
                f"{table_ref}: {table} type mismatches: "
                + "; ".join(type_mismatches)
            )

    return failures


def main(job_env, client, allow_extra_columns=False):
    logger = get_logger(__name__)
    spark = configure_spark()
    config = config_manager.load_config(job_env)

    logger.info("Running read-only PROD table contract smoke")
    logger.info(f"Resolved job_env: {job_env}")
    logger.info(f"Resolved client: {client}")
    logger.info(f"catalog_write: {config.catalog_write}")
    logger.info(f"schema_write: {config.schema_write}")
    logger.info(f"allow_extra_columns: {allow_extra_columns}")

    validate_prod_route(job_env, config)

    table_contracts = extract_table_paths(_as_dict(config.get("tables_write", {})))
    logger.info(f"Checking {len(table_contracts)} configured write table contracts")

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
    configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()

    JOB_ENV = jobparser.get_arg("--job_env")
    CLIENT = jobparser.get_arg("--client") or "next_uk"
    ALLOW_EXTRA_COLUMNS = jobparser.get_typed_arg("--allow_extra_columns", bool)

    main(
        job_env=JOB_ENV,
        client=CLIENT,
        allow_extra_columns=ALLOW_EXTRA_COLUMNS,
    )

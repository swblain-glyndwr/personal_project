import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
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
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from next_ads.common import config_manager
from next_ads.common.paths import resolve_sql_contract_path
from next_ads.common import etl


DEFAULT_COLUMN_VALUES = {
    "Audience": "'false'",
}

ASSIGNMENT_BUILD_STATE_TABLE_SUFFIXES = frozenset(
    {
        "_nextads_assignments_build_staging",
        "_nextads_assignments_v2_build_staging",
        "_nextads_assignment_build_events",
    }
)
ASSIGNMENT_PROVENANCE_COLUMNS = frozenset(
    {
        "CandidateBuildID",
        "CandidateBuildAttemptID",
        "PortfolioID",
        "PortfolioAttemptID",
        "CandidateFoundationSnapshotID",
    }
)


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    data_type: str
    nullable: bool
    raw_definition: str


@dataclass(frozen=True)
class SchemaDrift:
    missing_columns: list[ColumnSpec]
    extra_columns: list[str]
    order_drift: bool
    type_drift: list[str]
    nullability_drift: list[str]

    @property
    def has_drift(self) -> bool:
        return bool(
            self.missing_columns
            or self.extra_columns
            or self.order_drift
            or self.type_drift
            or self.nullability_drift
        )

    @property
    def requires_rebuild(self) -> bool:
        return bool(
            self.extra_columns
            or self.order_drift
            or self.type_drift
            or self.nullability_drift
            or [
                column
                for column in self.missing_columns
                if not can_auto_add_column(column.raw_definition)
            ]
        )

    def summary(self) -> str:
        details = []
        if self.missing_columns:
            details.append(
                "missing="
                + ", ".join(column.name for column in self.missing_columns)
            )
        if self.extra_columns:
            details.append("extra=" + ", ".join(self.extra_columns))
        if self.order_drift:
            details.append("order_drift=true")
        if self.type_drift:
            details.append("type_drift=" + ", ".join(self.type_drift))
        if self.nullability_drift:
            details.append(
                "nullability_drift=" + ", ".join(self.nullability_drift)
            )
        return "; ".join(details) if details else "none"

    @property
    def unsupported_missing_columns(self) -> list[str]:
        return [
            column.name
            for column in self.missing_columns
            if not can_auto_add_column(column.raw_definition)
            and column.name not in DEFAULT_COLUMN_VALUES
        ]


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


def _extract_outer_column_block(create_table_sql: str) -> str:
    """Return the text inside the CREATE TABLE column-list parentheses."""
    start = create_table_sql.find("(")
    if start == -1:
        return ""

    depth = 0
    for index, char in enumerate(create_table_sql[start:], start=start):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return create_table_sql[start + 1 : index]

    return ""


def _split_top_level_column_definitions(column_block: str) -> list[str]:
    """Split column definitions without splitting inside STRUCT/ARRAY types."""
    definitions = []
    current = []
    angle_depth = 0
    paren_depth = 0

    for char in column_block:
        if char == "<":
            angle_depth += 1
        elif char == ">" and angle_depth > 0:
            angle_depth -= 1
        elif char == "(":
            paren_depth += 1
        elif char == ")" and paren_depth > 0:
            paren_depth -= 1

        if char == "," and angle_depth == 0 and paren_depth == 0:
            definition = "".join(current).strip()
            if definition:
                definitions.append(definition)
            current = []
            continue

        current.append(char)

    definition = "".join(current).strip()
    if definition:
        definitions.append(definition)

    return definitions


def extract_create_table_columns(
    create_table_sql: str,
) -> list[tuple[str, str]]:
    """Extract top-level column definitions from a CREATE TABLE statement."""
    columns = []
    column_block = _extract_outer_column_block(create_table_sql)

    for definition in _split_top_level_column_definitions(column_block):
        line = " ".join(definition.split())
        upper_line = line.upper()
        if upper_line.startswith(("CONSTRAINT", "PRIMARY KEY")):
            continue

        parts = line.split(maxsplit=1)
        if len(parts) < 2:
            continue

        columns.append((parts[0].strip("`"), parts[1]))

    return columns


def normalize_data_type(data_type: str) -> str:
    """Normalize SQL/Spark type text for contract comparisons."""
    upper_type = re.sub(r"\s+", " ", data_type.strip().upper())
    upper_type = re.sub(r"\s+NOT\s+NULL\b", "", upper_type).strip()
    upper_type = re.sub(r"\s+", "", upper_type)
    aliases = {
        "INTEGER": "INT",
        "BOOLEAN": "BOOLEAN",
        "BOOL": "BOOLEAN",
    }
    return aliases.get(upper_type, upper_type)


def is_nullable_definition(data_type: str) -> bool:
    return "NOT NULL" not in data_type.upper()


def parse_column_specs(columns: list[tuple[str, str]]) -> list[ColumnSpec]:
    return [
        ColumnSpec(
            name=name,
            data_type=normalize_data_type(data_type),
            nullable=is_nullable_definition(data_type),
            raw_definition=data_type,
        )
        for name, data_type in columns
    ]


def spark_schema_column_specs(schema) -> list[ColumnSpec]:
    return [
        ColumnSpec(
            name=field.name,
            data_type=normalize_data_type(field.dataType.simpleString()),
            nullable=field.nullable,
            raw_definition=field.dataType.simpleString(),
        )
        for field in schema.fields
    ]


def compare_table_schema(
    expected_columns: list[ColumnSpec],
    actual_columns: list[ColumnSpec],
) -> SchemaDrift:
    expected_by_name = {column.name: column for column in expected_columns}
    actual_by_name = {column.name: column for column in actual_columns}
    expected_names = [column.name for column in expected_columns]
    actual_names = [column.name for column in actual_columns]

    missing_columns = [
        column
        for column in expected_columns
        if column.name not in actual_by_name
    ]
    extra_columns = [
        name for name in actual_names if name not in expected_by_name
    ]
    common_expected_names = [
        name for name in expected_names if name in actual_by_name
    ]
    common_actual_names = [
        name for name in actual_names if name in expected_by_name
    ]
    order_drift = common_expected_names != common_actual_names

    type_drift = []
    nullability_drift = []
    for name in common_expected_names:
        expected = expected_by_name[name]
        actual = actual_by_name[name]
        if expected.data_type != actual.data_type:
            type_drift.append(
                f"{name}: expected {expected.data_type}, found {actual.data_type}"
            )
        if expected.nullable is False and actual.nullable is True:
            nullability_drift.append(
                f"{name}: expected NOT NULL, found nullable"
            )

    return SchemaDrift(
        missing_columns=missing_columns,
        extra_columns=extra_columns,
        order_drift=order_drift,
        type_drift=type_drift,
        nullability_drift=nullability_drift,
    )


def can_use_additive_alter_only(
    expected_columns: list[ColumnSpec],
    actual_columns: list[ColumnSpec],
    drift: SchemaDrift,
) -> bool:
    if drift.extra_columns or drift.order_drift or drift.type_drift:
        return False
    if drift.nullability_drift:
        return False
    if any(
        not can_auto_add_column(column.raw_definition)
        for column in drift.missing_columns
    ):
        return False

    actual_names = [column.name for column in actual_columns]
    missing_names = [column.name for column in drift.missing_columns]
    expected_names = [column.name for column in expected_columns]
    return [*actual_names, *missing_names] == expected_names


def build_repair_create_table_query(
    create_table_sql: str, repair_table: str
) -> str:
    column_start = create_table_sql.find("(")
    if column_start == -1:
        raise ValueError("Could not locate CREATE TABLE column block")
    query = f"CREATE TABLE {repair_table} {create_table_sql[column_start:]}"
    constraint_suffix = repair_table.split(".")[-1].replace("-", "_")

    def suffix_constraint_name(match) -> str:
        name = match.group(1)
        if name.startswith("`") and name.endswith("`"):
            return f"CONSTRAINT `{name.strip('`')}_{constraint_suffix}`"
        return f"CONSTRAINT {name}_{constraint_suffix}"

    return re.sub(
        r"(?i)\bconstraint\s+([`A-Za-z0-9_]+)",
        suffix_constraint_name,
        query,
    )


def sql_repair_data_type(column: ColumnSpec) -> str:
    return re.sub(
        r"(?i)\s+NOT\s+NULL\b",
        "",
        column.raw_definition,
    ).strip()


def sql_select_expression(column: ColumnSpec, actual_names: set[str]) -> str:
    quoted_name = f"`{column.name}`"
    if column.name in actual_names:
        return quoted_name
    if column.name in DEFAULT_COLUMN_VALUES:
        return (
            f"CAST({DEFAULT_COLUMN_VALUES[column.name]} AS "
            f"{sql_repair_data_type(column)}) AS {quoted_name}"
        )
    if column.nullable:
        return f"CAST(NULL AS {sql_repair_data_type(column)}) AS {quoted_name}"
    raise ValueError(
        f"Missing required column {column.name!r} has no repair default"
    )


def build_repair_insert_query(
    repair_table: str,
    backup_table: str,
    expected_columns: list[ColumnSpec],
    actual_columns: list[ColumnSpec],
) -> str:
    actual_names = {column.name for column in actual_columns}
    insert_columns = ", ".join(
        f"`{column.name}`" for column in expected_columns
    )
    select_columns = ", ".join(
        sql_select_expression(column, actual_names)
        for column in expected_columns
    )
    return (
        f"INSERT INTO {repair_table} ({insert_columns}) "
        f"SELECT {select_columns} FROM {backup_table}"
    )


def build_repair_table_statements(
    *,
    table: str,
    create_table_sql: str,
    expected_columns: list[ColumnSpec],
    actual_columns: list[ColumnSpec],
) -> list[str]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    catalog, schema, table_name = table.split(".", maxsplit=2)
    backup_table = f"{catalog}.{schema}.{table_name}__backup_{timestamp}"
    repair_table = f"{catalog}.{schema}.{table_name}__repair_{timestamp}"
    return [
        f"CREATE TABLE {backup_table} AS SELECT * FROM {table}",
        build_repair_create_table_query(create_table_sql, repair_table),
        build_repair_insert_query(
            repair_table,
            backup_table,
            expected_columns,
            actual_columns,
        ),
        f"DROP TABLE {table}",
        f"ALTER TABLE {repair_table} RENAME TO {table}",
    ]


def requires_assignment_build_state_recreation(
    table: str,
    actual_columns: list[ColumnSpec],
) -> bool:
    table_name = table.split(".")[-1]
    is_assignment_build_state = any(
        table_name.endswith(suffix)
        for suffix in ASSIGNMENT_BUILD_STATE_TABLE_SUFFIXES
    )
    if not is_assignment_build_state:
        return False
    actual_names = {column.name for column in actual_columns}
    return bool(ASSIGNMENT_PROVENANCE_COLUMNS - actual_names)


def repair_table_to_contract(
    spark,
    *,
    table: str,
    create_table_sql: str,
    expected_columns: list[ColumnSpec],
    actual_columns: list[ColumnSpec],
    job_env: str,
    dry_run: bool,
    logger,
) -> None:
    if job_env.lower() == "prod":
        raise ValueError(
            f"Table {table} requires rebuild repair, but PROD rebuild repair "
            "is blocked. Use an explicit release/migration route."
        )

    if requires_assignment_build_state_recreation(table, actual_columns):
        raise ValueError(
            f"Table {table} requires explicit targeted recreation to add "
            "assignment provenance without backup-copying transient build "
            "state. Run recreate_tables for assignments_build_staging, "
            "assignments_v2_build_staging, and assignment_build_events only."
        )

    unsupported_missing = [
        column.name
        for column in expected_columns
        if column.name not in {actual.name for actual in actual_columns}
        and not column.nullable
        and column.name not in DEFAULT_COLUMN_VALUES
    ]
    if unsupported_missing:
        raise ValueError(
            f"Table {table} is missing required columns with no repair default: "
            + ", ".join(unsupported_missing)
        )

    statements = build_repair_table_statements(
        table=table,
        create_table_sql=create_table_sql,
        expected_columns=expected_columns,
        actual_columns=actual_columns,
    )
    logger.warning("Rebuilding %s to match SQL contract", table)
    for statement in statements:
        logger.info("Planned repair SQL: %s", statement)

    if dry_run:
        logger.info(
            "Dry run enabled; not executing rebuild repair for %s", table
        )
        return

    original_count = spark.table(table).count()
    for statement in statements:
        logger.info("Running: %s", statement)
        spark.sql(statement)

    repaired_count = spark.table(table).count()
    if repaired_count != original_count:
        raise ValueError(
            f"Repair row count mismatch for {table}: "
            f"before={original_count}, after={repaired_count}"
        )

    repaired_columns = spark_schema_column_specs(spark.table(table).schema)
    repaired_drift = compare_table_schema(expected_columns, repaired_columns)
    if repaired_drift.has_drift:
        raise ValueError(
            f"Repair did not produce expected schema for {table}: "
            f"{repaired_drift.summary()}"
        )


def table_matches_selection(
    table_ref: str, table: str, selected_tables: set[str]
) -> bool:
    if not selected_tables:
        return True
    table_name = table.split(".")[-1]
    return (
        table_ref in selected_tables
        or table in selected_tables
        or table_name in selected_tables
    )


def can_auto_add_column(data_type: str) -> bool:
    """Return whether a column definition is safe for additive auto-alter."""
    upper_data_type = data_type.upper()
    return "<" not in data_type and "NOT NULL" not in upper_data_type


def get_unsupported_missing_columns(
    expected_columns: list[tuple[str, str]],
    actual_columns: list[str],
) -> list[str]:
    """Return missing columns that should not be auto-added."""
    actual_column_set = set(actual_columns)
    return [
        name
        for name, data_type in expected_columns
        if name not in actual_column_set and not can_auto_add_column(data_type)
    ]


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
        if name not in actual_column_set and can_auto_add_column(data_type)
    ]

    if not missing_columns:
        return None

    columns_sql = ", ".join(
        f"`{name}` {data_type}" for name, data_type in missing_columns
    )
    return f"ALTER TABLE {table} ADD COLUMNS ({columns_sql})"


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    DROP_TABLES=False,
    ALTER_TABLES=False,
    ALLOW_NON_DEV_DROP=False,
    ALLOW_NON_DEV_ALTER=False,
    DRY_RUN=False,
    TABLES="",
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()

    logger.info(f"Running in job environment: {JOB_ENV}")
    if ALTER_TABLES and JOB_ENV.lower() != "dev" and not ALLOW_NON_DEV_ALTER:
        raise ValueError("--altertables is only supported for dev table setup")

    if not CLIENT:
        assert JOB_ENV.lower() == "dev", (
            f"Client must be specified when running in {JOB_ENV}"
        )
        CLIENT = "next_uk"  # Client can be specified for interactive debugging
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    logger.info(f"Configuring run for client: {CLIENT}")
    selected_tables = set(
        table.strip() for table in TABLES.split(",") if table.strip()
    )
    if selected_tables:
        logger.info(
            "Restricting table operation to: %s", ", ".join(selected_tables)
        )

    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    logger.info("Loaded configuration from Dynaconf settings")

    tbls_write = config.get("tables_write", {})
    write_catalog = config.get("catalog_write", "marketingdata_dev")
    schema_write = config.get("schema_write", "ds_sandbox")

    if not tbls_write:
        raise ValueError("tables_write not found in Dynaconf config")

    tbls = extract_table_paths(tbls_write)

    if not tbls:
        raise ValueError("No table paths found in tables_write config")

    logger.info(
        f"Using Dynaconf tables config with write_catalog={write_catalog}, schema_write={schema_write}"
    )
    logger.info(f"Extracted {len(tbls)} table definitions from config")

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
    # Tables are already resolved from Dynaconf, but SQL contract files still
    # use placeholders for reusable schema/client/catalog substitution.
    tbl_args = {
        "schema": schema_write,
        "client": CLIENT,
        "catalog": write_catalog,
    }

    resolved_tbls = {}
    for table_ref in tbls:
        table = tbls[table_ref]

        if table_matches_selection(table_ref, table, selected_tables):
            resolved_tbls[table_ref] = table

    if selected_tables and not resolved_tbls:
        raise ValueError(
            "No configured tables matched --tables selection: "
            + ", ".join(selected_tables)
        )

    # Check for missing SQL scripts before proceeding
    missing_scripts = []
    for table_ref in resolved_tbls:
        sql_script_path = resolve_sql_contract_path(table_ref)
        if not sql_script_path.exists():
            missing_scripts.append(str(sql_script_path))

    if missing_scripts:
        raise ValueError(
            f"Missing SQL create scripts: {', '.join(missing_scripts)}"
        )

    for table_ref, table in resolved_tbls.items():
        if DROP_TABLES and (JOB_ENV.lower() == "dev" or ALLOW_NON_DEV_DROP):
            logger.info(f"Dropping table {table} as --droptables is 'True'")
            logger.info(f"Running drop table if exists {table}")
            if DRY_RUN:
                logger.info(
                    "Dry run enabled; not executing drop for %s", table
                )
            else:
                spark.sql(f"drop table if exists {table}")

        logger.info(f"Checking existence of table {table}")
        # replace . with "_" for nested dynaconf table refs
        with open(resolve_sql_contract_path(table_ref)) as f:
            query = etl.map_tbl("".join(f.readlines()), **tbl_args)

        if spark.catalog.tableExists(table):
            if not ALTER_TABLES:
                logger.debug(f"Table {table} already exists - skipping")
                continue

            logger.info(f"Checking {table} against SQL contract")
            expected_columns = parse_column_specs(
                extract_create_table_columns(query)
            )
            actual_columns = spark_schema_column_specs(
                spark.table(table).schema
            )
            drift = compare_table_schema(expected_columns, actual_columns)
            if not drift.has_drift:
                logger.info(f"Table {table} matches SQL contract")
                continue

            logger.warning(
                "Table %s schema drift detected: %s", table, drift.summary()
            )
            if can_use_additive_alter_only(
                expected_columns, actual_columns, drift
            ):
                add_columns = [
                    (column.name, column.raw_definition)
                    for column in drift.missing_columns
                ]
                alter_query = build_add_missing_columns_query(
                    table,
                    add_columns,
                    [column.name for column in actual_columns],
                )
                if alter_query:
                    logger.info(f"Adding missing columns to {table}")
                    logger.info(f"Running: {alter_query}")
                    if DRY_RUN:
                        logger.info(
                            "Dry run enabled; not executing additive alter for %s",
                            table,
                        )
                    else:
                        spark.sql(alter_query)

                    if not DRY_RUN:
                        refreshed_columns = spark_schema_column_specs(
                            spark.table(table).schema
                        )
                        refreshed_drift = compare_table_schema(
                            expected_columns,
                            refreshed_columns,
                        )
                        if refreshed_drift.has_drift:
                            repair_table_to_contract(
                                spark,
                                table=table,
                                create_table_sql=query,
                                expected_columns=expected_columns,
                                actual_columns=refreshed_columns,
                                job_env=JOB_ENV,
                                dry_run=DRY_RUN,
                                logger=logger,
                            )
                continue

            repair_table_to_contract(
                spark,
                table=table,
                create_table_sql=query,
                expected_columns=expected_columns,
                actual_columns=actual_columns,
                job_env=JOB_ENV,
                dry_run=DRY_RUN,
                logger=logger,
            )
            continue

        logger.info(f"Creating {table_ref} table as: {table}")
        logger.info(f"Running: {query}")
        if DRY_RUN:
            logger.info("Dry run enabled; not creating %s", table)
        else:
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
    DRY_RUN = jobparser.get_typed_arg("--dry_run", bool)
    TABLES = jobparser.get_arg("--tables")
    main(
        JOB_ENV,
        CLIENT,
        LOG_LEVEL,
        DROP_TABLES,
        ALTER_TABLES,
        DRY_RUN=DRY_RUN,
        TABLES=TABLES,
    )

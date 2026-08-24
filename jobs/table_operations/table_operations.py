from __future__ import annotations

import argparse
import importlib
import logging
import sys
from pathlib import Path

from databricks.connect import DatabricksSession

SUPPORTED_OPERATIONS = {
    "drop_tables",
    "create_missing_tables",
    "alter_tables",
    "recreate_tables",
    "copy_prod_tables_to_dev",
}

ACTION_FLAGS = {
    "run_create_missing_tables": "create_missing_tables",
    "run_alter_tables": "alter_tables",
    "run_recreate_tables": "recreate_tables",
    "run_drop_tables": "drop_tables",
    "run_copy_prod_tables_to_dev": "copy_prod_tables_to_dev",
}


def resolve_project_root() -> Path:
    try:
        return Path(__file__).resolve().parents[2]
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
        return Path(notebook_path).parents[2]


def bootstrap_project_imports() -> None:
    project_root = resolve_project_root()
    src_root = project_root / "src"
    if src_root.exists() and str(src_root) not in sys.path:
        sys.path.insert(0, str(src_root))
    if str(project_root) not in sys.path:
        sys.path.insert(1, str(project_root))


def load_create_tables_module():
    bootstrap_project_imports()
    return importlib.import_module("jobs.table_operations.create_tables")


def load_mirror_prod_tables_module():
    bootstrap_project_imports()
    return importlib.import_module(
        "jobs.table_operations.mirror_prod_tables_in_dev"
    )


def parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value

    normalised = value.strip().lower()
    if normalised in {"true", "1", "yes", "y"}:
        return True
    if normalised in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")


def resolve_operation_from_flags(
    *,
    operation: str | None = None,
    **action_flags: str | bool | None,
) -> str:
    supplied_flags = {
        name: value
        for name, value in action_flags.items()
        if value is not None
    }
    selected = [
        ACTION_FLAGS[name]
        for name, value in supplied_flags.items()
        if parse_bool(value)
    ]

    if len(selected) > 1:
        raise ValueError(
            "Exactly one run_* action parameter must be true. Selected: "
            + ", ".join(selected)
        )
    if selected:
        selected_operation = selected[0]
        if operation and operation != selected_operation:
            raise ValueError(
                "--operation must match the selected run_* action parameter. "
                f"Got operation={operation!r}, selected={selected_operation!r}"
            )
        return selected_operation
    if supplied_flags:
        raise ValueError("Exactly one run_* action parameter must be true")
    if operation:
        return operation
    raise ValueError("No table operation selected")


def quote_identifier(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("Identifier parts must not be empty")
    return f"`{value.replace('`', '``')}`"


def split_table_names(tables: str | None) -> list[str]:
    if not tables:
        return []
    return [table.strip() for table in tables.split(",") if table.strip()]


def resolve_table_name(
    table: str, catalog: str, schema: str
) -> tuple[str, str, str]:
    if "*" in table or "?" in table:
        raise ValueError(f"Wildcard table names are not supported: {table!r}")

    parts = [part.strip().strip("`") for part in table.split(".")]
    if any(not part for part in parts):
        raise ValueError(f"Invalid table name: {table!r}")

    if len(parts) == 1:
        return catalog, schema, parts[0]

    if len(parts) != 3:
        raise ValueError(
            "Tables must be unqualified or fully qualified as catalog.schema.table: "
            f"{table!r}"
        )

    table_catalog, table_schema, table_name = parts
    if table_catalog != catalog or table_schema != schema:
        raise ValueError(
            "Fully qualified table names must match --catalog and --schema. "
            f"Got {table_catalog}.{table_schema}, expected {catalog}.{schema}"
        )
    return table_catalog, table_schema, table_name


def qualified_table_name(table_parts: tuple[str, str, str]) -> str:
    return ".".join(quote_identifier(part) for part in table_parts)


def build_drop_table_statement(table_parts: tuple[str, str, str]) -> str:
    return f"DROP TABLE IF EXISTS {qualified_table_name(table_parts)}"


def drop_tables(
    spark,
    *,
    catalog: str,
    schema: str,
    tables: str | None,
    confirm_destructive: bool,
    dry_run: bool,
    logger: logging.Logger | None = None,
) -> list[str]:
    logger = logger or logging.getLogger(__name__)
    table_names = split_table_names(tables)

    if not dry_run and not table_names:
        raise ValueError(
            "--tables must include at least one table when dry_run=false"
        )
    if not dry_run and not confirm_destructive:
        raise ValueError(
            "--confirm_destructive true is required when dry_run=false"
        )

    statements = []
    for table in table_names:
        table_parts = resolve_table_name(table, catalog, schema)
        statement = build_drop_table_statement(table_parts)
        statements.append(statement)
        logger.info(
            "Resolved table %s to %s", table, qualified_table_name(table_parts)
        )
        logger.info("Prepared statement: %s", statement)
        if dry_run:
            logger.info("Dry run enabled; not executing statement")
            continue
        logger.info("Executing: %s", statement)
        spark.sql(statement)

    if not table_names:
        logger.info("No tables supplied; nothing to do")
    return statements


def run_configured_table_operation(
    *,
    operation: str,
    job_env: str,
    client: str,
    log_level: str,
    tables: str | None,
    confirm_mutating: bool,
    confirm_destructive: bool,
    dry_run: bool,
    logger: logging.Logger | None = None,
) -> list[str]:
    logger = logger or logging.getLogger(__name__)
    if operation not in {
        "create_missing_tables",
        "alter_tables",
        "recreate_tables",
    }:
        raise ValueError(
            f"Unsupported configured table operation: {operation!r}"
        )

    if (
        not dry_run
        and operation in {"create_missing_tables", "alter_tables"}
        and not confirm_mutating
    ):
        raise ValueError(
            "--confirm_mutating true is required for create_missing_tables "
            "and alter_tables when dry_run=false"
        )
    if (
        not dry_run
        and operation == "recreate_tables"
        and not confirm_destructive
    ):
        raise ValueError(
            "--confirm_destructive true is required for recreate_tables "
            "when dry_run=false"
        )

    logger.info(
        "Running %s for client=%s job_env=%s dry_run=%s tables=%s",
        operation,
        client,
        job_env,
        dry_run,
        tables or "<all configured tables>",
    )
    create_tables = load_create_tables_module()
    create_tables.main(
        JOB_ENV=job_env,
        CLIENT=client,
        LOG_LEVEL=log_level,
        DROP_TABLES=operation == "recreate_tables",
        ALTER_TABLES=operation == "alter_tables",
        ALLOW_NON_DEV_DROP=operation == "recreate_tables",
        ALLOW_NON_DEV_ALTER=operation == "alter_tables",
        DRY_RUN=dry_run,
        TABLES=tables or "",
    )
    return []


def copy_prod_tables_to_dev(
    *,
    job_env: str,
    client: str,
    log_level: str,
    confirm_mutating: bool,
    dry_run: bool,
    history_days: int,
    input_tables_only: bool,
    logger: logging.Logger | None = None,
) -> list[str]:
    logger = logger or logging.getLogger(__name__)
    if job_env.lower() != "dev":
        raise ValueError("copy_prod_tables_to_dev only supports job_env=dev")
    if dry_run:
        logger.info(
            "Dry run enabled; would copy PROD tables into DEV for "
            "client=%s history_days=%s input_tables_only=%s",
            client,
            history_days,
            input_tables_only,
        )
        return []
    if not confirm_mutating:
        raise ValueError(
            "--confirm_mutating true is required for copy_prod_tables_to_dev "
            "when dry_run=false"
        )

    mirror_prod_tables = load_mirror_prod_tables_module()
    mirror_prod_tables.main(
        job_env=job_env,
        client=client,
        log_level=log_level,
        history_days=history_days,
        input_tables_only=input_tables_only,
    )
    return []


def create_missing_tables(
    *,
    job_env: str,
    client: str,
    log_level: str,
    confirm_mutating: bool,
    dry_run: bool,
    tables: str | None = None,
    logger: logging.Logger | None = None,
) -> list[str]:
    return run_configured_table_operation(
        operation="create_missing_tables",
        job_env=job_env,
        client=client,
        log_level=log_level,
        tables=tables,
        confirm_mutating=confirm_mutating,
        confirm_destructive=False,
        dry_run=dry_run,
        logger=logger,
    )


def alter_tables(
    *,
    job_env: str,
    client: str,
    log_level: str,
    confirm_mutating: bool,
    dry_run: bool,
    tables: str | None = None,
    logger: logging.Logger | None = None,
) -> list[str]:
    return run_configured_table_operation(
        operation="alter_tables",
        job_env=job_env,
        client=client,
        log_level=log_level,
        tables=tables,
        confirm_mutating=confirm_mutating,
        confirm_destructive=False,
        dry_run=dry_run,
        logger=logger,
    )


def recreate_tables(
    *,
    job_env: str,
    client: str,
    log_level: str,
    confirm_destructive: bool,
    dry_run: bool,
    tables: str | None = None,
    logger: logging.Logger | None = None,
) -> list[str]:
    return run_configured_table_operation(
        operation="recreate_tables",
        job_env=job_env,
        client=client,
        log_level=log_level,
        tables=tables,
        confirm_mutating=False,
        confirm_destructive=confirm_destructive,
        dry_run=dry_run,
        logger=logger,
    )


def run_operation(
    spark,
    *,
    operation: str | None,
    job_env: str,
    client: str,
    catalog: str,
    schema: str,
    tables: str | None,
    confirm_mutating: bool,
    confirm_destructive: bool,
    dry_run: bool,
    history_days: int = 1,
    input_tables_only: bool = True,
    run_create_missing_tables: str | bool | None = None,
    run_alter_tables: str | bool | None = None,
    run_recreate_tables: str | bool | None = None,
    run_drop_tables: str | bool | None = None,
    run_copy_prod_tables_to_dev: str | bool | None = None,
    log_level: str = "INFO",
    logger: logging.Logger | None = None,
) -> list[str]:
    operation = resolve_operation_from_flags(
        operation=operation,
        run_create_missing_tables=run_create_missing_tables,
        run_alter_tables=run_alter_tables,
        run_recreate_tables=run_recreate_tables,
        run_drop_tables=run_drop_tables,
        run_copy_prod_tables_to_dev=run_copy_prod_tables_to_dev,
    )
    if operation not in SUPPORTED_OPERATIONS:
        raise ValueError(
            f"Unsupported operation {operation!r}; expected one of "
            f"{sorted(SUPPORTED_OPERATIONS)}"
        )

    if operation == "drop_tables":
        if not catalog or not schema:
            raise ValueError(
                "--catalog and --schema are required for drop_tables"
            )
        return drop_tables(
            spark,
            catalog=catalog,
            schema=schema,
            tables=tables,
            confirm_destructive=confirm_destructive,
            dry_run=dry_run,
            logger=logger,
        )

    if operation == "copy_prod_tables_to_dev":
        return copy_prod_tables_to_dev(
            job_env=job_env,
            client=client,
            log_level=log_level,
            confirm_mutating=confirm_mutating,
            dry_run=dry_run,
            history_days=history_days,
            input_tables_only=input_tables_only,
            logger=logger,
        )

    return run_configured_table_operation(
        operation=operation,
        job_env=job_env,
        client=client,
        log_level=log_level,
        tables=tables,
        confirm_mutating=confirm_mutating,
        confirm_destructive=confirm_destructive,
        dry_run=dry_run,
        logger=logger,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run manual Next Ads table maintenance operations."
    )
    parser.add_argument(
        "--operation",
        default=None,
        choices=sorted(SUPPORTED_OPERATIONS),
    )
    parser.add_argument("--job_env", default="dev")
    parser.add_argument("--client", default="next_uk")
    parser.add_argument("--catalog", default="")
    parser.add_argument("--schema", default="")
    parser.add_argument("--tables", default="")
    parser.add_argument("--confirm_mutating", default="false")
    parser.add_argument("--confirm_destructive", default="false")
    parser.add_argument("--dry_run", default="true")
    parser.add_argument("--history_days", default="1")
    parser.add_argument("--input_tables_only", default="true")
    parser.add_argument("--run_create_missing_tables", default=None)
    parser.add_argument("--run_alter_tables", default=None)
    parser.add_argument("--run_recreate_tables", default=None)
    parser.add_argument("--run_drop_tables", default=None)
    parser.add_argument("--run_copy_prod_tables_to_dev", default=None)
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    logger = logging.getLogger(__name__)
    spark = DatabricksSession.builder.getOrCreate()

    statements = run_operation(
        spark,
        operation=args.operation,
        job_env=args.job_env,
        client=args.client,
        catalog=args.catalog,
        schema=args.schema,
        tables=args.tables,
        confirm_mutating=parse_bool(args.confirm_mutating),
        confirm_destructive=parse_bool(args.confirm_destructive),
        dry_run=parse_bool(args.dry_run),
        history_days=int(args.history_days),
        input_tables_only=parse_bool(args.input_tables_only),
        run_create_missing_tables=args.run_create_missing_tables,
        run_alter_tables=args.run_alter_tables,
        run_recreate_tables=args.run_recreate_tables,
        run_drop_tables=args.run_drop_tables,
        run_copy_prod_tables_to_dev=args.run_copy_prod_tables_to_dev,
        log_level=args.log_level,
        logger=logger,
    )
    logger.info("Prepared %s table operation statements", len(statements))


if __name__ == "__main__":
    main()

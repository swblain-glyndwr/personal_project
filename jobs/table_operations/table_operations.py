from __future__ import annotations

import argparse
import importlib
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

SUPPORTED_OPERATIONS = {
    "drop_tables",
    "create_missing_tables",
    "alter_tables",
    "recreate_tables",
}


def load_create_tables_module():
    return importlib.import_module("scripts.table_operations.create_tables")


def parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value

    normalised = value.strip().lower()
    if normalised in {"true", "1", "yes", "y"}:
        return True
    if normalised in {"false", "0", "no", "n"}:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")


def quote_identifier(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("Identifier parts must not be empty")
    return f"`{value.replace('`', '``')}`"


def split_table_names(tables: str | None) -> list[str]:
    if not tables:
        return []
    return [table.strip() for table in tables.split(",") if table.strip()]


def resolve_table_name(table: str, catalog: str, schema: str) -> tuple[str, str, str]:
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
        raise ValueError("--tables must include at least one table when dry_run=false")
    if not dry_run and not confirm_destructive:
        raise ValueError(
            "--confirm_destructive true is required when dry_run=false"
        )

    statements = []
    for table in table_names:
        table_parts = resolve_table_name(table, catalog, schema)
        statement = build_drop_table_statement(table_parts)
        statements.append(statement)
        logger.info("Resolved table %s to %s", table, qualified_table_name(table_parts))
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
    confirm_mutating: bool,
    confirm_destructive: bool,
    dry_run: bool,
    logger: logging.Logger | None = None,
) -> list[str]:
    logger = logger or logging.getLogger(__name__)
    if operation not in {"create_missing_tables", "alter_tables", "recreate_tables"}:
        raise ValueError(f"Unsupported configured table operation: {operation!r}")

    if dry_run:
        logger.info(
            "Dry run enabled; would run %s for client=%s job_env=%s",
            operation,
            client,
            job_env,
        )
        return []

    if operation in {"create_missing_tables", "alter_tables"} and not confirm_mutating:
        raise ValueError(
            "--confirm_mutating true is required for create_missing_tables "
            "and alter_tables when dry_run=false"
        )
    if operation == "recreate_tables" and not confirm_destructive:
        raise ValueError(
            "--confirm_destructive true is required for recreate_tables "
            "when dry_run=false"
        )

    logger.info("Running %s for client=%s job_env=%s", operation, client, job_env)
    create_tables = load_create_tables_module()
    create_tables.main(
        JOB_ENV=job_env,
        CLIENT=client,
        LOG_LEVEL=log_level,
        DROP_TABLES=operation == "recreate_tables",
        ALTER_TABLES=operation == "alter_tables",
        ALLOW_NON_DEV_DROP=operation == "recreate_tables",
        ALLOW_NON_DEV_ALTER=operation == "alter_tables",
    )
    return []


def create_missing_tables(
    *,
    job_env: str,
    client: str,
    log_level: str,
    confirm_mutating: bool,
    dry_run: bool,
    logger: logging.Logger | None = None,
) -> list[str]:
    return run_configured_table_operation(
        operation="create_missing_tables",
        job_env=job_env,
        client=client,
        log_level=log_level,
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
    logger: logging.Logger | None = None,
) -> list[str]:
    return run_configured_table_operation(
        operation="alter_tables",
        job_env=job_env,
        client=client,
        log_level=log_level,
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
    logger: logging.Logger | None = None,
) -> list[str]:
    return run_configured_table_operation(
        operation="recreate_tables",
        job_env=job_env,
        client=client,
        log_level=log_level,
        confirm_mutating=False,
        confirm_destructive=confirm_destructive,
        dry_run=dry_run,
        logger=logger,
    )


def run_operation(
    spark,
    *,
    operation: str,
    job_env: str,
    client: str,
    catalog: str,
    schema: str,
    tables: str | None,
    confirm_mutating: bool,
    confirm_destructive: bool,
    dry_run: bool,
    log_level: str = "INFO",
    logger: logging.Logger | None = None,
) -> list[str]:
    if operation not in SUPPORTED_OPERATIONS:
        raise ValueError(
            f"Unsupported operation {operation!r}; expected one of "
            f"{sorted(SUPPORTED_OPERATIONS)}"
        )

    if operation == "drop_tables":
        if not catalog or not schema:
            raise ValueError("--catalog and --schema are required for drop_tables")
        return drop_tables(
            spark,
            catalog=catalog,
            schema=schema,
            tables=tables,
            confirm_destructive=confirm_destructive,
            dry_run=dry_run,
            logger=logger,
        )

    return run_configured_table_operation(
        operation=operation,
        job_env=job_env,
        client=client,
        log_level=log_level,
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
        default="create_missing_tables",
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
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    logger = logging.getLogger(__name__)
    spark = SparkSession.builder.getOrCreate()

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
        log_level=args.log_level,
        logger=logger,
    )
    logger.info("Prepared %s table operation statements", len(statements))


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from pyspark.sql import SparkSession


def resolve_project_root() -> Path:
    """Resolve the synced repository root in local and Databricks runs."""
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


PROJECT_ROOT = resolve_project_root()
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(1, str(PROJECT_ROOT))

from next_ads.common.job_logging import configure_job_logging


DEV_JOB_ENV = "dev"
DEV_CATALOG = "marketingdata_dev"
DEV_SERVICE_PRINCIPAL = "7ecc733a-4b66-4783-b984-985333d55c38"
DEV_RELATION_SCOPE = "ALL_SP_OWNED_SCHEMAS"
PROD_JOB_ENV = "prod"
PROD_CATALOG = "marketingdata_prod"
PROD_RELATION_SCOPE = "WAREHOUSE_AND_DS_SANDBOX"
PROD_SCHEMAS = ("warehouse", "ds_sandbox")
PROD_SERVICE_PRINCIPAL = "2be8d1c2-d35b-4438-891e-558b9b5880f6"
ACCESS_RECIPIENTS = (
    "stephen_blain@next.co.uk",
    "claire_wilsonbarnes@next.co.uk",
    "hadi_miah@next.co.uk",
)
TABLE_TYPES = frozenset({"EXTERNAL", "MANAGED"})
READ_ONLY_TYPES = frozenset({"VIEW", "MATERIALIZED_VIEW"})
SUPPORTED_RELATION_TYPES = TABLE_TYPES | READ_ONLY_TYPES
MAX_RELATION_COUNT = 2000
MAXIMUM_GRANTABLE_ACCESS = ("ALL PRIVILEGES", "MANAGE")


@dataclass(frozen=True)
class RuntimeScope:
    job_env: str
    catalog: str
    relation_scope: str
    owner: str


@dataclass(frozen=True, order=True)
class Relation:
    catalog: str
    schema: str
    name: str
    relation_type: str
    owner: str


@dataclass(frozen=True)
class GrantOperation:
    relation: Relation
    principal: str
    securable_type: str
    privileges: tuple[str, ...]

    @property
    def statement(self) -> str:
        privilege_list = ", ".join(self.privileges)
        return (
            f"GRANT {privilege_list} ON {self.securable_type} "
            f"{qualified_name(self.relation)} TO {quote_identifier(self.principal)}"
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


def quote_identifier(value: str) -> str:
    if not value:
        raise ValueError("Identifiers and principal names must not be empty")
    return f"`{value.replace('`', '``')}`"


def qualified_name(relation: Relation) -> str:
    return ".".join(
        quote_identifier(part)
        for part in (relation.catalog, relation.schema, relation.name)
    )


def validate_runtime_scope(
    *, job_env: str, catalog: str, relation_scope: str, expected_owner: str
) -> RuntimeScope:
    scope = RuntimeScope(
        job_env=job_env,
        catalog=catalog,
        relation_scope=relation_scope,
        owner=expected_owner,
    )
    prod_scope = RuntimeScope(
        job_env=PROD_JOB_ENV,
        catalog=PROD_CATALOG,
        relation_scope=PROD_RELATION_SCOPE,
        owner=PROD_SERVICE_PRINCIPAL,
    )
    if scope == prod_scope:
        return scope
    if (
        scope.job_env == DEV_JOB_ENV
        and scope.catalog == DEV_CATALOG
        and scope.relation_scope == DEV_RELATION_SCOPE
        and scope.owner == DEV_SERVICE_PRINCIPAL
    ):
        return scope
    raise ValueError(
        "This job only supports the fixed DEV whole-catalog owner scope or "
        "the fixed PROD warehouse and ds_sandbox owner scope; "
        f"received {scope!r}"
    )


def current_principal(spark) -> str:
    row = spark.sql("SELECT current_user() AS principal").first()
    return str(row["principal"])


def assert_expected_execution_identity(spark, scope: RuntimeScope) -> str:
    principal = current_principal(spark)
    if principal != scope.owner:
        raise RuntimeError(
            "Refusing to continue because the job is not running as the "
            "expected service principal. "
            f"actual={principal!r} expected={scope.owner!r}"
        )
    return principal


def relation_scope_schemas(scope: RuntimeScope) -> frozenset[str] | None:
    if scope.relation_scope == DEV_RELATION_SCOPE:
        return None
    if scope.relation_scope == PROD_RELATION_SCOPE:
        return frozenset(PROD_SCHEMAS)
    raise ValueError(f"Unsupported relation scope: {scope.relation_scope!r}")


def owned_relations_query(scope: RuntimeScope) -> str:
    schema_filter = ""
    allowed_schemas = relation_scope_schemas(scope)
    if allowed_schemas is not None:
        schema_literals = ", ".join(f"'{schema}'" for schema in PROD_SCHEMAS)
        schema_filter = f"\n          AND table_schema IN ({schema_literals})"
    return f"""
        SELECT table_catalog, table_schema, table_name, table_type, table_owner
        FROM {quote_identifier(scope.catalog)}.information_schema.tables
        WHERE table_catalog = '{scope.catalog}'
          AND table_owner = current_user(){schema_filter}
        ORDER BY table_schema, table_name, table_type
    """.strip()


def _relation_from_row(row) -> Relation:
    return Relation(
        catalog=str(row["table_catalog"]),
        schema=str(row["table_schema"]),
        name=str(row["table_name"]),
        relation_type=str(row["table_type"]).upper(),
        owner=str(row["table_owner"]),
    )


def discover_owned_relations(spark, scope: RuntimeScope) -> list[Relation]:
    relations = sorted(
        _relation_from_row(row)
        for row in spark.sql(owned_relations_query(scope)).collect()
    )
    if not relations:
        allowed_schemas = relation_scope_schemas(scope)
        scope_name = scope.catalog
        if allowed_schemas is not None:
            scope_name += ".{" + ",".join(PROD_SCHEMAS) + "}"
        raise RuntimeError(
            "No service-principal-owned relations were found in the approved "
            f"scope {scope_name}; "
            "refusing to continue"
        )
    if len(relations) > MAX_RELATION_COUNT:
        raise RuntimeError(
            f"Unexpectedly large relation scope ({len(relations)}); "
            "refusing to continue"
        )

    seen_names: set[tuple[str, str, str]] = set()
    allowed_schemas = relation_scope_schemas(scope)
    for relation in relations:
        if relation.catalog != scope.catalog:
            raise RuntimeError(f"Unexpected relation namespace: {relation!r}")
        if (
            allowed_schemas is not None
            and relation.schema not in allowed_schemas
        ):
            raise RuntimeError(f"Unexpected relation namespace: {relation!r}")
        if relation.owner != scope.owner:
            raise RuntimeError(f"Unexpected relation owner: {relation!r}")
        if relation.relation_type not in SUPPORTED_RELATION_TYPES:
            raise RuntimeError(f"Unsupported relation type: {relation!r}")
        relation_key = (relation.catalog, relation.schema, relation.name)
        if relation_key in seen_names:
            raise RuntimeError(f"Duplicate relation discovered: {relation!r}")
        seen_names.add(relation_key)
    return relations


def relation_grant_policy(relation_type: str) -> tuple[str, tuple[str, ...]]:
    relation_type = relation_type.upper()
    if relation_type in TABLE_TYPES:
        return "TABLE", MAXIMUM_GRANTABLE_ACCESS
    if relation_type == "VIEW":
        return "VIEW", MAXIMUM_GRANTABLE_ACCESS
    if relation_type == "MATERIALIZED_VIEW":
        return "MATERIALIZED VIEW", MAXIMUM_GRANTABLE_ACCESS
    raise ValueError(f"Unsupported relation type: {relation_type!r}")


def build_grant_plan(
    relations: Iterable[Relation],
    principals: Sequence[str] = ACCESS_RECIPIENTS,
) -> list[GrantOperation]:
    operations = []
    for relation in sorted(relations):
        securable_type, grant_clauses = relation_grant_policy(
            relation.relation_type
        )
        for principal in principals:
            for grant_clause in grant_clauses:
                operations.append(
                    GrantOperation(
                        relation=relation,
                        principal=principal,
                        securable_type=securable_type,
                        privileges=(grant_clause,),
                    )
                )
    return operations


def show_grants_statement(relation: Relation) -> str:
    securable_type, _ = relation_grant_policy(relation.relation_type)
    return f"SHOW GRANTS ON {securable_type} {qualified_name(relation)}"


def verify_expected_grants(
    spark,
    relations: Sequence[Relation],
    principals: Sequence[str] = ACCESS_RECIPIENTS,
) -> None:
    missing = []
    for relation in relations:
        _, expected_privileges = relation_grant_policy(relation.relation_type)
        actual = {
            (str(row["principal"]), str(row["actionType"]).upper())
            for row in spark.sql(show_grants_statement(relation)).collect()
        }
        for principal in principals:
            for privilege in expected_privileges:
                if (principal, privilege) not in actual:
                    missing.append(
                        {
                            "relation": qualified_name(relation),
                            "principal": principal,
                            "privilege": privilege,
                        }
                    )
    if missing:
        raise RuntimeError(
            "Grant verification failed: " + json.dumps(missing, sort_keys=True)
        )


def reconcile_access(
    spark,
    *,
    scope: RuntimeScope,
    confirm_mutating: bool,
    dry_run: bool,
    logger: logging.Logger | None = None,
) -> dict[str, object]:
    logger = logger or logging.getLogger(__name__)
    scope = validate_runtime_scope(
        job_env=scope.job_env,
        catalog=scope.catalog,
        relation_scope=scope.relation_scope,
        expected_owner=scope.owner,
    )
    if not dry_run and not confirm_mutating:
        raise ValueError(
            "--confirm_mutating true is required when dry_run=false"
        )

    execution_identity = assert_expected_execution_identity(spark, scope)
    relations = discover_owned_relations(spark, scope)
    operations = build_grant_plan(relations)
    type_counts = Counter(relation.relation_type for relation in relations)
    relation_schemas = sorted({relation.schema for relation in relations})
    summary: dict[str, object] = {
        "status": "DRY_RUN" if dry_run else "APPLYING",
        "job_env": scope.job_env,
        "catalog": scope.catalog,
        "relation_scope": scope.relation_scope,
        "schema_count": len(relation_schemas),
        "schemas": relation_schemas,
        "run_as": execution_identity,
        "recipients": list(ACCESS_RECIPIENTS),
        "relation_count": len(relations),
        "statement_count": len(operations),
        "type_counts": dict(sorted(type_counts.items())),
    }
    logger.info(
        "Resolved grant scope: %s", json.dumps(summary, sort_keys=True)
    )

    if dry_run:
        for operation in operations:
            logger.info("DRY_RUN %s", operation.statement)
        return summary

    failures = []
    for operation in operations:
        try:
            logger.info("Executing: %s", operation.statement)
            spark.sql(operation.statement)
        except Exception as exc:  # noqa: BLE001 - report all grant failures
            failures.append(
                {"statement": operation.statement, "error": str(exc)}
            )
            logger.exception("Grant failed: %s", operation.statement)

    if failures:
        raise RuntimeError(
            "One or more grants failed; the job is safe to rerun: "
            + json.dumps(failures, sort_keys=True)
        )

    verify_expected_grants(spark, relations)
    summary["status"] = "SUCCEEDED"
    logger.info(
        "Grant reconciliation complete: %s",
        json.dumps(summary, sort_keys=True),
    )
    return summary


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Grant maximum object access to DEV or PROD "
            "service-principal-owned relations"
        )
    )
    parser.add_argument("--job_env", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--relation_scope", required=True)
    parser.add_argument("--expected_owner", required=True)
    parser.add_argument("--confirm_mutating", required=True, type=parse_bool)
    parser.add_argument("--dry_run", required=True, type=parse_bool)
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> dict[str, object]:
    args = parse_args(argv)
    configure_job_logging(
        args.log_level,
        log_format="%(asctime)s - %(levelname)s - %(message)s",
    )
    scope = validate_runtime_scope(
        job_env=args.job_env,
        catalog=args.catalog,
        relation_scope=args.relation_scope,
        expected_owner=args.expected_owner,
    )
    spark = SparkSession.builder.getOrCreate()
    result = reconcile_access(
        spark,
        scope=scope,
        confirm_mutating=args.confirm_mutating,
        dry_run=args.dry_run,
    )
    print(json.dumps(result, sort_keys=True))
    return result


if __name__ == "__main__":
    main()

"""Validate and publish one complete v1 or v2 assignment build."""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    # __file__ is not defined when running as a Databricks notebook.
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger

from next_ads.common import config_manager
from next_ads.decisioning.assignment_publication import (
    AssignmentColumnContract,
    AssignmentPublicationResult,
    AssignmentScopeContract,
    AssignmentTableContract,
    validate_and_publish_assignment_build,
)


VALID_JOB_ENVS = frozenset({"dev", "preprod", "prod"})
VALID_ROUTES = frozenset({"v1", "v2"})
SCOPE_MANIFEST_FIELDS = frozenset({"scope", "phase", "inherit_basic_from"})

V1_PUBLIC_COLUMNS = (
    "AccountNumber",
    "Location",
    "UniqueAdIDBasic",
    "UniqueAdIDBest",
    "UniqueAdIDBestChallenger",
    "UniqueAdIDNextGenAds",
    "Treatment",
    "UniqueAdIDMeasurement",
    "UniqueAdIDAssigned",
    "MASID",
    "rundate",
)
V2_PUBLIC_COLUMNS = (
    "AccountNumber",
    "PageType",
    "Rank",
    "UniqueAdIDBasic",
    "UniqueAdIDBest",
    "UniqueAdIDBestChallenger",
    "UniqueAdIDNextGenAds",
    "Treatment",
    "UniqueAdIDMeasurement",
    "UniqueAdIDAssigned",
    "TriggerScore",
    "rundate",
)


@dataclass(frozen=True)
class ScopeManifestEntry:
    """One expected assignment scope and its orchestration metadata."""

    scope: str
    phase: str | None = None
    inherit_basic_from: str | None = None


@dataclass(frozen=True)
class PublishBuildArguments:
    """Validated inputs required to publish an assignment build."""

    job_env: str
    client: str
    route: str
    run_date: date
    build_run_id: str
    scope_manifest: tuple[ScopeManifestEntry, ...]
    log_level: str | None = None


def _require_text(value: Any, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value.strip()


def _optional_text(value: Any, *, label: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, label=label)


def parse_scope_manifest_json(
    raw_manifest: Any,
) -> tuple[ScopeManifestEntry, ...]:
    """Parse an ordered, unique list of assignment scope definitions."""
    manifest_json = _require_text(
        raw_manifest,
        label="--scope_manifest_json",
    )
    try:
        parsed = json.loads(manifest_json)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "--scope_manifest_json must contain valid JSON"
        ) from exc

    if not isinstance(parsed, list) or not parsed:
        raise ValueError("--scope_manifest_json must be a non-empty JSON list")

    entries: list[ScopeManifestEntry] = []
    seen_scopes: set[str] = set()
    for index, raw_entry in enumerate(parsed):
        label = f"--scope_manifest_json entry {index}"
        if not isinstance(raw_entry, dict):
            raise ValueError(f"{label} must be a JSON object")

        unexpected_fields = sorted(set(raw_entry) - SCOPE_MANIFEST_FIELDS)
        if unexpected_fields:
            raise ValueError(
                f"{label} contains unsupported fields: "
                + ", ".join(unexpected_fields)
            )

        scope = _require_text(raw_entry.get("scope"), label=f"{label}.scope")
        if scope in seen_scopes:
            raise ValueError(
                f"--scope_manifest_json contains duplicate scope {scope!r}"
            )
        seen_scopes.add(scope)

        entries.append(
            ScopeManifestEntry(
                scope=scope,
                phase=_optional_text(
                    raw_entry.get("phase"),
                    label=f"{label}.phase",
                ),
                inherit_basic_from=_optional_text(
                    raw_entry.get("inherit_basic_from"),
                    label=f"{label}.inherit_basic_from",
                ),
            )
        )
    return tuple(entries)


def parse_publish_build_arguments(jobparser: Any) -> PublishBuildArguments:
    """Read and validate publisher arguments from the shared job parser."""
    job_env = _require_text(
        jobparser.get_arg("--job_env"),
        label="--job_env",
    ).lower()
    if job_env not in VALID_JOB_ENVS:
        raise ValueError(
            f"--job_env must be one of: {', '.join(sorted(VALID_JOB_ENVS))}"
        )

    client = _require_text(
        jobparser.get_arg("--client"),
        label="--client",
    )
    route = _require_text(
        jobparser.get_arg("--route"),
        label="--route",
    ).lower()
    if route not in VALID_ROUTES:
        raise ValueError(
            f"--route must be one of: {', '.join(sorted(VALID_ROUTES))}"
        )

    raw_run_date = _require_text(
        jobparser.get_arg("--run_date"),
        label="--run_date",
    )
    try:
        run_date = date.fromisoformat(raw_run_date)
    except ValueError as exc:
        raise ValueError("--run_date must use ISO format YYYY-MM-DD") from exc

    build_run_id = _require_text(
        jobparser.get_arg("--build_run_id"),
        label="--build_run_id",
    )
    expected_prefix = f"{route}_"
    if (
        not build_run_id.startswith(expected_prefix)
        or not build_run_id[len(expected_prefix) :]
    ):
        raise ValueError(
            f"--build_run_id must start with {expected_prefix!r} "
            "and include a run identifier"
        )

    log_level = _optional_text(
        jobparser.get_arg("--log_level"),
        label="--log_level",
    )
    return PublishBuildArguments(
        job_env=job_env,
        client=client,
        route=route,
        run_date=run_date,
        build_run_id=build_run_id,
        scope_manifest=parse_scope_manifest_json(
            jobparser.get_arg("--scope_manifest_json")
        ),
        log_level=log_level,
    )


def _get_mapping_value(mapping: Any, key: str) -> Any:
    if isinstance(mapping, Mapping):
        return mapping.get(key)
    return getattr(mapping, key, None)


def resolve_assignment_tables(
    config: Any,
    route: str,
) -> AssignmentTableContract:
    """Resolve the configured internal and public tables for one route."""
    if route not in VALID_ROUTES:
        raise ValueError(
            f"route must be one of: {', '.join(sorted(VALID_ROUTES))}"
        )

    tables_write = getattr(config, "tables_write", None)
    if tables_write is None:
        raise ValueError("Configuration is missing tables_write")

    table_keys = {
        "v1": {
            "staging_table": "assignments_build_staging",
            "history_table": "assignments",
            "latest_table": "assignments_latest",
        },
        "v2": {
            "staging_table": "assignments_v2_build_staging",
            "history_table": "assignments_v2",
            "latest_table": "assignments_v2_latest",
        },
    }[route]
    table_keys["event_table"] = "assignment_build_events"

    resolved: dict[str, str] = {}
    for contract_field, config_key in table_keys.items():
        resolved[contract_field] = _require_text(
            _get_mapping_value(tables_write, config_key),
            label=f"tables_write.{config_key}",
        )
    return AssignmentTableContract(**resolved)


def build_assignment_scope_contract(
    route: str,
    scope_manifest: Sequence[ScopeManifestEntry],
) -> AssignmentScopeContract:
    """Construct the exact public assignment contract for one route."""
    expected_scopes = tuple(entry.scope for entry in scope_manifest)
    if route == "v1":
        return AssignmentScopeContract(
            route="v1",
            scope_column="Location",
            expected_scopes=expected_scopes,
            key_columns=("AccountNumber", "Location"),
            public_columns=V1_PUBLIC_COLUMNS,
        )
    if route == "v2":
        return AssignmentScopeContract(
            route="v2",
            scope_column="PageType",
            expected_scopes=expected_scopes,
            key_columns=("AccountNumber", "PageType", "Rank"),
            public_columns=V2_PUBLIC_COLUMNS,
        )
    raise ValueError(
        f"route must be one of: {', '.join(sorted(VALID_ROUTES))}"
    )


def validate_configured_scope_manifest(
    config: Any,
    route: str,
    scope_manifest: Sequence[ScopeManifestEntry],
) -> None:
    """Reject a v2 manifest that does not match configured page types."""
    if route != "v2":
        return

    configured_page_types = getattr(config, "page_types", None)
    if configured_page_types is None or not hasattr(
        configured_page_types,
        "keys",
    ):
        raise ValueError("Configuration is missing page_types")

    expected_scopes = tuple(configured_page_types.keys())
    actual_scopes = tuple(entry.scope for entry in scope_manifest)
    if actual_scopes != expected_scopes:
        raise ValueError(
            "v2 scope manifest must exactly match configured page types: "
            + ", ".join(expected_scopes)
        )


def publish_assignment_build(
    spark: Any,
    config: Any,
    args: PublishBuildArguments,
) -> AssignmentPublicationResult:
    """Resolve contracts and invoke the complete-build publisher."""
    validate_configured_scope_manifest(
        config,
        args.route,
        args.scope_manifest,
    )
    return validate_and_publish_assignment_build(
        spark,
        tables=resolve_assignment_tables(config, args.route),
        columns=AssignmentColumnContract(),
        scope_contract=build_assignment_scope_contract(
            args.route,
            args.scope_manifest,
        ),
        build_run_id=args.build_run_id,
        build_date=args.run_date,
    )


def main() -> None:
    """Parse the job invocation and publish one complete assignment build."""
    jobparser = get_job_parser()
    jobparser._parse_args()
    args = parse_publish_build_arguments(jobparser)
    if args.log_level:
        configure_logging(log_level=args.log_level)
    else:
        configure_logging()
    logger = get_logger(__name__)

    logger.info(
        "Publishing %s assignment build %s for %s",
        args.route,
        args.build_run_id,
        args.run_date.isoformat(),
    )
    config = config_manager.load_config(args.job_env, client=args.client)
    result = publish_assignment_build(configure_spark(), config, args)
    logger.info(
        "Published %s assignment build %s: %s rows across %s scopes",
        result.route,
        result.build_run_id,
        result.row_count,
        len(result.events),
    )


if __name__ == "__main__":
    main()

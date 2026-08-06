from __future__ import annotations

import json
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from next_ads.candidates.foundation import (
    ACCEPTED_FOUNDATION_STATUSES,
    CANDIDATE_FOUNDATION_CONTRACT_VERSION,
    FALLBACK_PREVIOUS,
    parse_run_date,
)
from next_ads.common.delta_writes import (
    replace_scope_by_name,
    typed_table_frame,
    validate_typed_table_schema,
)
from next_ads.ranking.scoring_inputs import read_delta_version
from next_ads.candidates.foundation import schema_checksum


MAX_FALLBACK_AGE = timedelta(hours=24)


@dataclass(frozen=True)
class CandidateFoundationBuild:
    snapshot_id: str
    attempt_id: str
    run_date: date
    contract_version: str
    source_bindings_json: str
    output_bindings_json: str
    warning_count: int
    status: str
    fallback_source_snapshot_id: str | None
    fallback_source_run_date: date | None
    task_run_id: int
    execution_count: int
    git_commit: str
    completed_at: datetime


@dataclass(frozen=True)
class CandidateFoundationSelection:
    snapshot_id: str
    selection_status: str
    source_run_date: date
    output_bindings: Mapping[str, Mapping[str, Any]]


class CandidateFoundationNotReadyError(ValueError):
    """Raised when no accepted shared candidate foundation is available."""


BUILD_COLUMNS = (
    "CandidateFoundationSnapshotID",
    "CandidateFoundationAttemptID",
    "RunDate",
    "ContractVersion",
    "SourceBindingsJSON",
    "OutputBindingsJSON",
    "WarningCount",
    "Status",
    "FallbackSourceSnapshotID",
    "FallbackSourceRunDate",
    "TaskRunID",
    "ExecutionCount",
    "GitCommit",
    "CompletedAt",
)

BUILD_SCHEMA = StructType(
    [
        StructField("CandidateFoundationSnapshotID", StringType(), False),
        StructField("CandidateFoundationAttemptID", StringType(), False),
        StructField("RunDate", DateType(), False),
        StructField("ContractVersion", StringType(), False),
        StructField("SourceBindingsJSON", StringType(), False),
        StructField("OutputBindingsJSON", StringType(), False),
        StructField("WarningCount", LongType(), False),
        StructField("Status", StringType(), False),
        StructField("FallbackSourceSnapshotID", StringType(), True),
        StructField("FallbackSourceRunDate", DateType(), True),
        StructField("TaskRunID", LongType(), False),
        StructField("ExecutionCount", IntegerType(), False),
        StructField("GitCommit", StringType(), False),
        StructField("CompletedAt", TimestampType(), False),
    ]
)

SOURCE_COLUMNS = (
    "CandidateFoundationSnapshotID",
    "CandidateFoundationAttemptID",
    "RunDate",
    "SourceName",
    "SourceRole",
    "SourceTable",
    "DeltaVersion",
    "SchemaVersion",
    "SchemaChecksum",
    "IsRequired",
    "CapturedAt",
)

SOURCE_SCHEMA = StructType(
    [
        StructField("CandidateFoundationSnapshotID", StringType(), False),
        StructField("CandidateFoundationAttemptID", StringType(), False),
        StructField("RunDate", DateType(), False),
        StructField("SourceName", StringType(), False),
        StructField("SourceRole", StringType(), False),
        StructField("SourceTable", StringType(), False),
        StructField("DeltaVersion", LongType(), False),
        StructField("SchemaVersion", StringType(), False),
        StructField("SchemaChecksum", StringType(), False),
        StructField("IsRequired", BooleanType(), False),
        StructField("CapturedAt", TimestampType(), False),
    ]
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value.strip()


def _non_negative_int(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} must be a non-negative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be a non-negative integer") from exc
    if parsed < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return parsed


def parse_output_bindings(value: str) -> dict[str, Mapping[str, Any]]:
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError("OutputBindingsJSON must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError("OutputBindingsJSON must contain an object")
    required = {"customer_cells", "repeat_ad_exposure", "ad_feedback"}
    missing = sorted(required.difference(parsed))
    unexpected = sorted(set(parsed).difference(required))
    if missing or unexpected:
        details = []
        if missing:
            details.append("missing: " + ", ".join(missing))
        if unexpected:
            details.append("unexpected: " + ", ".join(unexpected))
        raise ValueError(
            "Invalid candidate foundation bindings ("
            + "; ".join(details)
            + ")"
        )
    for name, binding in parsed.items():
        if not isinstance(binding, dict):
            raise ValueError(f"Binding {name} must be an object")
        _required_text(binding.get("table"), f"{name}.table")
        _non_negative_int(
            binding.get("delta_version"), f"{name}.delta_version"
        )
        _required_text(binding.get("schema_version"), f"{name}.schema_version")
        _required_text(
            binding.get("schema_checksum"), f"{name}.schema_checksum"
        )
        _required_text(
            binding.get("write_receipt_id"), f"{name}.write_receipt_id"
        )
        _non_negative_int(binding.get("row_count"), f"{name}.row_count")
    return parsed


def parse_candidate_foundation_build(row: Any) -> CandidateFoundationBuild:
    values = (
        row.asDict(recursive=True) if hasattr(row, "asDict") else dict(row)
    )
    missing = [column for column in BUILD_COLUMNS if column not in values]
    if missing:
        raise ValueError(
            "Candidate foundation manifest is missing columns: "
            + ", ".join(missing)
        )
    run_date = parse_run_date(values["RunDate"])
    fallback_date = values["FallbackSourceRunDate"]
    build = CandidateFoundationBuild(
        snapshot_id=_required_text(
            values["CandidateFoundationSnapshotID"],
            "CandidateFoundationSnapshotID",
        ),
        attempt_id=_required_text(
            values["CandidateFoundationAttemptID"],
            "CandidateFoundationAttemptID",
        ),
        run_date=run_date,
        contract_version=_required_text(
            values["ContractVersion"],
            "ContractVersion",
        ),
        source_bindings_json=_required_text(
            values["SourceBindingsJSON"],
            "SourceBindingsJSON",
        ),
        output_bindings_json=_required_text(
            values["OutputBindingsJSON"],
            "OutputBindingsJSON",
        ),
        warning_count=_non_negative_int(
            values["WarningCount"], "WarningCount"
        ),
        status=_required_text(values["Status"], "Status"),
        fallback_source_snapshot_id=(
            None
            if values["FallbackSourceSnapshotID"] is None
            else _required_text(
                values["FallbackSourceSnapshotID"],
                "FallbackSourceSnapshotID",
            )
        ),
        fallback_source_run_date=(
            None if fallback_date is None else parse_run_date(fallback_date)
        ),
        task_run_id=_non_negative_int(values["TaskRunID"], "TaskRunID"),
        execution_count=_non_negative_int(
            values["ExecutionCount"],
            "ExecutionCount",
        ),
        git_commit=_required_text(values["GitCommit"], "GitCommit"),
        completed_at=_utc(values["CompletedAt"]),
    )
    parse_output_bindings(build.output_bindings_json)
    return build


def _latest_attempts(
    builds: Iterable[CandidateFoundationBuild],
) -> tuple[CandidateFoundationBuild, ...]:
    grouped: dict[str, CandidateFoundationBuild] = {}
    for build in builds:
        existing = grouped.get(build.snapshot_id)
        key = (
            build.execution_count,
            _utc(build.completed_at),
            build.task_run_id,
            build.attempt_id,
        )
        if existing is None or key > (
            existing.execution_count,
            _utc(existing.completed_at),
            existing.task_run_id,
            existing.attempt_id,
        ):
            grouped[build.snapshot_id] = build
    return tuple(grouped.values())


def select_candidate_foundation(
    builds: Iterable[CandidateFoundationBuild],
    *,
    run_date: date,
    selection_cutoff: datetime,
    requested_snapshot_id: str = "same_day",
    allow_fallback: bool = True,
) -> CandidateFoundationSelection:
    cutoff = _utc(selection_cutoff)
    requested = _required_text(requested_snapshot_id, "requested_snapshot_id")
    completed_before_cutoff = tuple(
        replace(build, completed_at=_utc(build.completed_at))
        for build in builds
        if _utc(build.completed_at) <= cutoff
    )
    relevant = _latest_attempts(completed_before_cutoff)
    if requested != "same_day":
        exact = [build for build in relevant if build.snapshot_id == requested]
        if not exact or exact[0].status not in ACCEPTED_FOUNDATION_STATUSES:
            raise CandidateFoundationNotReadyError(
                f"Candidate foundation {requested} is not accepted"
            )
        build = exact[0]
        return CandidateFoundationSelection(
            snapshot_id=build.snapshot_id,
            selection_status=build.status,
            source_run_date=build.run_date,
            output_bindings=parse_output_bindings(build.output_bindings_json),
        )

    same_day = [
        build
        for build in relevant
        if build.run_date == run_date
        and build.status in ACCEPTED_FOUNDATION_STATUSES
    ]
    if same_day:
        build = max(
            same_day,
            key=lambda value: (
                value.execution_count,
                value.completed_at,
                value.task_run_id,
                value.snapshot_id,
            ),
        )
        return CandidateFoundationSelection(
            snapshot_id=build.snapshot_id,
            selection_status=build.status,
            source_run_date=build.run_date,
            output_bindings=parse_output_bindings(build.output_bindings_json),
        )
    if not allow_fallback:
        raise CandidateFoundationNotReadyError(
            "No same-day candidate foundation is accepted"
        )
    oldest = cutoff - MAX_FALLBACK_AGE
    fallback = [
        build
        for build in relevant
        if build.status in ACCEPTED_FOUNDATION_STATUSES
        and build.run_date < run_date
        and oldest <= build.completed_at <= cutoff
    ]
    if not fallback:
        raise CandidateFoundationNotReadyError(
            "No same-day foundation or accepted fallback completed within 24 hours"
        )
    build = max(
        fallback,
        key=lambda value: (
            value.completed_at,
            value.execution_count,
            value.task_run_id,
            value.snapshot_id,
        ),
    )
    return CandidateFoundationSelection(
        snapshot_id=build.snapshot_id,
        selection_status=FALLBACK_PREVIOUS,
        source_run_date=build.run_date,
        output_bindings=parse_output_bindings(build.output_bindings_json),
    )


def load_candidate_foundation_builds(
    spark: Any,
    *,
    builds_table: str,
    minimum_run_date: date,
    maximum_run_date: date,
) -> tuple[CandidateFoundationBuild, ...]:
    rows = (
        spark.table(builds_table)
        .where(F.col("RunDate").between(minimum_run_date, maximum_run_date))
        .collect()
    )
    return tuple(parse_candidate_foundation_build(row) for row in rows)


def wait_for_candidate_foundation(
    spark: Any,
    *,
    builds_table: str,
    run_date: date,
    selection_cutoff: datetime,
    requested_snapshot_id: str,
    wait_seconds: int,
    poll_seconds: int,
    sleep=time.sleep,
    monotonic=time.monotonic,
    now=lambda: datetime.now(timezone.utc),
) -> CandidateFoundationSelection:
    if wait_seconds < 0 or poll_seconds < 1:
        raise ValueError("Foundation wait values are invalid")
    cutoff = _utc(selection_cutoff)
    seconds_until_cutoff = max(
        0.0,
        (cutoff - _utc(now())).total_seconds(),
    )
    deadline = monotonic() + min(wait_seconds, seconds_until_cutoff)
    while True:
        builds = load_candidate_foundation_builds(
            spark,
            builds_table=builds_table,
            minimum_run_date=run_date - timedelta(days=1),
            maximum_run_date=run_date,
        )
        try:
            return select_candidate_foundation(
                builds,
                run_date=run_date,
                selection_cutoff=cutoff,
                requested_snapshot_id=requested_snapshot_id,
                allow_fallback=monotonic() >= deadline,
            )
        except CandidateFoundationNotReadyError:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise
            sleep(min(poll_seconds, remaining))


def verify_output_binding(
    spark: Any,
    *,
    name: str,
    binding: Mapping[str, Any],
) -> None:
    table = _required_text(binding.get("table"), f"{name}.table")
    version = _non_negative_int(
        binding.get("delta_version"),
        f"{name}.delta_version",
    )
    frame = read_delta_version(spark, table, version)
    expected_schema = _required_text(
        binding.get("schema_checksum"),
        f"{name}.schema_checksum",
    )
    if schema_checksum(frame) != expected_schema:
        raise ValueError(f"Binding {name} schema does not match its output")
    _required_text(binding.get("write_receipt_id"), f"{name}.write_receipt_id")
    _non_negative_int(binding.get("row_count"), f"{name}.row_count")


def publish_candidate_foundation_manifest(
    spark: Any,
    *,
    snapshot_id: str,
    run_date: date,
    source_bindings: tuple[Mapping[str, Any], ...],
    output_bindings: Mapping[str, Mapping[str, Any]],
    warning_count: int,
    status: str,
    task_run_id: int,
    execution_count: int,
    git_commit: str,
    builds_table: str,
    sources_table: str,
    fallback_source_snapshot_id: str | None = None,
    fallback_source_run_date: date | None = None,
    completed_at: datetime | None = None,
) -> CandidateFoundationBuild:
    validate_typed_table_schema(spark, sources_table, SOURCE_COLUMNS)
    validate_typed_table_schema(
        spark,
        builds_table,
        BUILD_COLUMNS,
        nullable_columns=(
            "FallbackSourceSnapshotID",
            "FallbackSourceRunDate",
        ),
    )
    if status not in ACCEPTED_FOUNDATION_STATUSES:
        raise ValueError(f"Unsupported accepted foundation status: {status}")
    parse_output_bindings(canonical_json(output_bindings))
    names = [
        _required_text(source.get("name"), "source.name")
        for source in source_bindings
    ]
    if len(names) != len(set(names)):
        raise ValueError("Candidate foundation source names must be unique")
    completed = _utc(completed_at or datetime.now(timezone.utc))
    attempt_id = f"{snapshot_id}:attempt:{execution_count}:{task_run_id}"
    source_json = canonical_json(
        sorted(source_bindings, key=lambda value: value["name"])
    )
    output_json = canonical_json(output_bindings)

    source_rows: list[dict[str, Any]] = []
    for binding in source_bindings:
        source_rows.append(
            {
                "CandidateFoundationSnapshotID": snapshot_id,
                "CandidateFoundationAttemptID": attempt_id,
                "RunDate": run_date,
                "SourceName": _required_text(
                    binding.get("name"), "source.name"
                ),
                "SourceRole": _required_text(
                    binding.get("role"), "source.role"
                ),
                "SourceTable": _required_text(
                    binding.get("table"), "source.table"
                ),
                "DeltaVersion": _non_negative_int(
                    binding.get("delta_version"), "source.delta_version"
                ),
                "SchemaVersion": _required_text(
                    binding.get("schema_version"), "source.schema_version"
                ),
                "SchemaChecksum": _required_text(
                    binding.get("schema_checksum"), "source.schema_checksum"
                ),
                "IsRequired": bool(binding.get("required", True)),
                "CapturedAt": completed,
            }
        )
    source_frame = typed_table_frame(spark, sources_table, source_rows)
    replace_scope_by_name(
        source_frame,
        sources_table,
        {"CandidateFoundationAttemptID": attempt_id, "RunDate": run_date},
        SOURCE_COLUMNS,
        spark=spark,
        build_id=snapshot_id,
        attempt_id=attempt_id,
        git_commit=_required_text(git_commit, "git_commit"),
        capture_receipt=False,
    )

    build = CandidateFoundationBuild(
        snapshot_id=snapshot_id,
        attempt_id=attempt_id,
        run_date=run_date,
        contract_version=CANDIDATE_FOUNDATION_CONTRACT_VERSION,
        source_bindings_json=source_json,
        output_bindings_json=output_json,
        warning_count=_non_negative_int(warning_count, "warning_count"),
        status=status,
        fallback_source_snapshot_id=fallback_source_snapshot_id,
        fallback_source_run_date=fallback_source_run_date,
        task_run_id=_non_negative_int(task_run_id, "task_run_id"),
        execution_count=_non_negative_int(execution_count, "execution_count"),
        git_commit=_required_text(git_commit, "git_commit"),
        completed_at=completed,
    )
    build_frame = typed_table_frame(
        spark,
        builds_table,
        [
            {
                "CandidateFoundationSnapshotID": build.snapshot_id,
                "CandidateFoundationAttemptID": build.attempt_id,
                "RunDate": build.run_date,
                "ContractVersion": build.contract_version,
                "SourceBindingsJSON": build.source_bindings_json,
                "OutputBindingsJSON": build.output_bindings_json,
                "WarningCount": build.warning_count,
                "Status": build.status,
                "FallbackSourceSnapshotID": build.fallback_source_snapshot_id,
                "FallbackSourceRunDate": build.fallback_source_run_date,
                "TaskRunID": build.task_run_id,
                "ExecutionCount": build.execution_count,
                "GitCommit": build.git_commit,
                "CompletedAt": build.completed_at,
            }
        ],
    )
    replace_scope_by_name(
        build_frame,
        builds_table,
        {"CandidateFoundationAttemptID": attempt_id, "RunDate": run_date},
        BUILD_COLUMNS,
        spark=spark,
        build_id=snapshot_id,
        attempt_id=attempt_id,
        git_commit=build.git_commit,
        capture_receipt=False,
    )
    return build


__all__ = [
    "BUILD_COLUMNS",
    "CandidateFoundationBuild",
    "CandidateFoundationNotReadyError",
    "CandidateFoundationSelection",
    "canonical_json",
    "load_candidate_foundation_builds",
    "parse_candidate_foundation_build",
    "parse_output_bindings",
    "publish_candidate_foundation_manifest",
    "select_candidate_foundation",
    "verify_output_binding",
    "wait_for_candidate_foundation",
]

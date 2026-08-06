from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F

from next_ads.ranking.scoring_manifest import (
    FALLBACK_PREVIOUS,
    READY_FOR_NEXTADS,
    ScoreProviderBuild,
    validate_score_provider_builds,
)


MAX_FALLBACK_AGE = timedelta(hours=24)

PROVIDER_BUILD_COLUMNS = (
    "ProviderBuildID",
    "ProviderBuildAttemptID",
    "InputSnapshotID",
    "RunDate",
    "Capability",
    "UseCase",
    "ProviderID",
    "ProviderVersion",
    "ContractVersion",
    "ModelName",
    "ModelVersion",
    "ModelURI",
    "PipelineUpdateID",
    "OutputSnapshotID",
    "OutputTable",
    "OutputDeltaVersion",
    "RowCount",
    "OutputSchemaChecksum",
    "WriteReceiptID",
    "GitCommit",
    "WriteDurationMs",
    "RetryCount",
    "WarningCount",
    "Status",
    "TaskRunID",
    "ExecutionCount",
    "CompletedAt",
    "ScoringFoundationBuildID",
    "ScoringFoundationBuildAttemptID",
)


class ProviderBuildNotReadyError(ValueError):
    """Raised when no accepted provider build exists at the fixed cutoff."""


@dataclass(frozen=True)
class ProviderBuildSelection:
    """The immutable provider output selected for one NextAds route."""

    provider_build_id: str
    provider_build_attempt_id: str
    provider_signals_table: str
    provider_signals_delta_version: int
    input_snapshot_id: str
    scoring_foundation_build_id: str | None
    selection_status: str
    source_run_date: date

    def __post_init__(self) -> None:
        """Validate the exact immutable provider binding."""
        for field_name in (
            "provider_build_id",
            "provider_build_attempt_id",
            "provider_signals_table",
            "input_snapshot_id",
        ):
            _required_text(getattr(self, field_name), field_name)
        if self.scoring_foundation_build_id is not None:
            _required_text(
                self.scoring_foundation_build_id,
                "scoring_foundation_build_id",
            )
        if (
            isinstance(self.provider_signals_delta_version, bool)
            or not isinstance(self.provider_signals_delta_version, int)
            or self.provider_signals_delta_version < 0
        ):
            raise ValueError(
                "provider_signals_delta_version must be a non-negative integer"
            )
        if self.selection_status not in {
            READY_FOR_NEXTADS,
            FALLBACK_PREVIOUS,
        }:
            raise ValueError(
                f"Unsupported provider selection status: {self.selection_status}"
            )
        if isinstance(self.source_run_date, datetime) or not isinstance(
            self.source_run_date,
            date,
        ):
            raise ValueError("source_run_date must be a date")


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value.strip()


def _optional_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, label)


def _integer(value: Any, label: str, *, minimum: int = 0) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
    ):
        raise ValueError(f"{label} must be an integer of at least {minimum}")
    return value


def _date(value: Any, label: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as error:
            raise ValueError(f"{label} must be an ISO date") from error
    raise ValueError(f"{label} must be a date")


def _as_utc(value: datetime, label: str = "timestamp") -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{label} must be a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _timestamp(value: Any, label: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(f"{label} must be an ISO timestamp") from error
    return _as_utc(value, label)


def _row_mapping(row: Any) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return row
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    raise ValueError("Provider build row must be a mapping or Spark Row")


def _value(values: Mapping[str, Any], column: str) -> Any:
    if column not in values:
        raise ValueError(f"Provider build manifest is missing column {column}")
    return values[column]


def parse_score_provider_build(row: Any) -> ScoreProviderBuild:
    """Parse and validate one physical provider-build manifest row."""
    values = _row_mapping(row)
    missing = [
        column for column in PROVIDER_BUILD_COLUMNS if column not in values
    ]
    if missing:
        raise ValueError(
            "Provider build manifest is missing columns: " + ", ".join(missing)
        )

    return ScoreProviderBuild(
        provider_build_id=_required_text(
            _value(values, "ProviderBuildID"),
            "ProviderBuildID",
        ),
        provider_build_attempt_id=_required_text(
            _value(values, "ProviderBuildAttemptID"),
            "ProviderBuildAttemptID",
        ),
        input_snapshot_id=_required_text(
            _value(values, "InputSnapshotID"),
            "InputSnapshotID",
        ),
        run_date=_date(_value(values, "RunDate"), "RunDate"),
        capability=_required_text(
            _value(values, "Capability"),
            "Capability",
        ),
        use_case=_required_text(_value(values, "UseCase"), "UseCase"),
        provider_id=_required_text(
            _value(values, "ProviderID"),
            "ProviderID",
        ),
        provider_version=_required_text(
            _value(values, "ProviderVersion"),
            "ProviderVersion",
        ),
        contract_version=_required_text(
            _value(values, "ContractVersion"),
            "ContractVersion",
        ),
        model_name=_optional_text(_value(values, "ModelName"), "ModelName"),
        model_version=_optional_text(
            _value(values, "ModelVersion"),
            "ModelVersion",
        ),
        model_uri=_optional_text(_value(values, "ModelURI"), "ModelURI"),
        pipeline_update_id=_optional_text(
            _value(values, "PipelineUpdateID"),
            "PipelineUpdateID",
        ),
        output_snapshot_id=_optional_text(
            _value(values, "OutputSnapshotID"),
            "OutputSnapshotID",
        ),
        output_table=_optional_text(
            _value(values, "OutputTable"),
            "OutputTable",
        ),
        output_delta_version=(
            None
            if _value(values, "OutputDeltaVersion") is None
            else _integer(
                _value(values, "OutputDeltaVersion"),
                "OutputDeltaVersion",
            )
        ),
        row_count=_integer(_value(values, "RowCount"), "RowCount"),
        output_schema_checksum=_optional_text(
            _value(values, "OutputSchemaChecksum"),
            "OutputSchemaChecksum",
        ),
        write_receipt_id=_optional_text(
            _value(values, "WriteReceiptID"),
            "WriteReceiptID",
        ),
        git_commit=_required_text(_value(values, "GitCommit"), "GitCommit"),
        write_duration_ms=_integer(
            _value(values, "WriteDurationMs"), "WriteDurationMs"
        ),
        retry_count=_integer(_value(values, "RetryCount"), "RetryCount"),
        warning_count=_integer(
            _value(values, "WarningCount"),
            "WarningCount",
        ),
        status=_required_text(_value(values, "Status"), "Status"),
        task_run_id=_integer(
            _value(values, "TaskRunID"),
            "TaskRunID",
            minimum=1,
        ),
        execution_count=_integer(
            _value(values, "ExecutionCount"),
            "ExecutionCount",
        ),
        completed_at=_timestamp(
            _value(values, "CompletedAt"),
            "CompletedAt",
        ),
        scoring_foundation_build_id=_optional_text(
            _value(values, "ScoringFoundationBuildID"),
            "ScoringFoundationBuildID",
        ),
        scoring_foundation_build_attempt_id=_optional_text(
            _value(values, "ScoringFoundationBuildAttemptID"),
            "ScoringFoundationBuildAttemptID",
        ),
    )


def _normalise_build(build: ScoreProviderBuild) -> ScoreProviderBuild:
    return replace(
        build, completed_at=_as_utc(build.completed_at, "completed_at")
    )


def _selection_from_build(
    build: ScoreProviderBuild,
    *,
    status: str,
) -> ProviderBuildSelection:
    if build.output_delta_version is None:
        raise ValueError("Selected provider build has no OutputDeltaVersion")
    return ProviderBuildSelection(
        provider_build_id=_required_text(
            build.provider_build_id,
            "ProviderBuildID",
        ),
        provider_build_attempt_id=_required_text(
            build.provider_build_attempt_id,
            "ProviderBuildAttemptID",
        ),
        provider_signals_table=_required_text(
            build.output_table,
            "OutputTable",
        ),
        provider_signals_delta_version=build.output_delta_version,
        input_snapshot_id=_required_text(
            build.input_snapshot_id,
            "InputSnapshotID",
        ),
        scoring_foundation_build_id=build.scoring_foundation_build_id,
        selection_status=status,
        source_run_date=build.run_date,
    )


def _latest_ready(
    builds: Iterable[ScoreProviderBuild],
) -> ScoreProviderBuild | None:
    ready = [build for build in builds if build.status == READY_FOR_NEXTADS]
    if not ready:
        return None
    return max(
        ready,
        key=lambda build: (
            _as_utc(build.completed_at, "completed_at"),
            build.execution_count,
            build.task_run_id,
            build.provider_build_id,
        ),
    )


def select_score_provider_build(
    builds: Iterable[ScoreProviderBuild],
    *,
    run_date: date,
    selection_cutoff: datetime,
    provider_id: str,
    capability: str,
    use_case: str,
    allow_fallback: bool,
) -> ProviderBuildSelection:
    """Select the accepted provider output as it existed at a fixed cutoff."""
    cutoff = _as_utc(selection_cutoff, "selection_cutoff")
    identity = (
        _required_text(provider_id, "provider_id"),
        _required_text(capability, "capability"),
        _required_text(use_case, "use_case"),
    )
    oldest_completion = cutoff - MAX_FALLBACK_AGE
    minimum_run_date = run_date - timedelta(days=1)
    relevant = tuple(
        _normalise_build(build)
        for build in builds
        if (build.provider_id, build.capability, build.use_case) == identity
        and minimum_run_date <= build.run_date <= run_date
        and oldest_completion
        <= _as_utc(build.completed_at, "completed_at")
        <= cutoff
    )
    latest_attempts = validate_score_provider_builds(relevant)

    same_day = _latest_ready(
        build for build in latest_attempts if build.run_date == run_date
    )
    if same_day is not None:
        return _selection_from_build(same_day, status=READY_FOR_NEXTADS)

    if not allow_fallback:
        raise ProviderBuildNotReadyError(
            f"No same-day READY_FOR_NEXTADS build exists for {provider_id}"
        )

    fallback = _latest_ready(
        build
        for build in latest_attempts
        if build.run_date < run_date
        and oldest_completion <= build.completed_at <= cutoff
    )
    if fallback is None:
        raise ProviderBuildNotReadyError(
            "No same-day build or READY fallback completed within 24 hours "
            f"for {provider_id}"
        )
    return _selection_from_build(fallback, status=FALLBACK_PREVIOUS)


def load_score_provider_builds(
    spark: SparkSession,
    *,
    table: str,
    run_date: date,
    selection_cutoff: datetime,
    provider_id: str,
    capability: str,
    use_case: str,
) -> tuple[ScoreProviderBuild, ...]:
    """Load provider attempts visible at the fixed selection cutoff."""
    frame = spark.table(_required_text(table, "table"))
    missing = [
        column
        for column in PROVIDER_BUILD_COLUMNS
        if column not in frame.columns
    ]
    if missing:
        raise ValueError(
            "Provider build manifest is missing columns: " + ", ".join(missing)
        )

    cutoff_utc = _as_utc(selection_cutoff, "selection_cutoff")
    cutoff = cutoff_utc.replace(tzinfo=None)
    oldest_completion = (cutoff_utc - MAX_FALLBACK_AGE).replace(tzinfo=None)
    minimum_run_date = run_date - timedelta(days=1)
    filtered: DataFrame = frame.where(
        (F.col("ProviderID") == F.lit(provider_id))
        & (F.col("Capability") == F.lit(capability))
        & (F.col("UseCase") == F.lit(use_case))
        & (
            F.col("RunDate").between(
                F.lit(minimum_run_date),
                F.lit(run_date),
            )
            | F.col("RunDate").isNull()
        )
        & (
            F.col("CompletedAt").between(
                F.lit(oldest_completion),
                F.lit(cutoff),
            )
            | F.col("CompletedAt").isNull()
        )
    )
    return tuple(
        parse_score_provider_build(row)
        for row in filtered.select(*PROVIDER_BUILD_COLUMNS).collect()
    )


def wait_for_score_provider_build(
    spark: SparkSession,
    *,
    table: str,
    run_date: date,
    provider_id: str,
    capability: str,
    use_case: str,
    wait_seconds: float,
    poll_seconds: float,
    selection_cutoff: datetime | None = None,
    now_fn: Callable[[], datetime] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
    load_fn: Callable[..., tuple[ScoreProviderBuild, ...]] | None = None,
) -> ProviderBuildSelection:
    """Wait for same-day readiness, then use the fixed-cutoff fallback rule."""
    if wait_seconds < 0:
        raise ValueError("wait_seconds must not be negative")
    if poll_seconds <= 0:
        raise ValueError("poll_seconds must be positive")
    now = now_fn or (lambda: datetime.now(timezone.utc))
    sleep = sleep_fn or time.sleep
    loader = load_fn or load_score_provider_builds
    started_at = _as_utc(now(), "now")
    cutoff = (
        _as_utc(selection_cutoff, "selection_cutoff")
        if selection_cutoff is not None
        else started_at + timedelta(seconds=wait_seconds)
    )

    while True:
        builds = loader(
            spark,
            table=table,
            run_date=run_date,
            selection_cutoff=cutoff,
            provider_id=provider_id,
            capability=capability,
            use_case=use_case,
        )
        try:
            return select_score_provider_build(
                builds,
                run_date=run_date,
                selection_cutoff=cutoff,
                provider_id=provider_id,
                capability=capability,
                use_case=use_case,
                allow_fallback=False,
            )
        except ProviderBuildNotReadyError:
            current_time = _as_utc(now(), "now")
            if current_time >= cutoff:
                return select_score_provider_build(
                    builds,
                    run_date=run_date,
                    selection_cutoff=cutoff,
                    provider_id=provider_id,
                    capability=capability,
                    use_case=use_case,
                    allow_fallback=True,
                )
            sleep(min(poll_seconds, (cutoff - current_time).total_seconds()))


__all__ = [
    "MAX_FALLBACK_AGE",
    "PROVIDER_BUILD_COLUMNS",
    "ProviderBuildNotReadyError",
    "ProviderBuildSelection",
    "load_score_provider_builds",
    "parse_score_provider_build",
    "select_score_provider_build",
    "wait_for_score_provider_build",
]

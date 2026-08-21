from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from next_ads.common.delta_writes import (
    DeltaWriteReceipt,
    KeyValidationSummary,
    atomic_append_by_name,
    find_delta_write_receipt,
    replace_scope_by_name,
    replace_table_by_name,
    typed_table_frame,
    validate_target_columns,
    validate_typed_table_schema,
    validate_unique_non_null_keys,
)
from next_ads.common.output_locations import log_output_location
from next_ads.ranking.scoring_inputs import read_delta_version
from next_ads.common.snapshot_writes import (
    publish_history_and_latest,
    replace_validated_scope,
    with_run_date,
)


READY = "READY"
NO_ADS = "NO_ADS"
VALID_ASSIGNMENT_STATUSES = frozenset({READY, NO_ADS})
VALID_ASSIGNMENT_ROUTES = frozenset({"v1", "v2"})
V1_TEASER_LOCATIONS = ("PH3", "PH4")
V1_TEASER_CONTROL_TOKEN = "Z"
V1_SECONDARY_SCOPE_PARENTS = (
    ("SB1", "SB2"),
    ("OC1", "OC2"),
)


@dataclass(frozen=True)
class AssignmentTableContract:
    staging_table: str
    event_table: str
    history_table: str
    latest_table: str

    def __post_init__(self) -> None:
        """Validate that publication tables are explicit and isolated."""
        tables = [
            self.staging_table,
            self.event_table,
            self.history_table,
            self.latest_table,
        ]
        if any(not table.strip() for table in tables):
            raise ValueError("Assignment table names must not be empty")
        if len(set(tables)) != len(tables):
            raise ValueError("Assignment publication tables must be distinct")


@dataclass(frozen=True)
class AssignmentColumnContract:
    build_run_id: str = "BuildRunID"
    route: str = "Route"
    event_scope: str = "Scope"
    status: str = "Status"
    row_count: str = "RowCount"
    build_date: str = "BuildDate"
    task_run_id: str = "TaskRunID"
    execution_count: str = "ExecutionCount"
    completed_at: str = "CompletedAt"
    candidate_build_id: str = "CandidateBuildID"
    candidate_build_attempt_id: str = "CandidateBuildAttemptID"
    portfolio_id: str = "PortfolioID"
    portfolio_attempt_id: str = "PortfolioAttemptID"
    candidate_foundation_snapshot_id: str = "CandidateFoundationSnapshotID"

    def __post_init__(self) -> None:
        """Validate the injected staging and event metadata names."""
        columns = [
            self.build_run_id,
            self.route,
            self.event_scope,
            self.status,
            self.row_count,
            self.build_date,
            self.task_run_id,
            self.execution_count,
            self.completed_at,
            self.candidate_build_id,
            self.candidate_build_attempt_id,
            self.portfolio_id,
            self.portfolio_attempt_id,
            self.candidate_foundation_snapshot_id,
        ]
        if any(not column.strip() for column in columns):
            raise ValueError("Assignment metadata columns must not be empty")
        if len(set(columns)) != len(columns):
            raise ValueError("Assignment metadata columns must be distinct")


@dataclass(frozen=True)
class AssignmentProvenance:
    candidate_build_id: str
    candidate_build_attempt_id: str
    portfolio_id: str
    portfolio_attempt_id: str
    candidate_foundation_snapshot_id: str

    def __post_init__(self) -> None:
        """Require complete immutable candidate and portfolio identifiers."""
        for label, value in (
            ("CandidateBuildID", self.candidate_build_id),
            ("CandidateBuildAttemptID", self.candidate_build_attempt_id),
            ("PortfolioID", self.portfolio_id),
            ("PortfolioAttemptID", self.portfolio_attempt_id),
            (
                "CandidateFoundationSnapshotID",
                self.candidate_foundation_snapshot_id,
            ),
        ):
            _require_non_empty(value, label=label)

    def column_values(
        self,
        columns: AssignmentColumnContract,
    ) -> dict[str, str]:
        return {
            columns.candidate_build_id: self.candidate_build_id,
            columns.candidate_build_attempt_id: (
                self.candidate_build_attempt_id
            ),
            columns.portfolio_id: self.portfolio_id,
            columns.portfolio_attempt_id: self.portfolio_attempt_id,
            columns.candidate_foundation_snapshot_id: (
                self.candidate_foundation_snapshot_id
            ),
        }


def _provenance_columns(columns: AssignmentColumnContract) -> tuple[str, ...]:
    return (
        columns.candidate_build_id,
        columns.candidate_build_attempt_id,
        columns.portfolio_id,
        columns.portfolio_attempt_id,
        columns.candidate_foundation_snapshot_id,
    )


def _provenance_from_mapping(
    value: Mapping[str, Any],
    columns: AssignmentColumnContract,
) -> AssignmentProvenance:
    return AssignmentProvenance(
        candidate_build_id=value.get(columns.candidate_build_id),
        candidate_build_attempt_id=value.get(
            columns.candidate_build_attempt_id
        ),
        portfolio_id=value.get(columns.portfolio_id),
        portfolio_attempt_id=value.get(columns.portfolio_attempt_id),
        candidate_foundation_snapshot_id=value.get(
            columns.candidate_foundation_snapshot_id
        ),
    )


@dataclass(frozen=True)
class AssignmentScopeContract:
    route: str
    scope_column: str
    expected_scopes: Sequence[str]
    key_columns: Sequence[str]
    public_columns: Sequence[str]
    publication_date_column: str = "rundate"

    def __post_init__(self) -> None:
        """Normalise and validate one route's public assignment contract."""
        expected_scopes = tuple(self.expected_scopes)
        key_columns = tuple(self.key_columns)
        public_columns = tuple(self.public_columns)
        object.__setattr__(self, "expected_scopes", expected_scopes)
        object.__setattr__(self, "key_columns", key_columns)
        object.__setattr__(self, "public_columns", public_columns)

        if self.route not in VALID_ASSIGNMENT_ROUTES:
            raise ValueError("Assignment route must be one of: v1, v2")
        if not self.scope_column.strip():
            raise ValueError("Assignment scope column must not be empty")
        if not self.publication_date_column.strip():
            raise ValueError(
                "Assignment publication date column must not be empty"
            )
        if not expected_scopes or any(
            not scope.strip() for scope in expected_scopes
        ):
            raise ValueError("Expected assignment scopes must not be empty")
        if len(set(expected_scopes)) != len(expected_scopes):
            raise ValueError("Expected assignment scopes must be unique")
        if not key_columns or any(
            not column.strip() for column in key_columns
        ):
            raise ValueError("Assignment key columns must not be empty")
        if len(set(key_columns)) != len(key_columns):
            raise ValueError("Assignment key columns must be unique")
        if not public_columns or any(
            not column.strip() for column in public_columns
        ):
            raise ValueError("Public assignment columns must not be empty")
        if len(set(public_columns)) != len(public_columns):
            raise ValueError("Public assignment columns must be unique")

        missing_keys = sorted(set(key_columns) - set(public_columns))
        if missing_keys:
            raise ValueError(
                "Assignment keys missing from public columns: "
                + ", ".join(missing_keys)
            )
        if self.scope_column not in public_columns:
            raise ValueError("Assignment scope column must be public")
        if self.scope_column not in key_columns:
            raise ValueError("Assignment scope column must be part of the key")
        if self.publication_date_column not in public_columns:
            raise ValueError(
                "Assignment publication date column must be public"
            )


@dataclass(frozen=True)
class AssignmentScopeEvent:
    scope: str
    status: str
    row_count: int
    build_date: date
    task_run_id: int
    execution_count: int
    completed_at: datetime
    provenance: AssignmentProvenance


@dataclass(frozen=True)
class AssignmentStageResult:
    route: str
    build_run_id: str
    build_date: date
    scope: str
    task_run_id: int
    execution_count: int
    completed_at: datetime
    status: str
    row_count: int
    validation: KeyValidationSummary
    event_write: DeltaWriteReceipt
    provenance: AssignmentProvenance


@dataclass(frozen=True)
class AssignmentPublicationResult:
    route: str
    build_run_id: str
    build_date: date
    row_count: int
    events: tuple[AssignmentScopeEvent, ...]
    validation: KeyValidationSummary
    history_write: DeltaWriteReceipt
    latest_write: DeltaWriteReceipt
    provenance: AssignmentProvenance


@dataclass(frozen=True)
class _StagingSummary:
    scope: str
    publication_date: date
    task_run_id: int
    execution_count: int
    row_count: int
    provenance: AssignmentProvenance


def _normalise_build_date(value: date | datetime | str, *, label: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{label} must be an ISO date") from exc
    raise ValueError(f"{label} must be a date or ISO date")


def _require_non_empty(value: str, *, label: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must not be empty")


def _validate_build_identity(
    build_run_id: str,
    *,
    columns: AssignmentColumnContract,
    scope_contract: AssignmentScopeContract,
) -> None:
    _require_non_empty(build_run_id, label=columns.build_run_id)
    route_prefix = f"{scope_contract.route}_"
    if not build_run_id.startswith(route_prefix) or len(build_run_id) == len(
        route_prefix
    ):
        raise ValueError(
            f"{columns.build_run_id} must start with {route_prefix!r}"
        )


def _require_columns(
    df: DataFrame,
    required_columns: Sequence[str],
    *,
    label: str,
) -> None:
    missing = sorted(set(required_columns) - set(df.columns))
    if missing:
        raise ValueError(f"{label} missing columns: {', '.join(missing)}")


def _validate_contract_compatibility(
    columns: AssignmentColumnContract,
    scope_contract: AssignmentScopeContract,
) -> None:
    staging_metadata = {
        columns.build_run_id,
        columns.task_run_id,
        columns.execution_count,
        *_provenance_columns(columns),
    }
    collisions = sorted(
        staging_metadata.intersection(scope_contract.public_columns)
    )
    if collisions:
        raise ValueError(
            "Staging metadata collides with public assignment columns: "
            + ", ".join(collisions)
        )


def stage_assignment_scope(
    spark: Any,
    assignments: DataFrame,
    *,
    tables: AssignmentTableContract,
    columns: AssignmentColumnContract,
    scope_contract: AssignmentScopeContract,
    build_run_id: str,
    build_date: date | datetime | str,
    scope: str,
    task_run_id: int,
    execution_count: int,
    provenance: AssignmentProvenance,
    allow_no_ads: bool = True,
    completed_at: datetime | None = None,
) -> AssignmentStageResult:
    """Atomically replace one build scope in the assignment staging table."""
    _validate_contract_compatibility(columns, scope_contract)
    _validate_build_identity(
        build_run_id,
        columns=columns,
        scope_contract=scope_contract,
    )
    resolved_build_date = _normalise_build_date(
        build_date,
        label=columns.build_date,
    )
    resolved_task_run_id = _validate_integer(
        task_run_id,
        label=columns.task_run_id,
        minimum=1,
    )
    resolved_execution_count = _validate_integer(
        execution_count,
        label=columns.execution_count,
    )
    if completed_at is not None and not isinstance(completed_at, datetime):
        raise ValueError(f"{columns.completed_at} must be a timestamp")
    if not isinstance(allow_no_ads, bool):
        raise ValueError("allow_no_ads must be a boolean")
    if scope not in set(scope_contract.expected_scopes):
        raise ValueError(
            f"Unexpected assignment scope for {scope_contract.route}: {scope}"
        )

    input_columns = [
        column
        for column in scope_contract.public_columns
        if column != scope_contract.publication_date_column
    ]
    _require_columns(assignments, input_columns, label="Assignment dataframe")

    public_assignments = with_run_date(
        assignments.select(*input_columns),
        resolved_build_date,
        column=scope_contract.publication_date_column,
    ).select(*scope_contract.public_columns)
    staged_assignments = (
        public_assignments.withColumn(
            columns.build_run_id,
            F.lit(build_run_id),
        )
        .withColumn(
            columns.task_run_id,
            F.lit(resolved_task_run_id).cast("long"),
        )
        .withColumn(
            columns.execution_count,
            F.lit(resolved_execution_count).cast("int"),
        )
    )
    for column, value in provenance.column_values(columns).items():
        staged_assignments = staged_assignments.withColumn(
            column, F.lit(value)
        )
    staging_columns = [
        columns.build_run_id,
        columns.task_run_id,
        columns.execution_count,
        *_provenance_columns(columns),
        *scope_contract.public_columns,
    ]
    staged_assignments = staged_assignments.select(*staging_columns)
    replacement_scope = {
        columns.build_run_id: build_run_id,
        scope_contract.scope_column: scope,
    }
    validation = replace_validated_scope(
        spark,
        staged_assignments,
        table=tables.staging_table,
        scope=replacement_scope,
        key_columns=scope_contract.key_columns,
        columns=staging_columns,
    )
    if validation.row_count == 0 and not allow_no_ads:
        raise ValueError(
            f"Assignment scope {scope} unexpectedly produced no rows"
        )
    status = READY if validation.row_count else NO_ADS
    resolved_completed_at = completed_at or datetime.now(timezone.utc)
    event_columns = [
        columns.build_run_id,
        columns.route,
        columns.event_scope,
        columns.status,
        columns.row_count,
        columns.build_date,
        columns.task_run_id,
        columns.execution_count,
        columns.completed_at,
        *_provenance_columns(columns),
    ]
    event_values = {
        columns.build_run_id: build_run_id,
        columns.route: scope_contract.route,
        columns.event_scope: scope,
        columns.status: status,
        columns.row_count: validation.row_count,
        columns.build_date: resolved_build_date,
        columns.task_run_id: resolved_task_run_id,
        columns.execution_count: resolved_execution_count,
        columns.completed_at: resolved_completed_at,
        **provenance.column_values(columns),
    }
    event = (
        typed_table_frame(spark, tables.event_table, [event_values])
        .withColumn(
            columns.row_count,
            F.col(columns.row_count).cast("long"),
        )
        .withColumn(
            columns.task_run_id,
            F.col(columns.task_run_id).cast("long"),
        )
        .withColumn(
            columns.execution_count,
            F.col(columns.execution_count).cast("int"),
        )
        .select(*event_columns)
    )
    event_write = atomic_append_by_name(
        spark,
        event,
        target_table=tables.event_table,
        columns=event_columns,
    )
    return AssignmentStageResult(
        route=scope_contract.route,
        build_run_id=build_run_id,
        build_date=resolved_build_date,
        scope=scope,
        task_run_id=resolved_task_run_id,
        execution_count=resolved_execution_count,
        completed_at=resolved_completed_at,
        status=status,
        row_count=validation.row_count,
        validation=validation,
        event_write=event_write,
        provenance=provenance,
    )


def _read_build_frame(
    spark: Any,
    table: str,
    *,
    build_run_id_column: str,
    build_run_id: str,
) -> DataFrame:
    frame = spark.table(table)
    _require_columns(
        frame,
        [build_run_id_column],
        label=f"Table {table}",
    )
    return frame.where(
        F.col(build_run_id_column).eqNullSafe(F.lit(build_run_id))
    )


def _collect_event_rows(
    events: DataFrame,
    columns: AssignmentColumnContract,
) -> list[dict[str, Any]]:
    event_columns = [
        columns.build_run_id,
        columns.route,
        columns.event_scope,
        columns.status,
        columns.row_count,
        columns.build_date,
        columns.task_run_id,
        columns.execution_count,
        columns.completed_at,
        *_provenance_columns(columns),
    ]
    _require_columns(events, event_columns, label="Assignment event table")
    return [
        row.asDict(recursive=True)
        for row in events.select(*event_columns).collect()
    ]


def _validate_integer(
    value: Any,
    *,
    label: str,
    minimum: int = 0,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an integer")
    if value < minimum:
        raise ValueError(f"{label} must be at least {minimum}")
    return value


def _event_order(
    event: Mapping[str, Any],
    columns: AssignmentColumnContract,
) -> tuple[int, datetime, int]:
    execution_count = _validate_integer(
        event.get(columns.execution_count),
        label=columns.execution_count,
    )
    completed_at = event.get(columns.completed_at)
    if not isinstance(completed_at, datetime):
        raise ValueError(f"{columns.completed_at} must be a timestamp")
    task_run_id = _validate_integer(
        event.get(columns.task_run_id),
        label=columns.task_run_id,
        minimum=1,
    )
    return execution_count, completed_at, task_run_id


def _select_latest_scope_events(
    event_rows: Sequence[Mapping[str, Any]],
    *,
    columns: AssignmentColumnContract,
    scope_contract: AssignmentScopeContract,
    build_run_id: str,
    build_date: date,
) -> tuple[AssignmentScopeEvent, ...]:
    expected_scopes = set(scope_contract.expected_scopes)
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)

    for event in event_rows:
        status = event.get(columns.status)
        if status not in VALID_ASSIGNMENT_STATUSES:
            raise ValueError(f"Invalid assignment event status: {status}")
        if event.get(columns.build_run_id) != build_run_id:
            raise ValueError(
                "Assignment event BuildRunID does not match the build"
            )
        if event.get(columns.route) != scope_contract.route:
            raise ValueError(
                "Assignment event Route does not match the build route"
            )
        event_build_date = _normalise_build_date(
            event.get(columns.build_date),
            label=f"Event {columns.build_date}",
        )
        if event_build_date != build_date:
            raise ValueError(
                "Assignment event BuildDate does not match the build date"
            )

        event_scope = event.get(columns.event_scope)
        if not isinstance(event_scope, str) or not event_scope:
            raise ValueError("Assignment event Scope must not be empty")
        grouped[event_scope].append(event)

    observed_scopes = set(grouped)
    missing_scopes = sorted(expected_scopes - observed_scopes)
    unexpected_scopes = sorted(observed_scopes - expected_scopes)
    if missing_scopes or unexpected_scopes:
        details = []
        if missing_scopes:
            details.append("missing scopes: " + ", ".join(missing_scopes))
        if unexpected_scopes:
            details.append(
                "unexpected scopes: " + ", ".join(unexpected_scopes)
            )
        raise ValueError(
            "Assignment event scope mismatch: " + "; ".join(details)
        )

    selected_events = []
    for scope in scope_contract.expected_scopes:
        scope_events = grouped[scope]
        ordered = [
            (event, _event_order(event, columns)) for event in scope_events
        ]
        latest_order = max(order for _, order in ordered)
        latest_rows = [
            event for event, order in ordered if order == latest_order
        ]
        latest_payloads = {
            (
                event.get(columns.status),
                event.get(columns.row_count),
                _provenance_from_mapping(event, columns),
            )
            for event in latest_rows
        }
        if len(latest_payloads) != 1:
            raise ValueError(
                f"Contradictory latest assignment events for scope {scope}"
            )

        latest = latest_rows[0]
        status = latest.get(columns.status)
        if status not in VALID_ASSIGNMENT_STATUSES:
            raise ValueError(
                f"Invalid assignment event status for scope {scope}: {status}"
            )
        row_count = _validate_integer(
            latest.get(columns.row_count),
            label=columns.row_count,
        )
        if status == READY and row_count == 0:
            raise ValueError(
                f"READY assignment event for scope {scope} has zero rows"
            )
        if status == NO_ADS and row_count != 0:
            raise ValueError(
                f"NO_ADS assignment event for scope {scope} has rows"
            )

        execution_count, completed_at, task_run_id = latest_order
        selected_events.append(
            AssignmentScopeEvent(
                scope=scope,
                status=status,
                row_count=row_count,
                build_date=build_date,
                task_run_id=task_run_id,
                execution_count=execution_count,
                completed_at=completed_at,
                provenance=_provenance_from_mapping(latest, columns),
            )
        )

    provenances = {event.provenance for event in selected_events}
    if len(provenances) != 1:
        raise ValueError(
            "Assignment events do not share one accepted candidate provenance"
        )

    return tuple(selected_events)


def _collect_staging_summaries(
    staged: DataFrame,
    *,
    columns: AssignmentColumnContract,
    scope_contract: AssignmentScopeContract,
) -> tuple[_StagingSummary, ...]:
    required_columns = [
        columns.build_run_id,
        columns.task_run_id,
        columns.execution_count,
        *_provenance_columns(columns),
        scope_contract.scope_column,
        scope_contract.publication_date_column,
        *scope_contract.public_columns,
    ]
    _require_columns(
        staged, required_columns, label="Assignment staging table"
    )
    summaries = (
        staged.select(
            F.col(scope_contract.scope_column).alias("_scope"),
            F.col(scope_contract.publication_date_column).alias(
                "_publication_date"
            ),
            F.col(columns.task_run_id).alias("_task_run_id"),
            F.col(columns.execution_count).alias("_execution_count"),
            *[
                F.col(column).alias(f"_provenance_{index}")
                for index, column in enumerate(_provenance_columns(columns))
            ],
        )
        .groupBy(
            "_scope",
            "_publication_date",
            "_task_run_id",
            "_execution_count",
            *[
                f"_provenance_{index}"
                for index, _ in enumerate(_provenance_columns(columns))
            ],
        )
        .count()
        .collect()
    )
    return tuple(
        _StagingSummary(
            scope=row["_scope"],
            publication_date=_normalise_build_date(
                row["_publication_date"],
                label="Staged assignment publication date",
            ),
            task_run_id=_validate_integer(
                row["_task_run_id"],
                label=f"Staged {columns.task_run_id}",
                minimum=1,
            ),
            execution_count=_validate_integer(
                row["_execution_count"],
                label=f"Staged {columns.execution_count}",
            ),
            row_count=int(row["count"]),
            provenance=AssignmentProvenance(
                candidate_build_id=row["_provenance_0"],
                candidate_build_attempt_id=row["_provenance_1"],
                portfolio_id=row["_provenance_2"],
                portfolio_attempt_id=row["_provenance_3"],
                candidate_foundation_snapshot_id=row["_provenance_4"],
            ),
        )
        for row in summaries
    )


def _validate_staging_against_events(
    summaries: Sequence[_StagingSummary],
    *,
    selected_events: Sequence[AssignmentScopeEvent],
    scope_contract: AssignmentScopeContract,
    build_date: date,
) -> int:
    expected_scopes = set(scope_contract.expected_scopes)
    staged_counts: dict[str, int] = defaultdict(int)
    selected_by_scope = {event.scope: event for event in selected_events}
    for summary in summaries:
        if summary.scope not in expected_scopes:
            raise ValueError(
                f"Unexpected staged assignment scope: {summary.scope}"
            )
        if summary.publication_date != build_date:
            raise ValueError(
                "Staged assignment publication date does not match BuildDate"
            )
        selected_event = selected_by_scope[summary.scope]
        if (
            summary.task_run_id != selected_event.task_run_id
            or summary.execution_count != selected_event.execution_count
            or summary.provenance != selected_event.provenance
        ):
            raise ValueError(
                "Staged assignment attempt does not match the latest "
                f"completed event for scope {summary.scope}"
            )
        staged_counts[summary.scope] += summary.row_count

    for event in selected_events:
        staged_count = staged_counts.get(event.scope, 0)
        if staged_count != event.row_count:
            raise ValueError(
                "Assignment event/staging count mismatch for scope "
                f"{event.scope}: event={event.row_count}, staged={staged_count}"
            )
    return sum(staged_counts.values())


def _validate_v1_secondary_event_inheritance(
    selected_events: Sequence[AssignmentScopeEvent],
) -> None:
    """Require inherited scopes to remain empty when their parent has no ads."""
    events_by_scope = {event.scope: event for event in selected_events}
    for parent_scope, secondary_scope in V1_SECONDARY_SCOPE_PARENTS:
        parent_event = events_by_scope.get(parent_scope)
        secondary_event = events_by_scope.get(secondary_scope)
        if parent_event is None or secondary_event is None:
            continue
        if parent_event.status == NO_ADS and secondary_event.status != NO_ADS:
            raise ValueError(
                f"V1 assignment event {secondary_scope} must be NO_ADS "
                f"when inherited parent {parent_scope} is NO_ADS"
            )


def _remove_invalid_v1_teaser_assignments(
    assignments: DataFrame,
) -> DataFrame:
    """Remove PH3/PH4 rows for accounts with an invalid teaser pairing."""
    _require_columns(
        assignments,
        ["AccountNumber", "Location", "MASID"],
        label="V1 assignment dataframe",
    )
    teaser_accounts = (
        assignments.where(F.col("Location").isin(*V1_TEASER_LOCATIONS))
        .withColumn(
            "_teaser_assigned",
            F.when(
                F.col("MASID").endswith(f"_{V1_TEASER_CONTROL_TOKEN}"),
                F.lit(0),
            ).otherwise(F.lit(1)),
        )
        .withColumn(
            "_masid_token",
            F.split(F.col("MASID"), "_").getItem(1),
        )
        .groupBy("AccountNumber")
        .agg(
            F.sum("_teaser_assigned").alias("_teasers_assigned"),
            F.sort_array(F.collect_set("_masid_token")).alias("_token_set"),
        )
        .where(
            (F.col("_teasers_assigned") < len(V1_TEASER_LOCATIONS))
            | (F.array_size(F.col("_token_set")) < len(V1_TEASER_LOCATIONS))
        )
        .where(F.col("_token_set") != F.array(F.lit(V1_TEASER_CONTROL_TOKEN)))
        .select("AccountNumber")
        .withColumn("_invalid_teaser_assignment", F.lit(True))
    )
    return (
        assignments.join(teaser_accounts, on="AccountNumber", how="left")
        .where(
            F.col("_invalid_teaser_assignment").isNull()
            | ~F.col("Location").isin(*V1_TEASER_LOCATIONS)
        )
        .drop("_invalid_teaser_assignment")
        .select(*assignments.columns)
    )


def _prepare_v1_public_assignments(
    spark: Any,
    staged_assignments: DataFrame,
    *,
    tables: AssignmentTableContract,
    scope_contract: AssignmentScopeContract,
    selected_events: Sequence[AssignmentScopeEvent],
    build_date: date,
) -> DataFrame:
    """Remove intentional NO_ADS scopes and apply the v1 teaser correction."""
    if set(V1_TEASER_LOCATIONS).issubset(set(scope_contract.expected_scopes)):
        return _remove_invalid_v1_teaser_assignments(staged_assignments)
    return staged_assignments


def validate_and_publish_assignment_build(
    spark: Any,
    *,
    tables: AssignmentTableContract,
    columns: AssignmentColumnContract,
    scope_contract: AssignmentScopeContract,
    build_run_id: str,
    build_date: date | datetime | str,
) -> AssignmentPublicationResult:
    """Validate a complete staged assignment build and publish it atomically."""
    _validate_contract_compatibility(columns, scope_contract)
    _validate_build_identity(
        build_run_id,
        columns=columns,
        scope_contract=scope_contract,
    )
    resolved_build_date = _normalise_build_date(
        build_date,
        label=columns.build_date,
    )

    event_frame = _read_build_frame(
        spark,
        tables.event_table,
        build_run_id_column=columns.build_run_id,
        build_run_id=build_run_id,
    )
    event_rows = _collect_event_rows(event_frame, columns)
    selected_events = _select_latest_scope_events(
        event_rows,
        columns=columns,
        scope_contract=scope_contract,
        build_run_id=build_run_id,
        build_date=resolved_build_date,
    )
    if scope_contract.route == "v1":
        _validate_v1_secondary_event_inheritance(selected_events)

    staged = _read_build_frame(
        spark,
        tables.staging_table,
        build_run_id_column=columns.build_run_id,
        build_run_id=build_run_id,
    ).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        staging_summaries = _collect_staging_summaries(
            staged,
            columns=columns,
            scope_contract=scope_contract,
        )
        row_count = _validate_staging_against_events(
            staging_summaries,
            selected_events=selected_events,
            scope_contract=scope_contract,
            build_date=resolved_build_date,
        )

        staged_assignments = staged.select(*scope_contract.public_columns)
        validation_keys = list(scope_contract.key_columns)
        if scope_contract.publication_date_column not in validation_keys:
            validation_keys.append(scope_contract.publication_date_column)
        staged_validation = validate_unique_non_null_keys(
            staged_assignments,
            validation_keys,
        )
        if staged_validation.row_count != row_count:
            raise ValueError(
                "Validated assignment row count does not match staging events"
            )

        validate_target_columns(
            spark,
            tables.history_table,
            scope_contract.public_columns,
        )
        validate_target_columns(
            spark,
            tables.latest_table,
            scope_contract.public_columns,
        )

        public_assignments = staged_assignments
        if scope_contract.route == "v1":
            public_assignments = _prepare_v1_public_assignments(
                spark,
                staged_assignments,
                tables=tables,
                scope_contract=scope_contract,
                selected_events=selected_events,
                build_date=resolved_build_date,
            )

        publication = publish_history_and_latest(
            spark,
            public_assignments,
            history_table=tables.history_table,
            latest_table=tables.latest_table,
            key_columns=scope_contract.key_columns,
            run_date=resolved_build_date,
            run_date_column=scope_contract.publication_date_column,
            columns=scope_contract.public_columns,
        )
        return AssignmentPublicationResult(
            route=scope_contract.route,
            build_run_id=build_run_id,
            build_date=resolved_build_date,
            row_count=publication.validation.row_count,
            events=selected_events,
            validation=publication.validation,
            history_write=publication.history_write,
            latest_write=publication.latest_write,
            provenance=selected_events[0].provenance,
        )
    finally:
        staged.unpersist()


def publish_bulk_assignment_build(
    spark: Any,
    assignments: DataFrame,
    *,
    tables: AssignmentTableContract,
    columns: AssignmentColumnContract,
    scope_contract: AssignmentScopeContract,
    build_run_id: str,
    build_date: date | datetime | str,
    task_run_id: int,
    execution_count: int,
    provenance: AssignmentProvenance,
    git_commit: str,
) -> AssignmentPublicationResult:
    """Validate one route graph, write history once, then advance live last."""
    _validate_contract_compatibility(columns, scope_contract)
    _validate_build_identity(
        build_run_id,
        columns=columns,
        scope_contract=scope_contract,
    )
    resolved_date = _normalise_build_date(build_date, label=columns.build_date)
    resolved_task = _validate_integer(
        task_run_id, label=columns.task_run_id, minimum=1
    )
    resolved_execution = _validate_integer(
        execution_count, label=columns.execution_count
    )
    _require_non_empty(git_commit, label="GitCommit")
    event_columns = (
        columns.build_run_id,
        columns.route,
        columns.event_scope,
        columns.status,
        columns.row_count,
        columns.build_date,
        columns.task_run_id,
        columns.execution_count,
        columns.completed_at,
        *_provenance_columns(columns),
    )
    validate_typed_table_schema(spark, tables.event_table, event_columns)
    validate_target_columns(
        spark,
        tables.history_table,
        scope_contract.public_columns,
    )
    validate_target_columns(
        spark,
        tables.latest_table,
        scope_contract.public_columns,
    )
    attempt_id = f"{build_run_id}:attempt:{resolved_execution}:{resolved_task}"

    existing_history = find_delta_write_receipt(
        spark,
        target_table=tables.history_table,
        build_id=build_run_id,
        attempt_id=attempt_id,
    )
    if existing_history is not None:
        history_version = existing_history.delta_version
        if history_version is None:
            raise ValueError("Assignment history receipt has no Delta version")
        log_output_location(
            tables.history_table,
            kind="delta_table",
            details={
                "delta_version": history_version,
                "receipt_id": existing_history.receipt_id,
                "row_count": existing_history.row_count,
                "reused": True,
                "route": scope_contract.route,
            },
        )
        exact_history = read_delta_version(
            spark, tables.history_table, history_version
        ).where(
            F.col(scope_contract.publication_date_column)
            == F.lit(resolved_date)
        )
        latest_write = replace_table_by_name(
            exact_history.select(*scope_contract.public_columns),
            tables.latest_table,
            scope_contract.public_columns,
            spark=spark,
            build_id=build_run_id,
            attempt_id=attempt_id,
            git_commit=git_commit,
            commit_metadata={
                "route": scope_contract.route,
                "repair_from_history_version": history_version,
            },
            capture_receipt=False,
        )
        return AssignmentPublicationResult(
            route=scope_contract.route,
            build_run_id=build_run_id,
            build_date=resolved_date,
            row_count=existing_history.row_count or 0,
            events=(),
            validation=KeyValidationSummary(
                row_count=existing_history.row_count or 0,
                distinct_key_count=existing_history.row_count or 0,
                null_key_count=0,
            ),
            history_write=existing_history,
            latest_write=latest_write,
            provenance=provenance,
        )

    input_columns = tuple(
        column
        for column in scope_contract.public_columns
        if column != scope_contract.publication_date_column
    )
    _require_columns(assignments, input_columns, label="Bulk assignment frame")
    public_assignments = with_run_date(
        assignments.select(*input_columns),
        resolved_date,
        column=scope_contract.publication_date_column,
    ).select(*scope_contract.public_columns)
    if scope_contract.route == "v1":
        public_assignments = _remove_invalid_v1_teaser_assignments(
            public_assignments
        )
    # One Delta transaction remains a distributed Spark write.  Spread each
    # route across enough scope/account partitions to occupy the existing
    # four-worker D32 cluster without forcing the expanded frame into memory.
    # The v1 route expands roughly 12 million customers across 79 locations;
    # v2 expands them across five page types and several ranks.  These counts
    # keep files and shuffle blocks bounded on the existing four-worker D32
    # clusters while retaining enough tasks for all worker cores to stay busy.
    target_partitions = 2048 if scope_contract.route == "v1" else 512
    public_assignments = public_assignments.repartition(
        target_partitions,
        F.col(scope_contract.scope_column),
        F.col("AccountNumber"),
    ).persist(StorageLevel.DISK_ONLY)

    validation_keys = list(scope_contract.key_columns)
    if scope_contract.publication_date_column not in validation_keys:
        validation_keys.append(scope_contract.publication_date_column)
    null_key = None
    for key in validation_keys:
        condition = F.col(key).isNull()
        null_key = condition if null_key is None else (null_key | condition)
    keyed = (
        public_assignments.withColumn("_null_key", null_key)
        .groupBy(*validation_keys)
        .agg(
            F.count(F.lit(1)).alias("_key_count"),
            F.max(F.col("_null_key").cast("int")).alias("_has_null_key"),
        )
    )
    scope_rows = (
        keyed.groupBy(scope_contract.scope_column)
        .agg(
            F.sum("_key_count").alias("row_count"),
            F.sum(
                F.when(
                    F.col("_key_count") > 1, F.col("_key_count") - 1
                ).otherwise(F.lit(0))
            ).alias("duplicate_count"),
            F.sum(
                F.when(
                    F.col("_has_null_key") == 1, F.col("_key_count")
                ).otherwise(F.lit(0))
            ).alias("null_count"),
        )
        .collect()
    )
    summaries = {
        row[scope_contract.scope_column]: (
            int(row["row_count"]),
            int(row["duplicate_count"] or 0),
            int(row["null_count"] or 0),
        )
        for row in scope_rows
    }
    unexpected = sorted(set(summaries) - set(scope_contract.expected_scopes))
    if unexpected:
        raise ValueError(
            "Unexpected assignment scopes: " + ", ".join(unexpected)
        )
    duplicate_count = sum(value[1] for value in summaries.values())
    null_count = sum(value[2] for value in summaries.values())
    if duplicate_count or null_count:
        raise ValueError(
            "Final assignment key validation failed: "
            f"duplicates={duplicate_count}, nulls={null_count}"
        )
    row_count = sum(value[0] for value in summaries.values())
    completed = datetime.now(timezone.utc)
    events = tuple(
        AssignmentScopeEvent(
            scope=scope,
            status=READY if summaries.get(scope, (0, 0, 0))[0] else NO_ADS,
            row_count=summaries.get(scope, (0, 0, 0))[0],
            build_date=resolved_date,
            task_run_id=resolved_task,
            execution_count=resolved_execution,
            completed_at=completed,
            provenance=provenance,
        )
        for scope in scope_contract.expected_scopes
    )
    scope_commit_results = {
        event.scope: {"status": event.status, "row_count": event.row_count}
        for event in events
    }
    event_rows = [
        {
            columns.build_run_id: build_run_id,
            columns.route: scope_contract.route,
            columns.event_scope: event.scope,
            columns.status: event.status,
            columns.row_count: event.row_count,
            columns.build_date: resolved_date,
            columns.task_run_id: resolved_task,
            columns.execution_count: resolved_execution,
            columns.completed_at: completed,
            **provenance.column_values(columns),
        }
        for event in events
    ]
    event_frame = typed_table_frame(spark, tables.event_table, event_rows)
    replace_scope_by_name(
        event_frame,
        tables.event_table,
        {
            columns.build_run_id: build_run_id,
            columns.task_run_id: resolved_task,
            columns.execution_count: resolved_execution,
        },
        event_frame.columns,
        spark=spark,
        build_id=build_run_id,
        attempt_id=attempt_id,
        git_commit=git_commit,
        commit_metadata={
            "route": scope_contract.route,
            "scope_results": scope_commit_results,
        },
        capture_receipt=False,
    )
    history_write = replace_scope_by_name(
        public_assignments,
        tables.history_table,
        {scope_contract.publication_date_column: resolved_date},
        scope_contract.public_columns,
        spark=spark,
        build_id=build_run_id,
        attempt_id=attempt_id,
        git_commit=git_commit,
        commit_metadata={
            "route": scope_contract.route,
            "scope_results": scope_commit_results,
        },
    )
    latest_write = replace_table_by_name(
        public_assignments,
        tables.latest_table,
        scope_contract.public_columns,
        spark=spark,
        build_id=build_run_id,
        attempt_id=attempt_id,
        git_commit=git_commit,
        commit_metadata={
            "route": scope_contract.route,
            "scope_results": scope_commit_results,
            "history_delta_version": history_write.delta_version,
        },
        capture_receipt=False,
    )
    return AssignmentPublicationResult(
        route=scope_contract.route,
        build_run_id=build_run_id,
        build_date=resolved_date,
        row_count=row_count,
        events=events,
        validation=KeyValidationSummary(
            row_count=row_count,
            distinct_key_count=row_count,
            null_key_count=0,
        ),
        history_write=history_write,
        latest_write=latest_write,
        provenance=provenance,
    )


__all__ = [
    "NO_ADS",
    "READY",
    "AssignmentColumnContract",
    "AssignmentProvenance",
    "AssignmentPublicationResult",
    "AssignmentScopeContract",
    "AssignmentScopeEvent",
    "AssignmentStageResult",
    "AssignmentTableContract",
    "stage_assignment_scope",
    "publish_bulk_assignment_build",
    "validate_and_publish_assignment_build",
]

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from next_ads.common.delta_writes import (
    DeltaWriteReceipt,
    find_delta_write_receipt,
    replace_scope_by_name,
    typed_table_frame,
    validate_typed_table_schema,
)


CANDIDATE_CONTRACT_VERSION = "nextads_candidates/v2"
CANDIDATE_POLICY_VERSION = "nextads_candidate_policy/v1"
READY_FOR_NEXTADS = "READY_FOR_NEXTADS"
SERVING = "SERVING"
MAX_CANDIDATES_PER_AD_SET = 20
CANDIDATE_SCORE_COLUMNS = (
    "CandidateBuildID",
    "CandidateBuildAttemptID",
    "RunDate",
    "Route",
    "PortfolioEntryID",
    "ServingSlot",
    "ExperimentID",
    "VariantID",
    "ProviderBuildID",
    "ProviderBuildAttemptID",
    "AccountNumber",
    "AdSetID",
    "UniqueAdID",
    "Score",
    "TriggerScore",
    "Rank",
    "CandidateID",
)
CANDIDATE_AD_SET_COLUMNS = (
    "CandidateBuildID",
    "CandidateBuildAttemptID",
    "RunDate",
    "Route",
    "AdSetID",
    "ScopeType",
    "ScopeValue",
    "UniqueAdID",
)
CANDIDATE_BUILD_COLUMNS = (
    "CandidateBuildID",
    "CandidateBuildAttemptID",
    "RunDate",
    "Route",
    "OutputGrain",
    "PortfolioID",
    "PortfolioAttemptID",
    "CandidateFoundationSnapshotID",
    "ControlTable",
    "ControlDeltaVersion",
    "CandidateContractVersion",
    "CandidatePolicyVersion",
    "CandidatePolicyChecksum",
    "ProviderBindingsJSON",
    "Status",
    "EntryCount",
    "OutputBindingsJSON",
    "GitCommit",
    "RuntimeMs",
    "TaskRunID",
    "ExecutionCount",
    "CompletedAt",
)


@dataclass(frozen=True)
class ServingPortfolioEntry:
    portfolio_id: str
    portfolio_attempt_id: str
    portfolio_entry_id: str
    provider_build_id: str
    provider_build_attempt_id: str
    provider_output_table: str
    provider_output_delta_version: int
    provider_source_run_date: date
    input_snapshot_id: str
    serving_slot: str
    experiment_id: str
    variant_id: str

    @property
    def provider_binding(self) -> tuple[Any, ...]:
        return (
            self.provider_build_id,
            self.provider_build_attempt_id,
            self.provider_output_table,
            self.provider_output_delta_version,
            self.provider_source_run_date,
            self.input_snapshot_id,
        )


@dataclass(frozen=True)
class CandidateBuildContext:
    candidate_build_id: str
    candidate_build_attempt_id: str
    run_date: date
    route: str
    output_grain: str
    portfolio_id: str
    portfolio_attempt_id: str
    candidate_foundation_snapshot_id: str
    control_table: str
    control_delta_version: int
    candidate_policy_version: str
    candidate_policy_checksum: str
    provider_bindings_json: str
    task_run_id: int
    execution_count: int
    git_commit: str


@dataclass(frozen=True)
class CandidateBuild:
    candidate_build_id: str
    candidate_build_attempt_id: str
    run_date: date
    route: str
    portfolio_id: str
    portfolio_attempt_id: str
    candidate_foundation_snapshot_id: str
    status: str
    completed_at: datetime
    task_run_id: int
    execution_count: int


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def candidate_policy_checksum(
    cfg: Mapping[str, Any],
    *,
    output_grain: str,
    apply_ad_feedback: bool,
    ad_feedback_weight: float,
) -> str:
    """Bind the existing eligibility, feedback, exposure and rank policy."""
    payload = {
        "policy_version": CANDIDATE_POLICY_VERSION,
        "output_grain": output_grain,
        "eligibility": {
            "audience_only_excluded": True,
            "theme_required": True,
            "auto_trading_switch": cfg["incrementality"][
                "auto_trading_switch"
            ],
            "greedy_themes": cfg.get("greedy_themes", {}),
            "age_difference_min": 0,
            "age_difference_max": 1,
            "one_ad_per_theme": True,
        },
        "feedback": {
            "enabled": bool(apply_ad_feedback),
            "weight": float(ad_feedback_weight),
            "minimum_control_sessions": cfg["results_prm"]["min_c_sessions"],
            "lookback_days": cfg["incrementality"]["incremental_lookback"],
        },
        "exposure": {
            "lookback_days": 7,
            "session_weights": {
                "3": 0.84,
                "4": 0.8,
                "5": 0.7,
                "6_plus": 0.5,
            },
        },
        "ranking": {
            "maximum_candidates_per_ad_set": MAX_CANDIDATES_PER_AD_SET,
            "theme_tie_seed": 13,
            "ad_set_tie_seed": 17,
        },
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def validate_assignment_rank_limit(
    route_configuration: Mapping[str, Any],
    *,
    maximum_rank: int = MAX_CANDIDATES_PER_AD_SET,
) -> None:
    requested = []

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, nested in value.items():
                if key == "return_ranks":
                    if not isinstance(nested, list):
                        raise ValueError("return_ranks must be a list")
                    requested.extend(nested)
                else:
                    visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    visit(route_configuration)
    invalid = sorted(
        {
            repr(rank)
            for rank in requested
            if isinstance(rank, bool)
            or not isinstance(rank, int)
            or rank < 1
            or rank > maximum_rank
        }
    )
    if invalid:
        raise ValueError(
            "Assignment configuration requests candidate rank(s) outside "
            f"1-{maximum_rank}: {', '.join(invalid)}"
        )


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value.strip()


def _non_negative_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{label} must be a non-negative integer")
    return value


def _row_value(row: Mapping[str, Any], name: str) -> Any:
    if name not in row:
        raise ValueError(f"Portfolio entry is missing {name}")
    return row[name]


def load_serving_portfolio_entries(
    spark: Any,
    *,
    entries_table: str,
    portfolio_id: str,
    portfolio_attempt_id: str,
) -> tuple[ServingPortfolioEntry, ...]:
    rows = (
        spark.table(entries_table)
        .where(F.col("PortfolioID") == portfolio_id)
        .where(F.col("PortfolioAttemptID") == portfolio_attempt_id)
        .where(F.col("ExecutionMode") == SERVING)
        .collect()
    )
    entries = []
    for raw in rows:
        row = raw.asDict(recursive=True)
        source_date = _row_value(row, "ProviderSourceRunDate")
        if isinstance(source_date, datetime) or not isinstance(
            source_date, date
        ):
            raise ValueError("ProviderSourceRunDate must be a date")
        entries.append(
            ServingPortfolioEntry(
                portfolio_id=_required_text(
                    _row_value(row, "PortfolioID"), "PortfolioID"
                ),
                portfolio_attempt_id=_required_text(
                    _row_value(row, "PortfolioAttemptID"),
                    "PortfolioAttemptID",
                ),
                portfolio_entry_id=_required_text(
                    _row_value(row, "PortfolioEntryID"),
                    "PortfolioEntryID",
                ),
                provider_build_id=_required_text(
                    _row_value(row, "ProviderBuildID"),
                    "ProviderBuildID",
                ),
                provider_build_attempt_id=_required_text(
                    _row_value(row, "ProviderBuildAttemptID"),
                    "ProviderBuildAttemptID",
                ),
                provider_output_table=_required_text(
                    _row_value(row, "ProviderOutputTable"),
                    "ProviderOutputTable",
                ),
                provider_output_delta_version=_non_negative_int(
                    _row_value(row, "ProviderOutputDeltaVersion"),
                    "ProviderOutputDeltaVersion",
                ),
                provider_source_run_date=source_date,
                input_snapshot_id=_required_text(
                    _row_value(row, "InputSnapshotID"), "InputSnapshotID"
                ),
                serving_slot=_required_text(
                    _row_value(row, "ServingSlot"), "ServingSlot"
                ),
                experiment_id=_required_text(
                    _row_value(row, "ExperimentID"), "ExperimentID"
                ),
                variant_id=_required_text(
                    _row_value(row, "VariantID"), "VariantID"
                ),
            )
        )
    if not entries:
        raise ValueError("Portfolio attempt contains no serving entries")
    slots = [entry.serving_slot for entry in entries]
    if len(slots) != len(set(slots)):
        raise ValueError("Portfolio attempt contains duplicate serving slots")
    return tuple(sorted(entries, key=lambda entry: entry.portfolio_entry_id))


def group_serving_entries(
    entries: Iterable[ServingPortfolioEntry],
) -> tuple[tuple[ServingPortfolioEntry, ...], ...]:
    grouped: dict[tuple[Any, ...], list[ServingPortfolioEntry]] = {}
    for entry in entries:
        grouped.setdefault(entry.provider_binding, []).append(entry)
    return tuple(
        tuple(sorted(group, key=lambda entry: entry.portfolio_entry_id))
        for _, group in sorted(grouped.items(), key=lambda item: repr(item[0]))
    )


def build_candidate_context(
    *,
    run_date: date,
    route: str,
    output_grain: str,
    entries: tuple[ServingPortfolioEntry, ...],
    candidate_foundation_snapshot_id: str,
    control_table: str,
    control_delta_version: int,
    candidate_policy_checksum_value: str,
    task_run_id: int,
    execution_count: int,
    git_commit: str,
) -> CandidateBuildContext:
    if not entries:
        raise ValueError("Candidate build requires serving portfolio entries")
    portfolio_ids = {entry.portfolio_id for entry in entries}
    portfolio_attempt_ids = {entry.portfolio_attempt_id for entry in entries}
    if len(portfolio_ids) != 1 or len(portfolio_attempt_ids) != 1:
        raise ValueError("Candidate entries must share one portfolio attempt")
    bindings = [
        {
            "portfolio_entry_id": entry.portfolio_entry_id,
            "provider_build_id": entry.provider_build_id,
            "provider_build_attempt_id": entry.provider_build_attempt_id,
            "provider_output_table": entry.provider_output_table,
            "provider_output_delta_version": (
                entry.provider_output_delta_version
            ),
            "provider_source_run_date": (
                entry.provider_source_run_date.isoformat()
            ),
            "input_snapshot_id": entry.input_snapshot_id,
            "serving_slot": entry.serving_slot,
            "experiment_id": entry.experiment_id,
            "variant_id": entry.variant_id,
        }
        for entry in entries
    ]
    identity = {
        "contract_version": CANDIDATE_CONTRACT_VERSION,
        "run_date": run_date.isoformat(),
        "route": route,
        "output_grain": output_grain,
        "portfolio_id": next(iter(portfolio_ids)),
        "candidate_foundation_snapshot_id": (candidate_foundation_snapshot_id),
        "control_table": control_table,
        "control_delta_version": control_delta_version,
        "candidate_policy_checksum": candidate_policy_checksum_value,
        "provider_bindings": bindings,
        "git_commit": _required_text(git_commit, "git_commit"),
    }
    digest = hashlib.sha256(
        canonical_json(identity).encode("utf-8")
    ).hexdigest()
    build_id = f"candidates_{route}_{run_date:%Y%m%d}_{digest[:20]}"
    return CandidateBuildContext(
        candidate_build_id=build_id,
        candidate_build_attempt_id=(
            f"{build_id}:attempt:{execution_count}:{task_run_id}"
        ),
        run_date=run_date,
        route=_required_text(route, "route"),
        output_grain=_required_text(output_grain, "output_grain"),
        portfolio_id=next(iter(portfolio_ids)),
        portfolio_attempt_id=next(iter(portfolio_attempt_ids)),
        candidate_foundation_snapshot_id=_required_text(
            candidate_foundation_snapshot_id,
            "candidate_foundation_snapshot_id",
        ),
        control_table=_required_text(control_table, "control_table"),
        control_delta_version=_non_negative_int(
            control_delta_version, "control_delta_version"
        ),
        candidate_policy_version=CANDIDATE_POLICY_VERSION,
        candidate_policy_checksum=_required_text(
            candidate_policy_checksum_value,
            "candidate_policy_checksum",
        ),
        provider_bindings_json=canonical_json(bindings),
        task_run_id=_non_negative_int(task_run_id, "task_run_id"),
        execution_count=_non_negative_int(execution_count, "execution_count"),
        git_commit=_required_text(git_commit, "git_commit"),
    )


class CandidateBuildPublisher:
    """Materialise each canonical candidate table once, then publish READY."""

    def __init__(
        self,
        spark: Any,
        context: CandidateBuildContext,
        *,
        builds_table: str,
        scores_table: str,
        ad_sets_table: str,
        group_column: str,
    ) -> None:
        self.spark = spark
        self.context = context
        self.builds_table = builds_table
        self.scores_table = scores_table
        self.ad_sets_table = ad_sets_table
        self.group_column = group_column
        validate_typed_table_schema(
            spark,
            builds_table,
            CANDIDATE_BUILD_COLUMNS,
        )
        validate_typed_table_schema(
            spark,
            scores_table,
            CANDIDATE_SCORE_COLUMNS,
            nullable_columns=("TriggerScore",),
        )
        validate_typed_table_schema(
            spark,
            ad_sets_table,
            CANDIDATE_AD_SET_COLUMNS,
        )
        self._ad_sets: DataFrame | None = None
        self._score_frames: list[DataFrame] = []
        self._published_entry_ids: set[str] = set()
        self._started_at = time.monotonic()

    def _ordered(self, frame: DataFrame, table: str) -> DataFrame:
        return frame.select(*self.spark.table(table).columns)

    def publish_provider(
        self,
        entries: tuple[ServingPortfolioEntry, ...],
        ranked_scores: DataFrame,
        ad_set_to_group: DataFrame,
        ad_to_ad_set: DataFrame,
    ) -> None:
        context = self.context
        if self._ad_sets is None:
            self._ad_sets = self._ordered(
                ad_to_ad_set.join(ad_set_to_group, "AdSetID", "inner").select(
                    F.lit(context.candidate_build_id).alias(
                        "CandidateBuildID"
                    ),
                    F.lit(context.candidate_build_attempt_id).alias(
                        "CandidateBuildAttemptID"
                    ),
                    F.lit(context.run_date).cast("date").alias("RunDate"),
                    F.lit(context.route).alias("Route"),
                    "AdSetID",
                    F.lit(context.output_grain).alias("ScopeType"),
                    F.col(self.group_column)
                    .cast("string")
                    .alias("ScopeValue"),
                    "UniqueAdID",
                ),
                self.ad_sets_table,
            )

        duplicate_entries = sorted(
            entry.portfolio_entry_id
            for entry in entries
            if entry.portfolio_entry_id in self._published_entry_ids
        )
        if duplicate_entries:
            raise ValueError(
                "Candidate portfolio entry was published twice: "
                + ", ".join(duplicate_entries)
            )

        # Expand one provider calculation with a tiny literal relation.  This
        # keeps one expensive ranked lineage even when the same provider build
        # occupies multiple serving slots.
        entry_structs = [
            F.struct(
                F.lit(entry.portfolio_entry_id)
                .cast("string")
                .alias("PortfolioEntryID"),
                F.lit(entry.serving_slot).cast("string").alias("ServingSlot"),
                F.lit(entry.experiment_id)
                .cast("string")
                .alias("ExperimentID"),
                F.lit(entry.variant_id).cast("string").alias("VariantID"),
                F.lit(entry.provider_build_id)
                .cast("string")
                .alias("ProviderBuildID"),
                F.lit(entry.provider_build_attempt_id)
                .cast("string")
                .alias("ProviderBuildAttemptID"),
            )
            for entry in entries
        ]
        entry_frame = (
            self.spark.range(1)
            .select(F.explode(F.array(*entry_structs)).alias("entry"))
            .select("entry.*")
        )
        scores = (
            ranked_scores.where(
                F.col("Rank") <= F.lit(MAX_CANDIDATES_PER_AD_SET)
            )
            .crossJoin(F.broadcast(entry_frame))
            .select(
                F.lit(context.candidate_build_id).alias("CandidateBuildID"),
                F.lit(context.candidate_build_attempt_id).alias(
                    "CandidateBuildAttemptID"
                ),
                F.lit(context.run_date).cast("date").alias("RunDate"),
                F.lit(context.route).alias("Route"),
                "PortfolioEntryID",
                "ServingSlot",
                "ExperimentID",
                "VariantID",
                "ProviderBuildID",
                "ProviderBuildAttemptID",
                "AccountNumber",
                "AdSetID",
                "UniqueAdID",
                F.col("Score").cast("double").alias("Score"),
                F.col("TriggerScore").cast("double").alias("TriggerScore"),
                F.col("Rank").cast("int").alias("Rank"),
            )
            .withColumn(
                "CandidateID",
                F.concat(
                    F.lit("candidate_"),
                    F.sha2(
                        F.concat_ws(
                            "\u001f",
                            "CandidateBuildID",
                            "PortfolioEntryID",
                            "AccountNumber",
                            "AdSetID",
                            "UniqueAdID",
                        ),
                        256,
                    ),
                ),
            )
        )
        self._score_frames.append(self._ordered(scores, self.scores_table))
        self._published_entry_ids.update(
            entry.portfolio_entry_id for entry in entries
        )

    def _write_once(
        self,
        frame: DataFrame,
        table: str,
    ) -> DeltaWriteReceipt:
        context = self.context
        existing = find_delta_write_receipt(
            self.spark,
            target_table=table,
            build_id=context.candidate_build_id,
            attempt_id=context.candidate_build_attempt_id,
        )
        if existing is not None:
            return existing
        return replace_scope_by_name(
            frame,
            table,
            {"CandidateBuildAttemptID": (context.candidate_build_attempt_id)},
            frame.columns,
            spark=self.spark,
            build_id=context.candidate_build_id,
            attempt_id=context.candidate_build_attempt_id,
            git_commit=context.git_commit,
        )

    def finalize(
        self,
        entries: tuple[ServingPortfolioEntry, ...],
        *,
        completed_at: datetime | None = None,
        before_ready: Callable[[CandidateBuild], None] | None = None,
    ) -> CandidateBuild:
        expected_entries = {entry.portfolio_entry_id for entry in entries}
        if self._published_entry_ids != expected_entries:
            missing = sorted(expected_entries - self._published_entry_ids)
            raise ValueError(
                "Candidate build is missing serving entries: "
                + ", ".join(missing)
            )
        context = self.context
        if not self._score_frames or self._ad_sets is None:
            raise ValueError("Candidate build contains no canonical outputs")
        scores = self._score_frames[0]
        for frame in self._score_frames[1:]:
            scores = scores.unionByName(frame)
        ad_sets_receipt = self._write_once(self._ad_sets, self.ad_sets_table)
        scores_receipt = self._write_once(scores, self.scores_table)
        completed = completed_at or datetime.now(timezone.utc)
        build = CandidateBuild(
            candidate_build_id=context.candidate_build_id,
            candidate_build_attempt_id=context.candidate_build_attempt_id,
            run_date=context.run_date,
            route=context.route,
            portfolio_id=context.portfolio_id,
            portfolio_attempt_id=context.portfolio_attempt_id,
            candidate_foundation_snapshot_id=(
                context.candidate_foundation_snapshot_id
            ),
            status=READY_FOR_NEXTADS,
            completed_at=completed,
            task_run_id=context.task_run_id,
            execution_count=context.execution_count,
        )
        output_bindings = {
            "candidate_ad_sets": ad_sets_receipt.as_binding(),
            "candidate_scores": scores_receipt.as_binding(),
        }
        row = {
            "CandidateBuildID": build.candidate_build_id,
            "CandidateBuildAttemptID": build.candidate_build_attempt_id,
            "RunDate": build.run_date,
            "Route": build.route,
            "OutputGrain": context.output_grain,
            "PortfolioID": build.portfolio_id,
            "PortfolioAttemptID": build.portfolio_attempt_id,
            "CandidateFoundationSnapshotID": (
                build.candidate_foundation_snapshot_id
            ),
            "ControlTable": context.control_table,
            "ControlDeltaVersion": context.control_delta_version,
            "CandidateContractVersion": CANDIDATE_CONTRACT_VERSION,
            "CandidatePolicyVersion": context.candidate_policy_version,
            "CandidatePolicyChecksum": context.candidate_policy_checksum,
            "ProviderBindingsJSON": context.provider_bindings_json,
            "Status": build.status,
            "EntryCount": len(entries),
            "OutputBindingsJSON": canonical_json(output_bindings),
            "GitCommit": context.git_commit,
            "RuntimeMs": int((time.monotonic() - self._started_at) * 1000),
            "TaskRunID": build.task_run_id,
            "ExecutionCount": build.execution_count,
            "CompletedAt": build.completed_at,
        }
        if before_ready is not None:
            before_ready(build)
        frame = typed_table_frame(self.spark, self.builds_table, [row])
        replace_scope_by_name(
            frame,
            self.builds_table,
            {"CandidateBuildAttemptID": build.candidate_build_attempt_id},
            frame.columns,
            spark=self.spark,
            build_id=context.candidate_build_id,
            attempt_id=context.candidate_build_attempt_id,
            git_commit=context.git_commit,
            capture_receipt=False,
        )
        return build


def select_candidate_build(
    builds: Iterable[CandidateBuild],
    *,
    run_date: date,
    route: str,
) -> CandidateBuild:
    accepted = [
        build
        for build in builds
        if build.run_date == run_date
        and build.route == route
        and build.status == READY_FOR_NEXTADS
    ]
    if not accepted:
        raise ValueError(f"No accepted candidate build for {route} {run_date}")
    return max(
        accepted,
        key=lambda build: (
            build.execution_count,
            build.completed_at,
            build.task_run_id,
            build.candidate_build_attempt_id,
        ),
    )


__all__ = [
    "CANDIDATE_AD_SET_COLUMNS",
    "CANDIDATE_BUILD_COLUMNS",
    "CANDIDATE_SCORE_COLUMNS",
    "CANDIDATE_CONTRACT_VERSION",
    "CANDIDATE_POLICY_VERSION",
    "CandidateBuild",
    "CandidateBuildContext",
    "CandidateBuildPublisher",
    "MAX_CANDIDATES_PER_AD_SET",
    "ServingPortfolioEntry",
    "build_candidate_context",
    "candidate_policy_checksum",
    "group_serving_entries",
    "load_serving_portfolio_entries",
    "select_candidate_build",
    "validate_assignment_rank_limit",
]

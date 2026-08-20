from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from next_ads.decisioning.assignment_publication import AssignmentProvenance


READY_FOR_NEXTADS = "READY_FOR_NEXTADS"
REQUIRED_PUBLIC_SLOTS = ("best", "best_challenger")


@dataclass(frozen=True)
class AcceptedCandidateInputs:
    """One immutable candidate attempt and its public serving-slot bindings."""

    provenance: AssignmentProvenance
    route: str
    output_grain: str
    slot_entries: dict[str, str]
    scores: DataFrame
    ad_sets: DataFrame

    def candidates_for_scope(
        self,
        serving_slot: str,
        scope: str,
    ) -> DataFrame:
        if serving_slot not in self.slot_entries:
            raise ValueError(
                f"Accepted candidates do not contain slot {serving_slot!r}"
            )
        scope_rows = (
            self.ad_sets.where(F.col("ScopeType") == self.output_grain)
            .where(F.col("ScopeValue") == scope)
            .select("AdSetID", "UniqueAdID")
        )
        return (
            self.scores.where(
                F.col("PortfolioEntryID")
                == self.slot_entries[serving_slot]
            )
            .join(scope_rows, ["AdSetID", "UniqueAdID"], "inner")
            .select(
                "AccountNumber",
                "UniqueAdID",
                "Score",
                "TriggerScore",
                "Rank",
            )
        )


_CANDIDATE_INPUT_CACHE: dict[tuple[Any, ...], AcceptedCandidateInputs] = {}


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value.strip()


def _require_columns(frame: DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{label} is missing columns: {', '.join(missing)}")


def _slot_entries(provider_bindings_json: str) -> dict[str, str]:
    try:
        bindings = json.loads(provider_bindings_json)
    except json.JSONDecodeError as exc:
        raise ValueError("ProviderBindingsJSON must contain valid JSON") from exc
    if not isinstance(bindings, list):
        raise ValueError("ProviderBindingsJSON must contain a list")

    by_slot: dict[str, list[str]] = {}
    for index, binding in enumerate(bindings):
        if not isinstance(binding, dict):
            raise ValueError(
                f"ProviderBindingsJSON entry {index} must be an object"
            )
        slot = _required_text(
            binding.get("serving_slot"),
            f"ProviderBindingsJSON entry {index}.serving_slot",
        )
        entry_id = _required_text(
            binding.get("portfolio_entry_id"),
            f"ProviderBindingsJSON entry {index}.portfolio_entry_id",
        )
        by_slot.setdefault(slot, []).append(entry_id)

    resolved = {}
    for slot in REQUIRED_PUBLIC_SLOTS:
        entries = by_slot.get(slot, [])
        if len(entries) != 1:
            raise ValueError(
                f"Accepted candidate build must contain exactly one {slot} "
                f"entry; found {len(entries)}"
            )
        resolved[slot] = entries[0]
    return resolved


def load_accepted_candidate_inputs(
    spark: Any,
    *,
    builds_table: str,
    scores_table: str,
    ad_sets_table: str,
    candidate_build_attempt_id: str,
    route: str,
) -> AcceptedCandidateInputs:
    """Load one READY candidate attempt without consulting mutable latest data."""
    attempt_id = _required_text(
        candidate_build_attempt_id,
        "CandidateBuildAttemptID",
    )
    if route not in {"v1", "v2"}:
        raise ValueError("Candidate route must be one of: v1, v2")
    cache_key = (
        id(spark),
        builds_table,
        scores_table,
        ad_sets_table,
        attempt_id,
        route,
    )
    cached = _CANDIDATE_INPUT_CACHE.get(cache_key)
    if cached is not None:
        return cached

    builds = spark.table(builds_table)
    _require_columns(
        builds,
        {
            "CandidateBuildID",
            "CandidateBuildAttemptID",
            "Route",
            "OutputGrain",
            "PortfolioID",
            "PortfolioAttemptID",
            "CandidateFoundationSnapshotID",
            "ProviderBindingsJSON",
            "Status",
        },
        f"Candidate build table {builds_table}",
    )
    header_rows = (
        builds.where(F.col("CandidateBuildAttemptID") == attempt_id)
        .where(F.col("Route") == route)
        .where(F.col("Status") == READY_FOR_NEXTADS)
        .collect()
    )
    if len(header_rows) != 1:
        raise ValueError(
            "Expected exactly one READY candidate build for attempt "
            f"{attempt_id!r} and route {route}; found {len(header_rows)}"
        )
    header = header_rows[0].asDict(recursive=True)
    expected_grain = "location" if route == "v1" else "page_type"
    output_grain = _required_text(header["OutputGrain"], "OutputGrain")
    if output_grain != expected_grain:
        raise ValueError(
            f"Candidate route {route} requires {expected_grain} grain, "
            f"found {output_grain}"
        )
    candidate_build_id = _required_text(
        header["CandidateBuildID"], "CandidateBuildID"
    )
    provenance = AssignmentProvenance(
        candidate_build_id=candidate_build_id,
        candidate_build_attempt_id=attempt_id,
        portfolio_id=_required_text(header["PortfolioID"], "PortfolioID"),
        portfolio_attempt_id=_required_text(
            header["PortfolioAttemptID"], "PortfolioAttemptID"
        ),
        candidate_foundation_snapshot_id=_required_text(
            header["CandidateFoundationSnapshotID"],
            "CandidateFoundationSnapshotID",
        ),
    )
    slot_entries = _slot_entries(
        _required_text(header["ProviderBindingsJSON"], "ProviderBindingsJSON")
    )

    scores = spark.table(scores_table)
    _require_columns(
        scores,
        {
            "CandidateBuildID",
            "CandidateBuildAttemptID",
            "Route",
            "PortfolioEntryID",
            "AccountNumber",
            "AdSetID",
            "UniqueAdID",
            "Score",
            "TriggerScore",
            "Rank",
        },
        f"Candidate score table {scores_table}",
    )
    scores = (
        scores.where(F.col("CandidateBuildAttemptID") == attempt_id)
        .where(F.col("CandidateBuildID") == candidate_build_id)
        .where(F.col("Route") == route)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    ad_sets = spark.table(ad_sets_table)
    _require_columns(
        ad_sets,
        {
            "CandidateBuildID",
            "CandidateBuildAttemptID",
            "Route",
            "AdSetID",
            "ScopeType",
            "ScopeValue",
            "UniqueAdID",
        },
        f"Candidate ad-set table {ad_sets_table}",
    )
    ad_sets = (
        ad_sets.where(F.col("CandidateBuildAttemptID") == attempt_id)
        .where(F.col("CandidateBuildID") == candidate_build_id)
        .where(F.col("Route") == route)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    result = AcceptedCandidateInputs(
        provenance=provenance,
        route=route,
        output_grain=output_grain,
        slot_entries=slot_entries,
        scores=scores,
        ad_sets=ad_sets,
    )
    _CANDIDATE_INPUT_CACHE[cache_key] = result
    return result


def clear_candidate_input_cache() -> None:
    """Release candidate frames shared by repeated scopes in one bulk task."""
    while _CANDIDATE_INPUT_CACHE:
        _, candidate_inputs = _CANDIDATE_INPUT_CACHE.popitem()
        candidate_inputs.scores.unpersist()
        candidate_inputs.ad_sets.unpersist()


__all__ = [
    "AcceptedCandidateInputs",
    "clear_candidate_input_cache",
    "load_accepted_candidate_inputs",
]

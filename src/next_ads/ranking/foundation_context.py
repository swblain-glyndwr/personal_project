from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from typing import Any

from pyspark.sql import functions as F

from next_ads.common.delta_writes import (
    quote_identifier,
    quote_qualified_identifier,
    typed_table_frame,
)


ACTIVE = "ACTIVE"
FAILED = "FAILED"


def _non_empty_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must not be empty")
    return value.strip()


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if hasattr(value, "to_dict"):
        value = value.to_dict()
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a mapping")
    return dict(value)


@dataclass(frozen=True)
class ScoringFoundationContext:
    context_slot: str
    orchestration_run_id: int
    foundation_id: str
    foundation_version: str
    scoring_foundation_build_id: str
    scoring_foundation_build_attempt_id: str
    input_snapshot_id: str
    input_snapshot_attempt_id: str
    run_date: date
    bindings_json: str
    capability: str
    contract_version: str
    invocation_checksum: str
    expires_at: datetime

    def __post_init__(self) -> None:
        """Validate one leased provider-neutral foundation context."""
        for field in (
            "context_slot",
            "foundation_id",
            "foundation_version",
            "scoring_foundation_build_id",
            "scoring_foundation_build_attempt_id",
            "input_snapshot_id",
            "input_snapshot_attempt_id",
            "bindings_json",
            "capability",
            "contract_version",
            "invocation_checksum",
        ):
            _non_empty_text(getattr(self, field), field)
        if isinstance(self.run_date, datetime) or not isinstance(
            self.run_date, date
        ):
            raise ValueError("run_date must be a date")
        if (
            isinstance(self.orchestration_run_id, bool)
            or not isinstance(self.orchestration_run_id, int)
            or self.orchestration_run_id < 1
        ):
            raise ValueError("orchestration_run_id must be a positive integer")
        if self.expires_at.tzinfo is None:
            raise ValueError("expires_at must be timezone-aware")
        try:
            bindings = json.loads(self.bindings_json)
        except json.JSONDecodeError as error:
            raise ValueError("bindings_json must be valid JSON") from error
        if not isinstance(bindings, dict):
            raise ValueError("bindings_json must contain an object")


def build_foundation_invocation_checksum(
    foundation_config: Mapping[str, Any] | Any,
) -> str:
    """Hash foundation semantics without environment-specific table paths."""
    foundation = _mapping(foundation_config, "foundation_config")
    required_outputs = _mapping(
        foundation.get("required_outputs"),
        "foundation.required_outputs",
    )
    input_bindings = _mapping(
        foundation.get("input_bindings"),
        "foundation.input_bindings",
    )
    semantic_bindings = {}
    for name, binding in sorted(input_bindings.items()):
        definition = _mapping(binding, f"foundation.input_bindings.{name}")
        semantic_bindings[name] = {
            "schema_version": _non_empty_text(
                definition.get("schema_version"),
                f"foundation.input_bindings.{name}.schema_version",
            )
        }
    payload = {
        "foundation_id": _non_empty_text(
            foundation.get("foundation_id"),
            "foundation_id",
        ),
        "foundation_version": _non_empty_text(
            foundation.get("foundation_version"),
            "foundation_version",
        ),
        "capability": _non_empty_text(
            foundation.get("capability"),
            "capability",
        ),
        "contract_version": _non_empty_text(
            foundation.get("contract_version"),
            "contract_version",
        ),
        "required_outputs": required_outputs,
        "input_bindings": semantic_bindings,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def build_scoring_foundation_build_id(
    *,
    foundation_id: str,
    foundation_version: str,
    input_snapshot_id: str,
    invocation_checksum: str,
    run_date: date,
) -> str:
    """Build a provider- and model-neutral logical foundation identity."""
    if isinstance(run_date, datetime) or not isinstance(run_date, date):
        raise ValueError("run_date must be a date")
    values = (
        _non_empty_text(foundation_id, "foundation_id"),
        _non_empty_text(foundation_version, "foundation_version"),
        _non_empty_text(input_snapshot_id, "input_snapshot_id"),
        _non_empty_text(invocation_checksum, "invocation_checksum"),
        run_date.isoformat(),
    )
    digest = hashlib.sha256("|".join(values).encode("utf-8")).hexdigest()
    return f"{foundation_id}_{run_date:%Y%m%d}_{digest[:20]}"


def activate_foundation_context(
    spark: Any,
    *,
    context_table: str,
    context: ScoringFoundationContext,
    task_run_id: int,
    execution_count: int,
    activated_at: datetime,
    allow_serial_run_takeover: bool = False,
) -> None:
    """Claim one static pipeline slot with exact repair ownership."""
    if activated_at.tzinfo is None:
        raise ValueError("activated_at must be timezone-aware")
    if context.expires_at <= activated_at:
        raise ValueError("Foundation context lease must expire in the future")
    row = {
        "ContextSlot": context.context_slot,
        "OrchestrationRunID": context.orchestration_run_id,
        "FoundationID": context.foundation_id,
        "FoundationVersion": context.foundation_version,
        "ScoringFoundationBuildID": context.scoring_foundation_build_id,
        "ScoringFoundationBuildAttemptID": (
            context.scoring_foundation_build_attempt_id
        ),
        "InputSnapshotID": context.input_snapshot_id,
        "InputSnapshotAttemptID": context.input_snapshot_attempt_id,
        "RunDate": context.run_date,
        "BindingsJSON": context.bindings_json,
        "Capability": context.capability,
        "ContractVersion": context.contract_version,
        "InvocationChecksum": context.invocation_checksum,
        "Status": ACTIVE,
        "ExpiresAt": context.expires_at,
        "TaskRunID": task_run_id,
        "ExecutionCount": execution_count,
        "ActivatedAt": activated_at,
    }
    frame = typed_table_frame(spark, context_table, [row])
    source_view = f"_nextads_foundation_context_{uuid.uuid4().hex}"
    frame.createOrReplaceTempView(source_view)
    columns = list(row)
    assignments = ", ".join(
        f"{quote_identifier(column)} = source.{quote_identifier(column)}"
        for column in columns
    )
    insert_columns = ", ".join(quote_identifier(column) for column in columns)
    insert_values = ", ".join(
        f"source.{quote_identifier(column)}" for column in columns
    )
    serial_run_takeover = ""
    if allow_serial_run_takeover:
        serial_run_takeover = """
  OR (
    target.Status = 'ACTIVE'
    AND target.OrchestrationRunID <> source.OrchestrationRunID
    AND target.ExpiresAt > source.ActivatedAt
    AND (
      target.ActivatedAt IS NULL
      OR target.ActivatedAt < source.ActivatedAt
    )
  )
"""
    statement = f"""
MERGE INTO {quote_qualified_identifier(context_table)} AS target
USING {quote_qualified_identifier(source_view)} AS source
ON target.ContextSlot = source.ContextSlot
WHEN MATCHED AND (
  target.ExpiresAt <= source.ActivatedAt
  OR (
    target.ScoringFoundationBuildID = source.ScoringFoundationBuildID
    AND target.ScoringFoundationBuildAttemptID = source.ScoringFoundationBuildAttemptID
  )
  OR (
    target.OrchestrationRunID = source.OrchestrationRunID
    AND target.ScoringFoundationBuildID = source.ScoringFoundationBuildID
    AND source.ExecutionCount > target.ExecutionCount
  )
{serial_run_takeover}
) THEN UPDATE SET {assignments}
WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
"""
    try:
        spark.sql(statement)
    finally:
        spark.catalog.dropTempView(source_view)
    owner = load_active_foundation_context(
        spark,
        context_table=context_table,
        context_slot=context.context_slot,
        now=activated_at,
    )
    if (
        owner.orchestration_run_id != context.orchestration_run_id
        or owner.scoring_foundation_build_id
        != context.scoring_foundation_build_id
        or owner.scoring_foundation_build_attempt_id
        != context.scoring_foundation_build_attempt_id
    ):
        raise ValueError(
            f"Context slot {context.context_slot} already has an active lease"
        )


def load_reusable_failed_foundation_context(
    spark: Any,
    *,
    context_table: str,
    expected_context: ScoringFoundationContext,
    execution_count: int,
) -> ScoringFoundationContext | None:
    """Return an exact failed attempt that a repair may safely reactivate.

    The attempt identity is reused only within the same orchestration run and
    only when every immutable input and contract field still matches. The
    caller must additionally prove that the materialised output marker belongs
    to the returned attempt before activating it.
    """
    if (
        isinstance(execution_count, bool)
        or not isinstance(execution_count, int)
        or execution_count < 1
    ):
        return None
    rows = (
        spark.table(context_table)
        .where(F.col("ContextSlot") == expected_context.context_slot)
        .collect()
    )
    if not rows:
        return None
    if len(rows) != 1:
        raise ValueError(
            f"Expected one {expected_context.context_slot} context, "
            f"found {len(rows)}"
        )
    row = rows[0]
    if row["Status"] != FAILED:
        return None
    if int(row["ExecutionCount"]) >= execution_count:
        return None
    existing = _context_from_row(row)
    comparable_fields = (
        "context_slot",
        "orchestration_run_id",
        "foundation_id",
        "foundation_version",
        "scoring_foundation_build_id",
        "input_snapshot_id",
        "input_snapshot_attempt_id",
        "run_date",
        "bindings_json",
        "capability",
        "contract_version",
        "invocation_checksum",
    )
    if any(
        getattr(existing, field) != getattr(expected_context, field)
        for field in comparable_fields
    ):
        return None
    return replace(
        expected_context,
        scoring_foundation_build_attempt_id=(
            existing.scoring_foundation_build_attempt_id
        ),
    )


def transition_foundation_context(
    spark: Any,
    *,
    context_table: str,
    context: ScoringFoundationContext,
    status: str,
    completed_at: datetime,
) -> None:
    """Release a foundation lease only when all ownership fields match."""
    if status not in {"CONSUMED", "FAILED"}:
        raise ValueError(
            "Foundation context status must be CONSUMED or FAILED"
        )
    if completed_at.tzinfo is None:
        raise ValueError("completed_at must be timezone-aware")
    escaped_slot = context.context_slot.replace("'", "''")
    escaped_build = context.scoring_foundation_build_id.replace("'", "''")
    escaped_attempt = context.scoring_foundation_build_attempt_id.replace(
        "'", "''"
    )
    statement = f"""
UPDATE {quote_qualified_identifier(context_table)}
SET Status = '{status}', ExpiresAt = TIMESTAMP '{completed_at.isoformat()}'
WHERE ContextSlot = '{escaped_slot}'
  AND OrchestrationRunID = {context.orchestration_run_id}
  AND ScoringFoundationBuildID = '{escaped_build}'
  AND ScoringFoundationBuildAttemptID = '{escaped_attempt}'
  AND Status = '{ACTIVE}'
"""
    spark.sql(statement)
    rows = (
        spark.table(context_table)
        .where(F.col("ContextSlot") == context.context_slot)
        .select(
            "OrchestrationRunID",
            "ScoringFoundationBuildID",
            "ScoringFoundationBuildAttemptID",
            "Status",
        )
        .collect()
    )
    if len(rows) != 1 or (
        int(rows[0]["OrchestrationRunID"]),
        rows[0]["ScoringFoundationBuildID"],
        rows[0]["ScoringFoundationBuildAttemptID"],
        rows[0]["Status"],
    ) != (
        context.orchestration_run_id,
        context.scoring_foundation_build_id,
        context.scoring_foundation_build_attempt_id,
        status,
    ):
        raise ValueError("Foundation context release ownership check failed")


def load_active_foundation_context(
    spark: Any,
    *,
    context_table: str,
    context_slot: str,
    now: datetime | None = None,
) -> ScoringFoundationContext:
    """Load the only active, unexpired foundation context for a slot."""
    current_time = now or datetime.now(timezone.utc)
    if current_time.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    rows = (
        spark.table(context_table)
        .where(F.col("ContextSlot") == context_slot)
        .collect()
    )
    if len(rows) != 1:
        raise ValueError(
            f"Expected one active {context_slot} context, found {len(rows)}"
        )
    row = rows[0]
    if row["Status"] != ACTIVE:
        raise ValueError(f"{context_slot} foundation context is not ACTIVE")
    expires_at = row["ExpiresAt"]
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at <= current_time:
        raise ValueError(f"{context_slot} foundation context has expired")
    return _context_from_row(row)


def _context_from_row(row: Any) -> ScoringFoundationContext:
    expires_at = row["ExpiresAt"]
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    return ScoringFoundationContext(
        context_slot=row["ContextSlot"],
        orchestration_run_id=int(row["OrchestrationRunID"]),
        foundation_id=row["FoundationID"],
        foundation_version=row["FoundationVersion"],
        scoring_foundation_build_id=row["ScoringFoundationBuildID"],
        scoring_foundation_build_attempt_id=(
            row["ScoringFoundationBuildAttemptID"]
        ),
        input_snapshot_id=row["InputSnapshotID"],
        input_snapshot_attempt_id=row["InputSnapshotAttemptID"],
        run_date=row["RunDate"],
        bindings_json=row["BindingsJSON"],
        capability=row["Capability"],
        contract_version=row["ContractVersion"],
        invocation_checksum=row["InvocationChecksum"],
        expires_at=expires_at,
    )


__all__ = [
    "ScoringFoundationContext",
    "activate_foundation_context",
    "build_foundation_invocation_checksum",
    "build_scoring_foundation_build_id",
    "load_active_foundation_context",
    "load_reusable_failed_foundation_context",
    "transition_foundation_context",
]

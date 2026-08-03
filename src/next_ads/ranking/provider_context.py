from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from pyspark.sql import functions as F

from next_ads.common.delta_writes import (
    quote_identifier,
    quote_qualified_identifier,
)


ACTIVE = "ACTIVE"

_PROVIDER_INFERENCE_FIELDS = (
    "adapter",
    "capability",
    "entity_type",
    "score_direction",
    "max_entities_per_account",
    "account_number_column",
    "entity_id_column",
    "raw_score_column",
    "score_column",
)

_THEME_AFFINITY_INFERENCE_FIELDS = (
    "predict_rank_filter_threshold",
    "high_repurchase_penalty",
    "high_repurchase_manual_themes",
    "predict_table_cols",
    "model_input_cols",
)


@dataclass(frozen=True)
class ProviderContext:
    context_slot: str
    orchestration_run_id: int
    provider_id: str
    provider_build_id: str
    provider_build_attempt_id: str
    input_snapshot_id: str
    run_date: date
    model_uri: str
    bindings_json: str
    capability: str
    use_case: str
    invocation_checksum: str
    expires_at: datetime

    def __post_init__(self) -> None:
        """Validate one leased physical provider execution context."""
        for field in (
            "context_slot",
            "provider_id",
            "provider_build_id",
            "provider_build_attempt_id",
            "input_snapshot_id",
            "model_uri",
            "bindings_json",
            "capability",
            "use_case",
            "invocation_checksum",
        ):
            if not isinstance(getattr(self, field), str) or not getattr(
                self, field
            ).strip():
                raise ValueError(f"{field} must not be empty")
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


def build_provider_invocation_checksum(
    *,
    provider_id: str,
    provider_config: dict[str, Any],
    ranking_model_config: dict[str, Any] | None = None,
) -> str:
    """Hash only settings that can change a provider's inference output."""
    provider_semantics = {
        field: provider_config[field]
        for field in _PROVIDER_INFERENCE_FIELDS
        if field in provider_config
    }
    payload: dict[str, Any] = {"provider": provider_semantics}
    if provider_id == "theme_affinity":
        if ranking_model_config is None:
            raise ValueError(
                "Theme Affinity invocation requires ranking model config"
            )
        payload["ranking_model"] = {
            field: ranking_model_config[field]
            for field in _THEME_AFFINITY_INFERENCE_FIELDS
            if field in ranking_model_config
        }
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def build_provider_build_id(
    *,
    provider_id: str,
    provider_version: str,
    input_snapshot_id: str,
    model_uri: str,
    invocation_checksum: str,
    run_date: date,
) -> str:
    values = (
        provider_id,
        provider_version,
        input_snapshot_id,
        model_uri,
        invocation_checksum,
        run_date.isoformat(),
    )
    if any(not value for value in values):
        raise ValueError("Provider build identity values must not be empty")
    if "@" in model_uri:
        raise ValueError("Provider build identity requires an immutable model URI")
    if model_uri.startswith("models:/"):
        model_version = model_uri.rstrip("/").rsplit("/", 1)[-1]
        if not model_version.isdigit():
            raise ValueError(
                "Provider build identity requires an exact model version"
            )
    digest = hashlib.sha256("|".join(values).encode("utf-8")).hexdigest()
    return f"{provider_id}_{run_date:%Y%m%d}_{digest[:20]}"


def activate_provider_context(
    spark: Any,
    *,
    context_table: str,
    context: ProviderContext,
    task_run_id: int,
    execution_count: int,
    activated_at: datetime,
) -> None:
    if context.expires_at.tzinfo is None or activated_at.tzinfo is None:
        raise ValueError("Provider context timestamps must be timezone-aware")
    if context.expires_at <= activated_at:
        raise ValueError("Provider context lease must expire in the future")
    row = {
        "ProviderID": context.provider_id,
        "ContextSlot": context.context_slot,
        "OrchestrationRunID": context.orchestration_run_id,
        "ProviderBuildID": context.provider_build_id,
        "ProviderBuildAttemptID": context.provider_build_attempt_id,
        "InputSnapshotID": context.input_snapshot_id,
        "RunDate": context.run_date,
        "ModelURI": context.model_uri,
        "BindingsJSON": context.bindings_json,
        "Capability": context.capability,
        "UseCase": context.use_case,
        "InvocationChecksum": context.invocation_checksum,
        "Status": ACTIVE,
        "ExpiresAt": context.expires_at,
        "TaskRunID": task_run_id,
        "ExecutionCount": execution_count,
        "ActivatedAt": activated_at,
    }
    frame = spark.createDataFrame([row])
    source_view = "_nextads_provider_context_claim"
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
    statement = f"""
MERGE INTO {quote_qualified_identifier(context_table)} AS target
USING {quote_qualified_identifier(source_view)} AS source
ON target.ContextSlot = source.ContextSlot
WHEN MATCHED AND (
  target.ExpiresAt <= source.ActivatedAt
  OR (
    target.ProviderBuildID = source.ProviderBuildID
    AND target.ProviderBuildAttemptID = source.ProviderBuildAttemptID
  )
  OR (
    target.OrchestrationRunID = source.OrchestrationRunID
    AND target.ProviderBuildID = source.ProviderBuildID
    AND source.ExecutionCount > target.ExecutionCount
  )
) THEN UPDATE SET {assignments}
WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
"""
    try:
        spark.sql(statement)
    finally:
        spark.catalog.dropTempView(source_view)
    owner = load_active_provider_context(
        spark,
        context_table=context_table,
        context_slot=context.context_slot,
        now=activated_at,
    )
    if (
        owner.provider_build_id != context.provider_build_id
        or owner.provider_build_attempt_id
        != context.provider_build_attempt_id
    ):
        raise ValueError(
            f"Context slot {context.context_slot} already has an active lease"
        )


def transition_provider_context(
    spark: Any,
    *,
    context_table: str,
    context: ProviderContext,
    status: str,
    completed_at: datetime,
) -> None:
    if status not in {"CONSUMED", "FAILED"}:
        raise ValueError("Provider context status must be CONSUMED or FAILED")
    if completed_at.tzinfo is None:
        raise ValueError("completed_at must be timezone-aware")
    escaped_slot = context.context_slot.replace("'", "''")
    escaped_build = context.provider_build_id.replace("'", "''")
    escaped_attempt = context.provider_build_attempt_id.replace("'", "''")
    statement = f"""
UPDATE {quote_qualified_identifier(context_table)}
SET Status = '{status}', ExpiresAt = TIMESTAMP '{completed_at.isoformat()}'
WHERE ContextSlot = '{escaped_slot}'
  AND OrchestrationRunID = {context.orchestration_run_id}
  AND ProviderBuildID = '{escaped_build}'
  AND ProviderBuildAttemptID = '{escaped_attempt}'
  AND Status = '{ACTIVE}'
"""
    spark.sql(statement)
    rows = (
        spark.table(context_table)
        .where(F.col("ContextSlot") == context.context_slot)
        .select(
            "OrchestrationRunID",
            "ProviderBuildID",
            "ProviderBuildAttemptID",
            "Status",
        )
        .collect()
    )
    if len(rows) != 1 or (
        int(rows[0]["OrchestrationRunID"]),
        rows[0]["ProviderBuildID"],
        rows[0]["ProviderBuildAttemptID"],
        rows[0]["Status"],
    ) != (
        context.orchestration_run_id,
        context.provider_build_id,
        context.provider_build_attempt_id,
        status,
    ):
        raise ValueError("Provider context release ownership check failed")


def load_active_provider_context(
    spark: Any,
    *,
    context_table: str,
    context_slot: str,
    now: datetime | None = None,
) -> ProviderContext:
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
        raise ValueError(f"{context_slot} provider context is not ACTIVE")
    expires_at = row["ExpiresAt"]
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at <= current_time:
        raise ValueError(f"{context_slot} provider context has expired")
    return ProviderContext(
        context_slot=row["ContextSlot"],
        orchestration_run_id=int(row["OrchestrationRunID"]),
        provider_id=row["ProviderID"],
        provider_build_id=row["ProviderBuildID"],
        provider_build_attempt_id=row["ProviderBuildAttemptID"],
        input_snapshot_id=row["InputSnapshotID"],
        run_date=row["RunDate"],
        model_uri=row["ModelURI"],
        bindings_json=row["BindingsJSON"],
        capability=row["Capability"],
        use_case=row["UseCase"],
        invocation_checksum=row["InvocationChecksum"],
        expires_at=expires_at,
    )


def pinned_item_themes(
    spark: Any,
    context: ProviderContext,
    *,
    input_table: str,
):
    from next_ads.common.delta_writes import validate_unique_non_null_keys

    binding = json.loads(context.bindings_json).get("item_themes")
    expected_binding = {
        "table": input_table,
        "input_snapshot_id": context.input_snapshot_id,
        "run_date": context.run_date.isoformat(),
    }
    if binding != expected_binding:
        raise ValueError("Item-theme binding does not match provider context")
    frame = spark.table(input_table).where(
        (F.col("InputSnapshotID") == context.input_snapshot_id)
        & (F.col("RunDate") == F.lit(context.run_date))
    )
    summary = validate_unique_non_null_keys(
        frame,
        ["InputSnapshotID", "RunDate", "pid", "theme"],
    )
    if summary.row_count == 0:
        raise ValueError("Pinned item-theme snapshot is empty")
    if frame.where(F.col("theme_rank").isNull()).limit(1).count():
        raise ValueError("Pinned item-theme snapshot contains null ranks")
    return frame

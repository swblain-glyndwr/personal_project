from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from typing import Any

from pyspark.sql import functions as F

from next_ads.common.delta_writes import (
    quote_identifier,
    quote_qualified_identifier,
)


ACTIVE = "ACTIVE"
FAILED = "FAILED"

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
    scoring_foundation_build_id: str | None = None
    scoring_foundation_build_attempt_id: str | None = None

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
            if (
                not isinstance(getattr(self, field), str)
                or not getattr(self, field).strip()
            ):
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
        model_binding = bindings.get("model")
        if model_binding is not None:
            if not isinstance(model_binding, dict):
                raise ValueError("Provider model binding must be a mapping")
            for field in ("table", "schema_version", "schema_checksum"):
                value = model_binding.get(field)
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(
                        f"Provider model binding {field} must not be empty"
                    )
            model_version = model_binding.get("delta_version")
            if (
                isinstance(model_version, bool)
                or not isinstance(model_version, int)
                or model_version < 0
            ):
                raise ValueError(
                    "Provider model binding delta_version must be a "
                    "non-negative integer"
                )
        foundation_ids = (
            self.scoring_foundation_build_id,
            self.scoring_foundation_build_attempt_id,
        )
        if any(value is None for value in foundation_ids) and not all(
            value is None for value in foundation_ids
        ):
            raise ValueError(
                "Foundation build and attempt IDs must be supplied together"
            )
        foundation_binding = bindings.get("foundation")
        if all(value is None for value in foundation_ids):
            if foundation_binding is not None:
                raise ValueError(
                    "A foundation-free provider cannot contain a foundation binding"
                )
        else:
            if not isinstance(foundation_binding, dict):
                raise ValueError(
                    "Provider context is missing its foundation binding"
                )
            expected = {
                "scoring_foundation_build_id": (
                    self.scoring_foundation_build_id
                ),
                "scoring_foundation_build_attempt_id": (
                    self.scoring_foundation_build_attempt_id
                ),
            }
            mismatched = [
                field
                for field, value in expected.items()
                if foundation_binding.get(field) != value
            ]
            if mismatched:
                raise ValueError(
                    "Provider foundation binding does not match its IDs: "
                    + ", ".join(mismatched)
                )
            if not isinstance(foundation_binding.get("outputs"), dict):
                raise ValueError(
                    "Provider foundation outputs must be a mapping"
                )


@dataclass(frozen=True)
class FoundationOutputBinding:
    output_name: str
    table: str
    delta_version: int
    schema_version: str

    def __post_init__(self) -> None:
        """Validate one exact foundation output binding."""
        for field in ("output_name", "table", "schema_version"):
            if (
                not isinstance(getattr(self, field), str)
                or not getattr(self, field).strip()
            ):
                raise ValueError(f"{field} must not be empty")
        if (
            isinstance(self.delta_version, bool)
            or not isinstance(self.delta_version, int)
            or self.delta_version < 0
        ):
            raise ValueError("delta_version must be a non-negative integer")


def build_provider_invocation_checksum(
    *,
    provider_id: str,
    provider_config: dict[str, Any],
    ranking_model_config: dict[str, Any] | None = None,
    provider_implementation: str | None = None,
) -> str:
    """Hash only settings that can change a provider's inference output."""
    provider_semantics = {
        field: provider_config[field]
        for field in _PROVIDER_INFERENCE_FIELDS
        if field in provider_config
    }
    payload: dict[str, Any] = {"provider": provider_semantics}
    implementation = provider_implementation or provider_id
    if implementation == "theme_affinity":
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
    scoring_foundation_build_id: str | None = None,
) -> str:
    values = (
        provider_id,
        provider_version,
        input_snapshot_id,
        model_uri,
        invocation_checksum,
        scoring_foundation_build_id or "foundation-free",
        run_date.isoformat(),
    )
    if any(not value for value in values):
        raise ValueError("Provider build identity values must not be empty")
    if "@" in model_uri:
        raise ValueError(
            "Provider build identity requires an immutable model URI"
        )
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
    allow_serial_run_takeover: bool = False,
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
        "ScoringFoundationBuildID": context.scoring_foundation_build_id,
        "ScoringFoundationBuildAttemptID": (
            context.scoring_foundation_build_attempt_id
        ),
        "Status": ACTIVE,
        "ExpiresAt": context.expires_at,
        "TaskRunID": task_run_id,
        "ExecutionCount": execution_count,
        "ActivatedAt": activated_at,
    }
    frame = spark.createDataFrame(
        [row],
        schema=spark.table(context_table).schema,
    )
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
    target.ProviderBuildID = source.ProviderBuildID
    AND target.ProviderBuildAttemptID = source.ProviderBuildAttemptID
  )
  OR (
    target.OrchestrationRunID = source.OrchestrationRunID
    AND target.ProviderBuildID = source.ProviderBuildID
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
    owner = load_active_provider_context(
        spark,
        context_table=context_table,
        context_slot=context.context_slot,
        now=activated_at,
    )
    if (
        owner.orchestration_run_id != context.orchestration_run_id
        or owner.provider_build_id != context.provider_build_id
        or owner.provider_build_attempt_id != context.provider_build_attempt_id
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


def load_reusable_provider_context(
    spark: Any,
    *,
    context_table: str,
    expected_context: ProviderContext,
    execution_count: int,
) -> ProviderContext | None:
    """Return the exact incomplete attempt that a task repair may reclaim.

    A repair within the same orchestration run keeps the original physical
    attempt identity. The caller can therefore find an existing Delta receipt
    and publish READY without rebuilding the distributed dataframe. ACTIVE is
    reusable as well as FAILED because cluster loss can bypass finalisation;
    the higher Databricks task execution count proves the old execution ended.
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
    if row["Status"] not in {ACTIVE, FAILED}:
        return None
    if int(row["ExecutionCount"]) >= execution_count:
        return None
    existing = _context_from_row(row)
    comparable_fields = (
        "context_slot",
        "orchestration_run_id",
        "provider_id",
        "provider_build_id",
        "input_snapshot_id",
        "run_date",
        "model_uri",
        "bindings_json",
        "capability",
        "use_case",
        "invocation_checksum",
        "scoring_foundation_build_id",
        "scoring_foundation_build_attempt_id",
    )
    if any(
        getattr(existing, field) != getattr(expected_context, field)
        for field in comparable_fields
    ):
        return None
    return replace(
        expected_context,
        provider_build_attempt_id=existing.provider_build_attempt_id,
    )


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
    context = _context_from_row(row)
    if context.expires_at <= current_time:
        raise ValueError(f"{context_slot} provider context has expired")
    return context


def _context_from_row(row: Any) -> ProviderContext:
    expires_at = row["ExpiresAt"]
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
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
        scoring_foundation_build_id=_optional_row_value(
            row,
            "ScoringFoundationBuildID",
        ),
        scoring_foundation_build_attempt_id=(
            _optional_row_value(row, "ScoringFoundationBuildAttemptID")
        ),
    )


def _optional_row_value(row: Any, field: str):
    try:
        return row[field]
    except (KeyError, ValueError):
        return None


def foundation_output_binding(
    context: ProviderContext,
    output_name: str,
) -> FoundationOutputBinding:
    if context.scoring_foundation_build_id is None:
        raise ValueError(
            f"Provider {context.provider_id} has no scoring foundation"
        )
    foundation = json.loads(context.bindings_json).get("foundation")
    if not isinstance(foundation, dict):
        raise ValueError("Provider context has no foundation binding")
    outputs = foundation.get("outputs")
    definition = (
        outputs.get(output_name) if isinstance(outputs, dict) else None
    )
    if not isinstance(definition, dict):
        raise ValueError(f"Foundation output {output_name} is not bound")
    return FoundationOutputBinding(
        output_name=output_name,
        table=definition.get("table"),
        delta_version=definition.get("delta_version"),
        schema_version=definition.get("schema_version"),
    )


def read_bound_foundation_output(
    spark: Any,
    context: ProviderContext,
    output_name: str,
):
    from next_ads.ranking.scoring_inputs import read_delta_version

    binding = foundation_output_binding(context, output_name)
    return read_delta_version(spark, binding.table, binding.delta_version)


def pinned_item_themes(
    spark: Any,
    context: Any,
    *,
    input_table: str,
):
    binding = json.loads(context.bindings_json).get("item_themes")
    expected_binding = {
        "table": input_table,
        "input_snapshot_id": context.input_snapshot_id,
        "run_date": context.run_date.isoformat(),
    }
    if not isinstance(binding, dict) or any(
        binding.get(field) != value
        for field, value in expected_binding.items()
    ):
        raise ValueError("Item-theme binding does not match provider context")
    from next_ads.ranking.scoring_inputs import read_delta_version

    delta_version = binding.get("delta_version")
    frame = read_delta_version(spark, input_table, delta_version).where(
        (F.col("InputSnapshotID") == context.input_snapshot_id)
        & (F.col("RunDate") == F.lit(context.run_date))
    )
    required = {"InputSnapshotID", "RunDate", "pid", "theme", "theme_rank"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(
            "Pinned item-theme snapshot is missing columns: "
            + ", ".join(missing)
        )
    return frame


def pinned_provider_model(
    spark: Any,
    context: Any,
    *,
    required_columns: set[str] | frozenset[str] = frozenset(),
):
    """Read and validate the exact Delta model bound to a provider build."""
    binding = json.loads(context.bindings_json).get("model")
    if not isinstance(binding, dict):
        raise ValueError("Provider context has no exact model binding")
    from next_ads.ranking.scoring_inputs import (
        read_delta_version,
        schema_checksum,
    )

    frame = read_delta_version(
        spark,
        binding["table"],
        binding["delta_version"],
    )
    if schema_checksum(frame) != binding["schema_checksum"]:
        raise ValueError("Pinned provider model schema has changed")
    missing = sorted(set(required_columns).difference(frame.columns))
    if missing:
        raise ValueError(
            "Pinned provider model is missing columns: " + ", ".join(missing)
        )
    return frame

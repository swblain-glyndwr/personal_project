"""Mutable ownership and recovery checkpoints for one model research build."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timedelta, timezone
import json
from typing import Any
from uuid import uuid4

from next_ads.common.delta_writes import (
    quote_identifier,
    quote_qualified_identifier,
    sql_literal,
    typed_table_frame,
)
from next_ads.common.output_locations import log_output_location
from next_ads.model_development.research_store import (
    RESEARCH_CLAIM_TABLE,
    ResearchFrameBinding,
)
from next_ads.model_development.store import table_path


CLAIMED = "CLAIMED"
FRAME_READY = "FRAME_READY"
PARENT_READY = "PARENT_READY"
CANDIDATES_READY = "CANDIDATES_READY"
SELECTION_LOCKED = "SELECTION_LOCKED"
REGISTERED = "REGISTERED"
COMPLETE = "COMPLETE"
FAILED = "FAILED"

RESEARCH_CLAIM_CHECKPOINTS = (
    CLAIMED,
    FRAME_READY,
    PARENT_READY,
    CANDIDATES_READY,
    SELECTION_LOCKED,
    REGISTERED,
    COMPLETE,
)
TERMINAL_RESEARCH_CLAIM_CHECKPOINTS = frozenset({COMPLETE, FAILED})
DEFAULT_CLAIM_LEASE_SECONDS = 3600

_NEXT_CHECKPOINT = {
    current: following
    for current, following in zip(
        RESEARCH_CLAIM_CHECKPOINTS[:-1],
        RESEARCH_CLAIM_CHECKPOINTS[1:],
        strict=True,
    )
}
_IDENTITY_FIELDS = (
    "model_definition_checksum",
    "training_receipt_id",
    "research_plan_checksum",
    "evaluation_schema_version",
    "code_sha",
)
_LOGICAL_IDENTITY_FIELDS = tuple(
    field for field in _IDENTITY_FIELDS if field != "code_sha"
)
_MUTABLE_FIELDS = (
    "owner_invocation_id",
    "lease_token",
    "lease_expires_at",
    "checkpoint",
    "checkpoint_version",
    "research_frame_binding_json",
    "mlflow_experiment_id",
    "mlflow_parent_run_id",
    "selection_decision_id",
    "model_build_id",
    "failure_reason",
    "updated_at",
)


class ResearchClaimConflictError(ValueError):
    """Raised when another live invocation owns the logical research build."""


@dataclass(frozen=True)
class ResearchClaim:
    """One mutable control row; terminal outputs remain in immutable tables."""

    research_build_id: str
    research_attempt_id: str
    model_definition_checksum: str
    training_receipt_id: str
    research_plan_checksum: str
    evaluation_schema_version: str
    code_sha: str
    owner_invocation_id: str
    lease_token: str
    lease_expires_at: datetime
    checkpoint: str
    checkpoint_version: int
    research_frame_binding_json: str | None
    mlflow_experiment_id: str | None
    mlflow_parent_run_id: str | None
    selection_decision_id: str | None
    model_build_id: str | None
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        """Validate persisted control state before it is acted on."""
        for field_name in (
            "research_build_id",
            "research_attempt_id",
            *_IDENTITY_FIELDS,
            "owner_invocation_id",
            "lease_token",
        ):
            _required_text(getattr(self, field_name), field_name)
        if self.checkpoint not in {
            *RESEARCH_CLAIM_CHECKPOINTS,
            FAILED,
        }:
            raise ValueError(
                f"Unsupported research claim checkpoint: {self.checkpoint}"
            )
        if (
            isinstance(self.checkpoint_version, bool)
            or not isinstance(self.checkpoint_version, int)
            or self.checkpoint_version < 0
        ):
            raise ValueError("checkpoint_version cannot be negative")
        _timestamp(self.lease_expires_at, "lease_expires_at")
        created_at = _timestamp(self.created_at, "created_at")
        updated_at = _timestamp(self.updated_at, "updated_at")
        if updated_at < created_at:
            raise ValueError("updated_at cannot be earlier than created_at")
        if self.checkpoint == FAILED:
            _required_text(self.failure_reason, "failure_reason")
        elif self.failure_reason is not None:
            raise ValueError("failure_reason is only valid for FAILED claims")
        if self.research_frame_binding_json is not None:
            deserialize_research_frame_binding(
                self.research_frame_binding_json
            )

    @property
    def research_frame_binding(self) -> ResearchFrameBinding | None:
        """Return the exact frame binding recorded at FRAME_READY."""
        if self.research_frame_binding_json is None:
            return None
        return deserialize_research_frame_binding(
            self.research_frame_binding_json
        )

    @property
    def terminal(self) -> bool:
        """Whether no further mutation is allowed for this claim."""
        return self.checkpoint in TERMINAL_RESEARCH_CLAIM_CHECKPOINTS


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    return value.strip()


def _timestamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _now(value: datetime | None) -> datetime:
    return _timestamp(
        datetime.now(timezone.utc) if value is None else value,
        "now",
    )


def _lease_expiry(now: datetime, lease_seconds: int) -> datetime:
    if (
        isinstance(lease_seconds, bool)
        or not isinstance(lease_seconds, int)
        or lease_seconds < 1
    ):
        raise ValueError("lease_seconds must be a positive integer")
    return now + timedelta(seconds=lease_seconds)


def serialize_research_frame_binding(binding: ResearchFrameBinding) -> str:
    """Serialise the complete immutable frame receipt deterministically."""
    if not isinstance(binding, ResearchFrameBinding):
        raise TypeError("binding must be a ResearchFrameBinding")
    return json.dumps(
        asdict(binding),
        sort_keys=True,
        separators=(",", ":"),
    )


def deserialize_research_frame_binding(value: str) -> ResearchFrameBinding:
    """Validate and restore the exact frame receipt from a claim row."""
    try:
        payload = json.loads(
            _required_text(value, "research_frame_binding_json")
        )
    except json.JSONDecodeError as exc:
        raise ValueError(
            "research_frame_binding_json is invalid JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise ValueError("research_frame_binding_json must contain an object")
    expected = {field.name for field in fields(ResearchFrameBinding)}
    actual = set(payload)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        details = []
        if missing:
            details.append("missing=" + ",".join(missing))
        if unexpected:
            details.append("unexpected=" + ",".join(unexpected))
        raise ValueError(
            "research_frame_binding_json does not match its contract: "
            + "; ".join(details)
        )
    return ResearchFrameBinding(**payload)


def _load_claim_row(
    spark: Any,
    *,
    table: str,
    research_build_id: str,
) -> dict[str, Any] | None:
    from pyspark.sql import functions as F

    rows = (
        spark.table(table)
        .where(F.col("research_build_id") == F.lit(research_build_id))
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise ValueError(
            "Model research claim has duplicate primary key: "
            f"{research_build_id}"
        )
    return rows[0].asDict(recursive=True) if rows else None


def load_research_claim(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    research_build_id: str,
) -> ResearchClaim | None:
    """Load the single control row for one logical research build."""
    logical_id = _required_text(research_build_id, "research_build_id")
    values = _load_claim_row(
        spark,
        table=table_path(catalog, schema, RESEARCH_CLAIM_TABLE),
        research_build_id=logical_id,
    )
    return ResearchClaim(**values) if values is not None else None


def _temporary_view(frame: Any, operation: str) -> str:
    view = f"_nextads_research_claim_{operation}_{uuid4().hex}"
    frame.createOrReplaceTempView(view)
    return view


def _insert_columns(frame: Any) -> tuple[str, str]:
    columns = list(frame.columns)
    names = ", ".join(quote_identifier(column) for column in columns)
    values = ", ".join(
        f"source.{quote_identifier(column)}" for column in columns
    )
    return names, values


def _merge_claim_acquisition(
    spark: Any,
    *,
    table: str,
    frame: Any,
) -> None:
    """Insert or atomically take an expired claim without changing its attempt."""
    view = _temporary_view(frame, "acquire")
    insert_columns, insert_values = _insert_columns(frame)
    identity_match = " AND ".join(
        "target.{field} <=> source.{field}".format(
            field=quote_identifier(field_name)
        )
        for field_name in _IDENTITY_FIELDS
    )
    logical_identity_match = " AND ".join(
        "target.{field} <=> source.{field}".format(
            field=quote_identifier(field_name)
        )
        for field_name in _LOGICAL_IDENTITY_FIELDS
    )
    terminal_values = ", ".join(
        sql_literal(value)
        for value in sorted(TERMINAL_RESEARCH_CLAIM_CHECKPOINTS)
    )
    statement = f"""
MERGE INTO {quote_qualified_identifier(table)} AS target
USING {quote_qualified_identifier(view)} AS source
ON target.{quote_identifier("research_build_id")}
   <=> source.{quote_identifier("research_build_id")}
WHEN MATCHED
  AND target.{quote_identifier("checkpoint")} = {sql_literal(FAILED)}
  AND {logical_identity_match}
  AND NOT (
    target.{quote_identifier("code_sha")}
      <=> source.{quote_identifier("code_sha")}
  )
THEN UPDATE SET
  {quote_identifier("research_attempt_id")} =
    source.{quote_identifier("research_attempt_id")},
  {quote_identifier("code_sha")} = source.{quote_identifier("code_sha")},
  {quote_identifier("owner_invocation_id")} =
    source.{quote_identifier("owner_invocation_id")},
  {quote_identifier("lease_token")} =
    source.{quote_identifier("lease_token")},
  {quote_identifier("lease_expires_at")} =
    source.{quote_identifier("lease_expires_at")},
  {quote_identifier("checkpoint")} = {sql_literal(CLAIMED)},
  {quote_identifier("checkpoint_version")} =
    target.{quote_identifier("checkpoint_version")} + 1,
  {quote_identifier("research_frame_binding_json")} = NULL,
  {quote_identifier("mlflow_experiment_id")} = NULL,
  {quote_identifier("mlflow_parent_run_id")} = NULL,
  {quote_identifier("selection_decision_id")} = NULL,
  {quote_identifier("model_build_id")} = NULL,
  {quote_identifier("failure_reason")} = NULL,
  {quote_identifier("created_at")} = source.{quote_identifier("created_at")},
  {quote_identifier("updated_at")} = source.{quote_identifier("updated_at")}
WHEN MATCHED
  AND target.{quote_identifier("checkpoint")} NOT IN ({terminal_values})
  AND {identity_match}
  AND (
    target.{quote_identifier("owner_invocation_id")}
      <=> source.{quote_identifier("owner_invocation_id")}
    OR target.{quote_identifier("lease_expires_at")}
      <= source.{quote_identifier("updated_at")}
  )
THEN UPDATE SET
  {quote_identifier("owner_invocation_id")} =
    source.{quote_identifier("owner_invocation_id")},
  {quote_identifier("lease_token")} =
    source.{quote_identifier("lease_token")},
  {quote_identifier("lease_expires_at")} =
    source.{quote_identifier("lease_expires_at")},
  {quote_identifier("checkpoint_version")} =
    target.{quote_identifier("checkpoint_version")} + 1,
  {quote_identifier("updated_at")} = source.{quote_identifier("updated_at")}
WHEN NOT MATCHED THEN
  INSERT ({insert_columns}) VALUES ({insert_values})
"""
    try:
        spark.sql(statement)
    finally:
        spark.catalog.dropTempView(view)


def _merge_claim_transition(
    spark: Any,
    *,
    table: str,
    frame: Any,
    expected_checkpoint: str,
) -> None:
    """Advance only the live owner's row from the expected checkpoint."""
    if expected_checkpoint not in {
        *RESEARCH_CLAIM_CHECKPOINTS,
        FAILED,
    }:
        raise ValueError(
            f"Unsupported expected checkpoint: {expected_checkpoint}"
        )
    view = _temporary_view(frame, "transition")
    assignments = ",\n  ".join(
        f"{quote_identifier(field_name)} = "
        f"source.{quote_identifier(field_name)}"
        for field_name in _MUTABLE_FIELDS
    )
    statement = f"""
MERGE INTO {quote_qualified_identifier(table)} AS target
USING {quote_qualified_identifier(view)} AS source
ON target.{quote_identifier("research_build_id")}
   <=> source.{quote_identifier("research_build_id")}
WHEN MATCHED
  AND target.{quote_identifier("owner_invocation_id")}
    <=> source.{quote_identifier("owner_invocation_id")}
  AND target.{quote_identifier("lease_token")}
    <=> source.{quote_identifier("lease_token")}
  AND target.{quote_identifier("lease_expires_at")}
    > source.{quote_identifier("updated_at")}
  AND target.{quote_identifier("checkpoint")}
    = {sql_literal(expected_checkpoint)}
  AND target.{quote_identifier("checkpoint_version")}
    = source.{quote_identifier("checkpoint_version")} - 1
THEN UPDATE SET
  {assignments}
"""
    try:
        spark.sql(statement)
    finally:
        spark.catalog.dropTempView(view)


def _validate_identity(
    claim: ResearchClaim,
    *,
    expected: dict[str, str],
) -> None:
    differences = sorted(
        field_name
        for field_name, value in expected.items()
        if getattr(claim, field_name) != value
    )
    if differences:
        raise ResearchClaimConflictError(
            "Existing model research claim has different immutable fields: "
            + ", ".join(differences)
        )


def _validate_live_owner(
    claim: ResearchClaim,
    *,
    owner_invocation_id: str,
    lease_token: str,
    now: datetime,
) -> None:
    if claim.owner_invocation_id != owner_invocation_id:
        raise ResearchClaimConflictError(
            "Model research build is owned by another live invocation: "
            f"{claim.owner_invocation_id}"
        )
    if claim.lease_token != lease_token:
        raise ResearchClaimConflictError(
            "Model research claim is held by a newer lease token"
        )
    if _timestamp(claim.lease_expires_at, "lease_expires_at") <= now:
        raise ResearchClaimConflictError(
            "Model research claim lease expired before the operation completed"
        )


def _log_claim_output(
    *,
    catalog: str,
    schema: str,
    claim: ResearchClaim,
    operation: str,
    reused: bool,
) -> None:
    log_output_location(
        table_path(catalog, schema, RESEARCH_CLAIM_TABLE),
        kind="delta_table",
        details={
            "checkpoint": claim.checkpoint,
            "checkpoint_version": claim.checkpoint_version,
            "operation": operation,
            "reused": reused,
        },
    )


def claim_research_build(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    research_build_id: str,
    research_attempt_id: str,
    model_definition_checksum: str,
    training_receipt_id: str,
    research_plan_checksum: str,
    evaluation_schema_version: str,
    code_sha: str,
    owner_invocation_id: str,
    lease_seconds: int = DEFAULT_CLAIM_LEASE_SECONDS,
    now: datetime | None = None,
) -> ResearchClaim:
    """Acquire a build, or start a new attempt after a code-fixed failure."""
    acquired_at = _now(now)
    identity_values = {
        "model_definition_checksum": model_definition_checksum,
        "training_receipt_id": training_receipt_id,
        "research_plan_checksum": research_plan_checksum,
        "evaluation_schema_version": evaluation_schema_version,
        "code_sha": code_sha,
    }
    identity = {
        field_name: _required_text(value, field_name)
        for field_name, value in identity_values.items()
    }
    proposed = ResearchClaim(
        research_build_id=_required_text(
            research_build_id, "research_build_id"
        ),
        research_attempt_id=_required_text(
            research_attempt_id, "research_attempt_id"
        ),
        **identity,
        owner_invocation_id=_required_text(
            owner_invocation_id, "owner_invocation_id"
        ),
        lease_token=uuid4().hex,
        lease_expires_at=_lease_expiry(acquired_at, lease_seconds),
        checkpoint=CLAIMED,
        checkpoint_version=0,
        research_frame_binding_json=None,
        mlflow_experiment_id=None,
        mlflow_parent_run_id=None,
        selection_decision_id=None,
        model_build_id=None,
        failure_reason=None,
        created_at=acquired_at,
        updated_at=acquired_at,
    )
    target = table_path(catalog, schema, RESEARCH_CLAIM_TABLE)
    frame = typed_table_frame(spark, target, [asdict(proposed)])
    _merge_claim_acquisition(
        spark,
        table=target,
        frame=frame,
    )
    stored = load_research_claim(
        spark,
        catalog=catalog,
        schema=schema,
        research_build_id=proposed.research_build_id,
    )
    if stored is None:
        raise ResearchClaimConflictError(
            "Model research claim was not persisted"
        )
    _validate_identity(stored, expected=identity)
    if stored.terminal:
        _log_claim_output(
            catalog=catalog,
            schema=schema,
            claim=stored,
            operation="claim_research_build",
            reused=True,
        )
        return stored
    if stored.lease_token != proposed.lease_token:
        raise ResearchClaimConflictError(
            "Model research build is owned by another live lease"
        )
    _validate_live_owner(
        stored,
        owner_invocation_id=proposed.owner_invocation_id,
        lease_token=proposed.lease_token,
        now=acquired_at,
    )
    _log_claim_output(
        catalog=catalog,
        schema=schema,
        claim=stored,
        operation="claim_research_build",
        reused=False,
    )
    return stored


def _set_once(
    current: str | None,
    proposed: str | None,
    field_name: str,
) -> str | None:
    if proposed is None:
        return current
    value = _required_text(proposed, field_name)
    if current is not None and current != value:
        raise ResearchClaimConflictError(
            f"{field_name} is already locked to a different value"
        )
    return value


def _validate_checkpoint_payload(claim: ResearchClaim) -> None:
    if claim.checkpoint == FAILED:
        return
    checkpoint_index = RESEARCH_CLAIM_CHECKPOINTS.index(claim.checkpoint)
    if checkpoint_index >= RESEARCH_CLAIM_CHECKPOINTS.index(FRAME_READY):
        if claim.research_frame_binding is None:
            raise ValueError("FRAME_READY requires a research frame binding")
    if checkpoint_index >= RESEARCH_CLAIM_CHECKPOINTS.index(PARENT_READY):
        _required_text(claim.mlflow_experiment_id, "mlflow_experiment_id")
        _required_text(claim.mlflow_parent_run_id, "mlflow_parent_run_id")
    if checkpoint_index >= RESEARCH_CLAIM_CHECKPOINTS.index(SELECTION_LOCKED):
        _required_text(claim.selection_decision_id, "selection_decision_id")
        _required_text(claim.model_build_id, "model_build_id")


def _same_claim(left: ResearchClaim, right: ResearchClaim) -> bool:
    for field_name in (field.name for field in fields(ResearchClaim)):
        left_value = getattr(left, field_name)
        right_value = getattr(right, field_name)
        if isinstance(left_value, datetime) and isinstance(
            right_value, datetime
        ):
            if _timestamp(left_value, field_name) != _timestamp(
                right_value, field_name
            ):
                return False
        elif left_value != right_value:
            return False
    return True


def _write_transition(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    current: ResearchClaim,
    updated: ResearchClaim,
    expected_checkpoint: str,
    operation: str,
) -> ResearchClaim:
    target = table_path(catalog, schema, RESEARCH_CLAIM_TABLE)
    frame = typed_table_frame(spark, target, [asdict(updated)])
    _merge_claim_transition(
        spark,
        table=target,
        frame=frame,
        expected_checkpoint=expected_checkpoint,
    )
    stored = load_research_claim(
        spark,
        catalog=catalog,
        schema=schema,
        research_build_id=current.research_build_id,
    )
    if stored is None or not _same_claim(stored, updated):
        raise ResearchClaimConflictError(
            "Model research claim changed concurrently during transition"
        )
    _log_claim_output(
        catalog=catalog,
        schema=schema,
        claim=stored,
        operation=operation,
        reused=False,
    )
    return stored


def renew_research_claim(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    research_build_id: str,
    owner_invocation_id: str,
    lease_token: str,
    lease_seconds: int = DEFAULT_CLAIM_LEASE_SECONDS,
    now: datetime | None = None,
) -> ResearchClaim:
    """Extend an active lease without changing its recovery checkpoint."""
    renewed_at = _now(now)
    owner = _required_text(owner_invocation_id, "owner_invocation_id")
    token = _required_text(lease_token, "lease_token")
    current = load_research_claim(
        spark,
        catalog=catalog,
        schema=schema,
        research_build_id=research_build_id,
    )
    if current is None:
        raise ResearchClaimConflictError("Model research claim does not exist")
    if current.terminal:
        _log_claim_output(
            catalog=catalog,
            schema=schema,
            claim=current,
            operation="renew_research_claim",
            reused=True,
        )
        return current
    _validate_live_owner(
        current,
        owner_invocation_id=owner,
        lease_token=token,
        now=renewed_at,
    )
    updated = replace(
        current,
        lease_expires_at=_lease_expiry(renewed_at, lease_seconds),
        checkpoint_version=current.checkpoint_version + 1,
        updated_at=renewed_at,
    )
    return _write_transition(
        spark,
        catalog=catalog,
        schema=schema,
        current=current,
        updated=updated,
        expected_checkpoint=current.checkpoint,
        operation="renew_research_claim",
    )


def release_research_claim(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    research_build_id: str,
    owner_invocation_id: str,
    lease_token: str,
    now: datetime | None = None,
) -> ResearchClaim:
    """Release an active checkpoint for immediate ownership transfer."""
    released_at = _now(now)
    owner = _required_text(owner_invocation_id, "owner_invocation_id")
    token = _required_text(lease_token, "lease_token")
    current = load_research_claim(
        spark,
        catalog=catalog,
        schema=schema,
        research_build_id=research_build_id,
    )
    if current is None:
        raise ResearchClaimConflictError("Model research claim does not exist")
    if current.terminal:
        _log_claim_output(
            catalog=catalog,
            schema=schema,
            claim=current,
            operation="release_research_claim",
            reused=True,
        )
        return current
    _validate_live_owner(
        current,
        owner_invocation_id=owner,
        lease_token=token,
        now=released_at,
    )
    updated = replace(
        current,
        lease_expires_at=released_at,
        checkpoint_version=current.checkpoint_version + 1,
        updated_at=released_at,
    )
    return _write_transition(
        spark,
        catalog=catalog,
        schema=schema,
        current=current,
        updated=updated,
        expected_checkpoint=current.checkpoint,
        operation="release_research_claim",
    )


def advance_research_claim(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    research_build_id: str,
    owner_invocation_id: str,
    lease_token: str,
    expected_checkpoint: str,
    checkpoint: str,
    research_frame_binding: ResearchFrameBinding | None = None,
    mlflow_experiment_id: str | None = None,
    mlflow_parent_run_id: str | None = None,
    selection_decision_id: str | None = None,
    model_build_id: str | None = None,
    lease_seconds: int = DEFAULT_CLAIM_LEASE_SECONDS,
    now: datetime | None = None,
) -> ResearchClaim:
    """Advance exactly one checkpoint while retaining immutable references."""
    transitioned_at = _now(now)
    owner = _required_text(owner_invocation_id, "owner_invocation_id")
    token = _required_text(lease_token, "lease_token")
    if expected_checkpoint not in RESEARCH_CLAIM_CHECKPOINTS:
        raise ValueError(
            f"Unsupported expected checkpoint: {expected_checkpoint}"
        )
    if checkpoint not in RESEARCH_CLAIM_CHECKPOINTS:
        raise ValueError(f"Unsupported target checkpoint: {checkpoint}")
    current = load_research_claim(
        spark,
        catalog=catalog,
        schema=schema,
        research_build_id=research_build_id,
    )
    if current is None:
        raise ResearchClaimConflictError("Model research claim does not exist")
    if current.terminal:
        if current.checkpoint == checkpoint:
            _log_claim_output(
                catalog=catalog,
                schema=schema,
                claim=current,
                operation="advance_research_claim",
                reused=True,
            )
            return current
        raise ResearchClaimConflictError(
            f"Terminal research claim cannot advance from {current.checkpoint}"
        )
    _validate_live_owner(
        current,
        owner_invocation_id=owner,
        lease_token=token,
        now=transitioned_at,
    )
    if current.checkpoint == checkpoint:
        return renew_research_claim(
            spark,
            catalog=catalog,
            schema=schema,
            research_build_id=research_build_id,
            owner_invocation_id=owner,
            lease_token=token,
            lease_seconds=lease_seconds,
            now=transitioned_at,
        )
    if current.checkpoint != expected_checkpoint:
        raise ResearchClaimConflictError(
            "Research claim is not at the expected checkpoint: "
            f"expected={expected_checkpoint}, actual={current.checkpoint}"
        )
    if _NEXT_CHECKPOINT.get(expected_checkpoint) != checkpoint:
        raise ValueError(
            "Research claim checkpoints cannot be skipped: "
            f"{expected_checkpoint} -> {checkpoint}"
        )
    binding_json = (
        None
        if research_frame_binding is None
        else serialize_research_frame_binding(research_frame_binding)
    )
    updated = replace(
        current,
        lease_expires_at=_lease_expiry(transitioned_at, lease_seconds),
        checkpoint=checkpoint,
        checkpoint_version=current.checkpoint_version + 1,
        research_frame_binding_json=_set_once(
            current.research_frame_binding_json,
            binding_json,
            "research_frame_binding_json",
        ),
        mlflow_experiment_id=_set_once(
            current.mlflow_experiment_id,
            mlflow_experiment_id,
            "mlflow_experiment_id",
        ),
        mlflow_parent_run_id=_set_once(
            current.mlflow_parent_run_id,
            mlflow_parent_run_id,
            "mlflow_parent_run_id",
        ),
        selection_decision_id=_set_once(
            current.selection_decision_id,
            selection_decision_id,
            "selection_decision_id",
        ),
        model_build_id=_set_once(
            current.model_build_id,
            model_build_id,
            "model_build_id",
        ),
        updated_at=transitioned_at,
    )
    _validate_checkpoint_payload(updated)
    return _write_transition(
        spark,
        catalog=catalog,
        schema=schema,
        current=current,
        updated=updated,
        expected_checkpoint=expected_checkpoint,
        operation="advance_research_claim",
    )


def fail_research_claim(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    research_build_id: str,
    owner_invocation_id: str,
    lease_token: str,
    expected_checkpoint: str,
    failure_reason: str,
    lease_seconds: int = DEFAULT_CLAIM_LEASE_SECONDS,
    now: datetime | None = None,
) -> ResearchClaim:
    """Record the only terminal failure state from an owned checkpoint."""
    failed_at = _now(now)
    owner = _required_text(owner_invocation_id, "owner_invocation_id")
    token = _required_text(lease_token, "lease_token")
    reason = _required_text(failure_reason, "failure_reason")
    if expected_checkpoint not in RESEARCH_CLAIM_CHECKPOINTS[:-1]:
        raise ValueError(
            f"Unsupported failure checkpoint: {expected_checkpoint}"
        )
    current = load_research_claim(
        spark,
        catalog=catalog,
        schema=schema,
        research_build_id=research_build_id,
    )
    if current is None:
        raise ResearchClaimConflictError("Model research claim does not exist")
    if current.checkpoint == FAILED:
        if current.failure_reason != reason:
            raise ResearchClaimConflictError(
                "FAILED research claim has a different failure reason"
            )
        _log_claim_output(
            catalog=catalog,
            schema=schema,
            claim=current,
            operation="fail_research_claim",
            reused=True,
        )
        return current
    if current.terminal:
        raise ResearchClaimConflictError(
            "A COMPLETE research claim cannot fail"
        )
    _validate_live_owner(
        current,
        owner_invocation_id=owner,
        lease_token=token,
        now=failed_at,
    )
    if current.checkpoint != expected_checkpoint:
        raise ResearchClaimConflictError(
            "Research claim is not at the expected failure checkpoint: "
            f"expected={expected_checkpoint}, actual={current.checkpoint}"
        )
    updated = replace(
        current,
        lease_expires_at=_lease_expiry(failed_at, lease_seconds),
        checkpoint=FAILED,
        checkpoint_version=current.checkpoint_version + 1,
        failure_reason=reason,
        updated_at=failed_at,
    )
    return _write_transition(
        spark,
        catalog=catalog,
        schema=schema,
        current=current,
        updated=updated,
        expected_checkpoint=expected_checkpoint,
        operation="fail_research_claim",
    )


__all__ = [
    "CANDIDATES_READY",
    "CLAIMED",
    "COMPLETE",
    "DEFAULT_CLAIM_LEASE_SECONDS",
    "FAILED",
    "FRAME_READY",
    "PARENT_READY",
    "REGISTERED",
    "RESEARCH_CLAIM_CHECKPOINTS",
    "SELECTION_LOCKED",
    "TERMINAL_RESEARCH_CLAIM_CHECKPOINTS",
    "ResearchClaim",
    "ResearchClaimConflictError",
    "advance_research_claim",
    "claim_research_build",
    "deserialize_research_frame_binding",
    "fail_research_claim",
    "load_research_claim",
    "release_research_claim",
    "renew_research_claim",
    "serialize_research_frame_binding",
]

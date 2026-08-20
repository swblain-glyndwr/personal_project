"""Durable, fail-closed ownership for external AutoML discovery."""

from __future__ import annotations

from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timedelta, timezone
import hashlib
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
    AUTOML_DISCOVERY_CLAIM_TABLE,
)
from next_ads.model_development.store import table_path


CLAIMED = "CLAIMED"
RUNNING = "RUNNING"
EVIDENCE_READY = "EVIDENCE_READY"
COMPLETE = "COMPLETE"
FAILED = "FAILED"
AUTOML_CLAIM_CHECKPOINTS = (
    CLAIMED,
    RUNNING,
    EVIDENCE_READY,
    COMPLETE,
    FAILED,
)
TERMINAL_AUTOML_CLAIM_CHECKPOINTS = frozenset({COMPLETE, FAILED})
DEFAULT_AUTOML_CLAIM_LEASE_SECONDS = 10_800
_EVIDENCE_FIELDS = (
    "experiment_id",
    "trial_count",
    "best_trial_id",
    "primary_metric",
    "trial_evidence_json",
    "leaderboard_run_id",
    "leaderboard_artifact_sha256",
    "leaderboard_artifact_uri",
    "recipe_artifact_uri",
)
_IDENTITY_FIELDS = (
    "request_checksum",
    "research_build_id",
    "research_attempt_id",
    "research_frame_id",
    "research_frame_attempt_id",
    "research_frame_delta_version",
    "timeout_minutes",
    "experiment_path",
    "code_sha",
)
_MUTABLE_FIELDS = (
    "lease_expires_at",
    "checkpoint",
    "checkpoint_version",
    "experiment_id",
    "trial_count",
    "best_trial_id",
    "primary_metric",
    "trial_evidence_json",
    "leaderboard_run_id",
    "leaderboard_artifact_sha256",
    "leaderboard_artifact_uri",
    "recipe_artifact_uri",
    "failure_reason",
    "updated_at",
)


class AutoMLClaimConflictError(RuntimeError):
    """Raised when external discovery cannot be proved safe to start."""


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    return value.strip()


def _optional_text(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, field_name)


def _non_negative_integer(value: object, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _timestamp(value: object, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a timestamp")
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _now(value: datetime | None) -> datetime:
    return _timestamp(value or datetime.now(timezone.utc), "now")


def _lease_expiry(now: datetime, lease_seconds: int) -> datetime:
    seconds = _non_negative_integer(lease_seconds, "lease_seconds")
    if seconds < 1:
        raise ValueError("lease_seconds must be positive")
    return now + timedelta(seconds=seconds)


@dataclass(frozen=True)
class AutoMLDiscoveryClaim:
    """One durable external-discovery request and its recovery checkpoint."""

    discovery_id: str
    discovery_attempt_id: str
    request_checksum: str
    research_build_id: str
    research_attempt_id: str
    research_frame_id: str
    research_frame_attempt_id: str
    research_frame_delta_version: int
    timeout_minutes: int
    experiment_path: str
    code_sha: str
    owner_invocation_id: str
    lease_token: str
    lease_expires_at: datetime
    checkpoint: str
    checkpoint_version: int
    experiment_id: str | None
    trial_count: int | None
    best_trial_id: str | None
    primary_metric: str | None
    trial_evidence_json: str | None
    leaderboard_run_id: str | None
    leaderboard_artifact_sha256: str | None
    leaderboard_artifact_uri: str | None
    recipe_artifact_uri: str | None
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        """Validate immutable identity, state and recovery evidence."""
        for field_name in (
            "discovery_id",
            "discovery_attempt_id",
            "request_checksum",
            "research_build_id",
            "research_attempt_id",
            "research_frame_id",
            "research_frame_attempt_id",
            "experiment_path",
            "code_sha",
            "owner_invocation_id",
            "lease_token",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name),
            )
        _non_negative_integer(
            self.research_frame_delta_version,
            "research_frame_delta_version",
        )
        timeout = _non_negative_integer(
            self.timeout_minutes, "timeout_minutes"
        )
        if timeout < 1 or timeout > 120:
            raise ValueError("timeout_minutes must be from 1 to 120")
        _timestamp(self.lease_expires_at, "lease_expires_at")
        _timestamp(self.created_at, "created_at")
        _timestamp(self.updated_at, "updated_at")
        _non_negative_integer(self.checkpoint_version, "checkpoint_version")
        if self.checkpoint not in AUTOML_CLAIM_CHECKPOINTS:
            raise ValueError(
                f"Unsupported AutoML claim checkpoint: {self.checkpoint}"
            )
        for field_name in (*_EVIDENCE_FIELDS[:1], *_EVIDENCE_FIELDS[2:]):
            object.__setattr__(
                self,
                field_name,
                _optional_text(getattr(self, field_name), field_name),
            )
        if self.trial_count is not None:
            _non_negative_integer(self.trial_count, "trial_count")
        object.__setattr__(
            self,
            "failure_reason",
            _optional_text(self.failure_reason, "failure_reason"),
        )
        if self.checkpoint in {EVIDENCE_READY, COMPLETE}:
            if self.trial_count is None or self.trial_count < 1:
                raise ValueError(
                    f"{self.checkpoint} AutoML claim needs completed trials"
                )
            missing = [
                field_name
                for field_name in _EVIDENCE_FIELDS
                if getattr(self, field_name) is None
            ]
            if missing:
                raise ValueError(
                    f"{self.checkpoint} AutoML claim is missing: "
                    + ", ".join(missing)
                )
            assert self.trial_evidence_json is not None
            assert self.leaderboard_artifact_sha256 is not None
            assert self.leaderboard_run_id is not None
            assert self.leaderboard_artifact_uri is not None
            encoded = self.trial_evidence_json.encode("utf-8")
            if len(encoded) > 1_000_000:
                raise ValueError("AutoML claim leaderboard exceeds 1 MB")
            try:
                payload = json.loads(self.trial_evidence_json)
            except json.JSONDecodeError as error:
                raise ValueError(
                    "AutoML claim trial_evidence_json must contain JSON"
                ) from error
            if hashlib.sha256(encoded).hexdigest() != (
                self.leaderboard_artifact_sha256
            ):
                raise ValueError(
                    "AutoML claim leaderboard artifact checksum differs"
                )
            expected_uri = (
                f"runs:/{self.leaderboard_run_id}/"
                "automl_discovery/leaderboard.json"
            )
            if self.leaderboard_artifact_uri != expected_uri:
                raise ValueError(
                    "AutoML claim leaderboard URI must identify its artifact"
                )
            expected_fields = {
                "schema_version",
                "research_build_id",
                "discovery_id",
                "research_parent_run_id",
                "experiment_id",
                "primary_metric",
                "trial_count",
                "best_trial_id",
                "trials",
            }
            if (
                not isinstance(payload, dict)
                or set(payload) != expected_fields
            ):
                raise ValueError("AutoML claim leaderboard fields differ")
            expected_values = {
                "schema_version": "nextads_automl_leaderboard/v1",
                "research_build_id": self.research_build_id,
                "discovery_id": self.discovery_id,
                "experiment_id": self.experiment_id,
                "primary_metric": self.primary_metric,
                "trial_count": self.trial_count,
                "best_trial_id": self.best_trial_id,
            }
            if any(
                payload.get(name) != value
                for name, value in expected_values.items()
            ):
                raise ValueError(
                    "AutoML claim leaderboard differs from claim identity"
                )
            trial_rows = payload["trials"]
            if not isinstance(trial_rows, list) or len(trial_rows) != (
                self.trial_count
            ):
                raise ValueError(
                    "AutoML claim leaderboard trial count differs"
                )
            expected_trial_fields = {
                "rank",
                "trial_id",
                "primary_metric_value",
                "notebook_artifact_uri",
                "notebook_path",
                "notebook_url",
                "is_best_trial",
            }
            if any(
                not isinstance(row, dict) or set(row) != expected_trial_fields
                for row in trial_rows
            ):
                raise ValueError("AutoML claim trial evidence fields differ")
        elif any(getattr(self, name) is not None for name in _EVIDENCE_FIELDS):
            raise ValueError(
                "Only an evidence-ready AutoML claim can contain result links"
            )
        if self.checkpoint == FAILED:
            if self.failure_reason is None:
                raise ValueError("FAILED AutoML claim needs failure_reason")
        elif self.failure_reason is not None:
            raise ValueError("Only a FAILED AutoML claim has failure_reason")

    @property
    def terminal(self) -> bool:
        """Return whether the external request has a final claim state."""
        return self.checkpoint in TERMINAL_AUTOML_CLAIM_CHECKPOINTS


def load_automl_claim(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    discovery_id: str,
) -> AutoMLDiscoveryClaim | None:
    """Load the one claim controlling a logical discovery request."""
    from pyspark.sql import functions as F

    rows = (
        spark.table(table_path(catalog, schema, AUTOML_DISCOVERY_CLAIM_TABLE))
        .where(F.col("discovery_id") == F.lit(discovery_id))
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise AutoMLClaimConflictError(
            f"Duplicate AutoML discovery claims found for {discovery_id}"
        )
    return (
        AutoMLDiscoveryClaim(**rows[0].asDict(recursive=True))
        if rows
        else None
    )


def _temporary_view(frame: Any, operation: str) -> str:
    view = f"_nextads_automl_claim_{operation}_{uuid4().hex}"
    frame.createOrReplaceTempView(view)
    return view


def _merge_claim_insert(
    spark: Any,
    *,
    table: str,
    frame: Any,
) -> None:
    """Insert once; an unknown external attempt is never taken over."""
    view = _temporary_view(frame, "acquire")
    columns = list(frame.columns)
    insert_columns = ", ".join(quote_identifier(name) for name in columns)
    insert_values = ", ".join(
        f"source.{quote_identifier(name)}" for name in columns
    )
    statement = f"""
MERGE INTO {quote_qualified_identifier(table)} AS target
USING {quote_qualified_identifier(view)} AS source
ON target.{quote_identifier("discovery_id")}
   <=> source.{quote_identifier("discovery_id")}
WHEN NOT MATCHED THEN
  INSERT ({insert_columns}) VALUES ({insert_values})
"""
    try:
        spark.sql(statement)
    finally:
        spark.catalog.dropTempView(view)


def _merge_owned_transition(
    spark: Any,
    *,
    table: str,
    frame: Any,
    expected_checkpoint: str,
) -> None:
    """Transition only the current lease token and checkpoint version."""
    view = _temporary_view(frame, "transition")
    assignments = ",\n  ".join(
        f"{quote_identifier(name)} = source.{quote_identifier(name)}"
        for name in _MUTABLE_FIELDS
    )
    statement = f"""
MERGE INTO {quote_qualified_identifier(table)} AS target
USING {quote_qualified_identifier(view)} AS source
ON target.{quote_identifier("discovery_id")}
   <=> source.{quote_identifier("discovery_id")}
WHEN MATCHED
  AND target.{quote_identifier("owner_invocation_id")}
    <=> source.{quote_identifier("owner_invocation_id")}
  AND target.{quote_identifier("lease_token")}
    <=> source.{quote_identifier("lease_token")}
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


def _comparable(value: object) -> object:
    if isinstance(value, datetime):
        return _timestamp(value, "timestamp").replace(tzinfo=None)
    return value


def _same_claim(
    left: AutoMLDiscoveryClaim, right: AutoMLDiscoveryClaim
) -> bool:
    return all(
        _comparable(getattr(left, field.name))
        == _comparable(getattr(right, field.name))
        for field in fields(AutoMLDiscoveryClaim)
    )


def _validate_identity(
    claim: AutoMLDiscoveryClaim,
    *,
    expected: dict[str, object],
) -> None:
    changed = sorted(
        name
        for name, value in expected.items()
        if getattr(claim, name) != value
    )
    if changed:
        raise AutoMLClaimConflictError(
            "AutoML discovery claim has different immutable request fields: "
            + ", ".join(changed)
        )


def validate_automl_claim_identity(
    claim: AutoMLDiscoveryClaim,
    *,
    request_checksum: str,
    research_build_id: str,
    research_attempt_id: str,
    research_frame_id: str,
    research_frame_attempt_id: str,
    research_frame_delta_version: int,
    timeout_minutes: int,
    experiment_path: str,
    code_sha: str,
) -> None:
    """Require a stored claim to describe this exact external request."""
    expected = {
        "request_checksum": _required_text(
            request_checksum, "request_checksum"
        ),
        "research_build_id": _required_text(
            research_build_id, "research_build_id"
        ),
        "research_attempt_id": _required_text(
            research_attempt_id, "research_attempt_id"
        ),
        "research_frame_id": _required_text(
            research_frame_id, "research_frame_id"
        ),
        "research_frame_attempt_id": _required_text(
            research_frame_attempt_id, "research_frame_attempt_id"
        ),
        "research_frame_delta_version": _non_negative_integer(
            research_frame_delta_version,
            "research_frame_delta_version",
        ),
        "timeout_minutes": _non_negative_integer(
            timeout_minutes, "timeout_minutes"
        ),
        "experiment_path": _required_text(experiment_path, "experiment_path"),
        "code_sha": _required_text(code_sha, "code_sha"),
    }
    _validate_identity(claim, expected=expected)


def automl_claim_recovery_error(
    claim: AutoMLDiscoveryClaim,
) -> AutoMLClaimConflictError:
    """Describe why an existing claim cannot launch another experiment."""
    links = (
        ""
        if claim.experiment_id is None
        else f", experiment_id={claim.experiment_id}"
    )
    links += f", experiment_path={claim.experiment_path}"
    if claim.checkpoint == FAILED:
        links += f", failure_reason={claim.failure_reason}"
        action = (
            "Inspect the failed receipt and resolve the recorded cause; submit "
            "a changed request checksum before a new discovery."
        )
    else:
        action = (
            "Inspect the owning job and experiment path, then reconcile the "
            "claim from durable evidence. A duplicate experiment will not be "
            "launched automatically."
        )
    return AutoMLClaimConflictError(
        "AutoML discovery is fail-closed because an existing request is "
        f"{claim.checkpoint}: discovery_id={claim.discovery_id}, "
        f"attempt_id={claim.discovery_attempt_id}, "
        f"owner={claim.owner_invocation_id}{links}. {action}"
    )


def claim_automl_discovery(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    discovery_id: str,
    discovery_attempt_id: str,
    request_checksum: str,
    research_build_id: str,
    research_attempt_id: str,
    research_frame_id: str,
    research_frame_attempt_id: str,
    research_frame_delta_version: int,
    timeout_minutes: int,
    experiment_path: str,
    code_sha: str,
    owner_invocation_id: str,
    lease_seconds: int = DEFAULT_AUTOML_CLAIM_LEASE_SECONDS,
    now: datetime | None = None,
) -> AutoMLDiscoveryClaim:
    """Claim a new request without ever taking over unknown external work."""
    acquired_at = _now(now)
    proposed = AutoMLDiscoveryClaim(
        discovery_id=_required_text(discovery_id, "discovery_id"),
        discovery_attempt_id=_required_text(
            discovery_attempt_id, "discovery_attempt_id"
        ),
        request_checksum=_required_text(request_checksum, "request_checksum"),
        research_build_id=_required_text(
            research_build_id, "research_build_id"
        ),
        research_attempt_id=_required_text(
            research_attempt_id, "research_attempt_id"
        ),
        research_frame_id=_required_text(
            research_frame_id, "research_frame_id"
        ),
        research_frame_attempt_id=_required_text(
            research_frame_attempt_id, "research_frame_attempt_id"
        ),
        research_frame_delta_version=research_frame_delta_version,
        timeout_minutes=timeout_minutes,
        experiment_path=_required_text(experiment_path, "experiment_path"),
        code_sha=_required_text(code_sha, "code_sha"),
        owner_invocation_id=_required_text(
            owner_invocation_id, "owner_invocation_id"
        ),
        lease_token=uuid4().hex,
        lease_expires_at=_lease_expiry(acquired_at, lease_seconds),
        checkpoint=CLAIMED,
        checkpoint_version=0,
        experiment_id=None,
        trial_count=None,
        best_trial_id=None,
        primary_metric=None,
        trial_evidence_json=None,
        leaderboard_run_id=None,
        leaderboard_artifact_sha256=None,
        leaderboard_artifact_uri=None,
        recipe_artifact_uri=None,
        failure_reason=None,
        created_at=acquired_at,
        updated_at=acquired_at,
    )
    target = table_path(catalog, schema, AUTOML_DISCOVERY_CLAIM_TABLE)
    frame = typed_table_frame(spark, target, [asdict(proposed)])
    _merge_claim_insert(spark, table=target, frame=frame)
    stored = load_automl_claim(
        spark,
        catalog=catalog,
        schema=schema,
        discovery_id=proposed.discovery_id,
    )
    if stored is None:
        raise AutoMLClaimConflictError(
            "AutoML discovery claim was not persisted"
        )
    _validate_identity(
        stored,
        expected={name: getattr(proposed, name) for name in _IDENTITY_FIELDS},
    )
    if stored.lease_token != proposed.lease_token:
        raise automl_claim_recovery_error(stored)
    log_output_location(
        target,
        kind="delta_table",
        details={
            "checkpoint": stored.checkpoint,
            "checkpoint_version": stored.checkpoint_version,
            "operation": "claim_automl_discovery",
            "reused": False,
        },
    )
    return stored


def _owned_transition(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    current: AutoMLDiscoveryClaim,
    expected_checkpoint: str,
    checkpoint: str,
    operation: str,
    now: datetime | None = None,
    **changes: object,
) -> AutoMLDiscoveryClaim:
    transitioned_at = _now(now)
    if current.checkpoint != expected_checkpoint:
        raise AutoMLClaimConflictError(
            "AutoML discovery claim is not at the expected checkpoint: "
            f"expected={expected_checkpoint}, actual={current.checkpoint}"
        )
    updated = replace(
        current,
        checkpoint=checkpoint,
        checkpoint_version=current.checkpoint_version + 1,
        updated_at=transitioned_at,
        **changes,
    )
    target = table_path(catalog, schema, AUTOML_DISCOVERY_CLAIM_TABLE)
    frame = typed_table_frame(spark, target, [asdict(updated)])
    _merge_owned_transition(
        spark,
        table=target,
        frame=frame,
        expected_checkpoint=expected_checkpoint,
    )
    stored = load_automl_claim(
        spark,
        catalog=catalog,
        schema=schema,
        discovery_id=current.discovery_id,
    )
    if stored is None or not _same_claim(stored, updated):
        raise AutoMLClaimConflictError(
            "AutoML discovery claim changed concurrently during transition"
        )
    log_output_location(
        target,
        kind="delta_table",
        details={
            "checkpoint": stored.checkpoint,
            "checkpoint_version": stored.checkpoint_version,
            "operation": operation,
            "previous_checkpoint": expected_checkpoint,
            "reused": False,
        },
    )
    return stored


def start_automl_discovery(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    claim: AutoMLDiscoveryClaim,
    now: datetime | None = None,
) -> AutoMLDiscoveryClaim:
    """Mark the external operation as started before calling AutoML."""
    return _owned_transition(
        spark,
        catalog=catalog,
        schema=schema,
        current=claim,
        expected_checkpoint=CLAIMED,
        checkpoint=RUNNING,
        operation="start_automl_discovery",
        now=now,
    )


def record_automl_evidence(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    claim: AutoMLDiscoveryClaim,
    experiment_id: str,
    trial_count: int,
    best_trial_id: str,
    primary_metric: str,
    trial_evidence_json: str,
    leaderboard_run_id: str,
    leaderboard_artifact_sha256: str,
    leaderboard_artifact_uri: str,
    recipe_artifact_uri: str,
    now: datetime | None = None,
) -> AutoMLDiscoveryClaim:
    """Lock returned experiment and trial evidence before receipt writes."""
    return _owned_transition(
        spark,
        catalog=catalog,
        schema=schema,
        current=claim,
        expected_checkpoint=RUNNING,
        checkpoint=EVIDENCE_READY,
        operation="record_automl_evidence",
        now=now,
        experiment_id=_required_text(experiment_id, "experiment_id"),
        trial_count=_non_negative_integer(trial_count, "trial_count"),
        best_trial_id=_required_text(best_trial_id, "best_trial_id"),
        primary_metric=_required_text(primary_metric, "primary_metric"),
        trial_evidence_json=_required_text(
            trial_evidence_json, "trial_evidence_json"
        ),
        leaderboard_run_id=_required_text(
            leaderboard_run_id, "leaderboard_run_id"
        ),
        leaderboard_artifact_sha256=_required_text(
            leaderboard_artifact_sha256,
            "leaderboard_artifact_sha256",
        ),
        leaderboard_artifact_uri=_required_text(
            leaderboard_artifact_uri, "leaderboard_artifact_uri"
        ),
        recipe_artifact_uri=_required_text(
            recipe_artifact_uri, "recipe_artifact_uri"
        ),
    )


def complete_automl_claim(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    claim: AutoMLDiscoveryClaim,
    now: datetime | None = None,
) -> AutoMLDiscoveryClaim:
    """Complete an owned claim after its immutable READY receipt exists."""
    return _owned_transition(
        spark,
        catalog=catalog,
        schema=schema,
        current=claim,
        expected_checkpoint=EVIDENCE_READY,
        checkpoint=COMPLETE,
        operation="complete_automl_claim",
        now=now,
    )


def fail_automl_claim(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    claim: AutoMLDiscoveryClaim,
    failure_reason: str,
    now: datetime | None = None,
) -> AutoMLDiscoveryClaim:
    """Persist a known terminal failure without allowing an identical rerun."""
    if claim.checkpoint not in {CLAIMED, RUNNING}:
        raise AutoMLClaimConflictError(
            "AutoML evidence already exists and must be recovered, not failed"
        )
    return _owned_transition(
        spark,
        catalog=catalog,
        schema=schema,
        current=claim,
        expected_checkpoint=claim.checkpoint,
        checkpoint=FAILED,
        operation="fail_automl_claim",
        now=now,
        failure_reason=_required_text(failure_reason, "failure_reason"),
    )


__all__ = [
    "AUTOML_CLAIM_CHECKPOINTS",
    "CLAIMED",
    "COMPLETE",
    "DEFAULT_AUTOML_CLAIM_LEASE_SECONDS",
    "EVIDENCE_READY",
    "FAILED",
    "RUNNING",
    "TERMINAL_AUTOML_CLAIM_CHECKPOINTS",
    "AutoMLClaimConflictError",
    "AutoMLDiscoveryClaim",
    "automl_claim_recovery_error",
    "claim_automl_discovery",
    "complete_automl_claim",
    "fail_automl_claim",
    "load_automl_claim",
    "record_automl_evidence",
    "start_automl_discovery",
    "validate_automl_claim_identity",
]

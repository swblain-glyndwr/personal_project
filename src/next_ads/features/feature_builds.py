"""Immutable build and snapshot contracts for offline features."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import date, datetime
import re
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from next_ads.common.delta_writes import DeltaWriteReceipt


BUILDING = "BUILDING"
VALIDATING = "VALIDATING"
READY = "READY"
FAILED = "FAILED"
PASS = "PASS"
FAIL = "FAIL"

VALID_BUILD_STATUSES = frozenset({BUILDING, VALIDATING, READY, FAILED})
VALID_SNAPSHOT_STATUSES = frozenset({BUILDING, READY, FAILED})
VALID_VALIDATION_STATUSES = frozenset({PASS, FAIL})

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


def _text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    return value.strip()


def _optional_text(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    return _text(value, field_name)


def _count(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return value


def _optional_count(value: Any, field_name: str) -> int | None:
    if value is None:
        return None
    return _count(value, field_name)


def _date(value: Any, field_name: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ValueError(f"{field_name} must be a date")
    return value


def _timestamp(value: Any, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    return value


def _optional_timestamp(value: Any, field_name: str) -> datetime | None:
    if value is None:
        return None
    return _timestamp(value, field_name)


def _sha256(value: Any, field_name: str) -> str:
    value = _text(value, field_name)
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f"{field_name} must be a lowercase 64-character SHA-256 digest"
        )
    return value


def _identifiers(values: Iterable[str], field_name: str) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise ValueError(f"{field_name} must be a collection of identifiers")
    result = tuple(_text(value, field_name) for value in values)
    if not result:
        raise ValueError(f"{field_name} must contain at least one value")
    if len(result) != len(set(result)):
        raise ValueError(f"{field_name} must contain unique values")
    return result


@dataclass(frozen=True)
class FeatureSourceBinding:
    """One Delta source read at an exact version by a build attempt."""

    feature_build_id: str
    feature_build_attempt_id: str
    reference_date: date
    source_name: str
    source_table: str
    delta_version: int
    schema_checksum: str
    captured_at: datetime
    row_count: int | None = None
    source_feature_id: str | None = None
    source_feature_build_id: str | None = None
    source_feature_build_attempt_id: str | None = None
    source_write_receipt_id: str | None = None

    def __post_init__(self) -> None:
        """Validate an exact external or upstream feature binding."""
        for field_name in (
            "feature_build_id",
            "feature_build_attempt_id",
            "source_name",
            "source_table",
        ):
            _text(getattr(self, field_name), field_name)
        _date(self.reference_date, "reference_date")
        _count(self.delta_version, "delta_version")
        _sha256(self.schema_checksum, "schema_checksum")
        _timestamp(self.captured_at, "captured_at")
        _optional_count(self.row_count, "row_count")

        lineage = (
            self.source_feature_id,
            self.source_feature_build_id,
            self.source_feature_build_attempt_id,
            self.source_write_receipt_id,
        )
        if any(value is not None for value in lineage):
            if not all(value is not None for value in lineage):
                raise ValueError(
                    "Upstream feature lineage fields must be supplied together"
                )
            for field_name in (
                "source_feature_id",
                "source_feature_build_id",
                "source_feature_build_attempt_id",
                "source_write_receipt_id",
            ):
                _text(getattr(self, field_name), field_name)

    @classmethod
    def from_feature_output(
        cls,
        output: "FeatureOutputBinding",
        *,
        consumer_build_id: str,
        consumer_build_attempt_id: str,
        source_name: str,
        captured_at: datetime,
    ) -> "FeatureSourceBinding":
        """Pin an upstream feature attempt as a downstream Delta source."""
        return cls(
            feature_build_id=consumer_build_id,
            feature_build_attempt_id=consumer_build_attempt_id,
            reference_date=output.reference_date,
            source_name=source_name,
            source_table=output.backing_table,
            delta_version=output.delta_version,
            schema_checksum=output.output_schema_checksum,
            captured_at=captured_at,
            row_count=output.row_count,
            source_feature_id=output.feature_id,
            source_feature_build_id=output.feature_build_id,
            source_feature_build_attempt_id=(
                output.feature_build_attempt_id
            ),
            source_write_receipt_id=output.write_receipt_id,
        )


@dataclass(frozen=True)
class FeatureOutputBinding:
    """Validated backing-table output produced by one build attempt."""

    feature_build_id: str
    feature_build_attempt_id: str
    reference_date: date
    feature_id: str
    backing_table: str
    delta_version: int
    row_count: int
    contract_schema_checksum: str
    output_schema_checksum: str
    backing_schema_checksum: str
    value_checksum: str
    write_receipt_id: str
    write_duration_ms: int
    retry_count: int
    committed_at: datetime
    validated_at: datetime
    null_key_count: int
    duplicate_key_count: int
    freshness_status: str
    row_drift_status: str
    validation_status: str

    def __post_init__(self) -> None:
        """Validate exact output identity and its quality result."""
        for field_name in (
            "feature_build_id",
            "feature_build_attempt_id",
            "feature_id",
            "backing_table",
            "write_receipt_id",
        ):
            _text(getattr(self, field_name), field_name)
        _date(self.reference_date, "reference_date")
        for field_name in (
            "delta_version",
            "row_count",
            "write_duration_ms",
            "retry_count",
            "null_key_count",
            "duplicate_key_count",
        ):
            _count(getattr(self, field_name), field_name)
        for field_name in (
            "contract_schema_checksum",
            "output_schema_checksum",
            "backing_schema_checksum",
            "value_checksum",
        ):
            _sha256(getattr(self, field_name), field_name)
        _timestamp(self.committed_at, "committed_at")
        _timestamp(self.validated_at, "validated_at")
        if self.validated_at < self.committed_at:
            raise ValueError("validated_at cannot be before committed_at")
        for field_name in (
            "freshness_status",
            "row_drift_status",
            "validation_status",
        ):
            status = _text(getattr(self, field_name), field_name).upper()
            if status not in VALID_VALIDATION_STATUSES:
                raise ValueError(
                    f"Unsupported {field_name}: {getattr(self, field_name)}"
                )
            object.__setattr__(self, field_name, status)

    @property
    def passed(self) -> bool:
        """Return whether all publication-blocking checks passed."""
        return (
            self.validation_status == PASS
            and self.freshness_status == PASS
            and self.row_drift_status == PASS
            and self.null_key_count == 0
            and self.duplicate_key_count == 0
            and self.contract_schema_checksum
            == self.output_schema_checksum
        )

    @classmethod
    def from_delta_write_receipt(
        cls,
        receipt: "DeltaWriteReceipt",
        *,
        feature_build_id: str,
        feature_build_attempt_id: str,
        reference_date: date,
        feature_id: str,
        contract_schema_checksum: str,
        output_schema_checksum: str,
        value_checksum: str,
        validated_at: datetime,
        null_key_count: int,
        duplicate_key_count: int,
        freshness_status: str,
        row_drift_status: str,
        validation_status: str,
    ) -> "FeatureOutputBinding":
        """Create a binding from the common atomic Delta write receipt."""
        if receipt.build_id != feature_build_id:
            raise ValueError("Delta receipt build_id does not match the build")
        if receipt.attempt_id != feature_build_attempt_id:
            raise ValueError(
                "Delta receipt attempt_id does not match the build attempt"
            )
        if receipt.committed_at is None:
            raise ValueError("Delta receipt must include committed_at")

        binding = receipt.as_binding()
        return cls(
            feature_build_id=feature_build_id,
            feature_build_attempt_id=feature_build_attempt_id,
            reference_date=reference_date,
            feature_id=feature_id,
            backing_table=binding["table"],
            delta_version=binding["delta_version"],
            row_count=binding["row_count"],
            contract_schema_checksum=contract_schema_checksum,
            output_schema_checksum=output_schema_checksum,
            backing_schema_checksum=binding["schema_checksum"],
            value_checksum=value_checksum,
            write_receipt_id=binding["write_receipt_id"],
            write_duration_ms=binding["write_duration_ms"],
            retry_count=binding["retry_count"],
            committed_at=receipt.committed_at,
            validated_at=validated_at,
            null_key_count=null_key_count,
            duplicate_key_count=duplicate_key_count,
            freshness_status=freshness_status,
            row_drift_status=row_drift_status,
            validation_status=validation_status,
        )


@dataclass(frozen=True)
class FeatureBuild:
    """One immutable attempt to build the required offline feature graph."""

    feature_build_id: str
    feature_build_attempt_id: str
    reference_date: date
    registry_checksum: str
    git_commit: str
    required_feature_ids: tuple[str, ...]
    status: str
    started_at: datetime
    sources: tuple[FeatureSourceBinding, ...] = ()
    outputs: tuple[FeatureOutputBinding, ...] = ()
    completed_at: datetime | None = None
    failure_reason: str | None = None
    job_run_id: int | None = None
    execution_count: int = 0

    def __post_init__(self) -> None:
        """Ensure READY means every declared output passed validation."""
        for field_name in (
            "feature_build_id",
            "feature_build_attempt_id",
            "git_commit",
        ):
            _text(getattr(self, field_name), field_name)
        _date(self.reference_date, "reference_date")
        _sha256(self.registry_checksum, "registry_checksum")
        _timestamp(self.started_at, "started_at")
        _optional_timestamp(self.completed_at, "completed_at")
        _optional_text(self.failure_reason, "failure_reason")
        _optional_count(self.job_run_id, "job_run_id")
        _count(self.execution_count, "execution_count")

        if self.status not in VALID_BUILD_STATUSES:
            raise ValueError(f"Unsupported feature build status: {self.status}")
        required = _identifiers(
            self.required_feature_ids,
            "required_feature_ids",
        )
        object.__setattr__(self, "required_feature_ids", required)
        sources = tuple(self.sources)
        outputs = tuple(self.outputs)
        object.__setattr__(self, "sources", sources)
        object.__setattr__(self, "outputs", outputs)
        self._validate_sources(sources)
        self._validate_outputs(outputs, required)
        self._validate_status(required, sources, outputs)

    def _validate_sources(
        self,
        sources: tuple[FeatureSourceBinding, ...],
    ) -> None:
        names = [source.source_name for source in sources]
        if len(names) != len(set(names)):
            raise ValueError("Feature build source names must be unique")
        expected = (
            self.feature_build_id,
            self.feature_build_attempt_id,
            self.reference_date,
        )
        for source in sources:
            actual = (
                source.feature_build_id,
                source.feature_build_attempt_id,
                source.reference_date,
            )
            if actual != expected:
                raise ValueError("Feature sources must match the build attempt")

    def _validate_outputs(
        self,
        outputs: tuple[FeatureOutputBinding, ...],
        required: tuple[str, ...],
    ) -> None:
        feature_ids = [output.feature_id for output in outputs]
        if len(feature_ids) != len(set(feature_ids)):
            raise ValueError("Feature build output IDs must be unique")
        unexpected = sorted(set(feature_ids).difference(required))
        if unexpected:
            raise ValueError(
                "Feature build contains undeclared outputs: "
                + ", ".join(unexpected)
            )
        expected = (
            self.feature_build_id,
            self.feature_build_attempt_id,
            self.reference_date,
        )
        for output in outputs:
            actual = (
                output.feature_build_id,
                output.feature_build_attempt_id,
                output.reference_date,
            )
            if actual != expected:
                raise ValueError("Feature outputs must match the build attempt")

    def _validate_status(
        self,
        required: tuple[str, ...],
        sources: tuple[FeatureSourceBinding, ...],
        outputs: tuple[FeatureOutputBinding, ...],
    ) -> None:
        if self.status in {BUILDING, VALIDATING}:
            if self.completed_at is not None or self.failure_reason is not None:
                raise ValueError(
                    "An in-progress feature build cannot be completed or failed"
                )
            return
        if self.completed_at is None:
            raise ValueError("A terminal feature build needs completed_at")
        if self.completed_at < self.started_at:
            raise ValueError("completed_at cannot be before started_at")
        if any(output.validated_at > self.completed_at for output in outputs):
            raise ValueError("Feature outputs must be validated before completion")
        if self.status == FAILED:
            if self.failure_reason is None:
                raise ValueError("A failed feature build needs failure_reason")
            return
        if self.failure_reason is not None:
            raise ValueError("A READY feature build cannot have failure_reason")
        if not sources:
            raise ValueError("A READY feature build must contain sources")
        output_ids = {output.feature_id for output in outputs}
        if output_ids != set(required):
            raise ValueError(
                "A READY feature build must contain every required output"
            )
        failed = sorted(output.feature_id for output in outputs if not output.passed)
        if failed:
            raise ValueError(
                "A READY feature build contains failed outputs: "
                + ", ".join(failed)
            )


@dataclass(frozen=True)
class FeatureSnapshotBinding:
    """Exact retained backing data exposed by one READY snapshot."""

    feature_snapshot_id: str
    feature_snapshot_attempt_id: str
    feature_build_id: str
    feature_build_attempt_id: str
    reference_date: date
    feature_id: str
    backing_table: str
    delta_version: int
    row_count: int
    output_schema_checksum: str
    backing_schema_checksum: str
    value_checksum: str
    write_receipt_id: str
    bound_at: datetime

    def __post_init__(self) -> None:
        """Validate a snapshot's exact backing-table binding."""
        for field_name in (
            "feature_snapshot_id",
            "feature_snapshot_attempt_id",
            "feature_build_id",
            "feature_build_attempt_id",
            "feature_id",
            "backing_table",
            "write_receipt_id",
        ):
            _text(getattr(self, field_name), field_name)
        _date(self.reference_date, "reference_date")
        _count(self.delta_version, "delta_version")
        _count(self.row_count, "row_count")
        _sha256(self.output_schema_checksum, "output_schema_checksum")
        _sha256(self.backing_schema_checksum, "backing_schema_checksum")
        _sha256(self.value_checksum, "value_checksum")
        _timestamp(self.bound_at, "bound_at")

    @classmethod
    def from_feature_output(
        cls,
        output: FeatureOutputBinding,
        *,
        feature_snapshot_id: str,
        feature_snapshot_attempt_id: str,
        bound_at: datetime,
    ) -> "FeatureSnapshotBinding":
        """Bind a validated output without copying or rewriting its data."""
        if not output.passed:
            raise ValueError("A snapshot cannot bind a failed feature output")
        return cls(
            feature_snapshot_id=feature_snapshot_id,
            feature_snapshot_attempt_id=feature_snapshot_attempt_id,
            feature_build_id=output.feature_build_id,
            feature_build_attempt_id=output.feature_build_attempt_id,
            reference_date=output.reference_date,
            feature_id=output.feature_id,
            backing_table=output.backing_table,
            delta_version=output.delta_version,
            row_count=output.row_count,
            output_schema_checksum=output.output_schema_checksum,
            backing_schema_checksum=output.backing_schema_checksum,
            value_checksum=output.value_checksum,
            write_receipt_id=output.write_receipt_id,
            bound_at=bound_at,
        )


@dataclass(frozen=True)
class FeatureSnapshot:
    """Consumer-visible collection of exact feature output bindings."""

    feature_snapshot_id: str
    feature_snapshot_attempt_id: str
    feature_build_id: str
    feature_build_attempt_id: str
    reference_date: date
    registry_checksum: str
    git_commit: str
    required_feature_ids: tuple[str, ...]
    status: str
    created_at: datetime
    bindings: tuple[FeatureSnapshotBinding, ...] = ()
    completed_at: datetime | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        """Ensure only a complete, validated snapshot can be READY."""
        for field_name in (
            "feature_snapshot_id",
            "feature_snapshot_attempt_id",
            "feature_build_id",
            "feature_build_attempt_id",
            "git_commit",
        ):
            _text(getattr(self, field_name), field_name)
        _date(self.reference_date, "reference_date")
        _sha256(self.registry_checksum, "registry_checksum")
        _timestamp(self.created_at, "created_at")
        _optional_timestamp(self.completed_at, "completed_at")
        _optional_text(self.failure_reason, "failure_reason")
        if self.status not in VALID_SNAPSHOT_STATUSES:
            raise ValueError(f"Unsupported feature snapshot status: {self.status}")

        required = _identifiers(
            self.required_feature_ids,
            "required_feature_ids",
        )
        object.__setattr__(self, "required_feature_ids", required)
        bindings = tuple(self.bindings)
        object.__setattr__(self, "bindings", bindings)
        self._validate_bindings(bindings, required)
        self._validate_status(bindings, required)

    def _validate_bindings(
        self,
        bindings: tuple[FeatureSnapshotBinding, ...],
        required: tuple[str, ...],
    ) -> None:
        feature_ids = [binding.feature_id for binding in bindings]
        if len(feature_ids) != len(set(feature_ids)):
            raise ValueError("Feature snapshot binding IDs must be unique")
        unexpected = sorted(set(feature_ids).difference(required))
        if unexpected:
            raise ValueError(
                "Feature snapshot contains undeclared bindings: "
                + ", ".join(unexpected)
            )
        expected = (
            self.feature_snapshot_id,
            self.feature_snapshot_attempt_id,
            self.feature_build_id,
            self.feature_build_attempt_id,
            self.reference_date,
        )
        for binding in bindings:
            actual = (
                binding.feature_snapshot_id,
                binding.feature_snapshot_attempt_id,
                binding.feature_build_id,
                binding.feature_build_attempt_id,
                binding.reference_date,
            )
            if actual != expected:
                raise ValueError(
                    "Snapshot bindings must match the snapshot and build attempt"
                )
            if binding.bound_at < self.created_at:
                raise ValueError("Snapshot bindings cannot predate the snapshot")

    def _validate_status(
        self,
        bindings: tuple[FeatureSnapshotBinding, ...],
        required: tuple[str, ...],
    ) -> None:
        if self.status == BUILDING:
            if self.completed_at is not None or self.failure_reason is not None:
                raise ValueError(
                    "A BUILDING feature snapshot cannot be completed or failed"
                )
            return
        if self.completed_at is None:
            raise ValueError("A terminal feature snapshot needs completed_at")
        if self.completed_at < self.created_at:
            raise ValueError("completed_at cannot be before created_at")
        if any(binding.bound_at > self.completed_at for binding in bindings):
            raise ValueError("Snapshot bindings must be written before completion")
        if self.status == FAILED:
            if self.failure_reason is None:
                raise ValueError("A failed feature snapshot needs failure_reason")
            return
        if self.failure_reason is not None:
            raise ValueError("A READY feature snapshot cannot have failure_reason")
        if {binding.feature_id for binding in bindings} != set(required):
            raise ValueError(
                "A READY feature snapshot must contain every required binding"
            )


def mark_feature_build_ready(
    build: FeatureBuild,
    *,
    completed_at: datetime,
) -> FeatureBuild:
    """Finish a build only after its constructor can prove all outputs pass."""
    if build.status not in {BUILDING, VALIDATING}:
        raise ValueError("Only an in-progress feature build can become READY")
    return replace(
        build,
        status=READY,
        completed_at=completed_at,
        failure_reason=None,
    )


def mark_feature_build_failed(
    build: FeatureBuild,
    *,
    failure_reason: str,
    completed_at: datetime,
) -> FeatureBuild:
    """Fail one attempt without deleting any backing output it wrote."""
    if build.status not in {BUILDING, VALIDATING}:
        raise ValueError("Only an in-progress feature build can fail")
    return replace(
        build,
        status=FAILED,
        completed_at=completed_at,
        failure_reason=failure_reason,
    )


def prepare_feature_snapshot(
    build: FeatureBuild,
    *,
    feature_snapshot_id: str,
    feature_snapshot_attempt_id: str,
    created_at: datetime,
) -> FeatureSnapshot:
    """Prepare complete bindings while leaving the snapshot non-readable."""
    if build.status != READY:
        raise ValueError("Only a READY feature build can prepare a snapshot")
    bindings = tuple(
        FeatureSnapshotBinding.from_feature_output(
            output,
            feature_snapshot_id=feature_snapshot_id,
            feature_snapshot_attempt_id=feature_snapshot_attempt_id,
            bound_at=created_at,
        )
        for output in build.outputs
    )
    return FeatureSnapshot(
        feature_snapshot_id=feature_snapshot_id,
        feature_snapshot_attempt_id=feature_snapshot_attempt_id,
        feature_build_id=build.feature_build_id,
        feature_build_attempt_id=build.feature_build_attempt_id,
        reference_date=build.reference_date,
        registry_checksum=build.registry_checksum,
        git_commit=build.git_commit,
        required_feature_ids=build.required_feature_ids,
        status=BUILDING,
        created_at=created_at,
        bindings=bindings,
    )


def mark_feature_snapshot_ready(
    snapshot: FeatureSnapshot,
    build: FeatureBuild,
    *,
    persisted_feature_ids: Iterable[str],
    completed_at: datetime,
) -> FeatureSnapshot:
    """Mark READY only after every exact binding has been persisted."""
    if snapshot.status != BUILDING:
        raise ValueError("Only a BUILDING feature snapshot can become READY")
    if build.status != READY:
        raise ValueError("A feature snapshot requires a READY feature build")
    expected_build = (
        snapshot.feature_build_id,
        snapshot.feature_build_attempt_id,
        snapshot.reference_date,
        snapshot.registry_checksum,
        snapshot.git_commit,
        snapshot.required_feature_ids,
    )
    actual_build = (
        build.feature_build_id,
        build.feature_build_attempt_id,
        build.reference_date,
        build.registry_checksum,
        build.git_commit,
        build.required_feature_ids,
    )
    if actual_build != expected_build:
        raise ValueError("Feature snapshot does not match the READY build")

    persisted = set(
        _identifiers(persisted_feature_ids, "persisted_feature_ids")
    )
    expected = set(snapshot.required_feature_ids)
    if persisted != expected:
        raise ValueError(
            "Every required snapshot binding must be persisted before READY"
        )
    output_proof = {
        (
            output.feature_id,
            output.backing_table,
            output.delta_version,
            output.row_count,
            output.output_schema_checksum,
            output.backing_schema_checksum,
            output.value_checksum,
            output.write_receipt_id,
        )
        for output in build.outputs
    }
    binding_proof = {
        (
            binding.feature_id,
            binding.backing_table,
            binding.delta_version,
            binding.row_count,
            binding.output_schema_checksum,
            binding.backing_schema_checksum,
            binding.value_checksum,
            binding.write_receipt_id,
        )
        for binding in snapshot.bindings
    }
    if binding_proof != output_proof:
        raise ValueError("Snapshot bindings do not match the READY outputs")
    return replace(snapshot, status=READY, completed_at=completed_at)


def mark_feature_snapshot_failed(
    snapshot: FeatureSnapshot,
    *,
    failure_reason: str,
    completed_at: datetime,
) -> FeatureSnapshot:
    """Fail an unpublished attempt while retaining its backing data."""
    if snapshot.status != BUILDING:
        raise ValueError("Only a BUILDING feature snapshot can fail")
    return replace(
        snapshot,
        status=FAILED,
        completed_at=completed_at,
        failure_reason=failure_reason,
    )


def select_latest_ready_snapshot(
    snapshots: Iterable[FeatureSnapshot],
    *,
    reference_date: date | None = None,
) -> FeatureSnapshot | None:
    """Select only READY state, ignoring newer failed or partial attempts."""
    candidates = tuple(snapshots)
    attempt_ids = [item.feature_snapshot_attempt_id for item in candidates]
    if len(attempt_ids) != len(set(attempt_ids)):
        raise ValueError("Feature snapshot attempt IDs must be unique")
    ready = [
        item
        for item in candidates
        if item.status == READY
        and (reference_date is None or item.reference_date == reference_date)
    ]
    if not ready:
        return None
    return max(
        ready,
        key=lambda item: (
            item.completed_at,
            item.feature_snapshot_attempt_id,
        ),
    )


def resolve_ready_feature(
    snapshot: FeatureSnapshot,
    feature_id: str,
) -> FeatureSnapshotBinding:
    """Resolve one feature only through a complete READY snapshot."""
    if snapshot.status != READY:
        raise ValueError("Features can be resolved only from a READY snapshot")
    requested = _text(feature_id, "feature_id")
    for binding in snapshot.bindings:
        if binding.feature_id == requested:
            return binding
    raise KeyError(f"Feature is not bound by snapshot: {requested}")


__all__ = [
    "BUILDING",
    "FAIL",
    "FAILED",
    "FeatureBuild",
    "FeatureOutputBinding",
    "FeatureSnapshot",
    "FeatureSnapshotBinding",
    "FeatureSourceBinding",
    "PASS",
    "READY",
    "VALIDATING",
    "mark_feature_build_failed",
    "mark_feature_build_ready",
    "mark_feature_snapshot_failed",
    "mark_feature_snapshot_ready",
    "prepare_feature_snapshot",
    "resolve_ready_feature",
    "select_latest_ready_snapshot",
]

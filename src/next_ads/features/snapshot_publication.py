"""Publish validated feature groups through exact READY Delta bindings."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
import hashlib
from pathlib import Path
from typing import Any, Mapping

from next_ads.common.delta_writes import (
    DeltaWriteReceipt,
    schema_checksum,
    validate_unique_non_null_keys,
)
from next_ads.features.feature_build_store import (
    latest_ready_snapshot_row,
    persist_feature_build,
    persist_feature_snapshot,
    ready_snapshot_bindings,
)
from next_ads.features.feature_builds import (
    BUILDING,
    VALIDATING,
    FeatureBuild,
    FeatureOutputBinding,
    FeatureSourceBinding,
    feature_value_checksum,
    mark_feature_build_failed,
    mark_feature_build_ready,
    mark_feature_snapshot_ready,
    prepare_feature_snapshot,
)
from next_ads.features.feature_store_registry import (
    DEFAULT_REGISTRY_PATH,
    FeatureStoreRegistry,
)
from next_ads.features.materialization import (
    FeatureMaterializationResult,
    align_to_feature_table_contract,
    write_feature_table,
)


def registry_file_checksum(
    path: str | Path = DEFAULT_REGISTRY_PATH,
) -> str:
    """Return the exact repository registry checksum used by a build."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def external_delta_source(
    *,
    feature_build_id: str,
    feature_build_attempt_id: str,
    reference_date: date,
    source_name: str,
    source_table: str,
    delta_version: int,
    schema_checksum_value: str,
    captured_at: datetime,
    row_count: int | None = None,
) -> FeatureSourceBinding:
    """Convert one exact external Delta receipt to build lineage."""
    return FeatureSourceBinding(
        feature_build_id=feature_build_id,
        feature_build_attempt_id=feature_build_attempt_id,
        reference_date=reference_date,
        source_name=source_name,
        source_table=source_table,
        delta_version=delta_version,
        schema_checksum=schema_checksum_value,
        captured_at=captured_at,
        row_count=row_count,
    )


def begin_feature_build(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    feature_build_id: str,
    feature_build_attempt_id: str,
    reference_date: date,
    git_commit: str,
    required_feature_ids: tuple[str, ...],
    sources: tuple[FeatureSourceBinding, ...],
    started_at: datetime,
) -> FeatureBuild:
    """Record a non-readable build before any output can be accepted."""
    build = FeatureBuild(
        feature_build_id=feature_build_id,
        feature_build_attempt_id=feature_build_attempt_id,
        reference_date=reference_date,
        registry_checksum=registry_file_checksum(),
        git_commit=git_commit,
        required_feature_ids=required_feature_ids,
        status=BUILDING,
        started_at=started_at,
        sources=sources,
        job_run_id=(
            int(feature_build_id) if feature_build_id.isdigit() else None
        ),
    )
    persist_feature_build(
        spark,
        catalog=catalog,
        schema=schema,
        build=build,
    )
    return build


def _previous_row_counts(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    build: FeatureBuild,
) -> dict[str, int]:
    header = latest_ready_snapshot_row(
        spark,
        catalog=catalog,
        schema=schema,
        reference_date=build.reference_date,
        required_feature_ids=build.required_feature_ids,
    )
    if header is None:
        return {}
    bindings = ready_snapshot_bindings(
        spark,
        catalog=catalog,
        schema=schema,
        feature_snapshot_id=header["feature_snapshot_id"],
        feature_snapshot_attempt_id=header[
            "feature_snapshot_attempt_id"
        ],
    )
    return {
        row["feature_id"]: int(row["row_count"])
        for row in bindings.collect()
    }


def publish_ready_feature_group(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    group_id: str,
    build: FeatureBuild,
    frames: Mapping[str, Any],
    receipts: Mapping[str, DeltaWriteReceipt],
    registry: FeatureStoreRegistry,
) -> tuple[FeatureBuild, Any]:
    """Validate exact outputs and publish the group snapshot READY last."""
    if build.status != BUILDING:
        raise ValueError("Feature publication requires a BUILDING attempt")
    required = set(build.required_feature_ids)
    if set(frames) != required or set(receipts) != required:
        raise ValueError(
            "Feature publication frames and receipts must match the build"
        )
    previous_counts = _previous_row_counts(
        spark,
        catalog=catalog,
        schema=schema,
        build=build,
    )
    validated_at = datetime.now(timezone.utc)
    outputs = []
    for feature_id in build.required_feature_ids:
        frame = align_to_feature_table_contract(
            frames[feature_id],
            feature_id,
            registry,
        )
        spec = registry.table_spec(feature_id)
        summary = validate_unique_non_null_keys(frame, spec.primary_keys)
        receipt = receipts[feature_id]
        expected_schema_checksum = schema_checksum(frame)
        if receipt.schema_checksum != expected_schema_checksum:
            raise ValueError(
                f"{feature_id} committed schema does not match its contract"
            )
        previous_count = previous_counts.get(feature_id)
        row_drift_status = (
            "PASS"
            if previous_count is None or previous_count == summary.row_count
            else "FAIL"
        )
        outputs.append(
            FeatureOutputBinding.from_delta_write_receipt(
                receipt,
                feature_build_id=build.feature_build_id,
                feature_build_attempt_id=build.feature_build_attempt_id,
                reference_date=build.reference_date,
                feature_id=feature_id,
                contract_schema_checksum=expected_schema_checksum,
                output_schema_checksum=expected_schema_checksum,
                value_checksum=feature_value_checksum(
                    frame,
                    excluded_columns=("created_at", "updated_at"),
                ),
                validated_at=validated_at,
                null_key_count=summary.null_key_count,
                duplicate_key_count=(
                    summary.row_count - summary.distinct_key_count
                ),
                freshness_status="PASS",
                row_drift_status=row_drift_status,
                validation_status="PASS",
            )
        )

    validating_build = replace(
        build,
        status=VALIDATING,
        outputs=tuple(outputs),
    )
    ready_build = mark_feature_build_ready(
        validating_build,
        completed_at=datetime.now(timezone.utc),
    )
    persist_feature_build(
        spark,
        catalog=catalog,
        schema=schema,
        build=ready_build,
    )

    created_at = datetime.now(timezone.utc)
    snapshot = prepare_feature_snapshot(
        ready_build,
        feature_snapshot_id=(
            f"{group_id}:{build.reference_date.isoformat()}"
        ),
        feature_snapshot_attempt_id=build.feature_build_attempt_id,
        created_at=created_at,
    )
    ready_snapshot = mark_feature_snapshot_ready(
        snapshot,
        ready_build,
        persisted_feature_ids=ready_build.required_feature_ids,
        completed_at=datetime.now(timezone.utc),
    )
    persist_feature_snapshot(
        spark,
        catalog=catalog,
        schema=schema,
        snapshot=ready_snapshot,
    )
    return ready_build, ready_snapshot


def write_and_publish_feature_group(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    group_id: str,
    feature_build_id: str,
    feature_build_attempt_id: str,
    reference_date: date,
    git_commit: str,
    frames: Mapping[str, Any],
    sources: tuple[FeatureSourceBinding, ...],
    registry: FeatureStoreRegistry,
    replace_reference_date: bool = True,
    write_options: Mapping[str, Mapping[str, Any]] | None = None,
    fail_after_writes: int | None = None,
) -> tuple[FeatureBuild, Any]:
    """Write a complete group and publish its READY snapshot last."""
    required_feature_ids = tuple(frames)
    if not required_feature_ids:
        raise ValueError("Feature publication needs at least one output")
    if fail_after_writes is not None and not (
        1 <= fail_after_writes <= len(required_feature_ids)
    ):
        raise ValueError("fail_after_writes must identify an output position")
    build = begin_feature_build(
        spark,
        catalog=catalog,
        schema=schema,
        feature_build_id=feature_build_id,
        feature_build_attempt_id=feature_build_attempt_id,
        reference_date=reference_date,
        git_commit=git_commit,
        required_feature_ids=required_feature_ids,
        sources=sources,
        started_at=datetime.now(timezone.utc),
    )
    receipts = {}
    options_by_feature = write_options or {}
    try:
        for position, (feature_id, frame) in enumerate(frames.items(), start=1):
            feature = registry.table_spec(feature_id)
            options = dict(options_by_feature.get(feature_id, {}))
            result = write_feature_table(
                spark,
                feature_id,
                frame,
                catalog=catalog,
                schema=schema,
                reference_date=reference_date,
                reference_date_column=options.pop(
                    "reference_date_column",
                    feature.timestamp_key or "reference_date",
                ),
                replace_reference_date=options.pop(
                    "replace_reference_date", replace_reference_date
                ),
                mode=options.pop("mode", feature.write_mode),
                registry=registry,
                build_id=feature_build_id,
                attempt_id=feature_build_attempt_id,
                git_commit=git_commit,
                return_receipt=True,
                **options,
            )
            if not isinstance(result, FeatureMaterializationResult):
                raise RuntimeError(
                    f"Feature output {feature_id} has no Delta receipt"
                )
            receipts[feature_id] = result.receipt
            if fail_after_writes == position:
                raise RuntimeError(
                    "Intentional personal DEV failure after feature output "
                    f"{position}"
                )
        return publish_ready_feature_group(
            spark,
            catalog=catalog,
            schema=schema,
            group_id=group_id,
            build=build,
            frames=frames,
            receipts=receipts,
            registry=registry,
        )
    except Exception as exc:
        failed_build = mark_feature_build_failed(
            build,
            failure_reason=f"{type(exc).__name__}: {exc}",
            completed_at=datetime.now(timezone.utc),
        )
        persist_feature_build(
            spark,
            catalog=catalog,
            schema=schema,
            build=failed_build,
        )
        raise


__all__ = [
    "begin_feature_build",
    "external_delta_source",
    "publish_ready_feature_group",
    "write_and_publish_feature_group",
    "registry_file_checksum",
]

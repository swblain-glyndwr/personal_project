"""Read logical offline features only through exact READY snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from next_ads.common.delta_writes import schema_checksum
from next_ads.features.feature_build_store import (
    latest_ready_feature_binding_row,
)
from next_ads.features.feature_store_registry import (
    FeatureStoreRegistry,
    load_feature_store_registry,
)


@dataclass(frozen=True)
class ReadyFeatureBinding:
    """Consumer receipt for one logical feature at an exact Delta version."""

    feature_snapshot_id: str
    feature_snapshot_attempt_id: str
    feature_build_id: str
    feature_build_attempt_id: str
    reference_date: date
    registry_checksum: str
    git_commit: str
    completed_at: datetime
    feature_id: str
    backing_table: str
    delta_version: int
    row_count: int
    output_schema_checksum: str
    backing_schema_checksum: str
    value_checksum: str
    write_receipt_id: str

    @classmethod
    def from_row(cls, row: Any) -> "ReadyFeatureBinding":
        values = row.asDict() if hasattr(row, "asDict") else dict(row)
        return cls(
            **{
                field: values[field]
                for field in cls.__dataclass_fields__
            }
        )


def resolve_ready_feature_binding(
    spark: Any,
    feature_id: str,
    *,
    catalog: str,
    schema: str,
    reference_date: str | date | None = None,
) -> ReadyFeatureBinding:
    """Resolve a logical feature without falling back to a physical latest."""
    row = latest_ready_feature_binding_row(
        spark,
        catalog=catalog,
        schema=schema,
        feature_id=feature_id,
        reference_date=reference_date,
    )
    if row is None:
        raise ValueError(
            f"No READY Feature Snapshot contains {feature_id}"
        )
    return ReadyFeatureBinding.from_row(row)


def read_ready_feature(
    spark: Any,
    feature_id: str,
    *,
    catalog: str,
    schema: str,
    reference_date: str | date | None = None,
    registry: FeatureStoreRegistry | None = None,
) -> tuple[Any, ReadyFeatureBinding]:
    """Read the exact retained Delta version and its declared date scope."""
    from pyspark.sql import functions as F

    active_registry = registry or load_feature_store_registry()
    feature = active_registry.table_spec(feature_id)
    binding = resolve_ready_feature_binding(
        spark,
        feature_id,
        catalog=catalog,
        schema=schema,
        reference_date=reference_date,
    )
    frame = spark.read.option(
        "versionAsOf", binding.delta_version
    ).table(binding.backing_table)
    if schema_checksum(frame) != binding.backing_schema_checksum:
        raise ValueError(
            f"READY binding schema no longer matches {feature_id}"
        )
    if feature.timestamp_key:
        frame = frame.where(
            F.to_date(F.col(feature.timestamp_key))
            == F.lit(binding.reference_date)
        )
    actual_rows = frame.count()
    if actual_rows != binding.row_count:
        raise ValueError(
            f"READY binding row count changed for {feature_id}: "
            f"expected {binding.row_count}, found {actual_rows}"
        )
    return frame, binding


__all__ = [
    "ReadyFeatureBinding",
    "read_ready_feature",
    "resolve_ready_feature_binding",
]

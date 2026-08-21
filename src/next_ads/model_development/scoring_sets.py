"""Build label-free scoring rows from exact READY Feature Store inputs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from next_ads.common.delta_writes import (
    schema_checksum,
    validate_unique_non_null_keys,
)
from next_ads.features.feature_build_store import (
    SNAPSHOT_BINDING_TABLE,
    SNAPSHOT_TABLE,
    metadata_table_path,
)
from next_ads.features.feature_builds import feature_value_checksum
from next_ads.features.snapshot_reader import ReadyFeatureBinding
from next_ads.model_development.contracts import ModelDefinition
from next_ads.model_development.training_sets import (
    _apply_point_in_time_lookup,
    _read_feature_history,
    _require_training_safe_feature,
    validate_snapshot_time_boundary,
)


@dataclass(frozen=True)
class ScoringFeatureBinding:
    """One exact READY feature snapshot used for evaluation scoring."""

    feature_id: str
    reference_date: date
    feature_snapshot_id: str
    feature_snapshot_attempt_id: str
    feature_build_id: str
    feature_build_attempt_id: str
    backing_table: str
    delta_version: int
    row_count: int
    schema_checksum: str
    value_checksum: str
    write_receipt_id: str

    @classmethod
    def from_ready(
        cls,
        binding: ReadyFeatureBinding,
    ) -> "ScoringFeatureBinding":
        """Keep the exact physical proof needed to reproduce one lookup."""
        return cls(
            feature_id=binding.feature_id,
            reference_date=binding.reference_date,
            feature_snapshot_id=binding.feature_snapshot_id,
            feature_snapshot_attempt_id=(binding.feature_snapshot_attempt_id),
            feature_build_id=binding.feature_build_id,
            feature_build_attempt_id=binding.feature_build_attempt_id,
            backing_table=binding.backing_table,
            delta_version=binding.delta_version,
            row_count=binding.row_count,
            schema_checksum=binding.backing_schema_checksum,
            value_checksum=binding.value_checksum,
            write_receipt_id=binding.write_receipt_id,
        )


@dataclass(frozen=True)
class ScoringSetBuildResult:
    """A label-free model frame and the exact inputs that produced it."""

    frame: Any
    feature_bindings: tuple[ScoringFeatureBinding, ...]
    row_count: int
    schema_checksum: str
    value_checksum: str


def _latest_ready_reference_date(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    feature_id: str,
    on_or_before: date,
) -> date:
    """Resolve the latest READY date containing a feature before a cutoff."""
    from pyspark.sql import functions as F

    snapshots = spark.table(
        metadata_table_path(catalog, schema, SNAPSHOT_TABLE)
    ).alias("snapshot")
    bindings = spark.table(
        metadata_table_path(catalog, schema, SNAPSHOT_BINDING_TABLE)
    ).alias("binding")
    rows = (
        snapshots.where(F.col("snapshot.status") == F.lit("READY"))
        .where(F.col("snapshot.reference_date") <= F.lit(on_or_before))
        .join(
            bindings,
            (
                F.col("snapshot.feature_snapshot_id")
                == F.col("binding.feature_snapshot_id")
            )
            & (
                F.col("snapshot.feature_snapshot_attempt_id")
                == F.col("binding.feature_snapshot_attempt_id")
            ),
            "inner",
        )
        .where(F.col("binding.feature_id") == F.lit(feature_id))
        .select(
            F.col("snapshot.reference_date").alias("reference_date"),
            F.col("snapshot.completed_at").alias("completed_at"),
            F.col("snapshot.feature_snapshot_attempt_id").alias(
                "feature_snapshot_attempt_id"
            ),
        )
        .orderBy(
            F.col("reference_date").desc(),
            F.col("completed_at").desc(),
            F.col("feature_snapshot_attempt_id").desc(),
        )
        .limit(1)
        .collect()
    )
    if not rows:
        raise ValueError(
            f"No READY {feature_id} snapshot exists on or before "
            f"{on_or_before.isoformat()}"
        )
    return rows[0]["reference_date"]


def _normalise_reference_dates(
    values: tuple[str | date, ...] | None,
) -> tuple[date, ...] | None:
    if values is None:
        return None
    normalised = tuple(
        value
        if isinstance(value, date)
        else date.fromisoformat(str(value).strip())
        for value in values
    )
    if not normalised:
        raise ValueError("Feature reference dates cannot be empty")
    if len(normalised) != len(set(normalised)):
        raise ValueError("Feature reference dates must be unique")
    return tuple(sorted(normalised))


def build_label_free_scoring_set(
    spark: Any,
    definition: ModelDefinition,
    candidates: Any,
    *,
    catalog: str,
    schema: str,
    scoring_date: date,
    candidate_keys: tuple[str, ...],
    feature_reference_dates: tuple[str | date, ...] | None = None,
) -> ScoringSetBuildResult:
    """Apply the model's declared lookups without requiring a label column."""
    from pyspark.sql import functions as F
    from next_ads.features import load_feature_store_registry

    validate_unique_non_null_keys(candidates, candidate_keys)
    expected_rows = candidates.count()
    if expected_rows == 0:
        raise ValueError("Evaluation candidate input is empty")

    reference_dates = _normalise_reference_dates(feature_reference_dates)
    observation_timestamps = {
        lookup.observation_timestamp for lookup in definition.feature_lookups
    }
    scoring_frame = candidates
    for timestamp_column in observation_timestamps:
        if timestamp_column in scoring_frame.columns:
            raise ValueError(
                "Evaluation candidates already contain reserved scoring "
                f"timestamp {timestamp_column}"
            )
        scoring_frame = scoring_frame.withColumn(
            timestamp_column,
            F.lit(scoring_date.isoformat()).cast("timestamp"),
        )

    registry = load_feature_store_registry()
    bindings: list[ScoringFeatureBinding] = []
    for lookup in definition.feature_lookups:
        feature = _require_training_safe_feature(
            registry,
            lookup.feature_id,
        )
        if reference_dates is None:
            cutoff = scoring_date - timedelta(
                days=lookup.availability_lag_days
            )
            lookup_dates = (
                _latest_ready_reference_date(
                    spark,
                    catalog=catalog,
                    schema=schema,
                    feature_id=lookup.feature_id,
                    on_or_before=cutoff,
                ),
            )
        else:
            lookup_dates = reference_dates
        feature_frame, ready_bindings = _read_feature_history(
            spark,
            feature_id=lookup.feature_id,
            catalog=catalog,
            schema=schema,
            reference_dates=lookup_dates,
            registry=registry,
        )
        for ready in ready_bindings:
            validate_snapshot_time_boundary(ready, scoring_date)
            bindings.append(ScoringFeatureBinding.from_ready(ready))
        if not feature.timestamp_key:
            raise ValueError(
                "Point-in-time scoring needs a timestamp key: "
                f"{lookup.feature_id}"
            )
        scoring_frame = _apply_point_in_time_lookup(
            scoring_frame,
            feature_frame,
            lookup,
            feature_timestamp_key=feature.timestamp_key,
            observation_keys=candidate_keys,
        )

    missing_features = sorted(
        set(definition.model_feature_columns).difference(scoring_frame.columns)
    )
    if missing_features:
        raise ValueError(
            "Scoring frame is missing declared model features: "
            + ", ".join(missing_features)
        )
    validate_unique_non_null_keys(scoring_frame, candidate_keys)
    actual_rows = scoring_frame.count()
    if actual_rows != expected_rows:
        raise ValueError(
            "Point-in-time lookups changed evaluation candidate count: "
            f"expected {expected_rows}, found {actual_rows}"
        )
    return ScoringSetBuildResult(
        frame=scoring_frame,
        feature_bindings=tuple(bindings),
        row_count=actual_rows,
        schema_checksum=schema_checksum(scoring_frame),
        value_checksum=feature_value_checksum(scoring_frame),
    )


__all__ = [
    "ScoringFeatureBinding",
    "ScoringSetBuildResult",
    "build_label_free_scoring_set",
]

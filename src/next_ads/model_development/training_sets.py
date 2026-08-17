"""Build reproducible point-in-time training sets from READY features."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
from typing import Any

from next_ads.common.delta_writes import (
    schema_checksum,
    validate_unique_non_null_keys,
)
from next_ads.features.feature_builds import feature_value_checksum
from next_ads.features.snapshot_reader import (
    ReadyFeatureBinding,
    read_ready_feature,
)
from next_ads.model_development.contracts import (
    FeatureLookupSpec,
    ModelDefinition,
    TrainingFeatureBinding,
    TrainingSetReceipt,
)


@dataclass(frozen=True)
class TrainingSetBuildResult:
    """The in-memory training frame and its immutable receipt."""

    frame: Any
    receipt: TrainingSetReceipt


def validate_snapshot_time_boundary(
    binding: ReadyFeatureBinding,
    observation_end: date,
) -> None:
    """Reject a feature snapshot created after the observation window."""
    if binding.reference_date > observation_end:
        raise ValueError(
            f"Feature {binding.feature_id} snapshot date "
            f"{binding.reference_date.isoformat()} is after observation end "
            f"{observation_end.isoformat()}"
        )


def _training_feature_binding(
    binding: ReadyFeatureBinding,
) -> TrainingFeatureBinding:
    return TrainingFeatureBinding(
        feature_id=binding.feature_id,
        feature_snapshot_id=binding.feature_snapshot_id,
        feature_snapshot_attempt_id=binding.feature_snapshot_attempt_id,
        backing_table=binding.backing_table,
        delta_version=binding.delta_version,
        row_count=binding.row_count,
        schema_checksum=binding.backing_schema_checksum,
        value_checksum=binding.value_checksum,
    )


def _date_window(frame: Any, timestamp_column: str) -> tuple[date, date]:
    from pyspark.sql import functions as F

    if timestamp_column not in frame.columns:
        raise ValueError(
            f"Observation timestamp column is missing: {timestamp_column}"
        )
    row = frame.agg(
        F.min(F.to_date(F.col(timestamp_column))).alias("start"),
        F.max(F.to_date(F.col(timestamp_column))).alias("end"),
    ).first()
    if row is None or row["start"] is None or row["end"] is None:
        raise ValueError("Training observations have no usable timestamps")
    return row["start"], row["end"]


def _lookup_output_names(lookup: FeatureLookupSpec) -> tuple[str, ...]:
    renames = dict(lookup.renames)
    return tuple(
        renames.get(column, column) for column in lookup.selected_columns
    )


def _apply_point_in_time_lookup(
    observations: Any,
    feature_frame: Any,
    lookup: FeatureLookupSpec,
    *,
    feature_timestamp_key: str,
    observation_keys: tuple[str, ...],
) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    missing_observation = sorted(
        set(dict(lookup.key_mapping).values())
        .union({lookup.observation_timestamp})
        .difference(observations.columns)
    )
    if missing_observation:
        raise ValueError(
            "Training observations are missing lookup columns: "
            + ", ".join(missing_observation)
        )
    required_feature_columns = (
        set(dict(lookup.key_mapping))
        .union(lookup.selected_columns)
        .union({feature_timestamp_key})
    )
    missing_feature = sorted(
        required_feature_columns.difference(feature_frame.columns)
    )
    if missing_feature:
        raise ValueError(
            f"Feature {lookup.feature_id} is missing columns: "
            + ", ".join(missing_feature)
        )
    output_names = _lookup_output_names(lookup)
    collisions = sorted(set(output_names).intersection(observations.columns))
    if collisions:
        raise ValueError(
            "Feature lookup output columns already exist in observations: "
            + ", ".join(collisions)
        )

    key_mapping = tuple(lookup.key_mapping)
    feature_projection = feature_frame.select(
        *(
            F.col(feature_key).alias(f"_lookup_key_{index}")
            for index, (feature_key, _observation_key) in enumerate(
                key_mapping
            )
        ),
        F.col(feature_timestamp_key).alias("_lookup_feature_time"),
        *(
            F.col(column).alias(f"_lookup_value_{index}")
            for index, column in enumerate(lookup.selected_columns)
        ),
    )
    condition = F.lit(True)
    for index, (_feature_key, observation_key) in enumerate(key_mapping):
        condition = condition & (
            F.col(f"observation.{observation_key}")
            == F.col(f"feature._lookup_key_{index}")
        )
    condition = condition & (
        F.col("feature._lookup_feature_time")
        <= F.col(f"observation.{lookup.observation_timestamp}")
    )
    joined = observations.alias("observation").join(
        feature_projection.alias("feature"),
        on=condition,
        how="left",
    )
    latest = Window.partitionBy(
        *[F.col(f"observation.{column}") for column in observation_keys]
    ).orderBy(F.col("feature._lookup_feature_time").desc_nulls_last())
    selected = joined.withColumn("_lookup_rank", F.row_number().over(latest)).where(
        F.col("_lookup_rank") == F.lit(1)
    )
    defaults = dict(lookup.defaults)
    feature_values = []
    for index, (source_column, output_column) in enumerate(
        zip(lookup.selected_columns, output_names, strict=True)
    ):
        value = F.col(f"_lookup_value_{index}")
        if source_column in defaults:
            value = F.coalesce(value, F.lit(defaults[source_column]))
        feature_values.append(value.alias(output_column))
    return selected.select(
        *[F.col(f"observation.{column}").alias(column) for column in observations.columns],
        *feature_values,
    )


def _receipt_id(
    definition: ModelDefinition,
    bindings: tuple[TrainingFeatureBinding, ...],
    *,
    observation_start: date,
    observation_end: date,
    label_end: date,
    code_sha: str,
) -> str:
    payload = {
        "code_sha": code_sha,
        "definition_checksum": definition.checksum,
        "feature_bindings": [
            {
                "feature_id": binding.feature_id,
                "snapshot_id": binding.feature_snapshot_id,
                "snapshot_attempt_id": binding.feature_snapshot_attempt_id,
                "delta_version": binding.delta_version,
                "value_checksum": binding.value_checksum,
            }
            for binding in bindings
        ],
        "label_end": label_end.isoformat(),
        "observation_end": observation_end.isoformat(),
        "observation_start": observation_start.isoformat(),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _normalise_feature_reference_dates(
    *,
    feature_reference_date: str | date | None,
    feature_reference_dates: tuple[str | date, ...] | None,
) -> tuple[str | date, ...]:
    if feature_reference_date is not None and feature_reference_dates is not None:
        raise ValueError(
            "Supply feature_reference_date or feature_reference_dates, not both"
        )
    values = (
        tuple(feature_reference_dates)
        if feature_reference_dates is not None
        else ((feature_reference_date,) if feature_reference_date is not None else ())
    )
    if not values:
        raise ValueError("At least one feature reference date is required")
    try:
        normalised = tuple(
            date.fromisoformat(
                value.isoformat()
                if isinstance(value, date)
                else str(value).strip()
            ).isoformat()
            for value in values
        )
    except ValueError as exc:
        raise ValueError("Feature reference dates must be YYYY-MM-DD") from exc
    if len(normalised) != len(set(normalised)):
        raise ValueError("Feature reference dates must be unique")
    return tuple(sorted(normalised))


def _read_feature_history(
    spark: Any,
    *,
    feature_id: str,
    catalog: str,
    schema: str,
    reference_dates: tuple[str | date, ...],
    registry: Any,
) -> tuple[Any, tuple[ReadyFeatureBinding, ...]]:
    frames = []
    bindings = []
    for reference_date in reference_dates:
        frame, binding = read_ready_feature(
            spark,
            feature_id,
            catalog=catalog,
            schema=schema,
            reference_date=reference_date,
            registry=registry,
        )
        frames.append(frame)
        bindings.append(binding)
    history = frames[0]
    for frame in frames[1:]:
        history = history.unionByName(frame, allowMissingColumns=False)
    return history, tuple(bindings)


def build_training_set(
    spark: Any,
    definition: ModelDefinition,
    observations: Any,
    *,
    catalog: str,
    schema: str,
    feature_reference_date: str | date | None = None,
    feature_reference_dates: tuple[str | date, ...] | None = None,
    label_end: date,
    code_sha: str,
) -> TrainingSetBuildResult:
    """Build a checked training frame and READY receipt without training."""
    if definition.label not in observations.columns:
        raise ValueError(
            f"Training observations are missing label: {definition.label}"
        )
    validate_unique_non_null_keys(observations, definition.observation_keys)
    observation_timestamps = {
        lookup.observation_timestamp for lookup in definition.feature_lookups
    }
    if len(observation_timestamps) != 1:
        raise ValueError(
            "The first generic training route requires one observation "
            "timestamp shared by all lookups"
        )
    observation_timestamp = next(iter(observation_timestamps))
    observation_start, observation_end = _date_window(
        observations,
        observation_timestamp,
    )
    if label_end < observation_end:
        raise ValueError("label_end cannot predate the observation window")
    reference_dates = _normalise_feature_reference_dates(
        feature_reference_date=feature_reference_date,
        feature_reference_dates=feature_reference_dates,
    )

    from next_ads.features import load_feature_store_registry

    registry = load_feature_store_registry()
    training_frame = observations
    bindings = []
    for lookup in definition.feature_lookups:
        feature_frame, ready_bindings = _read_feature_history(
            spark,
            feature_id=lookup.feature_id,
            catalog=catalog,
            schema=schema,
            reference_dates=reference_dates,
            registry=registry,
        )
        for ready_binding in ready_bindings:
            validate_snapshot_time_boundary(ready_binding, observation_end)
        feature = registry.table_spec(lookup.feature_id)
        if not feature.timestamp_key:
            raise ValueError(
                f"Point-in-time lookup needs a timestamp key: {lookup.feature_id}"
            )
        training_frame = _apply_point_in_time_lookup(
            training_frame,
            feature_frame,
            lookup,
            feature_timestamp_key=feature.timestamp_key,
            observation_keys=definition.observation_keys,
        )
        bindings.extend(
            _training_feature_binding(ready_binding)
            for ready_binding in ready_bindings
        )

    completed_at = datetime.now(timezone.utc)
    binding_tuple = tuple(bindings)
    receipt = TrainingSetReceipt(
        receipt_id=_receipt_id(
            definition,
            binding_tuple,
            observation_start=observation_start,
            observation_end=observation_end,
            label_end=label_end,
            code_sha=code_sha,
        ),
        model_name=definition.model_name,
        model_definition_checksum=definition.checksum,
        feature_bindings=binding_tuple,
        observation_start=observation_start,
        observation_end=observation_end,
        label_end=label_end,
        schema_checksum=schema_checksum(training_frame),
        data_checksum=feature_value_checksum(training_frame),
        code_sha=code_sha,
        leakage_status="PASS",
        status="READY",
        created_at=completed_at,
        completed_at=completed_at,
    )
    return TrainingSetBuildResult(training_frame, receipt)


__all__ = [
    "TrainingSetBuildResult",
    "build_training_set",
    "validate_snapshot_time_boundary",
]

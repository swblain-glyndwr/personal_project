"""Delta persistence for immutable offline feature builds and snapshots."""

from __future__ import annotations

from dataclasses import fields
import json
from typing import Any

from next_ads.common.delta_writes import (
    replace_scope_by_name,
    typed_table_frame,
)
from next_ads.features.feature_builds import FeatureBuild, FeatureSnapshot


BUILD_TABLE = "next_uk_nextads_feature_builds"
SOURCE_TABLE = "next_uk_nextads_feature_build_sources"
OUTPUT_TABLE = "next_uk_nextads_feature_build_outputs"
SNAPSHOT_TABLE = "next_uk_nextads_feature_snapshots"
SNAPSHOT_BINDING_TABLE = (
    "next_uk_nextads_feature_snapshot_bindings"
)
METADATA_TABLES = (
    BUILD_TABLE,
    SOURCE_TABLE,
    OUTPUT_TABLE,
    SNAPSHOT_TABLE,
    SNAPSHOT_BINDING_TABLE,
)


def metadata_table_path(catalog: str, schema: str, table: str) -> str:
    values = (catalog, schema, table)
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise ValueError("Feature metadata paths require non-blank identifiers")
    return ".".join(value.strip() for value in values)


def metadata_table_paths(catalog: str, schema: str) -> dict[str, str]:
    return {
        table: metadata_table_path(catalog, schema, table)
        for table in METADATA_TABLES
    }


def _row(value: Any, *, exclude: tuple[str, ...] = ()) -> dict[str, Any]:
    return {
        field.name: getattr(value, field.name)
        for field in fields(value)
        if field.name not in exclude
    }


def _replace_rows(
    spark: Any,
    *,
    table: str,
    rows: list[dict[str, Any]],
    scope: dict[str, Any],
    build_id: str,
    attempt_id: str,
    operation: str,
) -> None:
    if not rows:
        return
    frame = typed_table_frame(spark, table, rows)
    replace_scope_by_name(
        frame,
        table,
        scope,
        spark=spark,
        build_id=build_id,
        attempt_id=attempt_id,
        commit_metadata={"operation": operation},
    )


def persist_feature_build(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    build: FeatureBuild,
) -> None:
    """Persist detail rows first and the build status row last."""
    paths = metadata_table_paths(catalog, schema)
    scope = {
        "feature_build_id": build.feature_build_id,
        "feature_build_attempt_id": build.feature_build_attempt_id,
    }
    _replace_rows(
        spark,
        table=paths[SOURCE_TABLE],
        rows=[_row(source) for source in build.sources],
        scope=scope,
        build_id=build.feature_build_id,
        attempt_id=build.feature_build_attempt_id,
        operation="feature_build_sources",
    )
    _replace_rows(
        spark,
        table=paths[OUTPUT_TABLE],
        rows=[_row(output) for output in build.outputs],
        scope=scope,
        build_id=build.feature_build_id,
        attempt_id=build.feature_build_attempt_id,
        operation="feature_build_outputs",
    )
    build_row = _row(
        build,
        exclude=("required_feature_ids", "sources", "outputs"),
    )
    build_row.update(
        {
            "required_feature_ids_json": json.dumps(
                build.required_feature_ids,
                separators=(",", ":"),
            ),
            "required_feature_count": len(build.required_feature_ids),
            "source_count": len(build.sources),
            "output_count": len(build.outputs),
        }
    )
    _replace_rows(
        spark,
        table=paths[BUILD_TABLE],
        rows=[build_row],
        scope=scope,
        build_id=build.feature_build_id,
        attempt_id=build.feature_build_attempt_id,
        operation="feature_build_status",
    )


def persist_feature_snapshot(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    snapshot: FeatureSnapshot,
) -> None:
    """Persist exact bindings first and the snapshot status row last."""
    paths = metadata_table_paths(catalog, schema)
    scope = {
        "feature_snapshot_id": snapshot.feature_snapshot_id,
        "feature_snapshot_attempt_id": snapshot.feature_snapshot_attempt_id,
    }
    _replace_rows(
        spark,
        table=paths[SNAPSHOT_BINDING_TABLE],
        rows=[_row(binding) for binding in snapshot.bindings],
        scope=scope,
        build_id=snapshot.feature_build_id,
        attempt_id=snapshot.feature_snapshot_attempt_id,
        operation="feature_snapshot_bindings",
    )
    snapshot_row = _row(
        snapshot,
        exclude=("required_feature_ids", "bindings"),
    )
    snapshot_row.update(
        {
            "required_feature_ids_json": json.dumps(
                snapshot.required_feature_ids,
                separators=(",", ":"),
            ),
            "required_feature_count": len(snapshot.required_feature_ids),
            "binding_count": len(snapshot.bindings),
        }
    )
    _replace_rows(
        spark,
        table=paths[SNAPSHOT_TABLE],
        rows=[snapshot_row],
        scope=scope,
        build_id=snapshot.feature_build_id,
        attempt_id=snapshot.feature_snapshot_attempt_id,
        operation="feature_snapshot_status",
    )


def latest_ready_snapshot_row(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    reference_date: Any | None = None,
    required_feature_ids: tuple[str, ...] | None = None,
) -> Any | None:
    """Return only a completed READY snapshot header."""
    from pyspark.sql import functions as F

    path = metadata_table_path(catalog, schema, SNAPSHOT_TABLE)
    frame = spark.table(path).where(F.col("status") == F.lit("READY"))
    if reference_date is not None:
        frame = frame.where(
            F.col("reference_date") == F.lit(reference_date)
        )
    if required_feature_ids is not None:
        required_json = json.dumps(
            required_feature_ids,
            separators=(",", ":"),
        )
        frame = frame.where(
            F.col("required_feature_ids_json") == F.lit(required_json)
        )
    return (
        frame.where(F.col("completed_at").isNotNull())
        .orderBy(
            F.col("completed_at").desc(),
            F.col("feature_snapshot_attempt_id").desc(),
        )
        .first()
    )


def ready_snapshot_bindings(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    feature_snapshot_id: str,
    feature_snapshot_attempt_id: str,
) -> Any:
    """Resolve bindings only after confirming their snapshot is READY."""
    from pyspark.sql import functions as F

    header = (
        spark.table(metadata_table_path(catalog, schema, SNAPSHOT_TABLE))
        .where(F.col("feature_snapshot_id") == F.lit(feature_snapshot_id))
        .where(
            F.col("feature_snapshot_attempt_id")
            == F.lit(feature_snapshot_attempt_id)
        )
        .where(F.col("status") == F.lit("READY"))
        .where(F.col("completed_at").isNotNull())
        .limit(2)
        .collect()
    )
    if len(header) != 1:
        raise ValueError("Feature bindings require exactly one READY snapshot")
    expected_count = int(header[0]["binding_count"])
    bindings = (
        spark.table(
            metadata_table_path(
                catalog,
                schema,
                SNAPSHOT_BINDING_TABLE,
            )
        )
        .where(F.col("feature_snapshot_id") == F.lit(feature_snapshot_id))
        .where(
            F.col("feature_snapshot_attempt_id")
            == F.lit(feature_snapshot_attempt_id)
        )
    )
    if bindings.count() != expected_count:
        raise ValueError("READY snapshot binding count does not match its header")
    return bindings


__all__ = [
    "BUILD_TABLE",
    "METADATA_TABLES",
    "OUTPUT_TABLE",
    "SNAPSHOT_BINDING_TABLE",
    "SNAPSHOT_TABLE",
    "SOURCE_TABLE",
    "latest_ready_snapshot_row",
    "metadata_table_path",
    "metadata_table_paths",
    "persist_feature_build",
    "persist_feature_snapshot",
    "ready_snapshot_bindings",
]

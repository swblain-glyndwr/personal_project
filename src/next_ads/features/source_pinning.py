"""Pin builder source reads to exact Delta versions for reproducibility."""

from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import re
from typing import Any

from next_ads.common.delta_writes import (
    quote_qualified_identifier,
    schema_checksum,
)
from next_ads.features.analytics_pctr_source import latest_delta_version
from next_ads.features.feature_builds import FeatureSourceBinding
from next_ads.features.feature_store_registry import (
    FeatureStoreRegistry,
    load_feature_store_registry,
)
from next_ads.features.snapshot_reader import read_ready_feature
from next_ads.features.snapshot_publication import external_delta_source


def _view_snapshot_table(
    *,
    source_name: str,
    source_view: str,
    feature_build_attempt_id: str,
    target_catalog: str,
    target_schema: str,
) -> str:
    safe_source_name = re.sub(r"[^a-z0-9_]", "_", source_name.lower())
    identity = hashlib.sha256(
        f"{source_view}:{feature_build_attempt_id}".encode("utf-8")
    ).hexdigest()[:16]
    return (
        f"{target_catalog}.{target_schema}."
        f"next_uk_nextads_fs_source_{safe_source_name}_{identity}"
    )


def snapshot_view_source(
    spark: Any,
    *,
    source_name: str,
    source_view: str,
    feature_build_attempt_id: str,
    target_catalog: str,
    target_schema: str,
) -> str:
    """Materialise a view once so a build can retain its exact input."""
    target = _view_snapshot_table(
        source_name=source_name,
        source_view=source_view,
        feature_build_attempt_id=feature_build_attempt_id,
        target_catalog=target_catalog,
        target_schema=target_schema,
    )
    if spark.catalog.tableExists(target):
        return target
    (
        spark.table(source_view)
        .write.format("delta")
        .mode("errorifexists")
        .saveAsTable(target)
    )
    escaped_source_view = source_view.replace("'", "''")
    spark.sql(
        "ALTER TABLE "
        f"{quote_qualified_identifier(target)} SET TBLPROPERTIES "
        f"('nextads.source_view' = '{escaped_source_view}')"
    )
    return target


class PinnedSourceSession:
    """Spark-session proxy that records and reuses exact table versions."""

    def __init__(
        self,
        spark: Any,
        *,
        feature_build_id: str,
        feature_build_attempt_id: str,
        reference_date: date,
        target_catalog: str,
        target_schema: str,
        captured_at: datetime | None = None,
        registry: FeatureStoreRegistry | None = None,
        allow_unready_feature_ids: tuple[str, ...] = (),
    ) -> None:
        self._spark = spark
        self._feature_build_id = feature_build_id
        self._feature_build_attempt_id = feature_build_attempt_id
        self._reference_date = reference_date
        self._target_catalog = target_catalog
        self._target_schema = target_schema
        self._captured_at = captured_at or datetime.now(timezone.utc)
        self._registry = registry or load_feature_store_registry()
        self._allow_unready_feature_ids = frozenset(
            allow_unready_feature_ids
        )
        self._bindings: dict[str, FeatureSourceBinding] = {}
        self._frames: dict[str, Any] = {}
        self._feature_ids_by_path = {
            self._registry.resolved_table_path(
                feature.name,
                catalog=target_catalog,
                schema=target_schema,
            ).lower(): feature.name
            for feature in self._registry.physical_tables
        }

    def __getattr__(self, name: str) -> Any:
        """Delegate non-table Spark operations to the active session."""
        return getattr(self._spark, name)

    def table(self, table_path: str) -> Any:
        """Return one exact source version and record its lineage once."""
        if table_path in self._frames:
            return self._frames[table_path]

        feature_id = self._feature_ids_by_path.get(table_path.lower())
        if feature_id not in self._allow_unready_feature_ids and feature_id:
            frame, ready = read_ready_feature(
                self._spark,
                feature_id,
                catalog=self._target_catalog,
                schema=self._target_schema,
                reference_date=self._reference_date,
                registry=self._registry,
            )
            self._bindings[table_path] = FeatureSourceBinding(
                feature_build_id=self._feature_build_id,
                feature_build_attempt_id=self._feature_build_attempt_id,
                reference_date=ready.reference_date,
                source_name=table_path,
                source_table=ready.backing_table,
                delta_version=ready.delta_version,
                schema_checksum=ready.backing_schema_checksum,
                captured_at=self._captured_at,
                row_count=ready.row_count,
                source_feature_id=ready.feature_id,
                source_feature_build_id=ready.feature_build_id,
                source_feature_build_attempt_id=(
                    ready.feature_build_attempt_id
                ),
                source_write_receipt_id=ready.write_receipt_id,
            )
            self._frames[table_path] = frame
            return frame

        table_type = self._spark.catalog.getTable(table_path).tableType.upper()
        pinned_table = table_path
        if table_type in {"VIEW", "MATERIALIZED_VIEW", "TEMPORARY"}:
            pinned_table = snapshot_view_source(
                self._spark,
                source_name=table_path.rsplit(".", 1)[-1],
                source_view=table_path,
                feature_build_attempt_id=self._feature_build_attempt_id,
                target_catalog=self._target_catalog,
                target_schema=self._target_schema,
            )
        version = latest_delta_version(self._spark, pinned_table)
        frame = self._spark.read.option("versionAsOf", version).table(
            pinned_table
        )
        self._bindings[table_path] = external_delta_source(
            feature_build_id=self._feature_build_id,
            feature_build_attempt_id=self._feature_build_attempt_id,
            reference_date=self._reference_date,
            source_name=table_path,
            source_table=pinned_table,
            delta_version=version,
            schema_checksum_value=schema_checksum(frame),
            captured_at=self._captured_at,
        )
        self._frames[table_path] = frame
        return frame

    @property
    def source_bindings(self) -> tuple[FeatureSourceBinding, ...]:
        """Return the exact sources in deterministic physical-path order."""
        return tuple(self._bindings[path] for path in sorted(self._bindings))


__all__ = ["PinnedSourceSession", "snapshot_view_source"]

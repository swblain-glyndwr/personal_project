from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from next_ads.common.delta_writes import (
    replace_scope_by_name,
)
from next_ads.ranking.scoring_inputs import (
    INPUT_CONTRACT_VERSION,
    ContentSummary,
    build_input_snapshot_id,
    summarise_content,
)


@dataclass(frozen=True)
class InputSource:
    name: str
    role: str
    table: str
    delta_version: int
    frame: DataFrame
    key_columns: tuple[str, ...]
    content_columns: tuple[str, ...]
    schema_version: str
    is_required: bool = True


@dataclass(frozen=True)
class AcceptedInputSnapshot:
    input_snapshot_id: str
    input_snapshot_attempt_id: str
    run_date: date
    status: str
    warning_count: int
    summaries: dict[str, ContentSummary]


def accept_input_snapshot(
    spark: Any,
    *,
    run_date: date,
    sources: tuple[InputSource, ...],
    item_themes_source_name: str,
    item_themes_snapshot_table: str,
    snapshots_table: str,
    snapshot_sources_table: str,
    input_snapshot_attempt_id: str,
    task_run_id: int,
    execution_count: int,
    completed_at: datetime,
    warning_count: int,
) -> AcceptedInputSnapshot:
    if not sources:
        raise ValueError("At least one input source is required")
    if len({source.name for source in sources}) != len(sources):
        raise ValueError("Input source names must be unique")
    if warning_count < 0:
        raise ValueError("Warning count cannot be negative")
    if task_run_id < 1 or execution_count < 0:
        raise ValueError("Invalid task run identity")
    frames = {
        source.name: source.frame.persist(StorageLevel.MEMORY_AND_DISK)
        for source in sources
    }
    try:
        summaries = {}
        for source in sources:
            summary = summarise_content(
                frames[source.name],
                key_columns=source.key_columns,
                content_columns=source.content_columns,
            )
            if source.is_required:
                summary.require_valid(source.name)
            summaries[source.name] = summary

        input_snapshot_id = build_input_snapshot_id(
            run_date,
            {
                source.name: summaries[source.name]
                for source in sources
                if source.is_required
            },
        )
        item_source = next(
            (
                source
                for source in sources
                if source.name == item_themes_source_name
            ),
            None,
        )
        if item_source is None:
            raise ValueError("The item-theme source is missing")
        item_snapshot = frames[item_source.name].select(
            F.lit(input_snapshot_id).alias("InputSnapshotID"),
            F.lit(run_date).cast("date").alias("RunDate"),
            F.col("pid").cast("string").alias("pid"),
            F.col("theme").cast("string").alias("theme"),
            F.col("theme_rank").cast("int").alias("theme_rank"),
        )
        replace_scope_by_name(
            item_snapshot,
            item_themes_snapshot_table,
            {"InputSnapshotID": input_snapshot_id},
            item_snapshot.columns,
            spark=spark,
        )

        source_rows = []
        for source in sources:
            summary = summaries[source.name]
            source_rows.append(
                {
                    "InputSnapshotID": input_snapshot_id,
                    "InputSnapshotAttemptID": input_snapshot_attempt_id,
                    "RunDate": run_date,
                    "SourceName": source.name,
                    "SourceRole": source.role,
                    "SourceTable": source.table,
                    "DeltaVersion": source.delta_version,
                    "SchemaVersion": source.schema_version,
                    "IsRequired": source.is_required,
                    "RowCount": summary.row_count,
                    "DistinctKeyCount": summary.distinct_key_count,
                    "NullKeyCount": summary.null_key_count,
                    "DuplicateKeyCount": summary.duplicate_key_count,
                    "ContentChecksum": summary.content_checksum,
                    "TaskRunID": task_run_id,
                    "ExecutionCount": execution_count,
                    "CapturedAt": completed_at,
                }
            )
        source_frame = spark.createDataFrame(source_rows)
        replace_scope_by_name(
            source_frame,
            snapshot_sources_table,
            {"InputSnapshotAttemptID": input_snapshot_attempt_id},
            list(source_rows[0]),
            spark=spark,
        )

        status = "READY_WITH_WARNINGS" if warning_count else "READY"
        snapshot_row = {
            "InputSnapshotID": input_snapshot_id,
            "InputSnapshotAttemptID": input_snapshot_attempt_id,
            "RunDate": run_date,
            "InputSchemaVersion": INPUT_CONTRACT_VERSION,
            "Status": status,
            "SourceCount": len(source_rows),
            "WarningCount": warning_count,
            "TaskRunID": task_run_id,
            "ExecutionCount": execution_count,
            "CompletedAt": completed_at,
        }
        replace_scope_by_name(
            spark.createDataFrame([snapshot_row]),
            snapshots_table,
            {"InputSnapshotAttemptID": input_snapshot_attempt_id},
            list(snapshot_row),
            spark=spark,
        )
        return AcceptedInputSnapshot(
            input_snapshot_id=input_snapshot_id,
            input_snapshot_attempt_id=input_snapshot_attempt_id,
            run_date=run_date,
            status=status,
            warning_count=warning_count,
            summaries=summaries,
        )
    finally:
        for frame in frames.values():
            frame.unpersist()

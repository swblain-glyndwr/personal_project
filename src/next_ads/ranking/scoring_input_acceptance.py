from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from next_ads.common.delta_writes import (
    DeltaWriteReceipt,
    replace_scope_by_name,
    typed_table_frame,
    validate_typed_table_schema,
)
from next_ads.ranking.scoring_inputs import (
    INPUT_CONTRACT_VERSION,
    InputVersionBinding,
    build_input_snapshot_id,
    schema_checksum,
)


INPUT_SNAPSHOT_COLUMNS = (
    "InputSnapshotID",
    "InputSnapshotAttemptID",
    "RunDate",
    "InputSchemaVersion",
    "GitCommit",
    "Status",
    "SourceCount",
    "WarningCount",
    "TaskRunID",
    "ExecutionCount",
    "CompletedAt",
)
INPUT_SOURCE_COLUMNS = (
    "InputSnapshotID",
    "InputSnapshotAttemptID",
    "RunDate",
    "SourceName",
    "SourceRole",
    "SourceTable",
    "DeltaVersion",
    "SchemaVersion",
    "SchemaChecksum",
    "IsRequired",
    "AcceptedTable",
    "AcceptedDeltaVersion",
    "AcceptedSchemaChecksum",
    "WriteReceiptID",
    "TaskRunID",
    "ExecutionCount",
    "CapturedAt",
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
    bindings: dict[str, InputVersionBinding]
    item_themes_receipt: DeltaWriteReceipt


def _validate_source_contract(source: InputSource) -> InputVersionBinding:
    required = set(source.key_columns) | set(source.content_columns)
    missing = sorted(required.difference(source.frame.columns))
    if missing:
        raise ValueError(
            f"Input source {source.name} is missing columns: "
            + ", ".join(missing)
        )
    if not source.key_columns or len(set(source.key_columns)) != len(
        source.key_columns
    ):
        raise ValueError(f"Input source {source.name} has invalid keys")
    return InputVersionBinding(
        table=source.table,
        delta_version=source.delta_version,
        schema_version=source.schema_version,
        schema_checksum=schema_checksum(
            source.frame.select(*source.content_columns)
        ),
    )


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
    git_commit: str,
) -> AcceptedInputSnapshot:
    """Bind source versions, write item themes once and publish READY last."""
    validate_typed_table_schema(
        spark,
        snapshots_table,
        INPUT_SNAPSHOT_COLUMNS,
    )
    validate_typed_table_schema(
        spark,
        snapshot_sources_table,
        INPUT_SOURCE_COLUMNS,
        nullable_columns=(
            "AcceptedTable",
            "AcceptedDeltaVersion",
            "AcceptedSchemaChecksum",
            "WriteReceiptID",
        ),
    )
    if not sources:
        raise ValueError("At least one input source is required")
    if len({source.name for source in sources}) != len(sources):
        raise ValueError("Input source names must be unique")
    if warning_count < 0:
        raise ValueError("Warning count cannot be negative")
    if task_run_id < 1 or execution_count < 0:
        raise ValueError("Invalid task run identity")
    if not git_commit:
        raise ValueError("Input acceptance requires a Git commit")

    bindings = {
        source.name: _validate_source_contract(source) for source in sources
    }
    input_snapshot_id = build_input_snapshot_id(
        run_date,
        {
            source.name: bindings[source.name]
            for source in sources
            if source.is_required
        },
        git_commit=git_commit,
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
    item_snapshot = item_source.frame.select(
        F.lit(input_snapshot_id).alias("InputSnapshotID"),
        F.lit(run_date).cast("date").alias("RunDate"),
        F.col("pid").cast("string").alias("pid"),
        F.col("theme").cast("string").alias("theme"),
        F.col("theme_rank").cast("int").alias("theme_rank"),
    )
    item_receipt = replace_scope_by_name(
        item_snapshot,
        item_themes_snapshot_table,
        {"InputSnapshotID": input_snapshot_id},
        item_snapshot.columns,
        spark=spark,
        build_id=input_snapshot_id,
        attempt_id=input_snapshot_attempt_id,
        git_commit=git_commit,
    )
    if item_receipt.delta_version is None or item_receipt.row_count is None:
        raise RuntimeError("Item-theme snapshot has no Delta write receipt")
    if item_receipt.row_count < 1:
        raise ValueError("The item-theme snapshot is empty")

    source_rows = []
    for source in sources:
        binding = bindings[source.name]
        is_item_themes = source.name == item_themes_source_name
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
                "SchemaChecksum": binding.schema_checksum,
                "IsRequired": source.is_required,
                "AcceptedTable": (
                    item_themes_snapshot_table if is_item_themes else None
                ),
                "AcceptedDeltaVersion": (
                    item_receipt.delta_version if is_item_themes else None
                ),
                "AcceptedSchemaChecksum": (
                    item_receipt.schema_checksum if is_item_themes else None
                ),
                "WriteReceiptID": (
                    item_receipt.receipt_id if is_item_themes else None
                ),
                "TaskRunID": task_run_id,
                "ExecutionCount": execution_count,
                "CapturedAt": completed_at,
            }
        )
    source_frame = typed_table_frame(
        spark,
        snapshot_sources_table,
        source_rows,
    )
    replace_scope_by_name(
        source_frame,
        snapshot_sources_table,
        {"InputSnapshotAttemptID": input_snapshot_attempt_id},
        source_frame.columns,
        spark=spark,
        capture_receipt=False,
    )

    status = "READY_WITH_WARNINGS" if warning_count else "READY"
    snapshot_row = {
        "InputSnapshotID": input_snapshot_id,
        "InputSnapshotAttemptID": input_snapshot_attempt_id,
        "RunDate": run_date,
        "InputSchemaVersion": INPUT_CONTRACT_VERSION,
        "GitCommit": git_commit,
        "Status": status,
        "SourceCount": len(source_rows),
        "WarningCount": warning_count,
        "TaskRunID": task_run_id,
        "ExecutionCount": execution_count,
        "CompletedAt": completed_at,
    }
    replace_scope_by_name(
        typed_table_frame(spark, snapshots_table, [snapshot_row]),
        snapshots_table,
        {"InputSnapshotAttemptID": input_snapshot_attempt_id},
        list(snapshot_row),
        spark=spark,
        capture_receipt=False,
    )
    return AcceptedInputSnapshot(
        input_snapshot_id=input_snapshot_id,
        input_snapshot_attempt_id=input_snapshot_attempt_id,
        run_date=run_date,
        status=status,
        warning_count=warning_count,
        bindings=bindings,
        item_themes_receipt=item_receipt,
    )

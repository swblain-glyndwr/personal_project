from __future__ import annotations

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import functions as F

from next_ads.common.delta_writes import (
    replace_scope_by_name,
    replace_table_by_name,
)
from next_ads.ranking.scoring_inputs import (
    latest_delta_version,
    read_delta_version,
)
from next_ads.ranking.scoring_manifest import (
    READY_FOR_PROVIDERS,
    ScoringFoundationBuild,
    ScoringFoundationOutput,
)


@dataclass(frozen=True)
class FoundationOutputSpec:
    output_name: str
    source_table: str
    target_table: str
    output_schema_version: str
    key_columns: tuple[str, ...]
    account_column: str
    entity_column: str
    logical_date_columns: tuple[str, ...] = ("reference_date", "rundate")
    required_non_null_columns: tuple[str, ...] = ()
    is_required: bool = True


@dataclass(frozen=True)
class FoundationOutputSummary:
    row_count: int
    account_count: int
    entity_count: int
    null_key_count: int
    duplicate_key_count: int
    invalid_value_count: int
    output_checksum: str

    def require_valid(self, output_name: str) -> None:
        if self.row_count == 0:
            raise ValueError(f"Foundation output {output_name} is empty")
        if self.null_key_count:
            raise ValueError(
                f"Foundation output {output_name} contains null keys"
            )
        if self.duplicate_key_count:
            raise ValueError(
                f"Foundation output {output_name} contains duplicate keys"
            )
        if self.invalid_value_count:
            raise ValueError(
                f"Foundation output {output_name} contains invalid values"
            )


def schema_checksum(frame: Any) -> str:
    """Hash ordered column names and Spark SQL types, excluding nullability."""
    signature = [
        (field.name, field.dataType.simpleString()) for field in frame.schema
    ]
    return hashlib.sha256(
        json.dumps(signature, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def validate_foundation_build_marker(
    spark: Any,
    *,
    context: Any,
    marker_table: str,
) -> None:
    """Prove the pipeline outputs belong to the leased foundation attempt."""
    rows = spark.table(marker_table).collect()
    if len(rows) != 1:
        raise ValueError(
            f"Expected one scoring foundation build marker, found {len(rows)}"
        )
    row = rows[0]
    expected = {
        "ContextSlot": context.context_slot,
        "OrchestrationRunID": context.orchestration_run_id,
        "FoundationID": context.foundation_id,
        "FoundationVersion": context.foundation_version,
        "ScoringFoundationBuildID": context.scoring_foundation_build_id,
        "ScoringFoundationBuildAttemptID": (
            context.scoring_foundation_build_attempt_id
        ),
        "InputSnapshotID": context.input_snapshot_id,
        "InputSnapshotAttemptID": context.input_snapshot_attempt_id,
        "RunDate": context.run_date,
        "InvocationChecksum": context.invocation_checksum,
    }
    mismatched = [
        field for field, value in expected.items() if row[field] != value
    ]
    if mismatched:
        raise ValueError(
            "Scoring foundation build marker does not match its context: "
            + ", ".join(mismatched)
        )


def summarise_foundation_output(
    frame,
    *,
    spec: FoundationOutputSpec,
    run_date: date,
) -> FoundationOutputSummary:
    """Validate keys, date, dimensions and content in one aggregation."""
    required_columns = set(spec.key_columns) | {
        spec.account_column,
        spec.entity_column,
        *spec.logical_date_columns,
        *spec.required_non_null_columns,
    }
    missing = sorted(required_columns.difference(frame.columns))
    if missing:
        raise ValueError(
            f"Foundation output {spec.output_name} is missing columns: "
            + ", ".join(missing)
        )
    if not spec.key_columns or len(spec.key_columns) != len(
        set(spec.key_columns)
    ):
        raise ValueError("Foundation output keys must be non-empty and unique")

    null_key = F.exists(
        F.array(*[F.col(column).isNull() for column in spec.key_columns]),
        lambda value: value,
    )
    invalid_value = F.lit(False)
    for column in spec.logical_date_columns:
        invalid_value = (
            invalid_value
            | F.col(column).isNull()
            | (F.col(column) != F.lit(run_date))
        )
    for column in spec.required_non_null_columns:
        invalid_value = invalid_value | F.col(column).isNull()
    canonical_row = F.to_json(
        F.struct(*[F.col(column) for column in frame.columns]),
        options={"ignoreNullFields": "false"},
    )
    row_hash = F.xxhash64(canonical_row)
    result = frame.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(
            F.struct(*[F.col(column) for column in spec.key_columns])
        ).alias("distinct_key_count"),
        F.countDistinct(F.col(spec.account_column)).alias("account_count"),
        F.countDistinct(F.col(spec.entity_column)).alias("entity_count"),
        F.coalesce(
            F.sum(F.when(null_key, 1).otherwise(0)),
            F.lit(0),
        ).alias("null_key_count"),
        F.coalesce(
            F.sum(F.when(invalid_value, 1).otherwise(0)),
            F.lit(0),
        ).alias("invalid_value_count"),
        F.coalesce(
            F.sum(row_hash.cast("decimal(38,0)")),
            F.lit(Decimal(0)),
        ).alias("hash_sum"),
        F.coalesce(F.min(row_hash), F.lit(0)).alias("hash_min"),
        F.coalesce(F.max(row_hash), F.lit(0)).alias("hash_max"),
    ).first()
    row_count = int(result["row_count"])
    distinct_key_count = int(result["distinct_key_count"])
    checksum_payload = "|".join(
        (
            str(row_count),
            str(result["hash_sum"]),
            str(result["hash_min"]),
            str(result["hash_max"]),
        )
    )
    return FoundationOutputSummary(
        row_count=row_count,
        account_count=int(result["account_count"]),
        entity_count=int(result["entity_count"]),
        null_key_count=int(result["null_key_count"]),
        duplicate_key_count=row_count - distinct_key_count,
        invalid_value_count=int(result["invalid_value_count"]),
        output_checksum=hashlib.sha256(
            checksum_payload.encode("utf-8")
        ).hexdigest(),
    )


def publish_required_foundation_outputs(
    spark: Any,
    *,
    context: Any,
    output_specs: tuple[FoundationOutputSpec, ...],
    max_workers: int = 2,
) -> tuple[ScoringFoundationOutput, ...]:
    """Copy and bind required outputs; no manifest is committed here."""
    if not output_specs:
        raise ValueError("At least one foundation output is required")
    names = [spec.output_name for spec in output_specs]
    if len(names) != len(set(names)):
        raise ValueError("Foundation output names must be unique")
    worker_count = min(max_workers, len(output_specs))
    if worker_count < 1:
        raise ValueError("max_workers must be at least one")

    results: list[ScoringFoundationOutput | None] = [None] * len(output_specs)
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {
            executor.submit(
                _publish_one_output,
                spark,
                context=context,
                spec=spec,
            ): index
            for index, spec in enumerate(output_specs)
        }
        try:
            for future in as_completed(futures):
                results[futures[future]] = future.result()
        except Exception:
            for future in futures:
                future.cancel()
            raise
    if any(result is None for result in results):
        raise RuntimeError("Foundation output publication did not complete")
    return tuple(result for result in results if result is not None)


def _publish_one_output(
    spark: Any,
    *,
    context: Any,
    spec: FoundationOutputSpec,
) -> ScoringFoundationOutput:
    source_version = latest_delta_version(spark, spec.source_table)
    frame = read_delta_version(
        spark,
        spec.source_table,
        source_version,
    ).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        if not spark.catalog.tableExists(spec.target_table):
            raise ValueError(
                "Foundation output target has no repo-owned schema contract: "
                f"{spec.target_table}"
            )
        source_schema_checksum = schema_checksum(frame)
        target_contract_checksum = schema_checksum(spark.table(spec.target_table))
        if source_schema_checksum != target_contract_checksum:
            raise ValueError(
                f"Foundation output {spec.output_name} does not match its "
                "repo-owned table schema"
            )
        summary = summarise_foundation_output(
            frame,
            spec=spec,
            run_date=context.run_date,
        )
        summary.require_valid(spec.output_name)
        previous_output_version = latest_delta_version(
            spark,
            spec.target_table,
        )
        replace_table_by_name(
            frame,
            spec.target_table,
            frame.columns,
            spark=spark,
        )
        output_version = latest_delta_version(spark, spec.target_table)
        if output_version != previous_output_version + 1:
            raise ValueError(
                f"Foundation output {spec.output_name} was published amid "
                "another table transaction"
            )
        output_frame = read_delta_version(
            spark,
            spec.target_table,
            output_version,
        )
        output_schema_checksum = schema_checksum(output_frame)
        if output_schema_checksum != source_schema_checksum:
            raise ValueError(
                f"Foundation output {spec.output_name} changed schema while "
                "being published"
            )
    finally:
        frame.unpersist()
    return ScoringFoundationOutput(
        scoring_foundation_build_id=context.scoring_foundation_build_id,
        scoring_foundation_build_attempt_id=(
            context.scoring_foundation_build_attempt_id
        ),
        run_date=context.run_date,
        output_name=spec.output_name,
        source_table=spec.source_table,
        source_delta_version=source_version,
        source_schema_checksum=source_schema_checksum,
        output_table=spec.target_table,
        output_delta_version=output_version,
        output_schema_version=spec.output_schema_version,
        output_schema_checksum=output_schema_checksum,
        is_required=spec.is_required,
        row_count=summary.row_count,
        account_count=summary.account_count,
        entity_count=summary.entity_count,
        null_key_count=summary.null_key_count,
        duplicate_key_count=summary.duplicate_key_count,
        invalid_value_count=summary.invalid_value_count,
        output_checksum=summary.output_checksum,
        published_at=datetime.now(timezone.utc),
    )


def register_ready_foundation(
    spark: Any,
    *,
    context: Any,
    outputs: tuple[ScoringFoundationOutput, ...],
    required_output_names: tuple[str, ...],
    pipeline_update_id: str | None,
    pipeline_id: str,
    pipeline_update_type: str | None,
    builds_table: str,
    outputs_table: str,
    task_run_id: int,
    execution_count: int,
    pipeline_task_run_id: int,
    completed_at: datetime | None = None,
) -> ScoringFoundationBuild:
    """Commit output bindings first and the ready-build manifest last."""
    completed_at = completed_at or datetime.now(timezone.utc)
    build = ScoringFoundationBuild(
        scoring_foundation_build_id=context.scoring_foundation_build_id,
        scoring_foundation_build_attempt_id=(
            context.scoring_foundation_build_attempt_id
        ),
        input_snapshot_id=context.input_snapshot_id,
        input_snapshot_attempt_id=context.input_snapshot_attempt_id,
        run_date=context.run_date,
        foundation_id=context.foundation_id,
        foundation_version=context.foundation_version,
        capability=context.capability,
        contract_version=context.contract_version,
        invocation_checksum=context.invocation_checksum,
        required_output_names=required_output_names,
        status=READY_FOR_PROVIDERS,
        warning_count=0,
        task_run_id=task_run_id,
        execution_count=execution_count,
        completed_at=completed_at,
        outputs=outputs,
        input_bindings_json=context.bindings_json,
        pipeline_id=pipeline_id,
        pipeline_update_id=pipeline_update_id,
        pipeline_task_run_id=pipeline_task_run_id,
        pipeline_update_type=pipeline_update_type,
    )
    output_rows = [_output_row(output) for output in outputs]
    output_frame = spark.createDataFrame(output_rows)
    replace_scope_by_name(
        output_frame,
        outputs_table,
        {
            "ScoringFoundationBuildAttemptID": (
                context.scoring_foundation_build_attempt_id
            )
        },
        output_frame.columns,
        spark=spark,
    )
    build_frame = spark.createDataFrame([_build_row(build)])
    replace_scope_by_name(
        build_frame,
        builds_table,
        {
            "ScoringFoundationBuildAttemptID": (
                context.scoring_foundation_build_attempt_id
            )
        },
        build_frame.columns,
        spark=spark,
    )
    return build


def foundation_output_bindings_json(
    build: ScoringFoundationBuild,
) -> str:
    return json.dumps(
        {
            "foundation": {
                "scoring_foundation_build_id": (
                    build.scoring_foundation_build_id
                ),
                "scoring_foundation_build_attempt_id": (
                    build.scoring_foundation_build_attempt_id
                ),
                "outputs": {
                    output.output_name: {
                        "source_table": output.source_table,
                        "source_delta_version": output.source_delta_version,
                        "table": output.output_table,
                        "delta_version": output.output_delta_version,
                        "schema_version": output.output_schema_version,
                        "schema_checksum": output.output_schema_checksum,
                        "output_checksum": output.output_checksum,
                    }
                    for output in sorted(
                        build.outputs,
                        key=lambda item: item.output_name,
                    )
                },
            }
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _output_row(output: ScoringFoundationOutput) -> dict[str, Any]:
    return {
        "ScoringFoundationBuildID": output.scoring_foundation_build_id,
        "ScoringFoundationBuildAttemptID": (
            output.scoring_foundation_build_attempt_id
        ),
        "RunDate": output.run_date,
        "OutputName": output.output_name,
        "SourceTable": output.source_table,
        "SourceDeltaVersion": output.source_delta_version,
        "SourceSchemaChecksum": output.source_schema_checksum,
        "OutputTable": output.output_table,
        "OutputDeltaVersion": output.output_delta_version,
        "OutputSchemaVersion": output.output_schema_version,
        "OutputSchemaChecksum": output.output_schema_checksum,
        "IsRequired": output.is_required,
        "RowCount": output.row_count,
        "AccountCount": output.account_count,
        "EntityCount": output.entity_count,
        "NullKeyCount": output.null_key_count,
        "DuplicateKeyCount": output.duplicate_key_count,
        "InvalidValueCount": output.invalid_value_count,
        "OutputChecksum": output.output_checksum,
        "PublishedAt": output.published_at,
    }


def _build_row(build: ScoringFoundationBuild) -> dict[str, Any]:
    return {
        "ScoringFoundationBuildID": build.scoring_foundation_build_id,
        "ScoringFoundationBuildAttemptID": (
            build.scoring_foundation_build_attempt_id
        ),
        "InputSnapshotID": build.input_snapshot_id,
        "InputSnapshotAttemptID": build.input_snapshot_attempt_id,
        "RunDate": build.run_date,
        "FoundationID": build.foundation_id,
        "FoundationVersion": build.foundation_version,
        "Capability": build.capability,
        "ContractVersion": build.contract_version,
        "InvocationChecksum": build.invocation_checksum,
        "RequiredOutputsJSON": json.dumps(
            build.required_output_names,
            separators=(",", ":"),
        ),
        "InputBindingsJSON": build.input_bindings_json,
        "PipelineID": build.pipeline_id,
        "PipelineUpdateID": build.pipeline_update_id,
        "PipelineTaskRunID": build.pipeline_task_run_id,
        "PipelineUpdateType": build.pipeline_update_type,
        "WarningCount": build.warning_count,
        "Status": build.status,
        "TaskRunID": build.task_run_id,
        "ExecutionCount": build.execution_count,
        "CompletedAt": build.completed_at,
    }


__all__ = [
    "FoundationOutputSpec",
    "FoundationOutputSummary",
    "foundation_output_bindings_json",
    "publish_required_foundation_outputs",
    "register_ready_foundation",
    "schema_checksum",
    "summarise_foundation_output",
    "validate_foundation_build_marker",
]

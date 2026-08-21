from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from time import monotonic
from typing import Any

from next_ads.common.delta_writes import (
    find_delta_write_receipt,
    replace_scope_by_name,
    replace_table_by_name,
    typed_table_frame,
    validate_typed_table_schema,
)
from next_ads.common.output_locations import log_output_location
from next_ads.ranking.scoring_inputs import (
    latest_delta_version,
    read_delta_version,
)
from next_ads.ranking.scoring_manifest import (
    READY_FOR_PROVIDERS,
    ScoringFoundationBuild,
    ScoringFoundationOutput,
)


LOGGER = logging.getLogger(__name__)

FOUNDATION_OUTPUT_COLUMNS = (
    "ScoringFoundationBuildID",
    "ScoringFoundationBuildAttemptID",
    "RunDate",
    "OutputName",
    "SourceTable",
    "SourceDeltaVersion",
    "SourceSchemaChecksum",
    "OutputTable",
    "OutputDeltaVersion",
    "OutputSchemaVersion",
    "OutputSchemaChecksum",
    "IsRequired",
    "RowCount",
    "WriteReceiptID",
    "GitCommit",
    "WriteDurationMs",
    "RetryCount",
    "PublishedAt",
)
FOUNDATION_BUILD_COLUMNS = (
    "ScoringFoundationBuildID",
    "ScoringFoundationBuildAttemptID",
    "InputSnapshotID",
    "InputSnapshotAttemptID",
    "RunDate",
    "FoundationID",
    "FoundationVersion",
    "Capability",
    "ContractVersion",
    "InvocationChecksum",
    "GitCommit",
    "RequiredOutputsJSON",
    "InputBindingsJSON",
    "PipelineID",
    "PipelineUpdateID",
    "PipelineTaskRunID",
    "PipelineUpdateType",
    "WarningCount",
    "Status",
    "TaskRunID",
    "ExecutionCount",
    "CompletedAt",
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
    source_kind: str = "delta"
    row_preserving_from: str | None = None


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


def validate_foundation_output_manifest_contract(
    spark: Any,
    *,
    outputs_table: str,
    builds_table: str,
    pipeline_relations: bool,
) -> None:
    """Fail before publication when the manifest cannot represent its source."""
    validate_typed_table_schema(
        spark,
        outputs_table,
        FOUNDATION_OUTPUT_COLUMNS,
        nullable_columns=("SourceDeltaVersion",),
    )
    validate_typed_table_schema(
        spark,
        builds_table,
        FOUNDATION_BUILD_COLUMNS,
        nullable_columns=(
            "PipelineUpdateID",
            "PipelineUpdateType",
        ),
    )
    fields = {field.name: field for field in spark.table(outputs_table).schema}
    source_version = fields.get("SourceDeltaVersion")
    if source_version is None:
        raise ValueError(
            "Scoring foundation output manifest is missing SourceDeltaVersion"
        )
    if pipeline_relations and not source_version.nullable:
        raise ValueError(
            "Scoring foundation output manifest must allow a null "
            "SourceDeltaVersion for pipeline-owned relations"
        )


def publish_required_foundation_outputs(
    spark: Any,
    *,
    context: Any,
    output_specs: tuple[FoundationOutputSpec, ...],
    git_commit: str,
) -> tuple[ScoringFoundationOutput, ...]:
    """Publish each configured physical output once, without content rescans."""
    if not output_specs:
        raise ValueError("At least one foundation output is required")
    names = [spec.output_name for spec in output_specs]
    if len(names) != len(set(names)):
        raise ValueError("Foundation output names must be unique")
    LOGGER.info(
        "Publishing %s foundation outputs with one write each",
        len(output_specs),
    )
    results = tuple(
        _publish_one_output(
            spark,
            context=context,
            spec=spec,
            git_commit=git_commit,
        )
        for spec in output_specs
    )
    LOGGER.info("Published all required foundation outputs")
    return results


def _publish_one_output(
    spark: Any,
    *,
    context: Any,
    spec: FoundationOutputSpec,
    git_commit: str,
) -> ScoringFoundationOutput:
    output_started = monotonic()
    LOGGER.info("Preparing foundation output %s", spec.output_name)
    if spec.source_kind == "delta":
        source_version = latest_delta_version(spark, spec.source_table)
        source_frame = read_delta_version(
            spark,
            spec.source_table,
            source_version,
        )
    elif spec.source_kind == "pipeline_relation":
        source_version = None
        source_frame = spark.table(spec.source_table)
    else:
        raise ValueError(
            f"Unsupported foundation source kind: {spec.source_kind!r}"
        )
    frame = source_frame
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
    LOGGER.info("Validated %s source and target schemas", spec.output_name)
    write_started = monotonic()
    receipt = find_delta_write_receipt(
        spark,
        target_table=spec.target_table,
        build_id=context.scoring_foundation_build_id,
        attempt_id=context.scoring_foundation_build_attempt_id,
    )
    if receipt is None:
        LOGGER.info(
            "Atomically replacing foundation output %s", spec.output_name
        )
        receipt = replace_table_by_name(
            frame,
            spec.target_table,
            frame.columns,
            spark=spark,
            build_id=context.scoring_foundation_build_id,
            attempt_id=context.scoring_foundation_build_attempt_id,
            git_commit=git_commit,
        )
    else:
        LOGGER.info(
            "Reusing foundation output %s at Delta version %s",
            spec.output_name,
            receipt.delta_version,
        )
        log_output_location(
            spec.target_table,
            kind="delta_table",
            details={
                "delta_version": receipt.delta_version,
                "output_name": spec.output_name,
                "receipt_id": receipt.receipt_id,
                "row_count": receipt.row_count,
                "reused": True,
            },
        )
    if receipt.delta_version is None or receipt.row_count is None:
        raise RuntimeError(
            f"Foundation output {spec.output_name} has no Delta receipt"
        )
    if receipt.row_count == 0 and spec.is_required:
        raise ValueError(f"Foundation output {spec.output_name} is empty")
    LOGGER.info(
        "Published foundation output %s at Delta version %s in %.1f seconds "
        "(total %.1f seconds)",
        spec.output_name,
        receipt.delta_version,
        monotonic() - write_started,
        monotonic() - output_started,
    )
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
        output_delta_version=receipt.delta_version,
        output_schema_version=spec.output_schema_version,
        output_schema_checksum=(
            receipt.schema_checksum or target_contract_checksum
        ),
        is_required=spec.is_required,
        row_count=receipt.row_count,
        write_receipt_id=receipt.receipt_id,
        git_commit=git_commit,
        write_duration_ms=receipt.write_duration_ms,
        retry_count=receipt.attempts - 1,
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
    git_commit: str,
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
        git_commit=git_commit,
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
    output_frame = typed_table_frame(spark, outputs_table, output_rows)
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
        capture_receipt=False,
    )
    build_frame = typed_table_frame(spark, builds_table, [_build_row(build)])
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
        capture_receipt=False,
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
                        "write_receipt_id": output.write_receipt_id,
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
        "WriteReceiptID": output.write_receipt_id,
        "GitCommit": output.git_commit,
        "WriteDurationMs": output.write_duration_ms,
        "RetryCount": output.retry_count,
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
        "GitCommit": build.git_commit,
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
    "FOUNDATION_BUILD_COLUMNS",
    "FOUNDATION_OUTPUT_COLUMNS",
    "FoundationOutputSpec",
    "foundation_output_bindings_json",
    "publish_required_foundation_outputs",
    "register_ready_foundation",
    "schema_checksum",
    "validate_foundation_build_marker",
]

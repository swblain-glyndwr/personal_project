from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any, Mapping

from next_ads.common.delta_writes import (
    DeltaWriteReceipt,
    find_delta_write_receipt,
    quote_qualified_identifier,
    replace_scope_by_name,
    sql_literal,
    typed_table_frame,
    validate_typed_table_schema,
)
from next_ads.common.output_locations import log_output_location
from next_ads.ranking.scoring_manifest import (
    READY_FOR_NEXTADS,
    ScoreProviderBuild,
)


PROVIDER_SIGNAL_COLUMNS = (
    "ProviderBuildID",
    "AccountNumber",
    "EntityType",
    "EntityID",
    "ProviderID",
    "RunDate",
    "RawScore",
    "Score",
    "ProviderRank",
)
PROVIDER_BUILD_COLUMNS = (
    "ProviderBuildID",
    "ProviderBuildAttemptID",
    "InputSnapshotID",
    "RunDate",
    "Capability",
    "UseCase",
    "ProviderID",
    "ProviderVersion",
    "ContractVersion",
    "ModelName",
    "ModelVersion",
    "ModelURI",
    "PipelineUpdateID",
    "OutputSnapshotID",
    "OutputTable",
    "OutputDeltaVersion",
    "RowCount",
    "OutputSchemaChecksum",
    "WriteReceiptID",
    "GitCommit",
    "WriteDurationMs",
    "RetryCount",
    "WarningCount",
    "Status",
    "TaskRunID",
    "ExecutionCount",
    "CompletedAt",
    "ScoringFoundationBuildID",
    "ScoringFoundationBuildAttemptID",
)


@dataclass(frozen=True)
class ProviderPublicationResult:
    build: ScoreProviderBuild
    compatibility_output_versions: Mapping[str, int]


def _config_value(config: Any, name: str) -> Any:
    if isinstance(config, Mapping):
        return config[name]
    return getattr(config, name)


def _schema_checksum(frame: Any) -> str:
    signature = [
        (field.name, field.dataType.simpleString()) for field in frame.schema
    ]
    return hashlib.sha256(
        json.dumps(signature, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _validate_signal_contract(spark: Any, frame: Any, table: str) -> None:
    missing = sorted(set(PROVIDER_SIGNAL_COLUMNS).difference(frame.columns))
    unexpected = sorted(set(frame.columns).difference(PROVIDER_SIGNAL_COLUMNS))
    if missing or unexpected:
        details = []
        if missing:
            details.append("missing " + ", ".join(missing))
        if unexpected:
            details.append("unexpected " + ", ".join(unexpected))
        raise ValueError(
            "Provider signal schema is invalid: " + "; ".join(details)
        )
    if not spark.catalog.tableExists(table):
        raise ValueError(f"Provider signals table is missing: {table}")
    target = spark.table(table).select(*PROVIDER_SIGNAL_COLUMNS)
    if _schema_checksum(
        frame.select(*PROVIDER_SIGNAL_COLUMNS)
    ) != _schema_checksum(target):
        raise ValueError("Provider signals do not match their table contract")


def validate_provider_publication_contract(
    spark: Any,
    *,
    signals_table: str,
    builds_table: str,
) -> None:
    """Fail on provider table drift before running an expensive scorer."""
    validate_typed_table_schema(spark, signals_table, PROVIDER_SIGNAL_COLUMNS)
    validate_typed_table_schema(
        spark,
        builds_table,
        PROVIDER_BUILD_COLUMNS,
        nullable_columns=(
            "ModelName",
            "ModelVersion",
            "ModelURI",
            "PipelineUpdateID",
            "OutputSnapshotID",
            "OutputTable",
            "OutputDeltaVersion",
            "OutputSchemaChecksum",
            "WriteReceiptID",
            "ScoringFoundationBuildID",
            "ScoringFoundationBuildAttemptID",
        ),
    )


def stage_provider_signals(
    spark: Any,
    frame: Any,
    *,
    context: Any,
    table: str,
    git_commit: str,
) -> DeltaWriteReceipt:
    """Write canonical provider signals once and return the exact commit."""
    _validate_signal_contract(spark, frame, table)
    existing = find_delta_write_receipt(
        spark,
        target_table=table,
        build_id=context.provider_build_id,
        attempt_id=context.provider_build_attempt_id,
    )
    if existing is not None:
        log_output_location(
            table,
            kind="delta_table",
            details={
                "delta_version": existing.delta_version,
                "receipt_id": existing.receipt_id,
                "row_count": existing.row_count,
                "reused": True,
            },
        )
        return existing
    return replace_scope_by_name(
        frame.select(*PROVIDER_SIGNAL_COLUMNS),
        table,
        {
            "ProviderBuildID": context.provider_build_id,
            "ProviderID": context.provider_id,
            "RunDate": context.run_date,
        },
        PROVIDER_SIGNAL_COLUMNS,
        spark=spark,
        build_id=context.provider_build_id,
        attempt_id=context.provider_build_attempt_id,
        git_commit=git_commit,
    )


def _model_identity(model_uri: str) -> tuple[str | None, str | None]:
    if not model_uri.startswith("models:/"):
        return None, None
    model_path = model_uri.removeprefix("models:/").strip("/")
    if "/" not in model_path:
        return model_path, None
    return tuple(model_path.rsplit("/", 1))


def _pipeline_update_id(context: Any) -> str | None:
    foundation = json.loads(context.bindings_json).get("foundation")
    if not isinstance(foundation, dict):
        return None
    return foundation.get("pipeline_update_id")


def _build_row(build: ScoreProviderBuild) -> dict[str, Any]:
    return {
        "ProviderBuildID": build.provider_build_id,
        "ProviderBuildAttemptID": build.provider_build_attempt_id,
        "InputSnapshotID": build.input_snapshot_id,
        "RunDate": build.run_date,
        "Capability": build.capability,
        "UseCase": build.use_case,
        "ProviderID": build.provider_id,
        "ProviderVersion": build.provider_version,
        "ContractVersion": build.contract_version,
        "ModelName": build.model_name,
        "ModelVersion": build.model_version,
        "ModelURI": build.model_uri,
        "PipelineUpdateID": build.pipeline_update_id,
        "OutputSnapshotID": build.output_snapshot_id,
        "OutputTable": build.output_table,
        "OutputDeltaVersion": build.output_delta_version,
        "RowCount": build.row_count,
        "OutputSchemaChecksum": build.output_schema_checksum,
        "WriteReceiptID": build.write_receipt_id,
        "GitCommit": build.git_commit,
        "WriteDurationMs": build.write_duration_ms,
        "RetryCount": build.retry_count,
        "WarningCount": build.warning_count,
        "Status": build.status,
        "TaskRunID": build.task_run_id,
        "ExecutionCount": build.execution_count,
        "CompletedAt": build.completed_at,
        "ScoringFoundationBuildID": build.scoring_foundation_build_id,
        "ScoringFoundationBuildAttemptID": (
            build.scoring_foundation_build_attempt_id
        ),
    }


def register_ready_provider_build(
    spark: Any,
    *,
    context: Any,
    receipt: DeltaWriteReceipt,
    builds_table: str,
    provider_config: Any,
    contract_version: str,
    git_commit: str,
    task_run_id: int,
    execution_count: int,
    completed_at: datetime,
) -> ScoreProviderBuild:
    """Publish the typed READY row after the signals commit is durable."""
    if receipt.delta_version is None or receipt.row_count is None:
        raise RuntimeError("Provider output has no complete Delta receipt")
    if receipt.row_count < 1:
        raise ValueError(
            f"Provider build {context.provider_build_id} is empty"
        )
    model_name, model_version = _model_identity(context.model_uri)
    build = ScoreProviderBuild(
        provider_build_id=context.provider_build_id,
        provider_build_attempt_id=context.provider_build_attempt_id,
        input_snapshot_id=context.input_snapshot_id,
        run_date=context.run_date,
        capability=context.capability,
        use_case=context.use_case,
        provider_id=context.provider_id,
        provider_version=_config_value(provider_config, "provider_version"),
        contract_version=contract_version,
        status=READY_FOR_NEXTADS,
        row_count=receipt.row_count,
        warning_count=0,
        output_schema_checksum=receipt.schema_checksum,
        write_receipt_id=receipt.receipt_id,
        git_commit=git_commit,
        write_duration_ms=receipt.write_duration_ms,
        retry_count=receipt.attempts - 1,
        task_run_id=task_run_id,
        execution_count=execution_count,
        completed_at=completed_at,
        model_name=model_name,
        model_version=model_version,
        model_uri=context.model_uri,
        pipeline_update_id=_pipeline_update_id(context),
        output_snapshot_id=context.provider_build_id,
        output_table=receipt.target_table,
        output_delta_version=receipt.delta_version,
        scoring_foundation_build_id=context.scoring_foundation_build_id,
        scoring_foundation_build_attempt_id=(
            context.scoring_foundation_build_attempt_id
        ),
    )
    frame = typed_table_frame(spark, builds_table, [_build_row(build)])
    replace_scope_by_name(
        frame,
        builds_table,
        {"ProviderBuildAttemptID": context.provider_build_attempt_id},
        frame.columns,
        spark=spark,
        build_id=context.provider_build_id,
        attempt_id=context.provider_build_attempt_id,
        git_commit=git_commit,
        capture_receipt=False,
    )
    return build


def _complete_provider_receipt(
    spark: Any,
    *,
    receipt: DeltaWriteReceipt,
    context: Any,
    signals_table: str,
) -> DeltaWriteReceipt:
    """Recover a missing platform metric from one exact committed build."""
    if receipt.delta_version is None:
        raise RuntimeError("Provider output has no committed Delta version")
    if receipt.row_count is not None:
        return receipt

    # Databricks normally supplies numOutputRows in Delta history. If that
    # metric is absent, count only this provider build at the already-recorded
    # version. This is a repair fallback, not a scan of the mutable latest
    # table and not a second model calculation.
    row = spark.sql(
        "\n".join(
            (
                "SELECT COUNT(*) AS `_nextads_row_count`",
                f"FROM {quote_qualified_identifier(signals_table)} "
                f"VERSION AS OF {receipt.delta_version}",
                "WHERE `ProviderBuildID` = "
                f"{sql_literal(context.provider_build_id)}",
                f"  AND `ProviderID` = {sql_literal(context.provider_id)}",
                f"  AND `RunDate` = {sql_literal(context.run_date)}",
            )
        )
    ).first()
    if row is None or row["_nextads_row_count"] is None:
        raise RuntimeError("Provider output row count could not be recovered")
    return replace(receipt, row_count=int(row["_nextads_row_count"]))


def publish_provider_build(
    spark: Any,
    *,
    context: Any,
    signals_table: str,
    signals_delta_version: int,
    write_receipt: DeltaWriteReceipt | None = None,
    builds_table: str,
    provider_config: Any,
    contract_version: str,
    git_commit: str,
    task_run_id: int,
    execution_count: int,
    completed_at: datetime | None = None,
) -> ProviderPublicationResult:
    """Bind one exact signals commit and publish READY without data scans."""
    expected_provider = {
        "provider_id": context.provider_id,
        "capability": context.capability,
        "entity_type": context.capability.removeprefix("account_"),
    }
    mismatched = [
        name
        for name, expected in expected_provider.items()
        if _config_value(provider_config, name) != expected
    ]
    if mismatched:
        raise ValueError(
            "Provider configuration does not match its active context: "
            + ", ".join(mismatched)
        )
    receipt = write_receipt or find_delta_write_receipt(
        spark,
        target_table=signals_table,
        build_id=context.provider_build_id,
        attempt_id=context.provider_build_attempt_id,
    )
    if receipt is None or receipt.delta_version != signals_delta_version:
        raise ValueError(
            "Provider publication is not bound to its exact write receipt"
        )
    if (
        receipt.target_table != signals_table
        or receipt.build_id != context.provider_build_id
        or receipt.attempt_id != context.provider_build_attempt_id
    ):
        raise ValueError("Provider publication received the wrong write receipt")
    receipt = _complete_provider_receipt(
        spark,
        receipt=receipt,
        context=context,
        signals_table=signals_table,
    )
    build = register_ready_provider_build(
        spark,
        context=context,
        receipt=receipt,
        builds_table=builds_table,
        provider_config=provider_config,
        contract_version=contract_version,
        git_commit=git_commit,
        task_run_id=task_run_id,
        execution_count=execution_count,
        completed_at=completed_at or datetime.now(timezone.utc),
    )
    return ProviderPublicationResult(
        build=build,
        compatibility_output_versions={},
    )


__all__ = [
    "PROVIDER_BUILD_COLUMNS",
    "PROVIDER_SIGNAL_COLUMNS",
    "ProviderPublicationResult",
    "publish_provider_build",
    "register_ready_provider_build",
    "stage_provider_signals",
    "validate_provider_publication_contract",
]

"""Delta persistence for reproducible training receipts and model builds."""

from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
from typing import Any

from next_ads.common.delta_writes import replace_scope_by_name, typed_table_frame
from next_ads.model_development.contracts import (
    ModelBuild,
    TrainingFeatureBinding,
    TrainingSetReceipt,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_ROOT = PROJECT_ROOT / "sql" / "model_development"
TRAINING_RECEIPT_TABLE = "next_uk_nextads_training_set_receipts"
MODEL_BUILD_TABLE = "next_uk_nextads_model_builds"
TABLE_CONTRACTS = {
    TRAINING_RECEIPT_TABLE: (
        SQL_ROOT / "create_table_next_uk_nextads_training_set_receipts.sql"
    ),
    MODEL_BUILD_TABLE: SQL_ROOT / "create_table_next_uk_nextads_model_builds.sql",
}


def table_path(catalog: str, schema: str, table: str) -> str:
    values = (catalog, schema, table)
    if any(not isinstance(value, str) or not value.strip() for value in values):
        raise ValueError("Model-development table paths cannot be blank")
    return ".".join(value.strip() for value in values)


def create_model_development_tables(
    spark: Any,
    *,
    catalog: str,
    schema: str,
) -> tuple[str, ...]:
    """Create only the small receipt tables used by model-development jobs."""
    paths = []
    for table, contract in TABLE_CONTRACTS.items():
        spark.sql(contract.read_text().format(catalog=catalog, schema=schema))
        paths.append(table_path(catalog, schema, table))
    return tuple(paths)


def _replace_row(
    spark: Any,
    *,
    table: str,
    row: dict[str, object],
    key: str,
    value: str,
    operation: str,
) -> None:
    frame = typed_table_frame(spark, table, [row])
    replace_scope_by_name(
        frame,
        table,
        {key: value},
        spark=spark,
        build_id=value,
        attempt_id=value,
        commit_metadata={"operation": operation},
    )


def persist_training_set_receipt(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    receipt: TrainingSetReceipt,
) -> str:
    """Write one deterministic training receipt by receipt ID."""
    target = table_path(catalog, schema, TRAINING_RECEIPT_TABLE)
    row = {
        "receipt_id": receipt.receipt_id,
        "model_name": receipt.model_name,
        "model_definition_checksum": receipt.model_definition_checksum,
        "feature_bindings_json": json.dumps(
            [asdict(binding) for binding in receipt.feature_bindings],
            sort_keys=True,
            separators=(",", ":"),
        ),
        "observation_start": receipt.observation_start,
        "observation_end": receipt.observation_end,
        "label_end": receipt.label_end,
        "schema_checksum": receipt.schema_checksum,
        "data_checksum": receipt.data_checksum,
        "code_sha": receipt.code_sha,
        "leakage_status": receipt.leakage_status,
        "status": receipt.status,
        "created_at": receipt.created_at,
        "completed_at": receipt.completed_at,
        "failure_reason": receipt.failure_reason,
    }
    _replace_row(
        spark,
        table=target,
        row=row,
        key="receipt_id",
        value=receipt.receipt_id,
        operation="training_set_receipt",
    )
    return target


def persist_model_build(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    build: ModelBuild,
) -> str:
    """Write one model-build state by its deterministic build ID."""
    target = table_path(catalog, schema, MODEL_BUILD_TABLE)
    row = {
        "model_build_id": build.model_build_id,
        "model_name": build.model_name,
        "training_receipt_id": build.training_receipt_id,
        "model_definition_checksum": build.model_definition_checksum,
        "runtime_profile": build.runtime_profile,
        "status": build.status,
        "created_at": build.created_at,
        "mlflow_run_id": build.mlflow_run_id,
        "registered_model_name": build.registered_model_name,
        "registered_model_version": build.registered_model_version,
        "model_uri": build.model_uri,
        "artifact_digest": build.artifact_digest,
        "metrics_json": json.dumps(
            dict(build.metrics), sort_keys=True, separators=(",", ":")
        ),
        "completed_at": build.completed_at,
        "failure_reason": build.failure_reason,
    }
    _replace_row(
        spark,
        table=target,
        row=row,
        key="model_build_id",
        value=build.model_build_id,
        operation="model_build",
    )
    return target


def _one_ready_row(frame: Any, identity: str, object_name: str) -> Any | None:
    rows = frame.where("status = 'READY'").limit(2).collect()
    if len(rows) > 1:
        raise ValueError(f"More than one READY {object_name} found for {identity}")
    return rows[0] if rows else None


def load_ready_training_set_receipt(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    receipt_id: str,
) -> TrainingSetReceipt | None:
    """Load a READY receipt for retry reuse without rebuilding its data."""
    from pyspark.sql import functions as F

    target = table_path(catalog, schema, TRAINING_RECEIPT_TABLE)
    row = _one_ready_row(
        spark.table(target).where(F.col("receipt_id") == F.lit(receipt_id)),
        receipt_id,
        "training receipt",
    )
    if row is None:
        return None
    values = row.asDict()
    values["feature_bindings"] = tuple(
        TrainingFeatureBinding(**binding)
        for binding in json.loads(values.pop("feature_bindings_json"))
    )
    return TrainingSetReceipt(**values)


def load_ready_model_build(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    model_build_id: str,
) -> ModelBuild | None:
    """Load the exact READY artifact created by an earlier identical attempt."""
    from pyspark.sql import functions as F

    target = table_path(catalog, schema, MODEL_BUILD_TABLE)
    row = _one_ready_row(
        spark.table(target).where(
            F.col("model_build_id") == F.lit(model_build_id)
        ),
        model_build_id,
        "model build",
    )
    if row is None:
        return None
    values = row.asDict()
    values["metrics"] = tuple(
        (name, float(value))
        for name, value in json.loads(values.pop("metrics_json")).items()
    )
    return ModelBuild(**values)


__all__ = [
    "MODEL_BUILD_TABLE",
    "TRAINING_RECEIPT_TABLE",
    "create_model_development_tables",
    "load_ready_model_build",
    "load_ready_training_set_receipt",
    "persist_model_build",
    "persist_training_set_receipt",
    "table_path",
]

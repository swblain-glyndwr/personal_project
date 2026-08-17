"""Model-neutral execution with deterministic retry and failure records."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
from typing import Any

from next_ads.model_development.contracts import (
    ModelBuild,
    ModelDefinition,
    Trainer,
    TrainingSetReceipt,
)
from next_ads.model_development.store import (
    load_ready_model_build,
    persist_model_build,
)


def model_build_id(
    definition: ModelDefinition,
    receipt: TrainingSetReceipt,
) -> str:
    """Identify one model result by definition and exact training receipt."""
    payload = {
        "definition_checksum": definition.checksum,
        "model_name": definition.model_name,
        "runtime_profile": definition.runtime_profile,
        "training_receipt_id": receipt.receipt_id,
        "trainer": definition.trainer,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _validate_ready_build(
    build: ModelBuild,
    *,
    expected_id: str,
    definition: ModelDefinition,
    receipt: TrainingSetReceipt,
) -> None:
    if build.status != "READY":
        raise ValueError("A trainer must return a READY ModelBuild")
    expected = {
        "model_build_id": expected_id,
        "model_name": definition.model_name,
        "training_receipt_id": receipt.receipt_id,
        "model_definition_checksum": definition.checksum,
        "runtime_profile": definition.runtime_profile,
    }
    actual = {field: getattr(build, field) for field in expected}
    if actual != expected:
        raise ValueError("Trainer returned a model build for different inputs")


def train_or_reuse_model(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    definition: ModelDefinition,
    receipt: TrainingSetReceipt,
    training_frame: Any,
    trainer: Trainer,
) -> tuple[ModelBuild, bool]:
    """Train once, persist failure, and reuse the exact READY build on retry."""
    if receipt.status != "READY":
        raise ValueError("Model training requires a READY TrainingSetReceipt")
    if receipt.model_name != definition.model_name:
        raise ValueError("Training receipt belongs to a different model")
    if receipt.model_definition_checksum != definition.checksum:
        raise ValueError("Training receipt uses a different model definition")
    expected_id = model_build_id(definition, receipt)
    existing = load_ready_model_build(
        spark,
        catalog=catalog,
        schema=schema,
        model_build_id=expected_id,
    )
    if existing is not None:
        _validate_ready_build(
            existing,
            expected_id=expected_id,
            definition=definition,
            receipt=receipt,
        )
        return existing, True

    started_at = datetime.now(timezone.utc)
    building = ModelBuild(
        model_build_id=expected_id,
        model_name=definition.model_name,
        training_receipt_id=receipt.receipt_id,
        model_definition_checksum=definition.checksum,
        runtime_profile=definition.runtime_profile,
        status="TRAINING",
        created_at=started_at,
    )
    persist_model_build(
        spark, catalog=catalog, schema=schema, build=building
    )
    try:
        ready = trainer.train(definition, receipt, training_frame)
        _validate_ready_build(
            ready,
            expected_id=expected_id,
            definition=definition,
            receipt=receipt,
        )
    except Exception as exc:
        failed = replace(
            building,
            status="FAILED",
            completed_at=datetime.now(timezone.utc),
            failure_reason=f"{type(exc).__name__}: {exc}"[:1000],
        )
        persist_model_build(
            spark, catalog=catalog, schema=schema, build=failed
        )
        raise
    persist_model_build(spark, catalog=catalog, schema=schema, build=ready)
    return ready, False


__all__ = ["model_build_id", "train_or_reuse_model"]

"""Publish model-development scores through the NextAds provider contract."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from typing import Any

from next_ads.ranking.provider_context import ProviderContext
from next_ads.ranking.provider_publication import (
    ProviderPublicationResult,
    publish_provider_build,
    stage_provider_signals,
    validate_provider_publication_contract,
)


PROVIDER_CONTRACT_VERSION = "account_entity_scores/v1"


def publish_evaluation_provider(
    spark: Any,
    scores: Any,
    *,
    provider_id: str,
    provider_version: str,
    use_case: str,
    provider_build_id: str,
    provider_build_attempt_id: str,
    input_snapshot_id: str,
    run_date: date,
    model_uri: str,
    signals_table: str,
    builds_table: str,
    git_commit: str,
    orchestration_run_id: int,
    task_run_id: int,
    execution_count: int,
    completed_at: datetime | None = None,
) -> ProviderPublicationResult:
    """Publish one contract-compatible EVALUATE result outside serving."""
    accepted_at = completed_at or datetime.now(timezone.utc)
    invocation = {
        "contract_version": PROVIDER_CONTRACT_VERSION,
        "input_snapshot_id": input_snapshot_id,
        "model_uri": model_uri,
        "provider_build_attempt_id": provider_build_attempt_id,
        "provider_build_id": provider_build_id,
        "provider_id": provider_id,
        "provider_version": provider_version,
        "use_case": use_case,
        "run_date": run_date.isoformat(),
    }
    context = ProviderContext(
        context_slot=f"model_development:{provider_id}",
        orchestration_run_id=orchestration_run_id,
        provider_id=provider_id,
        provider_build_id=provider_build_id,
        provider_build_attempt_id=provider_build_attempt_id,
        input_snapshot_id=input_snapshot_id,
        run_date=run_date,
        model_uri=model_uri,
        bindings_json="{}",
        capability="account_ad",
        use_case=use_case,
        invocation_checksum=hashlib.sha256(
            json.dumps(
                invocation,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
        expires_at=accepted_at + timedelta(hours=8),
    )
    provider_config = {
        "provider_id": provider_id,
        "provider_version": provider_version,
        "capability": "account_ad",
        "entity_type": "ad",
    }
    validate_provider_publication_contract(
        spark,
        signals_table=signals_table,
        builds_table=builds_table,
    )
    receipt = stage_provider_signals(
        spark,
        scores,
        context=context,
        table=signals_table,
        git_commit=git_commit,
    )
    if receipt.delta_version is None:
        raise RuntimeError("Evaluation provider output has no Delta version")
    return publish_provider_build(
        spark,
        context=context,
        signals_table=signals_table,
        signals_delta_version=receipt.delta_version,
        write_receipt=receipt,
        builds_table=builds_table,
        provider_config=provider_config,
        contract_version=PROVIDER_CONTRACT_VERSION,
        git_commit=git_commit,
        task_run_id=task_run_id,
        execution_count=execution_count,
        completed_at=accepted_at,
    )


__all__ = [
    "PROVIDER_CONTRACT_VERSION",
    "publish_evaluation_provider",
]

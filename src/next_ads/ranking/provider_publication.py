from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Mapping

from pyspark import StorageLevel
from pyspark.sql import functions as F

from next_ads.common.delta_writes import replace_scope_by_name
from next_ads.ranking.scoring_inputs import (
    latest_delta_version,
    read_delta_version,
)
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


@dataclass(frozen=True)
class ProviderOutputSummary:
    row_count: int
    account_count: int
    entity_count: int
    null_key_count: int
    duplicate_key_count: int
    wrong_metadata_count: int
    invalid_score_count: int
    invalid_rank_count: int
    output_checksum: str

    def require_valid(self, provider_build_id: str) -> None:
        """Reject any provider output that cannot be accepted by NextAds."""
        if self.row_count == 0:
            raise ValueError(f"Provider build {provider_build_id} is empty")
        invalid = {
            "null keys": self.null_key_count,
            "duplicate keys": self.duplicate_key_count,
            "wrong metadata": self.wrong_metadata_count,
            "invalid scores": self.invalid_score_count,
            "invalid ranks": self.invalid_rank_count,
        }
        failures = [
            f"{count} {label}"
            for label, count in invalid.items()
            if count
        ]
        if failures:
            raise ValueError(
                f"Provider build {provider_build_id} contains "
                + ", ".join(failures)
            )


@dataclass(frozen=True)
class ProviderPublicationResult:
    build: ScoreProviderBuild
    compatibility_output_versions: Mapping[str, int]


def _missing_columns(frame, required: tuple[str, ...]) -> list[str]:
    return sorted(set(required).difference(frame.columns))


def summarise_provider_signals(
    frame,
    *,
    context: Any,
    max_entities_per_account: int,
) -> ProviderOutputSummary:
    """Validate one canonical provider build with one Spark action."""
    missing = _missing_columns(frame, PROVIDER_SIGNAL_COLUMNS)
    if missing:
        raise ValueError(
            "Provider output is missing columns: " + ", ".join(missing)
        )
    if (
        isinstance(max_entities_per_account, bool)
        or not isinstance(max_entities_per_account, int)
        or max_entities_per_account < 1
    ):
        raise ValueError("max_entities_per_account must be a positive integer")

    key_columns = (
        "ProviderBuildID",
        "AccountNumber",
        "EntityType",
        "EntityID",
    )
    null_key = F.exists(
        F.array(*[F.col(column).isNull() for column in key_columns]),
        lambda value: value,
    )
    capability_prefix = "account_"
    if not context.capability.startswith(capability_prefix):
        raise ValueError(
            f"Unsupported provider capability: {context.capability}"
        )
    expected_metadata = {
        "ProviderBuildID": context.provider_build_id,
        "ProviderID": context.provider_id,
        "EntityType": context.capability.removeprefix(capability_prefix),
        "RunDate": context.run_date,
    }
    wrong_metadata = F.lit(False)
    for column, expected in expected_metadata.items():
        wrong_metadata = (
            wrong_metadata
            | F.col(column).isNull()
            | (F.col(column) != F.lit(expected))
        )
    invalid_score = F.lit(False)
    for column in ("RawScore", "Score"):
        invalid_score = (
            invalid_score
            | F.col(column).isNull()
            | F.isnan(column)
            | F.col(column).isin(float("inf"), float("-inf"))
        )
    invalid_rank = (
        F.col("ProviderRank").isNull()
        | (F.col("ProviderRank") < 1)
        | (F.col("ProviderRank") > max_entities_per_account)
    )
    canonical_row = F.to_json(
        F.struct(*[F.col(column) for column in PROVIDER_SIGNAL_COLUMNS]),
        options={"ignoreNullFields": "false"},
    )
    row_hash = F.xxhash64(canonical_row)
    values = frame.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(
            F.struct(*[F.col(column) for column in key_columns])
        ).alias("distinct_key_count"),
        F.countDistinct("AccountNumber").alias("account_count"),
        F.countDistinct("EntityID").alias("entity_count"),
        F.countDistinct(
            F.struct("AccountNumber", "ProviderRank")
        ).alias("distinct_rank_count"),
        F.coalesce(
            F.sum(F.when(null_key, 1).otherwise(0)),
            F.lit(0),
        ).alias("null_key_count"),
        F.coalesce(
            F.sum(F.when(wrong_metadata, 1).otherwise(0)),
            F.lit(0),
        ).alias("wrong_metadata_count"),
        F.coalesce(
            F.sum(F.when(invalid_score, 1).otherwise(0)),
            F.lit(0),
        ).alias("invalid_score_count"),
        F.coalesce(
            F.sum(F.when(invalid_rank, 1).otherwise(0)),
            F.lit(0),
        ).alias("invalid_rank_count"),
        F.coalesce(
            F.sum(row_hash.cast("decimal(38,0)")),
            F.lit(Decimal(0)),
        ).alias("hash_sum"),
        F.coalesce(F.min(row_hash), F.lit(0)).alias("hash_min"),
        F.coalesce(F.max(row_hash), F.lit(0)).alias("hash_max"),
    ).first()
    row_count = int(values["row_count"])
    distinct_key_count = int(values["distinct_key_count"])
    duplicate_rank_count = row_count - int(values["distinct_rank_count"])
    checksum_payload = "|".join(
        (
            str(row_count),
            str(values["hash_sum"]),
            str(values["hash_min"]),
            str(values["hash_max"]),
        )
    )
    return ProviderOutputSummary(
        row_count=row_count,
        account_count=int(values["account_count"]),
        entity_count=int(values["entity_count"]),
        null_key_count=int(values["null_key_count"]),
        duplicate_key_count=row_count - distinct_key_count,
        wrong_metadata_count=int(values["wrong_metadata_count"]),
        invalid_score_count=int(values["invalid_score_count"]),
        invalid_rank_count=(
            int(values["invalid_rank_count"]) + duplicate_rank_count
        ),
        output_checksum=hashlib.sha256(
            checksum_payload.encode("utf-8")
        ).hexdigest(),
    )


def stage_provider_signals(
    spark: Any,
    frame,
    *,
    context: Any,
    table: str,
) -> int:
    """Replace one unaccepted provider-build scope and return its version."""
    selected = frame.select(*PROVIDER_SIGNAL_COLUMNS).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        previous_version = latest_delta_version(spark, table)
        replace_scope_by_name(
            selected,
            table,
            {
                "ProviderBuildID": context.provider_build_id,
                "ProviderID": context.provider_id,
                "RunDate": context.run_date,
            },
            PROVIDER_SIGNAL_COLUMNS,
            spark=spark,
        )
        output_version = latest_delta_version(spark, table)
        if output_version != previous_version + 1:
            raise ValueError(
                "Provider signals were staged amid another table transaction"
            )
        return output_version
    finally:
        selected.unpersist()


def _config_value(config: Any, name: str) -> Any:
    if isinstance(config, Mapping):
        return config[name]
    return getattr(config, name)


def _model_identity(model_uri: str) -> tuple[str | None, str | None]:
    if not model_uri.startswith("models:/"):
        return None, None
    model_path = model_uri.removeprefix("models:/").strip("/")
    if "/" not in model_path:
        return model_path, None
    model_name, model_version = model_path.rsplit("/", 1)
    return model_name, model_version


def _pipeline_update_id(context: Any) -> str | None:
    bindings = json.loads(context.bindings_json)
    foundation = bindings.get("foundation")
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
        "AccountCount": build.account_count,
        "EntityCount": build.entity_count,
        "NullKeyCount": build.null_key_count,
        "DuplicateKeyCount": build.duplicate_key_count,
        "InvalidScoreCount": build.invalid_score_count,
        "OutputChecksum": build.output_checksum,
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
    summary: ProviderOutputSummary,
    signals_table: str,
    signals_delta_version: int,
    builds_table: str,
    provider_config: Any,
    contract_version: str,
    task_run_id: int,
    execution_count: int,
    completed_at: datetime,
) -> ScoreProviderBuild:
    """Write the READY manifest after every provider output is durable."""
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
        row_count=summary.row_count,
        account_count=summary.account_count,
        entity_count=summary.entity_count,
        null_key_count=summary.null_key_count,
        duplicate_key_count=summary.duplicate_key_count,
        invalid_score_count=summary.invalid_score_count,
        warning_count=0,
        output_checksum=summary.output_checksum,
        task_run_id=task_run_id,
        execution_count=execution_count,
        completed_at=completed_at,
        model_name=model_name,
        model_version=model_version,
        model_uri=context.model_uri,
        pipeline_update_id=_pipeline_update_id(context),
        output_snapshot_id=context.provider_build_id,
        output_table=signals_table,
        output_delta_version=signals_delta_version,
        scoring_foundation_build_id=(
            context.scoring_foundation_build_id
        ),
        scoring_foundation_build_attempt_id=(
            context.scoring_foundation_build_attempt_id
        ),
    )
    row = _build_row(build)
    frame = spark.createDataFrame(
        [row],
        schema=spark.table(builds_table).schema,
    )
    replace_scope_by_name(
        frame,
        builds_table,
        {"ProviderBuildAttemptID": context.provider_build_attempt_id},
        list(row),
        spark=spark,
    )
    return build


def publish_provider_build(
    spark: Any,
    *,
    context: Any,
    signals_table: str,
    signals_delta_version: int,
    builds_table: str,
    provider_config: Any,
    contract_version: str,
    compatibility_publisher: Callable[[Any, datetime], Mapping[str, int]],
    task_run_id: int,
    execution_count: int,
    completed_at: datetime | None = None,
) -> ProviderPublicationResult:
    """Validate and publish one exact provider output; commit READY last."""
    completed_at = completed_at or datetime.now(timezone.utc)
    expected_provider = {
        "provider_id": context.provider_id,
        "capability": context.capability,
        "entity_type": context.capability.removeprefix("account_"),
    }
    mismatched_config = [
        name
        for name, expected in expected_provider.items()
        if _config_value(provider_config, name) != expected
    ]
    if mismatched_config:
        raise ValueError(
            "Provider configuration does not match its active context: "
            + ", ".join(mismatched_config)
        )
    max_entities = int(
        _config_value(provider_config, "max_entities_per_account")
    )
    signals = (
        read_delta_version(spark, signals_table, signals_delta_version)
        .where(F.col("ProviderBuildID") == context.provider_build_id)
        .select(*PROVIDER_SIGNAL_COLUMNS)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    try:
        summary = summarise_provider_signals(
            signals,
            context=context,
            max_entities_per_account=max_entities,
        )
        summary.require_valid(context.provider_build_id)
        compatibility_versions = dict(
            compatibility_publisher(signals, completed_at)
        )
        build = register_ready_provider_build(
            spark,
            context=context,
            summary=summary,
            signals_table=signals_table,
            signals_delta_version=signals_delta_version,
            builds_table=builds_table,
            provider_config=provider_config,
            contract_version=contract_version,
            task_run_id=task_run_id,
            execution_count=execution_count,
            completed_at=completed_at,
        )
        return ProviderPublicationResult(
            build=build,
            compatibility_output_versions=compatibility_versions,
        )
    finally:
        signals.unpersist()


__all__ = [
    "PROVIDER_SIGNAL_COLUMNS",
    "ProviderOutputSummary",
    "ProviderPublicationResult",
    "publish_provider_build",
    "register_ready_provider_build",
    "stage_provider_signals",
    "summarise_provider_signals",
]

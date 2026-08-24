"""Immutable Delta receipts for model research attempts and data frames."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

from next_ads.common.delta_writes import (
    atomic_append_by_name,
    find_delta_write_receipt,
    quote_identifier,
    quote_qualified_identifier,
    schema_checksum,
    typed_table_frame,
    validate_replace_source_scope,
    validate_unique_non_null_keys,
)
from next_ads.common.output_locations import log_output_location
from next_ads.features.feature_builds import feature_value_checksum
from next_ads.model_development.research_contracts import (
    AutoMLDiscoveryReceipt,
    CandidateEvaluation,
    ModelResearchBuild,
    ModelSelectionDecision,
)
from next_ads.model_development.research_data import (
    RESEARCH_FRAME_COLUMNS,
    ResearchFrameSchemas,
    automl_discovery_partition,
    selected_test_partition,
    training_partition,
    unpack_research_frame,
    validation_partition,
)
from next_ads.model_development.store import table_path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_ROOT = PROJECT_ROOT / "sql" / "model_development"
RESEARCH_BUILD_TABLE = "next_uk_nextads_model_research_builds"
RESEARCH_CLAIM_TABLE = "next_uk_nextads_model_research_claims"
CANDIDATE_EVALUATION_TABLE = "next_uk_nextads_candidate_evaluations"
SELECTION_DECISION_TABLE = "next_uk_nextads_model_selection_decisions"
RESEARCH_FRAME_TABLE = "next_uk_nextads_model_research_frames"
AUTOML_DISCOVERY_TABLE = "next_uk_nextads_automl_discovery_receipts"
AUTOML_DISCOVERY_CLAIM_TABLE = "next_uk_nextads_automl_discovery_claims"
RESEARCH_TABLE_CONTRACTS = {
    RESEARCH_CLAIM_TABLE: (
        SQL_ROOT / "create_table_next_uk_nextads_model_research_claims.sql"
    ),
    RESEARCH_BUILD_TABLE: (
        SQL_ROOT / "create_table_next_uk_nextads_model_research_builds.sql"
    ),
    CANDIDATE_EVALUATION_TABLE: (
        SQL_ROOT / "create_table_next_uk_nextads_candidate_evaluations.sql"
    ),
    SELECTION_DECISION_TABLE: (
        SQL_ROOT / "create_table_next_uk_nextads_model_selection_decisions.sql"
    ),
    RESEARCH_FRAME_TABLE: (
        SQL_ROOT / "create_table_next_uk_nextads_model_research_frames.sql"
    ),
    AUTOML_DISCOVERY_TABLE: (
        SQL_ROOT / "create_table_next_uk_nextads_automl_discovery_receipts.sql"
    ),
    AUTOML_DISCOVERY_CLAIM_TABLE: (
        SQL_ROOT / "create_table_next_uk_nextads_automl_discovery_claims.sql"
    ),
}
_FRAME_CHECKSUM_EXCLUDED = (
    "research_frame_attempt_id",
    "research_attempt_id",
    "created_at",
)


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    return value.strip()


def _identity(kind: str, values: dict[str, object]) -> str:
    payload = {key: values[key] for key in sorted(values)}
    digest = hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return f"{_text(kind, 'identity kind')}:{digest}"


def research_build_id(
    *,
    model_definition_checksum: str,
    training_receipt_id: str,
    research_plan_checksum: str,
    evaluation_schema_version: str,
) -> str:
    """Identify equivalent research independently from any run attempt."""
    return _identity(
        "research",
        {
            "evaluation_schema_version": _text(
                evaluation_schema_version,
                "evaluation_schema_version",
            ),
            "model_definition_checksum": _text(
                model_definition_checksum,
                "model_definition_checksum",
            ),
            "research_plan_checksum": _text(
                research_plan_checksum,
                "research_plan_checksum",
            ),
            "training_receipt_id": _text(
                training_receipt_id,
                "training_receipt_id",
            ),
        },
    )


def research_frame_id(
    *,
    research_build_id: str,
    frame_plan_checksum: str,
) -> str:
    """Identify the PII-reduced data contract for one logical research run."""
    return _identity(
        "research-frame",
        {
            "frame_plan_checksum": _text(
                frame_plan_checksum,
                "frame_plan_checksum",
            ),
            "research_build_id": _text(
                research_build_id,
                "research_build_id",
            ),
        },
    )


def candidate_evaluation_id(
    *,
    research_build_id: str,
    candidate_id: str,
    candidate_spec_checksum: str,
) -> str:
    """Identify one declared candidate independently from its attempts."""
    return _identity(
        "candidate",
        {
            "candidate_id": _text(candidate_id, "candidate_id"),
            "candidate_spec_checksum": _text(
                candidate_spec_checksum,
                "candidate_spec_checksum",
            ),
            "research_build_id": _text(
                research_build_id,
                "research_build_id",
            ),
        },
    )


def selection_decision_id(
    *,
    research_build_id: str,
    selection_mode: str,
    recommended_candidate_id: str,
    selected_candidate_id: str,
    reason: str,
) -> str:
    """Identify the exact automatic or reviewed selection decision."""
    return _identity(
        "selection",
        {
            "reason": _text(reason, "reason"),
            "recommended_candidate_id": _text(
                recommended_candidate_id,
                "recommended_candidate_id",
            ),
            "research_build_id": _text(
                research_build_id,
                "research_build_id",
            ),
            "selected_candidate_id": _text(
                selected_candidate_id,
                "selected_candidate_id",
            ),
            "selection_mode": _text(selection_mode, "selection_mode"),
        },
    )


def automl_discovery_id(
    *,
    research_build_id: str,
    research_frame_id: str,
    research_frame_delta_version: int,
    request_checksum: str,
) -> str:
    """Identify discovery against one exact, PII-reduced frame version."""
    if (
        isinstance(research_frame_delta_version, bool)
        or not isinstance(research_frame_delta_version, int)
        or research_frame_delta_version < 0
    ):
        raise ValueError("research_frame_delta_version cannot be negative")
    return _identity(
        "automl-discovery",
        {
            "research_build_id": _text(
                research_build_id,
                "research_build_id",
            ),
            "research_frame_delta_version": research_frame_delta_version,
            "research_frame_id": _text(
                research_frame_id,
                "research_frame_id",
            ),
            "request_checksum": _text(
                request_checksum,
                "request_checksum",
            ),
        },
    )


def attempt_id(*, logical_id: str, invocation_id: str) -> str:
    """Derive a distinct attempt from an external job or run identity."""
    return _identity(
        "attempt",
        {
            "invocation_id": _text(invocation_id, "invocation_id"),
            "logical_id": _text(logical_id, "logical_id"),
        },
    )


@dataclass(frozen=True)
class ResearchFrameBinding:
    """Exact Delta version written for a PII-reduced research frame."""

    research_frame_id: str
    research_frame_attempt_id: str
    research_build_id: str
    research_attempt_id: str
    training_receipt_id: str
    research_frame_table: str
    research_frame_delta_version: int
    research_frame_row_count: int
    research_frame_schema_checksum: str
    research_frame_data_checksum: str
    research_frame_write_receipt_id: str
    research_frame_feature_schema_json: str
    research_frame_slice_schema_json: str


def create_research_tables(
    spark: Any,
    *,
    catalog: str,
    schema: str,
) -> tuple[str, ...]:
    """Create the DEV research tables from repository contracts."""
    paths = []
    for table, contract in RESEARCH_TABLE_CONTRACTS.items():
        spark.sql(contract.read_text().format(catalog=catalog, schema=schema))
        paths.append(table_path(catalog, schema, table))
    return tuple(paths)


def _existing_attempt(
    spark: Any,
    *,
    table: str,
    keys: dict[str, str],
) -> dict[str, Any] | None:
    from pyspark.sql import functions as F

    frame = spark.table(table)
    for column, value in keys.items():
        frame = frame.where(F.col(column) == F.lit(value))
    rows = frame.limit(2).collect()
    if len(rows) > 1:
        raise ValueError(
            "Immutable research attempt has duplicate composite keys: "
            + ", ".join(f"{key}={value}" for key, value in keys.items())
        )
    return rows[0].asDict(recursive=True) if rows else None


def _persist_immutable_row(
    spark: Any,
    *,
    table: str,
    row: dict[str, Any],
    keys: tuple[str, str],
    operation: str,
) -> str:
    if row.get("completed_at") is None:
        raise ValueError("Only terminal research attempts can be persisted")
    identity = {key: _text(row[key], key) for key in keys}
    existing = _existing_attempt(spark, table=table, keys=identity)
    if existing is not None:
        differences = sorted(
            column
            for column, value in row.items()
            if _comparable(existing.get(column)) != _comparable(value)
        )
        if differences:
            raise ValueError(
                "Research attempt rows are immutable; changed columns: "
                + ", ".join(differences)
            )
        log_output_location(
            table,
            kind="delta_table",
            details={
                "operation": operation,
                "reused": True,
                "status": row.get("status"),
            },
        )
        return table
    frame = typed_table_frame(spark, table, [row])
    _merge_insert_only(
        spark,
        table=table,
        frame=frame,
        keys=keys,
        operation=operation,
    )
    stored = _existing_attempt(spark, table=table, keys=identity)
    if stored is None:
        raise ValueError("Immutable research attempt was not persisted")
    differences = sorted(
        column
        for column, value in row.items()
        if _comparable(stored.get(column)) != _comparable(value)
    )
    if differences:
        raise ValueError(
            "Concurrent immutable research attempt differs in columns: "
            + ", ".join(differences)
        )
    log_output_location(
        table,
        kind="delta_table",
        details={
            "operation": operation,
            "reused": False,
            "status": row.get("status"),
        },
    )
    return table


def _merge_insert_only(
    spark: Any,
    *,
    table: str,
    frame: Any,
    keys: tuple[str, str],
    operation: str,
) -> None:
    """Atomically insert one immutable row when its composite key is absent."""
    view = f"_nextads_research_{operation}_{uuid4().hex}"
    frame.createOrReplaceTempView(view)
    columns = list(frame.columns)
    condition = " AND ".join(
        f"target.{quote_identifier(key)} <=> source.{quote_identifier(key)}"
        for key in keys
    )
    insert_columns = ", ".join(quote_identifier(column) for column in columns)
    insert_values = ", ".join(
        f"source.{quote_identifier(column)}" for column in columns
    )
    statement = f"""
MERGE INTO {quote_qualified_identifier(table)} AS target
USING {quote_qualified_identifier(view)} AS source
ON {condition}
WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
"""
    try:
        spark.sql(statement)
    finally:
        spark.catalog.dropTempView(view)


def _comparable(value: Any) -> Any:
    """Normalise Spark timestamp round trips before idempotence checks."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _attempt_has_rows(
    spark: Any,
    *,
    table: str,
    keys: dict[str, str],
) -> bool:
    from pyspark.sql import functions as F

    frame = spark.table(table)
    for column, value in keys.items():
        frame = frame.where(F.col(column) == F.lit(value))
    return bool(frame.limit(1).collect())


def persist_research_build(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    build: ModelResearchBuild,
) -> str:
    """Append one terminal research attempt without replacing older failures."""
    target = table_path(catalog, schema, RESEARCH_BUILD_TABLE)
    return _persist_immutable_row(
        spark,
        table=target,
        row=asdict(build),
        keys=("research_build_id", "research_attempt_id"),
        operation="model_research_build",
    )


def persist_candidate_evaluation(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    evaluation: CandidateEvaluation,
) -> str:
    """Append one candidate attempt, including declared failures."""
    target = table_path(catalog, schema, CANDIDATE_EVALUATION_TABLE)
    row = asdict(evaluation)
    row["metrics_json"] = json.dumps(
        dict(row.pop("metrics")),
        sort_keys=True,
        separators=(",", ":"),
    )
    return _persist_immutable_row(
        spark,
        table=target,
        row=row,
        keys=("candidate_evaluation_id", "candidate_attempt_id"),
        operation="candidate_evaluation",
    )


def persist_selection_decision(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    decision: ModelSelectionDecision,
) -> str:
    """Append the exact automatic or reviewed model-selection decision."""
    if decision.status == "READY" and (
        decision.model_build_id is None
        or decision.registered_model_name is None
        or decision.decision_code_sha is None
    ):
        raise ValueError(
            "A READY selection must lock its deterministic model_build_id, "
            "registration target and decision code SHA before test data is "
            "read"
        )
    target = table_path(catalog, schema, SELECTION_DECISION_TABLE)
    return _persist_immutable_row(
        spark,
        table=target,
        row=asdict(decision),
        keys=("selection_decision_id", "selection_attempt_id"),
        operation="model_selection_decision",
    )


def persist_automl_discovery_receipt(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    receipt: AutoMLDiscoveryReceipt,
) -> str:
    """Append one bounded AutoML discovery receipt without registering it."""
    target = table_path(catalog, schema, AUTOML_DISCOVERY_TABLE)
    return _persist_immutable_row(
        spark,
        table=target,
        row=asdict(receipt),
        keys=("discovery_id", "discovery_attempt_id"),
        operation="automl_discovery_receipt",
    )


def _ready_attempt(
    spark: Any,
    *,
    table: str,
    logical_key: str,
    logical_id: str,
    object_name: str,
) -> dict[str, Any] | None:
    from pyspark.sql import functions as F

    rows = (
        spark.table(table)
        .where(F.col(logical_key) == F.lit(logical_id))
        .where(F.col("status") == F.lit("READY"))
        .where(F.col("completed_at").isNotNull())
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise ValueError(
            f"More than one READY {object_name} found for {logical_id}"
        )
    return rows[0].asDict(recursive=True) if rows else None


def load_ready_research_build(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    research_build_id: str,
) -> ModelResearchBuild | None:
    """Reuse only a complete READY research attempt."""
    values = _ready_attempt(
        spark,
        table=table_path(catalog, schema, RESEARCH_BUILD_TABLE),
        logical_key="research_build_id",
        logical_id=research_build_id,
        object_name="research build",
    )
    return ModelResearchBuild(**values) if values is not None else None


def load_terminal_research_build_for_attempt(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    research_build_id: str,
    research_attempt_id: str,
) -> ModelResearchBuild | None:
    """Load one exact completed READY, awaiting or failed attempt."""
    from pyspark.sql import functions as F

    rows = (
        spark.table(table_path(catalog, schema, RESEARCH_BUILD_TABLE))
        .where(F.col("research_build_id") == F.lit(research_build_id))
        .where(F.col("research_attempt_id") == F.lit(research_attempt_id))
        .where(F.col("status").isin("AWAITING_SELECTION", "READY", "FAILED"))
        .where(F.col("completed_at").isNotNull())
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise ValueError(
            "More than one terminal research build found for attempt "
            f"{research_build_id}/{research_attempt_id}"
        )
    return (
        ModelResearchBuild(**rows[0].asDict(recursive=True)) if rows else None
    )


def load_selectable_research_build(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    research_build_id: str,
) -> ModelResearchBuild | None:
    """Resolve the one completed attempt available to reviewed selection."""
    from pyspark.sql import functions as F

    target = table_path(catalog, schema, RESEARCH_BUILD_TABLE)
    rows = (
        spark.table(target)
        .where(F.col("research_build_id") == F.lit(research_build_id))
        .where(F.col("status").isin("AWAITING_SELECTION", "READY"))
        .where(F.col("completed_at").isNotNull())
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise ValueError(
            "Reviewed selection requires exactly one completed research "
            f"attempt for {research_build_id}"
        )
    return (
        ModelResearchBuild(**rows[0].asDict(recursive=True)) if rows else None
    )


def load_ready_candidate_evaluation(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    candidate_evaluation_id: str,
    research_attempt_id: str | None = None,
) -> CandidateEvaluation | None:
    """Reuse only one complete candidate evaluation."""
    from pyspark.sql import functions as F

    frame = spark.table(
        table_path(catalog, schema, CANDIDATE_EVALUATION_TABLE)
    ).where(F.col("candidate_evaluation_id") == F.lit(candidate_evaluation_id))
    if research_attempt_id is not None:
        frame = frame.where(
            F.col("research_attempt_id") == F.lit(research_attempt_id)
        )
    rows = (
        frame.where(F.col("status") == F.lit("READY"))
        .where(F.col("completed_at").isNotNull())
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise ValueError(
            "More than one READY candidate evaluation found for "
            f"{candidate_evaluation_id}"
        )
    if not rows:
        return None
    values = rows[0].asDict(recursive=True)
    values["metrics"] = tuple(
        (name, float(value))
        for name, value in json.loads(values.pop("metrics_json")).items()
    )
    return CandidateEvaluation(**values)


def load_terminal_candidate_evaluation(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    candidate_evaluation_id: str,
    research_attempt_id: str,
) -> CandidateEvaluation | None:
    """Load the exact READY or FAILED candidate from one research attempt."""
    from pyspark.sql import functions as F

    rows = (
        spark.table(table_path(catalog, schema, CANDIDATE_EVALUATION_TABLE))
        .where(
            F.col("candidate_evaluation_id") == F.lit(candidate_evaluation_id)
        )
        .where(F.col("research_attempt_id") == F.lit(research_attempt_id))
        .where(F.col("status").isin("READY", "FAILED"))
        .where(F.col("completed_at").isNotNull())
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise ValueError(
            "More than one terminal candidate evaluation found for "
            f"{candidate_evaluation_id} in {research_attempt_id}"
        )
    if not rows:
        return None
    values = rows[0].asDict(recursive=True)
    values["metrics"] = tuple(
        (name, float(value))
        for name, value in json.loads(values.pop("metrics_json")).items()
    )
    return CandidateEvaluation(**values)


def load_ready_selection_decision(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    selection_decision_id: str,
) -> ModelSelectionDecision | None:
    """Reuse only one complete automatic or reviewed selection."""
    values = _ready_attempt(
        spark,
        table=table_path(catalog, schema, SELECTION_DECISION_TABLE),
        logical_key="selection_decision_id",
        logical_id=selection_decision_id,
        object_name="selection decision",
    )
    return ModelSelectionDecision(**values) if values is not None else None


def _ready_selection_rows_for_research_attempt(
    spark: Any,
    *,
    table: str,
    research_build_id: str,
    research_attempt_id: str,
) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    rows = (
        spark.table(table)
        .where(
            F.col("research_build_id")
            == F.lit(_text(research_build_id, "research_build_id"))
        )
        .where(
            F.col("research_attempt_id")
            == F.lit(_text(research_attempt_id, "research_attempt_id"))
        )
        .where(F.col("status") == F.lit("READY"))
        .where(F.col("completed_at").isNotNull())
        .limit(2)
        .collect()
    )
    return [row.asDict(recursive=True) for row in rows]


def load_ready_selection_for_research_attempt(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    research_build_id: str,
    research_attempt_id: str,
) -> ModelSelectionDecision | None:
    """Load the only READY selection locked to one research attempt."""
    rows = _ready_selection_rows_for_research_attempt(
        spark,
        table=table_path(catalog, schema, SELECTION_DECISION_TABLE),
        research_build_id=research_build_id,
        research_attempt_id=research_attempt_id,
    )
    if len(rows) > 1:
        raise ValueError(
            "More than one READY selection decision found for research "
            f"attempt {research_build_id}/{research_attempt_id}"
        )
    return ModelSelectionDecision(**rows[0]) if rows else None


def load_ready_automl_discovery_receipt(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    discovery_id: str,
) -> AutoMLDiscoveryReceipt | None:
    """Reuse only a successful bounded discovery attempt."""
    values = _ready_attempt(
        spark,
        table=table_path(catalog, schema, AUTOML_DISCOVERY_TABLE),
        logical_key="discovery_id",
        logical_id=discovery_id,
        object_name="AutoML discovery receipt",
    )
    return AutoMLDiscoveryReceipt(**values) if values is not None else None


def persist_research_frame(
    spark: Any,
    frame: Any,
    *,
    catalog: str,
    schema: str,
    research_frame_id: str,
    research_frame_attempt_id: str,
    research_build_id: str,
    research_attempt_id: str,
    training_receipt_id: str,
    schemas: ResearchFrameSchemas,
    git_commit: str,
) -> ResearchFrameBinding:
    """Append one exact packed frame and reuse only its tagged retry."""
    identities = {
        "research_frame_id": _text(research_frame_id, "research_frame_id"),
        "research_frame_attempt_id": _text(
            research_frame_attempt_id,
            "research_frame_attempt_id",
        ),
        "research_build_id": _text(research_build_id, "research_build_id"),
        "research_attempt_id": _text(
            research_attempt_id, "research_attempt_id"
        ),
        "training_receipt_id": _text(
            training_receipt_id,
            "training_receipt_id",
        ),
    }
    _text(git_commit, "git_commit")
    if tuple(frame.columns) != RESEARCH_FRAME_COLUMNS:
        raise ValueError(
            "Packed research frame columns do not match the immutable contract"
        )
    validate_replace_source_scope(frame, identities)
    target = table_path(catalog, schema, RESEARCH_FRAME_TABLE)
    key_summary = validate_unique_non_null_keys(
        frame,
        ("research_frame_id", "research_frame_attempt_id", "row_id"),
    )
    if key_summary.row_count < 1:
        raise ValueError("A research frame must contain at least one row")
    data_checksum = feature_value_checksum(
        frame,
        excluded_columns=_FRAME_CHECKSUM_EXCLUDED,
    )
    packed_schema_checksum = schema_checksum(frame)
    prior = find_delta_write_receipt(
        spark,
        target_table=target,
        build_id=identities["research_frame_id"],
        attempt_id=identities["research_frame_attempt_id"],
    )
    if prior is not None:
        stored = spark.table(target)
        from pyspark.sql import functions as F

        stored = stored.where(
            (F.col("research_frame_id") == F.lit(research_frame_id))
            & (
                F.col("research_frame_attempt_id")
                == F.lit(research_frame_attempt_id)
            )
        )
        if stored.count() != key_summary.row_count:
            raise ValueError(
                "Persisted research frame retry has a row-count drift"
            )
        if (
            feature_value_checksum(
                stored,
                excluded_columns=_FRAME_CHECKSUM_EXCLUDED,
            )
            != data_checksum
        ):
            raise ValueError("Persisted research frame retry has a data drift")
        receipt = prior
    else:
        existing = _attempt_has_rows(
            spark,
            table=target,
            keys={
                "research_frame_id": research_frame_id,
                "research_frame_attempt_id": research_frame_attempt_id,
            },
        )
        if existing:
            raise ValueError(
                "Research frame rows exist without their exact Delta receipt"
            )
        receipt = atomic_append_by_name(
            spark,
            frame,
            target_table=target,
            build_id=research_frame_id,
            attempt_id=research_frame_attempt_id,
            git_commit=git_commit,
            commit_metadata={"operation": "model_research_frame"},
        )
    if (
        receipt.delta_version is None
        or receipt.row_count is None
        or receipt.schema_checksum is None
        or not receipt.receipt_id
    ):
        raise ValueError("Research frame Delta receipt is incomplete")
    if int(receipt.row_count) != key_summary.row_count:
        raise ValueError(
            "Research frame Delta receipt row count does not match"
        )
    if receipt.schema_checksum != packed_schema_checksum:
        raise ValueError("Research frame Delta receipt schema does not match")
    if prior is not None:
        log_output_location(
            target,
            kind="delta_table",
            details={
                "delta_version": int(receipt.delta_version),
                "operation": "model_research_frame",
                "receipt_id": receipt.receipt_id,
                "reused": True,
                "row_count": key_summary.row_count,
            },
        )
    return ResearchFrameBinding(
        **identities,
        research_frame_table=target,
        research_frame_delta_version=int(receipt.delta_version),
        research_frame_row_count=key_summary.row_count,
        research_frame_schema_checksum=packed_schema_checksum,
        research_frame_data_checksum=data_checksum,
        research_frame_write_receipt_id=receipt.receipt_id,
        research_frame_feature_schema_json=schemas.feature_schema_json,
        research_frame_slice_schema_json=schemas.slice_schema_json,
    )


def read_research_frame(
    spark: Any,
    *,
    binding: ResearchFrameBinding,
) -> Any:
    """Read one exact packed frame at its recorded Delta version."""
    from pyspark.sql import functions as F

    frame = (
        spark.read.option(
            "versionAsOf",
            binding.research_frame_delta_version,
        )
        .table(binding.research_frame_table)
        .where(F.col("research_frame_id") == F.lit(binding.research_frame_id))
        .where(
            F.col("research_frame_attempt_id")
            == F.lit(binding.research_frame_attempt_id)
        )
    )
    if frame.count() != binding.research_frame_row_count:
        raise ValueError(
            "Research frame row count no longer matches its receipt"
        )
    if schema_checksum(frame) != binding.research_frame_schema_checksum:
        raise ValueError("Research frame schema no longer matches its receipt")
    if (
        feature_value_checksum(
            frame,
            excluded_columns=_FRAME_CHECKSUM_EXCLUDED,
        )
        != binding.research_frame_data_checksum
    ):
        raise ValueError("Research frame data no longer matches its receipt")
    return frame


def read_unpacked_research_frame(
    spark: Any,
    *,
    binding: ResearchFrameBinding,
) -> Any:
    """Read and reconstruct exact candidate inputs without source joins."""
    packed = read_research_frame(spark, binding=binding)
    return unpack_research_frame(
        packed,
        schemas=ResearchFrameSchemas(
            feature_schema_json=(binding.research_frame_feature_schema_json),
            slice_schema_json=binding.research_frame_slice_schema_json,
        ),
    )


def read_training_frame(
    spark: Any,
    *,
    binding: ResearchFrameBinding,
) -> Any:
    """Read only rows that a candidate is allowed to fit."""
    return training_partition(
        read_unpacked_research_frame(spark, binding=binding)
    )


def read_validation_frame(
    spark: Any,
    *,
    binding: ResearchFrameBinding,
) -> Any:
    """Read only rows allowed for pre-selection comparison."""
    return validation_partition(
        read_unpacked_research_frame(spark, binding=binding)
    )


def read_automl_discovery_frame(
    spark: Any,
    *,
    binding: ResearchFrameBinding,
) -> Any:
    """Read train and validation while withholding the test split."""
    return automl_discovery_partition(
        read_unpacked_research_frame(spark, binding=binding)
    )


def read_selected_test_frame(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    binding: ResearchFrameBinding,
    selection_decision_id: str,
    selected_candidate_id: str,
    selected_candidate_evaluation_id: str,
) -> Any:
    """Read test only after the exact, durable selection has been recorded."""
    decision = load_ready_selection_decision(
        spark,
        catalog=catalog,
        schema=schema,
        selection_decision_id=selection_decision_id,
    )
    if decision is None:
        raise ValueError(
            "Untouched test data requires a persisted READY selection decision"
        )
    expected = {
        "research_build_id": binding.research_build_id,
        "research_attempt_id": binding.research_attempt_id,
        "selected_candidate_id": _text(
            selected_candidate_id, "selected_candidate_id"
        ),
        "selected_candidate_evaluation_id": _text(
            selected_candidate_evaluation_id,
            "selected_candidate_evaluation_id",
        ),
    }
    changed = [
        field
        for field, value in expected.items()
        if getattr(decision, field) != value
    ]
    if changed:
        raise ValueError(
            "Persisted selection does not match the requested test frame: "
            + ", ".join(changed)
        )
    if decision.model_build_id is None:
        raise ValueError(
            "Persisted selection has not locked a deterministic model build"
        )
    if decision.registered_model_name is None:
        raise ValueError(
            "Persisted selection has not locked a registration target"
        )
    return selected_test_partition(
        read_unpacked_research_frame(spark, binding=binding),
        selection_decision_id=decision.selection_decision_id,
    )


__all__ = [
    "AUTOML_DISCOVERY_CLAIM_TABLE",
    "AUTOML_DISCOVERY_TABLE",
    "CANDIDATE_EVALUATION_TABLE",
    "RESEARCH_BUILD_TABLE",
    "RESEARCH_CLAIM_TABLE",
    "RESEARCH_FRAME_TABLE",
    "RESEARCH_TABLE_CONTRACTS",
    "SELECTION_DECISION_TABLE",
    "ResearchFrameBinding",
    "attempt_id",
    "automl_discovery_id",
    "candidate_evaluation_id",
    "create_research_tables",
    "load_ready_automl_discovery_receipt",
    "load_ready_candidate_evaluation",
    "load_terminal_candidate_evaluation",
    "load_ready_research_build",
    "load_terminal_research_build_for_attempt",
    "load_ready_selection_decision",
    "load_ready_selection_for_research_attempt",
    "load_selectable_research_build",
    "persist_automl_discovery_receipt",
    "persist_candidate_evaluation",
    "persist_research_build",
    "persist_research_frame",
    "persist_selection_decision",
    "read_automl_discovery_frame",
    "read_selected_test_frame",
    "read_training_frame",
    "read_validation_frame",
    "research_build_id",
    "research_frame_id",
    "selection_decision_id",
]

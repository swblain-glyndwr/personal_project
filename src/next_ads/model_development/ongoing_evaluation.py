"""Immutable DEV evaluation scoring over accepted NextAds candidates."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
from typing import Any

from next_ads.common.delta_writes import (
    replace_scope_by_name,
    schema_checksum,
    typed_table_frame,
    validate_unique_non_null_keys,
)
from next_ads.features.feature_builds import feature_value_checksum
from next_ads.model_development.contracts import ModelBuild, ModelDefinition
from next_ads.model_development.plugins import ModelPluginRegistry
from next_ads.model_development.scoring_sets import ScoringFeatureBinding


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SQL_ROOT = PROJECT_ROOT / "sql" / "model_development"
EVALUATION_SCORING_BUILD_TABLE = (
    "next_uk_nextads_model_evaluation_scoring_builds"
)
EVALUATION_SCORE_TABLE = "next_uk_nextads_model_evaluation_scores"
BUILDING = "BUILDING"
READY = "READY"
FAILED = "FAILED"
VALID_BUILD_STATUSES = frozenset({BUILDING, READY, FAILED})
CANDIDATE_KEYS = (
    "route",
    "scope_type",
    "scope_value",
    "account_number",
    "advert_id",
)
SHOPPING_BAG_SCOPES = (
    ("v1", "location", "SB1"),
    ("v1", "location", "SB2"),
    ("v2", "page_type", "ShoppingBagPage"),
)


def _require_non_empty_candidate_scope(
    frame: Any,
    *,
    route: str,
    scope_type: str,
    scope_value: str,
) -> None:
    """Reject a partial evaluation that omits one declared SB placement."""
    if frame.limit(1).count() == 0:
        raise ValueError(
            "Accepted Shopping Bag candidate scope is empty: "
            f"route={route}, {scope_type}={scope_value}"
        )


def _required_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    return value.strip()


def _table_path(catalog: str, schema: str, table: str) -> str:
    return ".".join(
        _required_text(value, label)
        for value, label in (
            (catalog, "catalog"),
            (schema, "schema"),
            (table, "table"),
        )
    )


@dataclass(frozen=True)
class CandidateInputBinding:
    """Exact accepted candidate attempt used by one evaluation build."""

    route: str
    output_grain: str
    candidate_build_id: str
    candidate_build_attempt_id: str
    portfolio_id: str
    portfolio_attempt_id: str
    candidate_foundation_snapshot_id: str
    serving_slot: str
    scopes: tuple[str, ...]
    builds_table: str
    scores_table: str
    ad_sets_table: str

    def __post_init__(self) -> None:
        """Require route-correct grain and complete candidate provenance."""
        if self.route not in {"v1", "v2"}:
            raise ValueError("Candidate binding route must be v1 or v2")
        expected_grain = "location" if self.route == "v1" else "page_type"
        if self.output_grain != expected_grain:
            raise ValueError(
                f"Candidate binding {self.route} requires {expected_grain}"
            )
        for field_name in (
            "candidate_build_id",
            "candidate_build_attempt_id",
            "portfolio_id",
            "portfolio_attempt_id",
            "candidate_foundation_snapshot_id",
            "serving_slot",
            "builds_table",
            "scores_table",
            "ad_sets_table",
        ):
            _required_text(getattr(self, field_name), field_name)
        scopes = tuple(_required_text(value, "scope") for value in self.scopes)
        if not scopes or len(scopes) != len(set(scopes)):
            raise ValueError(
                "Candidate binding scopes must be unique and non-empty"
            )
        object.__setattr__(self, "scopes", scopes)


@dataclass(frozen=True)
class OngoingEvaluationBuild:
    """One immutable attempt to score an exact model and candidate set."""

    scoring_build_id: str
    scoring_build_attempt_id: str
    model_build_id: str
    model_name: str
    model_definition_checksum: str
    registered_model_name: str
    registered_model_version: int
    model_uri: str
    artifact_digest: str
    run_date: date
    serving_slot: str
    candidate_bindings: tuple[CandidateInputBinding, ...]
    feature_bindings: tuple[ScoringFeatureBinding, ...]
    input_row_count: int
    input_schema_checksum: str
    input_value_checksum: str
    git_commit: str
    orchestration_run_id: int
    task_run_id: int
    execution_count: int
    status: str
    created_at: datetime
    output_table: str | None = None
    output_delta_version: int | None = None
    output_row_count: int | None = None
    output_schema_checksum: str | None = None
    output_value_checksum: str | None = None
    completed_at: datetime | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        """Enforce the BUILDING, READY and FAILED publication lifecycle."""
        for field_name in (
            "scoring_build_id",
            "scoring_build_attempt_id",
            "model_build_id",
            "model_name",
            "model_definition_checksum",
            "registered_model_name",
            "model_uri",
            "artifact_digest",
            "serving_slot",
            "input_schema_checksum",
            "input_value_checksum",
            "git_commit",
        ):
            _required_text(getattr(self, field_name), field_name)
        if self.registered_model_version < 1:
            raise ValueError("registered_model_version must be positive")
        if self.input_row_count < 1:
            raise ValueError("input_row_count must be positive")
        if not self.candidate_bindings or not self.feature_bindings:
            raise ValueError(
                "Evaluation builds need candidate and feature bindings"
            )
        if self.status not in VALID_BUILD_STATUSES:
            raise ValueError(f"Unsupported evaluation status: {self.status}")
        output_fields = (
            self.output_table,
            self.output_delta_version,
            self.output_row_count,
            self.output_schema_checksum,
            self.output_value_checksum,
        )
        if self.status == BUILDING:
            if any(value is not None for value in output_fields):
                raise ValueError("A BUILDING evaluation cannot have output")
            if (
                self.completed_at is not None
                or self.failure_reason is not None
            ):
                raise ValueError("A BUILDING evaluation cannot be terminal")
        elif self.status == READY:
            if any(value is None for value in output_fields):
                raise ValueError("A READY evaluation needs exact output proof")
            if self.completed_at is None or self.failure_reason is not None:
                raise ValueError(
                    "A READY evaluation must complete without failure"
                )
        elif self.completed_at is None or not self.failure_reason:
            raise ValueError(
                "A FAILED evaluation needs completion and a reason"
            )

    def as_row(self) -> dict[str, object]:
        """Return the typed-table row persisted as the build manifest."""
        return {
            "scoring_build_id": self.scoring_build_id,
            "scoring_build_attempt_id": self.scoring_build_attempt_id,
            "model_build_id": self.model_build_id,
            "model_name": self.model_name,
            "model_definition_checksum": self.model_definition_checksum,
            "registered_model_name": self.registered_model_name,
            "registered_model_version": self.registered_model_version,
            "model_uri": self.model_uri,
            "artifact_digest": self.artifact_digest,
            "run_date": self.run_date,
            "serving_slot": self.serving_slot,
            "candidate_bindings_json": json.dumps(
                [asdict(binding) for binding in self.candidate_bindings],
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ),
            "feature_bindings_json": json.dumps(
                [asdict(binding) for binding in self.feature_bindings],
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ),
            "input_row_count": self.input_row_count,
            "input_schema_checksum": self.input_schema_checksum,
            "input_value_checksum": self.input_value_checksum,
            "output_table": self.output_table,
            "output_delta_version": self.output_delta_version,
            "output_row_count": self.output_row_count,
            "output_schema_checksum": self.output_schema_checksum,
            "output_value_checksum": self.output_value_checksum,
            "git_commit": self.git_commit,
            "orchestration_run_id": self.orchestration_run_id,
            "task_run_id": self.task_run_id,
            "execution_count": self.execution_count,
            "status": self.status,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "failure_reason": self.failure_reason,
        }


@dataclass(frozen=True)
class EvaluationScoreWriteResult:
    """Exact Delta output committed before a build becomes READY."""

    target_table: str
    delta_version: int
    row_count: int
    schema_checksum: str
    value_checksum: str


@dataclass(frozen=True)
class EvaluationScoreResult:
    """Scoped scores plus proof that the declared provider contract ran."""

    frame: Any
    provider_contract: str
    provider_row_count: int
    provider_schema_checksum: str
    provider_value_checksum: str


def create_ongoing_evaluation_tables(
    spark: Any,
    *,
    catalog: str,
    schema: str,
) -> tuple[str, str]:
    """Create only the isolated DEV evaluation manifest and history tables."""
    contracts = (
        (
            EVALUATION_SCORING_BUILD_TABLE,
            SQL_ROOT
            / "create_table_next_uk_nextads_model_evaluation_scoring_builds.sql",
        ),
        (
            EVALUATION_SCORE_TABLE,
            SQL_ROOT
            / "create_table_next_uk_nextads_model_evaluation_scores.sql",
        ),
    )
    paths = []
    for table, contract in contracts:
        spark.sql(contract.read_text().format(catalog=catalog, schema=schema))
        paths.append(_table_path(catalog, schema, table))
    return tuple(paths)  # type: ignore[return-value]


def scoring_build_attempt_id(
    orchestration_run_id: int,
    task_run_id: int,
    execution_count: int,
) -> str:
    """Identify one retry without overwriting a previous READY attempt."""
    values = (orchestration_run_id, task_run_id, execution_count)
    if any(isinstance(value, bool) or int(value) < 0 for value in values):
        raise ValueError("Evaluation run identifiers must be non-negative")
    return ":".join(str(int(value)) for value in values)


def evaluation_scoring_build_id(
    *,
    definition: ModelDefinition,
    model_build: ModelBuild,
    run_date: date,
    serving_slot: str,
    candidate_bindings: tuple[CandidateInputBinding, ...],
    feature_bindings: tuple[ScoringFeatureBinding, ...],
    input_row_count: int,
    input_schema_checksum: str,
    input_value_checksum: str,
    git_commit: str,
) -> str:
    """Hash every input that can change one day's evaluation scores."""
    payload = {
        "contract_version": "shopping_bag_ongoing_evaluation/v1",
        "model_build_id": model_build.model_build_id,
        "model_definition_checksum": definition.checksum,
        "model_uri": model_build.model_uri,
        "artifact_digest": model_build.artifact_digest,
        "run_date": run_date.isoformat(),
        "serving_slot": _required_text(serving_slot, "serving_slot"),
        "candidate_bindings": [
            asdict(binding)
            for binding in sorted(
                candidate_bindings,
                key=lambda value: value.route,
            )
        ],
        "feature_bindings": [
            asdict(binding)
            for binding in sorted(
                feature_bindings,
                key=lambda value: (
                    value.feature_id,
                    value.reference_date,
                    value.feature_snapshot_attempt_id,
                ),
            )
        ],
        "input_row_count": input_row_count,
        "input_schema_checksum": input_schema_checksum,
        "input_value_checksum": input_value_checksum,
        "git_commit": _required_text(git_commit, "git_commit"),
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
    ).hexdigest()


def resolve_candidate_attempt_id(
    spark: Any,
    *,
    builds_table: str,
    run_date: date,
    route: str,
    requested_attempt_id: str,
) -> str:
    """Resolve and validate one READY attempt for the requested route/date."""
    from pyspark.sql import functions as F

    requested = _required_text(
        requested_attempt_id,
        "candidate_build_attempt_id",
    )
    if route not in {"v1", "v2"}:
        raise ValueError("Candidate route must be v1 or v2")
    candidates = (
        spark.table(builds_table)
        .where(F.col("RunDate") == F.lit(run_date))
        .where(F.col("Route") == F.lit(route))
        .where(F.col("Status") == F.lit("READY_FOR_NEXTADS"))
    )
    if requested.upper() != "AUTO":
        candidates = candidates.where(
            F.col("CandidateBuildAttemptID") == F.lit(requested)
        )
    rows = (
        candidates.select(
            "CandidateBuildAttemptID",
            "CompletedAt",
        )
        .orderBy(
            F.col("CompletedAt").desc(),
            F.col("CandidateBuildAttemptID").desc(),
        )
        .limit(1)
        .collect()
    )
    if not rows:
        requested_message = (
            "latest" if requested.upper() == "AUTO" else requested
        )
        raise ValueError(
            f"No READY {route} candidate attempt {requested_message} exists "
            f"for {run_date.isoformat()}"
        )
    return rows[0]["CandidateBuildAttemptID"]


def candidate_input_binding(
    accepted: Any,
    *,
    serving_slot: str,
    scopes: tuple[str, ...],
    builds_table: str,
    scores_table: str,
    ad_sets_table: str,
) -> CandidateInputBinding:
    """Convert an accepted candidate loader result into durable provenance."""
    provenance = accepted.provenance
    return CandidateInputBinding(
        route=accepted.route,
        output_grain=accepted.output_grain,
        candidate_build_id=provenance.candidate_build_id,
        candidate_build_attempt_id=(provenance.candidate_build_attempt_id),
        portfolio_id=provenance.portfolio_id,
        portfolio_attempt_id=provenance.portfolio_attempt_id,
        candidate_foundation_snapshot_id=(
            provenance.candidate_foundation_snapshot_id
        ),
        serving_slot=serving_slot,
        scopes=scopes,
        builds_table=builds_table,
        scores_table=scores_table,
        ad_sets_table=ad_sets_table,
    )


def build_shopping_bag_candidate_frame(
    accepted_v1: Any,
    accepted_v2: Any,
    *,
    serving_slot: str,
) -> Any:
    """Read current SB scopes from exact accepted v1 and v2 attempts."""
    from pyspark.sql import functions as F

    inputs = {"v1": accepted_v1, "v2": accepted_v2}
    frames = []
    for route, scope_type, scope_value in SHOPPING_BAG_SCOPES:
        accepted = inputs[route]
        frame = accepted.candidates_for_scope(
            serving_slot,
            scope_value,
        ).select(
            F.col("AccountNumber").cast("string").alias("account_number"),
            F.col("UniqueAdID").cast("string").alias("advert_id"),
            F.col("Score").cast("double").alias("incumbent_score"),
            F.col("TriggerScore")
            .cast("double")
            .alias("incumbent_trigger_score"),
            F.col("Rank").cast("int").alias("incumbent_rank"),
        )
        _require_non_empty_candidate_scope(
            frame,
            route=route,
            scope_type=scope_type,
            scope_value=scope_value,
        )
        provenance = accepted.provenance
        frames.append(
            frame.withColumn("route", F.lit(route))
            .withColumn("scope_type", F.lit(scope_type))
            .withColumn("scope_value", F.lit(scope_value))
            .withColumn("location", F.lit(scope_value))
            .withColumn(
                "candidate_build_id",
                F.lit(provenance.candidate_build_id),
            )
            .withColumn(
                "candidate_build_attempt_id",
                F.lit(provenance.candidate_build_attempt_id),
            )
            .withColumn("portfolio_id", F.lit(provenance.portfolio_id))
            .withColumn(
                "portfolio_attempt_id",
                F.lit(provenance.portfolio_attempt_id),
            )
            .withColumn(
                "candidate_foundation_snapshot_id",
                F.lit(provenance.candidate_foundation_snapshot_id),
            )
            .withColumn("serving_slot", F.lit(serving_slot))
        )
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    validate_unique_non_null_keys(combined, CANDIDATE_KEYS)
    if combined.limit(1).count() == 0:
        raise ValueError("Accepted Shopping Bag candidate scopes are empty")
    return combined


def score_shopping_bag_candidates(
    definition: ModelDefinition,
    model_build: ModelBuild,
    scoring_frame: Any,
    *,
    scoring_build_id: str,
    scoring_build_attempt_id: str,
    run_date: date,
) -> EvaluationScoreResult:
    """Score through the declared plug-in and retain input provenance."""
    from pyspark.sql import functions as F

    if model_build.status != READY or not model_build.model_uri:
        raise ValueError("Ongoing scoring requires a READY exact model build")
    if (
        model_build.model_name != definition.model_name
        or model_build.model_definition_checksum != definition.checksum
    ):
        raise ValueError("Model build does not match its model definition")
    provider = ModelPluginRegistry().score_provider(
        definition,
        run_date=run_date,
    )
    canonical, scoped = provider.score_with_evaluation_scope(
        definition,
        model_build,
        scoring_frame,
        scope_columns=("route", "scope_type", "scope_value"),
    )
    required_provider_columns = {
        "ProviderBuildID",
        "AccountNumber",
        "EntityType",
        "EntityID",
        "ProviderID",
        "RunDate",
        "RawScore",
        "Score",
        "ProviderRank",
    }
    missing_provider_columns = sorted(
        required_provider_columns.difference(canonical.columns)
    )
    if missing_provider_columns:
        raise ValueError(
            "Declared score provider did not emit account_entity_scores/v1: "
            + ", ".join(missing_provider_columns)
        )
    validate_unique_non_null_keys(
        canonical,
        ("ProviderBuildID", "AccountNumber", "EntityType", "EntityID"),
    )
    provider_row_count = canonical.count()
    if provider_row_count == 0:
        raise ValueError("Declared score provider produced no canonical rows")

    source = scoring_frame.alias("source")
    provider_scores = scoped.alias("provider")
    joined = source.join(
        provider_scores,
        (F.col("source.account_number") == F.col("provider.AccountNumber"))
        & (F.col("source.advert_id") == F.col("provider.EntityID"))
        & (F.col("source.route") == F.col("provider.route"))
        & (F.col("source.scope_type") == F.col("provider.scope_type"))
        & (F.col("source.scope_value") == F.col("provider.scope_value")),
        "inner",
    )
    scored = joined.select(
        F.lit(scoring_build_id).alias("scoring_build_id"),
        F.lit(scoring_build_attempt_id).alias("scoring_build_attempt_id"),
        F.lit(run_date).cast("date").alias("run_date"),
        F.lit(model_build.model_build_id).alias("model_build_id"),
        *[
            F.col(f"source.{column}").alias(column)
            for column in (
                "route",
                "scope_type",
                "scope_value",
                "candidate_build_id",
                "candidate_build_attempt_id",
                "portfolio_id",
                "portfolio_attempt_id",
                "candidate_foundation_snapshot_id",
                "serving_slot",
                "account_number",
                "advert_id",
                "incumbent_score",
                "incumbent_trigger_score",
                "incumbent_rank",
            )
        ],
        F.col("provider.Score").cast("double").alias("predicted_pctr"),
        F.col("provider.ProviderRank").cast("int").alias("evaluation_rank"),
        F.current_timestamp().alias("created_at"),
    )
    validate_unique_non_null_keys(
        scored,
        (
            "scoring_build_id",
            "scoring_build_attempt_id",
            *CANDIDATE_KEYS,
        ),
    )
    return EvaluationScoreResult(
        frame=scored,
        provider_contract="account_entity_scores/v1",
        provider_row_count=provider_row_count,
        provider_schema_checksum=schema_checksum(canonical),
        provider_value_checksum=feature_value_checksum(canonical),
    )


def persist_evaluation_build(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    build: OngoingEvaluationBuild,
) -> str:
    """Replace only one attempt header, leaving every other attempt intact."""
    target = _table_path(
        catalog,
        schema,
        EVALUATION_SCORING_BUILD_TABLE,
    )
    frame = typed_table_frame(spark, target, [build.as_row()])
    replace_scope_by_name(
        frame,
        target,
        {
            "scoring_build_id": build.scoring_build_id,
            "scoring_build_attempt_id": build.scoring_build_attempt_id,
        },
        spark=spark,
        build_id=build.scoring_build_id,
        attempt_id=build.scoring_build_attempt_id,
        git_commit=build.git_commit,
        commit_metadata={
            "operation": "ongoing_model_evaluation_manifest",
            "status": build.status,
        },
    )
    return target


def persist_evaluation_scores(
    spark: Any,
    scores: Any,
    *,
    catalog: str,
    schema: str,
    scoring_build_id: str,
    scoring_build_attempt_id: str,
    git_commit: str,
) -> EvaluationScoreWriteResult:
    """Commit one attempt's rows without deleting another day or retry."""
    target = _table_path(catalog, schema, EVALUATION_SCORE_TABLE)
    row_count = scores.count()
    if row_count == 0:
        raise ValueError("Evaluation score output is empty")
    output_schema_checksum = schema_checksum(scores)
    output_value_checksum = feature_value_checksum(
        scores,
        excluded_columns=("created_at",),
    )
    receipt = replace_scope_by_name(
        scores,
        target,
        {
            "scoring_build_id": scoring_build_id,
            "scoring_build_attempt_id": scoring_build_attempt_id,
        },
        scores.columns,
        spark=spark,
        build_id=scoring_build_id,
        attempt_id=scoring_build_attempt_id,
        git_commit=git_commit,
        commit_metadata={"operation": "ongoing_model_evaluation_scores"},
    )
    if receipt.delta_version is None or receipt.row_count != row_count:
        raise RuntimeError("Evaluation output has no complete Delta receipt")
    return EvaluationScoreWriteResult(
        target_table=target,
        delta_version=receipt.delta_version,
        row_count=row_count,
        schema_checksum=output_schema_checksum,
        value_checksum=output_value_checksum,
    )


__all__ = [
    "BUILDING",
    "CANDIDATE_KEYS",
    "EVALUATION_SCORE_TABLE",
    "EVALUATION_SCORING_BUILD_TABLE",
    "FAILED",
    "READY",
    "SHOPPING_BAG_SCOPES",
    "CandidateInputBinding",
    "EvaluationScoreResult",
    "EvaluationScoreWriteResult",
    "OngoingEvaluationBuild",
    "build_shopping_bag_candidate_frame",
    "candidate_input_binding",
    "create_ongoing_evaluation_tables",
    "evaluation_scoring_build_id",
    "persist_evaluation_build",
    "persist_evaluation_scores",
    "resolve_candidate_attempt_id",
    "score_shopping_bag_candidates",
    "scoring_build_attempt_id",
]

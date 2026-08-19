"""Register one reviewed candidate from an immutable research attempt."""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
import logging
import math
from pathlib import Path
import sys
from typing import Any, Iterable, Mapping


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"
if not (SRC_ROOT / "next_ads").is_dir():
    raise RuntimeError(f"Canonical NextAds package not found under {SRC_ROOT}")
sys.path.insert(0, str(SRC_ROOT))
sys.path.insert(1, str(PROJECT_ROOT))


from next_ads.model_development.contracts import (
    ModelBuild,
    ModelDefinition,
    TrainingSetReceipt,
)
from next_ads.model_development.promotion import (
    validate_registered_model_build,
)
from next_ads.model_development.registry import (
    load_model_definition,
    load_model_research_plan,
)
from next_ads.model_development.research_contracts import (
    READY,
    REVIEW_REQUIRED,
    CandidateEvaluation,
    ModelResearchBuild,
    ModelSelectionDecision,
    ResearchPlan,
)
from next_ads.model_development.research_evaluation import (
    EvaluationConfig,
    SliceEvaluationSpec,
    deterministic_selected_test_confidence_intervals,
    evaluate_binary_predictions,
    require_complete_binary_evaluation,
    require_complete_confidence_intervals,
)
from next_ads.model_development.research_claims import (
    CANDIDATES_READY as CLAIM_CANDIDATES_READY,
    COMPLETE as CLAIM_COMPLETE,
    FAILED as CLAIM_FAILED,
    REGISTERED as CLAIM_REGISTERED,
    SELECTION_LOCKED as CLAIM_SELECTION_LOCKED,
    advance_research_claim,
    claim_research_build,
)
from next_ads.model_development.research_selection import (
    normalize_registered_model_name,
    recommend_candidate,
    register_selected_candidate,
    selected_model_build_id,
    validate_score_reproduction,
)
from next_ads.model_development.research_scoring import (
    validate_prediction_adapter_output,
)
from next_ads.model_development.research_store import (
    CANDIDATE_EVALUATION_TABLE,
    SELECTION_DECISION_TABLE,
    ResearchFrameBinding,
    attempt_id,
    create_research_tables,
    load_selectable_research_build,
    persist_selection_decision,
    read_selected_test_frame,
    selection_decision_id,
)
from next_ads.model_development.store import (
    create_model_development_tables,
    load_ready_model_build,
    load_ready_training_set_receipt,
    persist_model_build,
    table_path,
)
from next_ads.ml.lifecycle.registry import configure_mlflow


LOGGER = logging.getLogger(__name__)
EVIDENCE_PREFIX = "MODEL_RESEARCH_REVIEWED_SELECTION="
_SELECTION_CLAIM_LEASE_SECONDS = 12_600


def _required_text(value: str) -> str:
    """Reject missing values and unchanged manual-job placeholders."""
    text = str(value).strip()
    if not text or text.upper() == "REQUIRED":
        raise argparse.ArgumentTypeError("a non-placeholder value is required")
    return text


def _execution_count(value: str) -> int:
    """Parse a non-negative Databricks task execution count."""
    try:
        count = int(value)
    except (TypeError, ValueError) as error:
        raise argparse.ArgumentTypeError(
            "execution_count must be a non-negative integer"
        ) from error
    if count < 0:
        raise argparse.ArgumentTypeError(
            "execution_count must be a non-negative integer"
        )
    return count


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse one explicit, human-reviewed selection."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_name")
    parser.add_argument(
        "--research_build_id", type=_required_text, required=True
    )
    parser.add_argument("--candidate_id", type=_required_text, required=True)
    parser.add_argument("--written_reason", type=_required_text, required=True)
    parser.add_argument("--reviewed_by", type=_required_text, required=True)
    parser.add_argument("--model_catalog", type=_required_text, required=True)
    parser.add_argument("--model_schema", type=_required_text, required=True)
    parser.add_argument(
        "--registered_model_name", type=_required_text, required=True
    )
    parser.add_argument("--code_sha", type=_required_text, required=True)
    parser.add_argument(
        "--orchestration_run_id", type=_required_text, required=True
    )
    parser.add_argument("--task_run_id", type=_required_text, required=True)
    parser.add_argument(
        "--execution_count", type=_execution_count, required=True
    )
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args(argv)


def _spark_session() -> Any:
    """Return the active task Spark session."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    )


def _frame_binding(build: ModelResearchBuild) -> ResearchFrameBinding:
    """Reconstruct the exact immutable frame binding from its build receipt."""
    return ResearchFrameBinding(
        research_frame_id=build.research_frame_id,
        research_frame_attempt_id=build.research_frame_attempt_id,
        research_build_id=build.research_build_id,
        research_attempt_id=build.research_attempt_id,
        training_receipt_id=build.training_receipt_id,
        research_frame_table=build.research_frame_table,
        research_frame_delta_version=build.research_frame_delta_version,
        research_frame_row_count=build.research_frame_row_count,
        research_frame_schema_checksum=build.research_frame_schema_checksum,
        research_frame_data_checksum=build.research_frame_data_checksum,
        research_frame_write_receipt_id=(
            build.research_frame_write_receipt_id
        ),
        research_frame_feature_schema_json=(
            build.research_frame_feature_schema_json
        ),
        research_frame_slice_schema_json=(
            build.research_frame_slice_schema_json
        ),
    )


def _candidate_from_values(values: Mapping[str, Any]) -> CandidateEvaluation:
    """Decode one candidate table row into its typed immutable receipt."""
    decoded = dict(values)
    raw_metrics = decoded.pop("metrics_json")
    metrics = json.loads(raw_metrics)
    if not isinstance(metrics, dict):
        raise ValueError("Candidate metrics_json must contain an object")
    decoded["metrics"] = tuple(
        sorted((str(name), float(value)) for name, value in metrics.items())
    )
    return CandidateEvaluation(**decoded)


def _load_ready_candidate_evaluations(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    build: ModelResearchBuild,
) -> tuple[CandidateEvaluation, ...]:
    """Load all and only READY candidates from one research attempt."""
    from pyspark.sql import functions as F

    target = table_path(catalog, schema, CANDIDATE_EVALUATION_TABLE)
    rows = (
        spark.table(target)
        .where(F.col("research_build_id") == F.lit(build.research_build_id))
        .where(
            F.col("research_attempt_id") == F.lit(build.research_attempt_id)
        )
        .where(F.col("status") == F.lit(READY))
        .where(F.col("completed_at").isNotNull())
        .limit(build.candidate_count + 1)
        .collect()
    )
    evaluations = tuple(
        _candidate_from_values(row.asDict(recursive=True)) for row in rows
    )
    if len(evaluations) != build.successful_candidate_count:
        raise ValueError(
            "READY candidate count does not match the research build receipt"
        )
    candidate_ids = [item.candidate_id for item in evaluations]
    logical_ids = [item.candidate_evaluation_id for item in evaluations]
    if len(candidate_ids) != len(set(candidate_ids)) or len(
        logical_ids
    ) != len(set(logical_ids)):
        raise ValueError(
            "Research attempt contains duplicate READY candidate evaluations"
        )
    return tuple(sorted(evaluations, key=lambda item: item.candidate_id))


def _decision_from_values(
    values: Mapping[str, Any],
) -> ModelSelectionDecision:
    return ModelSelectionDecision(**dict(values))


def _load_ready_decision_for_attempt(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    build: ModelResearchBuild,
) -> ModelSelectionDecision | None:
    """Allow one reviewed decision per immutable research attempt."""
    from pyspark.sql import functions as F

    target = table_path(catalog, schema, SELECTION_DECISION_TABLE)
    rows = (
        spark.table(target)
        .where(F.col("research_build_id") == F.lit(build.research_build_id))
        .where(
            F.col("research_attempt_id") == F.lit(build.research_attempt_id)
        )
        .where(F.col("status") == F.lit(READY))
        .where(F.col("completed_at").isNotNull())
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise ValueError(
            "Research attempt has more than one READY selection decision"
        )
    if not rows:
        return None
    return _decision_from_values(rows[0].asDict(recursive=True))


def _validate_research_inputs(
    definition: ModelDefinition,
    plan: ResearchPlan,
    build: ModelResearchBuild,
    receipt: TrainingSetReceipt,
    evaluations: Iterable[CandidateEvaluation],
) -> None:
    """Prove every selected object belongs to the same declared attempt."""
    if plan.selection_mode != REVIEW_REQUIRED:
        raise ValueError("Manual selection requires REVIEW_REQUIRED policy")
    if (
        build.model_name != definition.model_name
        or build.model_definition_checksum != definition.checksum
        or build.research_plan_checksum != plan.checksum
    ):
        raise ValueError("Research build does not match its model declaration")
    if (
        receipt.receipt_id != build.training_receipt_id
        or receipt.model_name != definition.model_name
        or receipt.model_definition_checksum != definition.checksum
        or receipt.status != READY
    ):
        raise ValueError(
            "Research build does not match its READY training receipt"
        )
    declared = {item.candidate_id: item for item in plan.candidates}
    for evaluation in evaluations:
        spec = declared.get(evaluation.candidate_id)
        if spec is None or evaluation.candidate_spec_checksum != spec.checksum:
            raise ValueError(
                "Candidate evaluation does not match its declared candidate"
            )
        if (
            evaluation.research_build_id != build.research_build_id
            or evaluation.research_attempt_id != build.research_attempt_id
            or evaluation.required == spec.failure_allowed
        ):
            raise ValueError(
                "Candidate evaluation belongs to a different research attempt"
            )


def _new_reviewed_decision(
    *,
    definition: ModelDefinition,
    plan: ResearchPlan,
    build: ModelResearchBuild,
    receipt: TrainingSetReceipt,
    evaluations: tuple[CandidateEvaluation, ...],
    candidate_id: str,
    reason: str,
    reviewed_by: str,
    registered_model_name: str,
    decision_code_sha: str,
    invocation_id: str,
    now: datetime,
) -> tuple[ModelSelectionDecision, CandidateEvaluation]:
    """Construct the deterministic reviewed choice before exposing test."""
    recommended = recommend_candidate(plan, evaluations)
    if build.automatic_candidate_id != recommended.candidate_id:
        raise ValueError(
            "Recalculated recommendation differs from the research receipt"
        )
    selected = [
        item for item in evaluations if item.candidate_id == candidate_id
    ]
    if len(selected) != 1:
        raise ValueError(
            f"No exact READY candidate is available for {candidate_id}"
        )
    candidate = selected[0]
    logical_id = selection_decision_id(
        research_build_id=build.research_build_id,
        selection_mode=REVIEW_REQUIRED,
        recommended_candidate_id=recommended.candidate_id,
        selected_candidate_id=candidate.candidate_id,
        reason=reason,
    )
    decision = ModelSelectionDecision(
        selection_decision_id=logical_id,
        selection_attempt_id=attempt_id(
            logical_id=logical_id,
            invocation_id=invocation_id,
        ),
        research_build_id=build.research_build_id,
        research_attempt_id=build.research_attempt_id,
        selection_mode=REVIEW_REQUIRED,
        recommended_candidate_id=recommended.candidate_id,
        selected_candidate_id=candidate.candidate_id,
        selected_candidate_evaluation_id=(candidate.candidate_evaluation_id),
        reason=reason,
        status=READY,
        created_at=now,
        completed_at=now,
        reviewed_by=reviewed_by,
        registered_model_name=normalize_registered_model_name(
            registered_model_name
        ),
        decision_code_sha=_required_text(decision_code_sha),
    )
    model_build_id = selected_model_build_id(
        definition,
        receipt,
        build,
        candidate,
        decision,
    )
    return replace(decision, model_build_id=model_build_id), candidate


def _reuse_decision(
    expected: ModelSelectionDecision,
    existing: ModelSelectionDecision | None,
) -> tuple[ModelSelectionDecision, bool]:
    if existing is None:
        return expected, False
    stable_fields = (
        "selection_decision_id",
        "research_build_id",
        "research_attempt_id",
        "selection_mode",
        "recommended_candidate_id",
        "selected_candidate_id",
        "selected_candidate_evaluation_id",
        "reason",
        "reviewed_by",
        "model_build_id",
        "registered_model_name",
    )
    changed = [
        field
        for field in stable_fields
        if getattr(existing, field) != getattr(expected, field)
    ]
    if changed:
        raise ValueError(
            "Research attempt already has a different READY selection: "
            + ", ".join(changed)
        )
    return existing, True


def _slice_specs(
    plan: ResearchPlan,
    frame: Any,
) -> tuple[SliceEvaluationSpec, ...]:
    available = set(frame.columns)
    missing = [
        item.column
        for item in plan.slices
        if not item.if_present and item.column not in available
    ]
    if missing:
        raise ValueError(
            "Selected test is missing required reporting slices: "
            + ", ".join(sorted(missing))
        )
    return tuple(
        SliceEvaluationSpec(
            slice_id=item.slice_id,
            column=item.column,
            values=item.values,
            minimum_rows=item.minimum_rows,
        )
        for item in plan.slices
        if item.column in available
    )


def _evaluate_selected_test(
    source_model: Any,
    test_frame: Any,
    *,
    plan: ResearchPlan,
) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    """Score the withheld split and return aggregate evidence only."""
    predictions = source_model.transform(test_frame)
    field_types = {
        field.name: field.dataType.simpleString()
        for field in predictions.schema.fields
    }
    if field_types.get("prediction") != "double":
        raise ValueError("Selected model prediction output must be DOUBLE")
    if field_types.get("score") != "double":
        raise ValueError("Selected model score output must be DOUBLE")
    slice_specs = _slice_specs(plan, test_frame)
    validate_prediction_adapter_output(
        test_frame,
        predictions,
        label_column="label",
        slice_columns=tuple(spec.column for spec in slice_specs),
        context="Reviewed selected-test model",
    )
    rules = plan.evaluation_rules
    config = EvaluationConfig(
        min_rows=rules.minimum_slice_rows,
        top_percentages=tuple(
            int(round(fraction * 100)) for fraction in rules.top_fractions
        ),
    )
    evaluation = evaluate_binary_predictions(
        predictions,
        label_column="label",
        score_column="score",
        row_id_hash_column="row_id",
        slice_specs=slice_specs,
        config=config,
    )
    require_complete_binary_evaluation(
        evaluation,
        required_metrics=rules.required_metrics,
        context="Reviewed selected test",
    )
    intervals = deterministic_selected_test_confidence_intervals(
        predictions,
        label_column="label",
        score_column="score",
        split_column="split",
        row_id_hash_column="row_id",
        iterations=rules.confidence_interval_resamples,
        confidence=rules.confidence_level,
        seed=rules.confidence_interval_seed,
        curve_bins=config.curve_bins,
    )
    require_complete_confidence_intervals(
        intervals,
        context="Reviewed selected test",
    )
    return predictions, evaluation, intervals


def _json_aggregate(value: Mapping[str, Any], name: str) -> dict[str, Any]:
    """Reject non-finite or non-JSON aggregate evidence before MLflow."""
    try:
        encoded = json.dumps(
            dict(value),
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} evidence is not bounded JSON") from error
    return json.loads(encoded)


def _log_selected_test_evidence(
    mlflow_module: Any,
    *,
    candidate: CandidateEvaluation,
    decision: ModelSelectionDecision,
    evaluation: Mapping[str, Any],
    confidence_intervals: Mapping[str, Any],
) -> None:
    """Attach aggregate test evidence to the selected child run only."""
    aggregate_evaluation = _json_aggregate(evaluation, "evaluation")
    aggregate_intervals = _json_aggregate(
        confidence_intervals,
        "confidence interval",
    )
    metrics = {
        f"test_{name}": float(value)
        for name, value in (aggregate_evaluation.get("metrics") or {}).items()
        if isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    }
    with mlflow_module.start_run(run_id=candidate.mlflow_run_id):
        mlflow_module.log_dict(
            aggregate_evaluation,
            "selected_test/evaluation.json",
        )
        mlflow_module.log_dict(
            aggregate_intervals,
            "selected_test/confidence_intervals.json",
        )
        mlflow_module.log_metrics(metrics)
        mlflow_module.set_tags(
            {
                "nextads_selected_test_evidence": "COMPLETE",
                "nextads_selection_decision_id": (
                    decision.selection_decision_id
                ),
            }
        )


def _log_parent_selection(
    mlflow_module: Any,
    *,
    research_build: ModelResearchBuild,
    decision: ModelSelectionDecision,
    model_build: ModelBuild,
    test_evaluation: Mapping[str, Any],
    confidence_intervals: Mapping[str, Any],
    score_reproduction_checksum: str,
    selection_execution_code_sha: str,
) -> str:
    """Record the final reviewed choice on the existing parent run."""
    payload = _json_aggregate(
        {
            "selection_decision_id": decision.selection_decision_id,
            "selection_mode": decision.selection_mode,
            "recommended_candidate_id": decision.recommended_candidate_id,
            "selected_candidate_id": decision.selected_candidate_id,
            "selected_candidate_evaluation_id": (
                decision.selected_candidate_evaluation_id
            ),
            "reason": decision.reason,
            "reviewed_by": decision.reviewed_by,
            "research_code_sha": research_build.code_sha,
            "decision_code_sha": decision.decision_code_sha,
            "registration_code_sha": model_build.registration_code_sha,
            "selection_execution_code_sha": selection_execution_code_sha,
            "model_build_id": model_build.model_build_id,
            "model_uri": model_build.model_uri,
            "registered_model_version": model_build.registered_model_version,
            "artifact_digest": model_build.artifact_digest,
            "score_reproduction_checksum": score_reproduction_checksum,
            "test_metrics": test_evaluation.get("metrics") or {},
            "test_confidence_intervals": confidence_intervals,
        },
        "reviewed selection",
    )
    encoded = json.dumps(
        payload,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    with mlflow_module.start_run(run_id=research_build.mlflow_parent_run_id):
        mlflow_module.log_dict(payload, "research/reviewed_selection.json")
        mlflow_module.log_dict(
            {
                "artifact": "research/reviewed_selection.json",
                "sha256": digest,
            },
            "research/reviewed_selection_manifest.json",
        )
        mlflow_module.set_tags(
            {
                "nextads_model_research_status": READY,
                "nextads_reviewed_selection_sha256": digest,
                "nextads_research_code_sha": research_build.code_sha,
                "nextads_decision_code_sha": decision.decision_code_sha,
                "nextads_registration_code_sha": (
                    model_build.registration_code_sha
                ),
                "nextads_selection_execution_code_sha": (
                    selection_execution_code_sha
                ),
                "nextads_selection_decision_id": (
                    decision.selection_decision_id
                ),
                "nextads_selected_candidate_id": (
                    decision.selected_candidate_id
                ),
            }
        )
    return digest


def _validate_ready_build(
    build: ModelBuild,
    *,
    definition: ModelDefinition,
    receipt: TrainingSetReceipt,
    research_build: ModelResearchBuild,
    candidate: CandidateEvaluation,
    decision: ModelSelectionDecision,
    registered_model_name: str,
) -> None:
    expected = {
        "model_build_id": decision.model_build_id,
        "model_name": definition.model_name,
        "training_receipt_id": receipt.receipt_id,
        "research_build_id": research_build.research_build_id,
        "selection_decision_id": decision.selection_decision_id,
        "selected_candidate_id": candidate.candidate_id,
        "selected_candidate_evaluation_id": (
            candidate.candidate_evaluation_id
        ),
        "registered_model_name": registered_model_name,
        "status": READY,
    }
    changed = [
        name
        for name, value in expected.items()
        if getattr(build, name) != value
    ]
    if changed:
        raise ValueError(
            "READY model build does not match reviewed selection: "
            + ", ".join(changed)
        )
    if build.registration_code_sha is None:
        raise ValueError(
            "READY reviewed model build lacks registration code provenance"
        )


def _mlflow_dependencies(
    mlflow_module: Any | None,
    client: Any | None,
) -> tuple[Any, Any]:
    if mlflow_module is None:
        import mlflow as mlflow_module
    configure_mlflow(mlflow_module)
    if client is None:
        from mlflow.tracking import MlflowClient

        client = MlflowClient()
    return mlflow_module, client


def _effective_review_plan(model_name: str, checksum: str) -> ResearchPlan:
    """Resolve the exact reviewed plan even when the declaration defaults AUTO."""
    declared = load_model_research_plan(model_name)
    if declared is None:
        raise ValueError("Model definition has no research plan")
    reviewed = replace(declared, selection_policy=REVIEW_REQUIRED)
    if reviewed.checksum != checksum:
        raise ValueError(
            "Research build does not match the effective REVIEW_REQUIRED plan"
        )
    return reviewed


def _load_research_inputs(
    args: argparse.Namespace,
    *,
    spark: Any,
) -> tuple[
    ModelDefinition,
    ResearchPlan,
    ModelResearchBuild,
    TrainingSetReceipt,
    tuple[CandidateEvaluation, ...],
]:
    build = load_selectable_research_build(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        research_build_id=args.research_build_id,
    )
    if build is None:
        raise ValueError(
            "No AWAITING_SELECTION or READY research build exists for "
            f"{args.research_build_id}"
        )
    expected_model_name = getattr(args, "model_name", None)
    if expected_model_name and build.model_name != expected_model_name:
        raise ValueError(
            "Research build does not belong to the requested model"
        )
    definition = load_model_definition(build.model_name)
    plan = _effective_review_plan(
        build.model_name,
        build.research_plan_checksum,
    )
    receipt = load_ready_training_set_receipt(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        receipt_id=build.training_receipt_id,
    )
    if receipt is None:
        raise ValueError(
            f"No READY training receipt exists for {build.training_receipt_id}"
        )
    evaluations = _load_ready_candidate_evaluations(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        build=build,
    )
    _validate_research_inputs(
        definition,
        plan,
        build,
        receipt,
        evaluations,
    )
    create_model_development_tables(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
    )
    create_research_tables(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
    )
    return definition, plan, build, receipt, evaluations


def run_selection(
    args: argparse.Namespace,
    *,
    spark: Any,
    mlflow_module: Any | None = None,
    client: Any | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Run or reuse one exact reviewed selection and registered version."""
    mlflow_module, client = _mlflow_dependencies(mlflow_module, client)
    registered_model_name = normalize_registered_model_name(
        args.registered_model_name
    )
    selection_execution_code_sha = _required_text(args.code_sha)
    definition, plan, research_build, receipt, evaluations = (
        _load_research_inputs(args, spark=spark)
    )
    claim = claim_research_build(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        research_build_id=research_build.research_build_id,
        research_attempt_id=research_build.research_attempt_id,
        model_definition_checksum=research_build.model_definition_checksum,
        training_receipt_id=research_build.training_receipt_id,
        research_plan_checksum=research_build.research_plan_checksum,
        evaluation_schema_version=research_build.evaluation_schema_version,
        code_sha=research_build.code_sha,
        owner_invocation_id=args.orchestration_run_id,
        lease_seconds=_SELECTION_CLAIM_LEASE_SECONDS,
    )
    if claim.checkpoint == CLAIM_FAILED:
        raise ValueError(
            "Cannot select from a failed research claim: "
            f"{claim.failure_reason}"
        )
    if claim.checkpoint not in {
        CLAIM_CANDIDATES_READY,
        CLAIM_SELECTION_LOCKED,
        CLAIM_REGISTERED,
        CLAIM_COMPLETE,
    }:
        raise ValueError(
            "Research claim is not ready for reviewed selection: "
            f"{claim.checkpoint}"
        )
    expected_decision, selected = _new_reviewed_decision(
        definition=definition,
        plan=plan,
        build=research_build,
        receipt=receipt,
        evaluations=evaluations,
        candidate_id=args.candidate_id,
        reason=args.written_reason,
        reviewed_by=args.reviewed_by,
        registered_model_name=registered_model_name,
        decision_code_sha=selection_execution_code_sha,
        invocation_id=research_build.research_attempt_id,
        now=now or datetime.now(timezone.utc),
    )
    prior_decision = _load_ready_decision_for_attempt(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        build=research_build,
    )
    decision, reused_decision = _reuse_decision(
        expected_decision,
        prior_decision,
    )
    if claim.checkpoint == CLAIM_CANDIDATES_READY:
        persist_selection_decision(
            spark,
            catalog=args.model_catalog,
            schema=args.model_schema,
            decision=decision,
        )
        claim = advance_research_claim(
            spark,
            catalog=args.model_catalog,
            schema=args.model_schema,
            research_build_id=research_build.research_build_id,
            owner_invocation_id=args.orchestration_run_id,
            lease_token=claim.lease_token,
            expected_checkpoint=CLAIM_CANDIDATES_READY,
            checkpoint=CLAIM_SELECTION_LOCKED,
            selection_decision_id=decision.selection_decision_id,
            model_build_id=decision.model_build_id,
            lease_seconds=_SELECTION_CLAIM_LEASE_SECONDS,
        )
    elif (
        claim.selection_decision_id != decision.selection_decision_id
        or claim.model_build_id != decision.model_build_id
    ):
        raise ValueError(
            "Research attempt is already locked to a different selection"
        )

    test_frame = read_selected_test_frame(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        binding=_frame_binding(research_build),
        selection_decision_id=decision.selection_decision_id,
        selected_candidate_id=selected.candidate_id,
        selected_candidate_evaluation_id=selected.candidate_evaluation_id,
    )
    source_model = mlflow_module.spark.load_model(selected.model_uri)
    predictions, test_evaluation, confidence_intervals = (
        _evaluate_selected_test(source_model, test_frame, plan=plan)
    )
    _log_selected_test_evidence(
        mlflow_module,
        candidate=selected,
        decision=decision,
        evaluation=test_evaluation,
        confidence_intervals=confidence_intervals,
    )

    model_build = load_ready_model_build(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        model_build_id=decision.model_build_id,
    )
    reused_model = model_build is not None
    if model_build is None:
        selected_metrics = {
            **{
                f"validation_{name}": float(value)
                for name, value in selected.metrics
            },
            **{
                f"test_{name}": float(value)
                for name, value in test_evaluation["metrics"].items()
            },
        }
        model_build = register_selected_candidate(
            definition,
            receipt,
            research_build,
            selected,
            decision,
            registered_model_name=registered_model_name,
            selection_execution_code_sha=selection_execution_code_sha,
            selected_metrics=selected_metrics,
            mlflow_module=mlflow_module,
            client=client,
        )
    _validate_ready_build(
        model_build,
        definition=definition,
        receipt=receipt,
        research_build=research_build,
        candidate=selected,
        decision=decision,
        registered_model_name=registered_model_name,
    )
    validate_registered_model_build(client, model_build)
    registered_model = mlflow_module.spark.load_model(model_build.model_uri)
    score_checksum = validate_score_reproduction(
        source_model,
        registered_model,
        test_frame,
    )
    persist_model_build(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        build=model_build,
    )
    if claim.checkpoint == CLAIM_SELECTION_LOCKED:
        claim = advance_research_claim(
            spark,
            catalog=args.model_catalog,
            schema=args.model_schema,
            research_build_id=research_build.research_build_id,
            owner_invocation_id=args.orchestration_run_id,
            lease_token=claim.lease_token,
            expected_checkpoint=CLAIM_SELECTION_LOCKED,
            checkpoint=CLAIM_REGISTERED,
            selection_decision_id=decision.selection_decision_id,
            model_build_id=model_build.model_build_id,
            lease_seconds=_SELECTION_CLAIM_LEASE_SECONDS,
        )
    selection_artifact_digest = _log_parent_selection(
        mlflow_module,
        research_build=research_build,
        decision=decision,
        model_build=model_build,
        test_evaluation=test_evaluation,
        confidence_intervals=confidence_intervals,
        score_reproduction_checksum=score_checksum,
        selection_execution_code_sha=selection_execution_code_sha,
    )
    persist_selection_decision(
        spark,
        catalog=args.model_catalog,
        schema=args.model_schema,
        decision=decision,
    )
    if claim.checkpoint == CLAIM_REGISTERED:
        claim = advance_research_claim(
            spark,
            catalog=args.model_catalog,
            schema=args.model_schema,
            research_build_id=research_build.research_build_id,
            owner_invocation_id=args.orchestration_run_id,
            lease_token=claim.lease_token,
            expected_checkpoint=CLAIM_REGISTERED,
            checkpoint=CLAIM_COMPLETE,
            selection_decision_id=decision.selection_decision_id,
            model_build_id=model_build.model_build_id,
            lease_seconds=_SELECTION_CLAIM_LEASE_SECONDS,
        )
    return {
        "candidate_id": selected.candidate_id,
        "candidate_evaluation_id": selected.candidate_evaluation_id,
        "research_code_sha": research_build.code_sha,
        "decision_code_sha": decision.decision_code_sha,
        "registration_code_sha": model_build.registration_code_sha,
        "selection_execution_code_sha": selection_execution_code_sha,
        "model_build_id": model_build.model_build_id,
        "model_version": model_build.registered_model_version,
        "recommended_candidate_id": decision.recommended_candidate_id,
        "registered_model_name": model_build.registered_model_name,
        "research_attempt_id": research_build.research_attempt_id,
        "research_build_id": research_build.research_build_id,
        "reused": reused_decision and reused_model,
        "score_reproduction_checksum": score_checksum,
        "selection_attempt_id": decision.selection_attempt_id,
        "selection_decision_id": decision.selection_decision_id,
        "selection_mode": decision.selection_mode,
        "selection_artifact_digest": selection_artifact_digest,
        "status": READY,
        "test_metrics": {
            name: float(value)
            for name, value in sorted(test_evaluation["metrics"].items())
        },
    }


def main(argv: list[str] | None = None) -> None:
    """Run reviewed selection and emit bounded, PII-free evidence."""
    args = parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    evidence = run_selection(args, spark=_spark_session())
    LOGGER.info(
        "%s%s",
        EVIDENCE_PREFIX,
        json.dumps(
            evidence,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


if __name__ == "__main__":
    main()

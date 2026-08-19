"""Run declared model research as immutable parent and candidate attempts."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Mapping

from next_ads.model_development.contracts import ModelBuild, ModelDefinition
from next_ads.model_development.research_contracts import (
    AUTO,
    AWAITING_SELECTION,
    FAILED,
    READY,
    CandidateEvaluation,
    ModelResearchBuild,
    ModelSelectionDecision,
    ResearchPlan,
)
from next_ads.model_development.research_data import (
    ResearchFramePlan,
    declared_research_schemas,
    pack_research_frame,
)
from next_ads.model_development.research_evaluation import (
    COMPLETE,
    EvaluationConfig,
    FeatureCoverageSpec,
    SliceEvaluationSpec,
    deterministic_selected_test_confidence_intervals,
    evaluate_binary_predictions,
    profile_feature_coverage,
    require_complete_binary_evaluation,
    require_complete_confidence_intervals,
)
from next_ads.model_development.research_evidence import (
    FAILED as EVIDENCE_FAILED,
    NOT_APPLICABLE,
    build_artifact_manifest,
    log_evidence_bundle,
    validate_optional_evidence_result,
    write_candidate_comparison_evidence,
    write_candidate_evidence,
)
from next_ads.model_development.research_failures import safe_failure_reason
from next_ads.model_development.research_explainability import (
    FeatureNameMapping,
    produce_global_explanation,
)
from next_ads.model_development.research_plugins import (
    resolve_candidate_plugin,
    resolve_evidence_producer,
)
from next_ads.model_development.research_claims import (
    CANDIDATES_READY as CLAIM_CANDIDATES_READY,
    CLAIMED as CLAIM_CLAIMED,
    COMPLETE as CLAIM_COMPLETE,
    FAILED as CLAIM_FAILED,
    FRAME_READY as CLAIM_FRAME_READY,
    PARENT_READY as CLAIM_PARENT_READY,
    REGISTERED as CLAIM_REGISTERED,
    SELECTION_LOCKED as CLAIM_SELECTION_LOCKED,
    advance_research_claim,
    claim_research_build,
    fail_research_claim,
    release_research_claim,
)
from next_ads.model_development.research_selection import (
    normalize_registered_model_name,
    recommend_candidate,
    register_selected_candidate,
    selected_model_build_id,
    validate_score_reproduction,
)
from next_ads.model_development.research_scoring import (
    validate_persisted_prediction_equivalence,
    validate_prediction_adapter_output,
)
from next_ads.model_development.promotion import (
    validate_registered_model_build,
)
from next_ads.model_development.research_store import (
    ResearchFrameBinding,
    attempt_id,
    candidate_evaluation_id,
    load_ready_selection_decision,
    load_ready_selection_for_research_attempt,
    load_selectable_research_build,
    load_terminal_research_build_for_attempt,
    load_terminal_candidate_evaluation,
    persist_candidate_evaluation,
    persist_research_build,
    persist_research_frame,
    persist_selection_decision,
    read_selected_test_frame,
    read_research_frame,
    read_training_frame,
    read_validation_frame,
    research_build_id,
    research_frame_id,
    selection_decision_id,
)
from next_ads.model_development.spark_research import (
    BUILTIN_CANDIDATES,
    bounded_xgboost_contribution_frame,
    log_research_model_with_signature,
    readable_feature_mapping,
)
from next_ads.model_development.spark_training import artifact_directory_digest
from next_ads.model_development.store import (
    load_ready_model_build,
    persist_model_build,
)
from next_ads.model_development.training_sets import (
    TrainingSetBuildResult,
    feature_default_audit_column,
    feature_missing_audit_column,
    summarise_binary_labels,
)


AUTOMATIC_SELECTION_REASON = (
    "Highest validation PR-AUC, then lowest validation log loss, then "
    "candidate ID."
)
_RESEARCH_BUILD_TAG = "nextads_research_build_id"
_RESEARCH_ATTEMPT_TAG = "nextads_research_attempt_id"
_RUN_ROLE_TAG = "nextads_research_run_role"
_CANDIDATE_ID_TAG = "nextads_candidate_id"
_CANDIDATE_EVALUATION_TAG = "nextads_candidate_evaluation_id"
_CANDIDATE_ATTEMPT_TAG = "nextads_candidate_attempt_id"
_CANDIDATE_STATUS_TAG = "nextads_candidate_status"
_CANDIDATE_MODEL_PATH_TAG = "nextads_candidate_model_artifact_path"
_CANDIDATE_MANIFEST_TAG = "nextads_candidate_artifact_manifest_digest"
_CANDIDATE_EXPLANATION_TAG = "nextads_candidate_explanation_status"
_CANDIDATE_FAILURE_TAG = "nextads_candidate_failure_reason"
_CLAIM_LEASE_SECONDS = 23_400


@dataclass(frozen=True)
class ResearchRunResult:
    """Terminal evidence returned to the Databricks research entrypoint."""

    research_build: ModelResearchBuild
    candidate_evaluations: tuple[CandidateEvaluation, ...]
    recommended_candidate_id: str
    selection_decision: ModelSelectionDecision | None = None
    model_build: ModelBuild | None = None
    reused: bool = False


def _mlflow_filter_value(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


def _claim_owner_id(invocation_id: str) -> str:
    """Use the stable job-run identity across Databricks task retries."""
    value = str(invocation_id).strip()
    owner = value.partition(":")[0]
    if not owner:
        raise ValueError("invocation_id must not be empty")
    return owner


def _find_exact_tagged_run(
    client: Any,
    *,
    experiment_id: str,
    tags: Mapping[str, str],
) -> Any | None:
    """Find at most one run whose identity tags were set at creation."""
    clauses = [
        f"tags.`{name}` = '{_mlflow_filter_value(value)}'"
        for name, value in sorted(tags.items())
    ]
    runs = list(
        client.search_runs(
            experiment_ids=[str(experiment_id)],
            filter_string=" AND ".join(clauses),
            max_results=2,
        )
    )
    exact = [
        run
        for run in runs
        if all(
            str(run.data.tags.get(name)) == value
            for name, value in tags.items()
        )
    ]
    if len(exact) > 1:
        raise ValueError(
            "More than one MLflow run has the same research identity"
        )
    return exact[0] if exact else None


def _find_or_create_tagged_run(
    client: Any,
    *,
    experiment_id: str,
    run_name: str,
    tags: Mapping[str, str],
) -> Any:
    """Recover a tagged run or atomically tag a newly created one."""
    existing = _find_exact_tagged_run(
        client,
        experiment_id=experiment_id,
        tags=tags,
    )
    if existing is not None:
        return existing
    return client.create_run(
        str(experiment_id),
        tags={"mlflow.runName": run_name, **dict(tags)},
    )


def _run_timestamp(milliseconds: Any, fallback: datetime) -> datetime:
    if milliseconds is None:
        return fallback
    return datetime.fromtimestamp(
        float(milliseconds) / 1000.0, tz=timezone.utc
    )


def _tag_candidate_terminal(
    client: Any,
    *,
    run_id: str,
    evaluation: CandidateEvaluation,
    model_artifact_path: str | None = None,
) -> None:
    values = {
        _CANDIDATE_EVALUATION_TAG: evaluation.candidate_evaluation_id,
        _CANDIDATE_ATTEMPT_TAG: evaluation.candidate_attempt_id,
    }
    if model_artifact_path is not None:
        values[_CANDIDATE_MODEL_PATH_TAG] = model_artifact_path
    if evaluation.artifact_manifest_digest is not None:
        values[_CANDIDATE_MANIFEST_TAG] = evaluation.artifact_manifest_digest
    if evaluation.explanation_status is not None:
        values[_CANDIDATE_EXPLANATION_TAG] = evaluation.explanation_status
    if evaluation.failure_reason is not None:
        values[_CANDIDATE_FAILURE_TAG] = evaluation.failure_reason
    for key, value in values.items():
        client.set_tag(run_id, key, str(value))
    client.set_tag(run_id, _CANDIDATE_STATUS_TAG, evaluation.status)


def _recover_tagged_candidate(
    run: Any,
    *,
    candidate: Any,
    logical_candidate_id: str,
    candidate_attempt: str,
    logical_build_id: str,
    research_attempt: str,
) -> CandidateEvaluation | None:
    """Recover a terminal receipt after MLflow completed before Delta."""
    tags = run.data.tags
    status = tags.get(_CANDIDATE_STATUS_TAG)
    if status not in {READY, FAILED}:
        return None
    expected_tags = {
        _RESEARCH_BUILD_TAG: logical_build_id,
        _RESEARCH_ATTEMPT_TAG: research_attempt,
        _CANDIDATE_ID_TAG: candidate.candidate_id,
        _CANDIDATE_EVALUATION_TAG: logical_candidate_id,
        _CANDIDATE_ATTEMPT_TAG: candidate_attempt,
    }
    changed = [
        key for key, value in expected_tags.items() if tags.get(key) != value
    ]
    if changed:
        raise ValueError(
            "Tagged candidate run has inconsistent identity: "
            + ", ".join(changed)
        )
    now = datetime.now(timezone.utc)
    created_at = _run_timestamp(run.info.start_time, now)
    completed_at = _run_timestamp(run.info.end_time, now)
    common = {
        "candidate_evaluation_id": logical_candidate_id,
        "candidate_attempt_id": candidate_attempt,
        "research_build_id": logical_build_id,
        "research_attempt_id": research_attempt,
        "candidate_id": candidate.candidate_id,
        "candidate_spec_checksum": candidate.checksum,
        "required": not candidate.failure_allowed,
        "status": status,
        "created_at": created_at,
        "completed_at": completed_at,
        "mlflow_run_id": run.info.run_id,
    }
    if status == FAILED:
        return CandidateEvaluation(
            **common,
            failure_reason=(
                tags.get(_CANDIDATE_FAILURE_TAG)
                or "Candidate run failed before its Delta receipt was written"
            ),
        )
    model_path = tags.get(_CANDIDATE_MODEL_PATH_TAG)
    manifest = tags.get(_CANDIDATE_MANIFEST_TAG)
    explanation = tags.get(_CANDIDATE_EXPLANATION_TAG)
    if not model_path or not manifest or explanation != READY:
        raise ValueError(
            "READY candidate run is missing terminal artifact tags"
        )
    metrics = tuple(
        sorted(
            (
                name.removeprefix("validation_"),
                float(value),
            )
            for name, value in run.data.metrics.items()
            if name.startswith("validation_")
        )
    )
    return CandidateEvaluation(
        **common,
        model_uri=f"runs:/{run.info.run_id}/{model_path}",
        metrics=metrics,
        artifact_manifest_digest=manifest,
        explanation_status=explanation,
    )


def _validate_recovered_candidate(
    stored: CandidateEvaluation,
    recovered: CandidateEvaluation,
) -> None:
    stable_fields = (
        "candidate_evaluation_id",
        "candidate_attempt_id",
        "research_build_id",
        "research_attempt_id",
        "candidate_id",
        "candidate_spec_checksum",
        "required",
        "status",
        "mlflow_run_id",
        "model_uri",
        "metrics",
        "artifact_manifest_digest",
        "explanation_status",
        "failure_reason",
    )
    changed = [
        field
        for field in stable_fields
        if getattr(stored, field) != getattr(recovered, field)
    ]
    if changed:
        raise ValueError(
            "Candidate Delta receipt disagrees with its MLflow run: "
            + ", ".join(changed)
        )


def _date_range(start: date, end: date) -> tuple[date, ...]:
    return tuple(
        start + timedelta(days=offset)
        for offset in range((end - start).days + 1)
    )


def plan_observation_dates(plan: ResearchPlan) -> tuple[date, ...]:
    """Return every exact date in the declared temporal research split."""
    split = plan.temporal_split
    return (
        *_date_range(split.train_start, split.train_end),
        *_date_range(split.validate_start, split.validate_end),
        *_date_range(split.test_start, split.test_end),
    )


def _frame_binding(build: ModelResearchBuild) -> ResearchFrameBinding:
    fields = (
        "research_frame_id",
        "research_frame_attempt_id",
        "research_build_id",
        "research_attempt_id",
        "training_receipt_id",
        "research_frame_table",
        "research_frame_delta_version",
        "research_frame_row_count",
        "research_frame_schema_checksum",
        "research_frame_data_checksum",
        "research_frame_write_receipt_id",
        "research_frame_feature_schema_json",
        "research_frame_slice_schema_json",
    )
    return ResearchFrameBinding(
        **{field: getattr(build, field) for field in fields}
    )


def _reporting_slice_specs(
    plan: ResearchPlan,
    frame: Any,
) -> tuple[SliceEvaluationSpec, ...]:
    specs = []
    for slice_spec in plan.slices:
        if slice_spec.column in frame.columns:
            specs.append(
                SliceEvaluationSpec(
                    slice_id=slice_spec.slice_id,
                    column=slice_spec.column,
                    values=slice_spec.values,
                    minimum_rows=slice_spec.minimum_rows,
                )
            )
        elif not slice_spec.if_present:
            raise ValueError(
                f"Required research slice is missing: {slice_spec.column}"
            )
    return tuple(specs)


def _audit_columns(definition: ModelDefinition, frame: Any) -> tuple[str, ...]:
    columns = []
    for feature in definition.model_feature_columns:
        for column in (
            feature_missing_audit_column(feature),
            feature_default_audit_column(feature),
        ):
            if column in frame.columns:
                columns.append(column)
    return tuple(columns)


def prepare_research_frame(
    training_frame: Any,
    *,
    definition: ModelDefinition,
    plan: ResearchPlan,
    logical_research_build_id: str,
    research_attempt_id: str,
    logical_research_frame_id: str,
    research_frame_attempt_id: str,
    training_receipt_id: str,
) -> tuple[Any, Any, ResearchFramePlan]:
    """Create and pack exact train/validate/test data without retaining keys."""
    from pyspark.sql import functions as F

    timestamp = definition.training_observation.observation_timestamp
    observation_date_column = (
        definition.training_observation.observation_date_column
    )
    required = {
        timestamp,
        observation_date_column,
        definition.label,
        *definition.observation_keys,
        *definition.model_feature_columns,
    }
    missing = sorted(required.difference(training_frame.columns))
    if missing:
        raise ValueError(
            "Research source is missing declared columns: "
            + ", ".join(missing)
        )
    prepared = training_frame.withColumn(
        "observation_date", F.to_date(F.col(observation_date_column))
    )
    expected_dates = {
        value.isoformat() for value in plan_observation_dates(plan)
    }
    observed_dates = {
        row["observation_date"].isoformat()
        for row in prepared.select("observation_date").distinct().collect()
        if row["observation_date"] is not None
    }
    if observed_dates != expected_dates:
        missing_dates = sorted(expected_dates.difference(observed_dates))
        unexpected_dates = sorted(observed_dates.difference(expected_dates))
        raise ValueError(
            "Research frame does not cover every declared observation date: "
            f"missing={missing_dates}, unexpected={unexpected_dates}"
        )
    reporting_slices = tuple(
        spec.column for spec in _reporting_slice_specs(plan, prepared)
    )
    audits = _audit_columns(definition, prepared)
    split = plan.temporal_split
    frame_plan = ResearchFramePlan(
        observation_date_column="observation_date",
        label_column=definition.label,
        raw_key_columns=definition.observation_keys,
        feature_columns=definition.model_feature_columns,
        slice_columns=tuple(dict.fromkeys((*reporting_slices, *audits))),
        train_dates=_date_range(split.train_start, split.train_end),
        validation_dates=_date_range(split.validate_start, split.validate_end),
        test_dates=_date_range(split.test_start, split.test_end),
    )
    packed = pack_research_frame(
        prepared,
        plan=frame_plan,
        research_frame_id=logical_research_frame_id,
        research_frame_attempt_id=research_frame_attempt_id,
        research_build_id=logical_research_build_id,
        research_attempt_id=research_attempt_id,
        training_receipt_id=training_receipt_id,
    )
    return (
        packed,
        declared_research_schemas(prepared, plan=frame_plan),
        frame_plan,
    )


def _candidate_metrics(
    evaluation: Mapping[str, Any],
) -> tuple[tuple[str, float], ...]:
    metrics = evaluation.get("metrics") or {}
    return tuple(
        sorted((name, float(value)) for name, value in metrics.items())
    )


def _candidate_comparison_row(
    evaluation: CandidateEvaluation,
) -> dict[str, Any]:
    return {
        "candidate_id": evaluation.candidate_id,
        "status": evaluation.status,
        "metrics": dict(evaluation.metrics),
        "selectable": evaluation.status == READY,
        "mlflow_run_id": evaluation.mlflow_run_id,
        "artifact_manifest_digest": evaluation.artifact_manifest_digest,
        "failure_reason": evaluation.failure_reason,
    }


def _coverage_specs(
    definition: ModelDefinition,
    frame: Any,
) -> tuple[FeatureCoverageSpec, ...]:
    defaults: dict[str, Any] = {}
    for lookup in definition.feature_lookups:
        renames = dict(lookup.renames)
        for source, value in lookup.defaults:
            defaults[renames.get(source, source)] = value
    specs = []
    for feature in definition.model_feature_columns:
        missing_indicator = feature_missing_audit_column(feature)
        default_indicator = feature_default_audit_column(feature)
        specs.append(
            FeatureCoverageSpec(
                column=feature,
                display_name=feature,
                default_values=(defaults[feature],)
                if feature in defaults
                else (),
                missing_indicator_column=(
                    missing_indicator
                    if missing_indicator in frame.columns
                    else None
                ),
                default_indicator_column=(
                    default_indicator
                    if default_indicator in frame.columns
                    else None
                ),
            )
        )
    return tuple(specs)


def _evaluation_config(plan: ResearchPlan) -> EvaluationConfig:
    percentages = tuple(
        int(round(fraction * 100))
        for fraction in plan.evaluation_rules.top_fractions
    )
    return EvaluationConfig(
        min_rows=plan.evaluation_rules.minimum_slice_rows,
        top_percentages=percentages,
    )


def _permutation_evaluator(
    plugin: Any,
    definition: ModelDefinition,
    candidate: Any,
    fitted_model: Any,
    validation_frame: Any,
    *,
    config: EvaluationConfig,
) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    def evaluate(feature: str, seed: int) -> float:
        base = validation_frame.withColumn(
            "__permutation_row",
            F.row_number().over(Window.orderBy(F.col("row_id").asc())),
        )
        shuffled = validation_frame.select("row_id", feature).withColumn(
            "__permutation_row",
            F.row_number().over(
                Window.orderBy(
                    F.sha2(
                        F.concat_ws(
                            "|",
                            F.col("row_id"),
                            F.lit(str(seed)),
                        ),
                        256,
                    )
                )
            ),
        )
        replacement = shuffled.select(
            "__permutation_row",
            F.col(feature).alias("__permuted_value"),
        )
        permuted = (
            base.drop(feature)
            .join(replacement, on="__permutation_row", how="inner")
            .withColumnRenamed("__permuted_value", feature)
            .drop("__permutation_row")
        )
        predictions = plugin.prediction_adapter.predict(
            definition,
            candidate,
            fitted_model,
            permuted,
        )
        validate_prediction_adapter_output(
            permuted,
            predictions,
            label_column=definition.label,
            context=f"{candidate.candidate_id} permutation prediction",
        )
        evidence = evaluate_binary_predictions(
            predictions,
            label_column=definition.label,
            row_id_hash_column="row_id",
            config=config,
        )
        return float(evidence["metrics"]["auc_pr"])

    return evaluate


def _global_explanation(
    plugin: Any,
    definition: ModelDefinition,
    candidate: Any,
    fitted_model: Any,
    training_frame: Any,
    validation_frame: Any,
    validation_evaluation: Mapping[str, Any],
    *,
    config: EvaluationConfig,
) -> Any:
    mapping = _candidate_feature_mapping(
        candidate.plugin,
        fitted_model,
        training_frame,
        definition,
    )
    contribution_frame = None
    if candidate.plugin == "spark_xgboost":
        contribution_frame = bounded_xgboost_contribution_frame(
            fitted_model,
            validation_frame,
            row_id_column="row_id",
        )
    baseline = float(validation_evaluation["metrics"]["auc_pr"])
    evaluator = None
    if candidate.plugin not in {
        "spark_logistic_regression",
        "spark_random_forest",
        "spark_gradient_boosted_trees",
        "spark_xgboost",
    }:
        evaluator = _permutation_evaluator(
            plugin,
            definition,
            candidate,
            fitted_model,
            validation_frame,
            config=config,
        )
    return produce_global_explanation(
        candidate.plugin,
        fitted_model,
        mapping,
        contribution_frame=contribution_frame,
        permutation_baseline_metric=baseline,
        permutation_evaluator=evaluator,
        seed=candidate.seed,
    )


def _candidate_feature_mapping(
    candidate_plugin: str,
    fitted_model: Any,
    training_frame: Any,
    definition: ModelDefinition,
) -> tuple[FeatureNameMapping, ...]:
    """Resolve readable names without imposing Spark vectors on plug-ins."""
    if candidate_plugin in BUILTIN_CANDIDATES:
        return readable_feature_mapping(
            fitted_model,
            training_frame,
            definition,
        )
    return tuple(
        FeatureNameMapping(vector_index=index, source_column=column)
        for index, column in enumerate(definition.model_feature_columns)
    )


def _optional_evidence(
    plan: ResearchPlan,
    definition: ModelDefinition,
    candidate: Any,
    fitted_model: Any,
    aggregate_evidence: Mapping[str, Any],
    feature_names: tuple[str, ...],
) -> dict[str, Mapping[str, Any]]:
    evidence = {}
    for identifier in plan.evidence_producers:
        producer = resolve_evidence_producer(identifier)
        try:
            result = producer.produce(
                definition,
                candidate,
                fitted_model,
                aggregate_evidence,
                feature_names,
            )
            declared_status = result.get("status")
            if declared_status in {COMPLETE, EVIDENCE_FAILED, NOT_APPLICABLE}:
                outcome = dict(result)
                if declared_status == COMPLETE and "evidence" not in outcome:
                    raise ValueError(
                        "Completed optional evidence must include evidence"
                    )
                if declared_status in {
                    EVIDENCE_FAILED,
                    NOT_APPLICABLE,
                } and not (str(outcome.get("reason", "")).strip()):
                    raise ValueError(
                        f"{declared_status} optional evidence needs a reason"
                    )
                if declared_status == EVIDENCE_FAILED:
                    outcome["reason"] = safe_failure_reason(
                        str(outcome["reason"]),
                        stage=f"optional_evidence_{identifier}",
                    )
                evidence[identifier] = outcome
            else:
                evidence[identifier] = {
                    "status": COMPLETE,
                    "evidence": result,
                }
            validate_optional_evidence_result(
                evidence[identifier],
                identifier=identifier,
            )
        except (
            Exception
        ) as exc:  # Optional evidence cannot mask standard output.
            evidence[identifier] = {
                "status": EVIDENCE_FAILED,
                "reason": safe_failure_reason(
                    exc,
                    stage=f"optional_evidence_{identifier}",
                ),
            }
    return evidence


def _model_artifact_manifest_digest(
    client: Any,
    *,
    run_id: str,
    artifact_path: str,
    evidence_digest: str,
) -> tuple[str, str]:
    model_path = client.download_artifacts(run_id, artifact_path)
    model_digest = artifact_directory_digest(model_path)
    payload = {
        "evidence_manifest_sha256": evidence_digest,
        "model_artifact_sha256": model_digest,
    }
    combined = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    return combined, model_digest


def _safe_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, default=str, sort_keys=True, indent=2),
        encoding="utf-8",
    )


def _manifest_parent(root: Path) -> str:
    manifest = build_artifact_manifest(root)
    path = root / "artifact_manifest.json"
    _safe_json(
        path,
        {"artifacts": [artifact.__dict__ for artifact in manifest]},
    )
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _split_profile(
    frame: Any,
    label_column: str,
    *,
    withheld_splits: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    rows = (
        frame.groupBy("split")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(
                F.when(
                    ~F.col("split").isin(*withheld_splits),
                    F.col(label_column).cast("long"),
                )
            ).alias("positives"),
            F.min("observation_date").alias("start_date"),
            F.max("observation_date").alias("end_date"),
        )
        .orderBy("split")
        .collect()
    )
    profile = []
    for row in rows:
        values = row.asDict(recursive=True)
        if values["split"] in withheld_splits:
            values.pop("positives", None)
            values["label_status"] = "WITHHELD_UNTIL_SELECTION"
        else:
            values["label_status"] = "AVAILABLE_FOR_RESEARCH"
        profile.append(values)
    return profile


def _load_attempt_candidates(
    spark: Any,
    *,
    catalog: str,
    schema: str,
    build: ModelResearchBuild,
    plan: ResearchPlan,
) -> tuple[CandidateEvaluation, ...]:
    evaluations = []
    for candidate in plan.candidates:
        logical_id = candidate_evaluation_id(
            research_build_id=build.research_build_id,
            candidate_id=candidate.candidate_id,
            candidate_spec_checksum=candidate.checksum,
        )
        evaluation = load_terminal_candidate_evaluation(
            spark,
            catalog=catalog,
            schema=schema,
            candidate_evaluation_id=logical_id,
            research_attempt_id=build.research_attempt_id,
        )
        if evaluation is not None:
            evaluations.append(evaluation)
    if len(evaluations) != build.candidate_count:
        raise ValueError(
            "Completed research build is missing terminal candidate attempts"
        )
    successful = sum(item.status == READY for item in evaluations)
    if successful != build.successful_candidate_count:
        raise ValueError(
            "Completed research build has inconsistent successful candidate count"
        )
    return tuple(evaluations)


def _prevalence_baseline(
    training_frame: Any,
    validation_frame: Any,
    *,
    label_column: str,
    slice_specs: tuple[SliceEvaluationSpec, ...],
    config: EvaluationConfig,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    row = training_frame.agg(
        F.avg(F.col(label_column).cast("double")).alias("prevalence")
    ).first()
    if row is None or row["prevalence"] is None:
        raise ValueError("Prevalence baseline training frame is empty")
    prevalence = float(row["prevalence"])
    predictions = validation_frame.withColumn(
        "score", F.lit(prevalence).cast("double")
    ).withColumn("prediction", (F.col("score") >= F.lit(0.5)).cast("double"))
    return evaluate_binary_predictions(
        predictions,
        label_column=label_column,
        row_id_hash_column="row_id",
        slice_specs=slice_specs,
        config=config,
    )


def _validate_reuse(
    definition: ModelDefinition,
    plan: ResearchPlan,
    training: TrainingSetBuildResult,
    build: ModelResearchBuild,
) -> None:
    expected = {
        "model_name": definition.model_name,
        "training_receipt_id": training.receipt.receipt_id,
        "model_definition_checksum": definition.checksum,
        "research_plan_checksum": plan.checksum,
        "evaluation_schema_version": plan.evaluation_schema_version,
    }
    if any(getattr(build, key) != value for key, value in expected.items()):
        raise ValueError("Stored research build belongs to different inputs")


def _finalize_parent_run(
    mlflow_client: Any,
    build: ModelResearchBuild,
    *,
    status: str,
    decision: ModelSelectionDecision | None = None,
) -> None:
    """Idempotently reconcile MLflow with immutable research state."""
    if not build.mlflow_parent_run_id:
        raise ValueError("Completed research build is missing its parent run")
    mlflow_client.set_tag(
        build.mlflow_parent_run_id,
        "nextads_model_research_status",
        status,
    )
    if decision is not None:
        mlflow_client.set_tag(
            build.mlflow_parent_run_id,
            "nextads_selection_decision_id",
            decision.selection_decision_id,
        )
        mlflow_client.set_tag(
            build.mlflow_parent_run_id,
            "nextads_selected_candidate_id",
            decision.selected_candidate_id,
        )
    mlflow_client.set_terminated(
        build.mlflow_parent_run_id,
        status="FINISHED",
    )


def _reused_research_result(
    spark: Any,
    *,
    definition: ModelDefinition,
    plan: ResearchPlan,
    build: ModelResearchBuild,
    catalog: str,
    schema: str,
    registered_model_name: str,
    mlflow_client: Any,
) -> ResearchRunResult:
    """Reload every terminal object for an identical completed retry."""
    registered_model_name = normalize_registered_model_name(
        registered_model_name
    )
    evaluations = _load_attempt_candidates(
        spark,
        catalog=catalog,
        schema=schema,
        build=build,
        plan=plan,
    )
    recommendation = recommend_candidate(plan, evaluations)
    if build.automatic_candidate_id != recommendation.candidate_id:
        raise ValueError("Stored automatic recommendation no longer matches")
    if plan.selection_policy != AUTO:
        if build.status != AWAITING_SELECTION:
            raise ValueError(
                "Reviewed research retry requires AWAITING_SELECTION status"
            )
        decision = load_ready_selection_for_research_attempt(
            spark,
            catalog=catalog,
            schema=schema,
            research_build_id=build.research_build_id,
            research_attempt_id=build.research_attempt_id,
        )
        if decision is None:
            _finalize_parent_run(
                mlflow_client,
                build,
                status=AWAITING_SELECTION,
            )
            return ResearchRunResult(
                research_build=build,
                candidate_evaluations=evaluations,
                recommended_candidate_id=recommendation.candidate_id,
                reused=True,
            )
        if (
            decision.recommended_candidate_id != recommendation.candidate_id
            or not decision.model_build_id
            or decision.registered_model_name != registered_model_name
        ):
            raise ValueError(
                "Reviewed research selection has inconsistent lineage"
            )
        model_build = load_ready_model_build(
            spark,
            catalog=catalog,
            schema=schema,
            model_build_id=decision.model_build_id,
        )
        if model_build is None:
            raise ValueError("Reviewed selection is missing its model build")
        expected_reviewed_model = {
            "research_build_id": build.research_build_id,
            "selection_decision_id": decision.selection_decision_id,
            "selected_candidate_id": decision.selected_candidate_id,
            "selected_candidate_evaluation_id": (
                decision.selected_candidate_evaluation_id
            ),
            "registered_model_name": registered_model_name,
            "status": READY,
        }
        if any(
            getattr(model_build, field) != value
            for field, value in expected_reviewed_model.items()
        ):
            raise ValueError(
                "Reviewed selected model has inconsistent lineage"
            )
        validate_registered_model_build(mlflow_client, model_build)
        if model_build.registration_code_sha is None:
            raise ValueError(
                "Reviewed selected model lacks registration code provenance"
            )
        return ResearchRunResult(
            research_build=build,
            candidate_evaluations=evaluations,
            recommended_candidate_id=recommendation.candidate_id,
            selection_decision=decision,
            model_build=model_build,
            reused=True,
        )
    if build.status != READY:
        raise ValueError("Automatic research retry requires a READY build")
    logical_decision_id = selection_decision_id(
        research_build_id=build.research_build_id,
        selection_mode=AUTO,
        recommended_candidate_id=recommendation.candidate_id,
        selected_candidate_id=recommendation.candidate_id,
        reason=AUTOMATIC_SELECTION_REASON,
    )
    decision = load_ready_selection_decision(
        spark,
        catalog=catalog,
        schema=schema,
        selection_decision_id=logical_decision_id,
    )
    if decision is None or not decision.model_build_id:
        raise ValueError("READY automatic research is missing its selection")
    expected_decision = {
        "research_build_id": build.research_build_id,
        "research_attempt_id": build.research_attempt_id,
        "recommended_candidate_id": recommendation.candidate_id,
        "selected_candidate_id": recommendation.candidate_id,
        "selected_candidate_evaluation_id": (
            recommendation.candidate_evaluation_id
        ),
        "registered_model_name": registered_model_name,
    }
    if any(
        getattr(decision, field) != value
        for field, value in expected_decision.items()
    ):
        raise ValueError("READY automatic selection has inconsistent lineage")
    model_build = load_ready_model_build(
        spark,
        catalog=catalog,
        schema=schema,
        model_build_id=decision.model_build_id,
    )
    if model_build is None:
        raise ValueError("READY automatic research is missing its model build")
    expected_model = {
        "research_build_id": build.research_build_id,
        "selection_decision_id": decision.selection_decision_id,
        "selected_candidate_id": recommendation.candidate_id,
        "selected_candidate_evaluation_id": (
            recommendation.candidate_evaluation_id
        ),
        "registered_model_name": registered_model_name,
        "status": READY,
    }
    if any(
        getattr(model_build, field) != value
        for field, value in expected_model.items()
    ):
        raise ValueError("READY automatic model has inconsistent lineage")
    validate_registered_model_build(mlflow_client, model_build)
    if model_build.registration_code_sha is None:
        raise ValueError(
            "READY automatic model lacks registration code provenance"
        )
    _finalize_parent_run(
        mlflow_client,
        build,
        status=READY,
        decision=decision,
    )
    return ResearchRunResult(
        research_build=build,
        candidate_evaluations=evaluations,
        recommended_candidate_id=recommendation.candidate_id,
        selection_decision=decision,
        model_build=model_build,
        reused=True,
    )


def _selected_test_evidence(
    mlflow_module: Any,
    definition: ModelDefinition,
    plan: ResearchPlan,
    candidate_spec: Any,
    candidate_evaluation: CandidateEvaluation,
    decision: ModelSelectionDecision,
    frame_binding: ResearchFrameBinding,
    spark: Any,
    *,
    catalog: str,
    schema: str,
    output_directory: Path,
) -> tuple[dict[str, Any], dict[str, Any], Any, Any, str]:
    plugin = resolve_candidate_plugin(candidate_spec)
    test_frame = read_selected_test_frame(
        spark,
        catalog=catalog,
        schema=schema,
        binding=frame_binding,
        selection_decision_id=decision.selection_decision_id,
        selected_candidate_id=candidate_evaluation.candidate_id,
        selected_candidate_evaluation_id=(
            candidate_evaluation.candidate_evaluation_id
        ),
    ).withColumnRenamed("label", definition.label)
    summarise_binary_labels(test_frame, definition.label)
    source_model = mlflow_module.spark.load_model(
        candidate_evaluation.model_uri
    )
    predictions = plugin.prediction_adapter.predict(
        definition,
        candidate_spec,
        source_model,
        test_frame,
    )
    slice_specs = _reporting_slice_specs(plan, test_frame)
    validate_prediction_adapter_output(
        test_frame,
        predictions,
        label_column=definition.label,
        slice_columns=tuple(spec.column for spec in slice_specs),
        context=f"{candidate_evaluation.candidate_id} selected-test prediction",
    )
    config = _evaluation_config(plan)
    evaluation = evaluate_binary_predictions(
        predictions,
        label_column=definition.label,
        row_id_hash_column="row_id",
        slice_specs=slice_specs,
        config=config,
    )
    require_complete_binary_evaluation(
        evaluation,
        required_metrics=plan.evaluation_rules.required_metrics,
        context="Selected test",
    )
    intervals = deterministic_selected_test_confidence_intervals(
        predictions,
        label_column=definition.label,
        score_column="score",
        split_column="split",
        row_id_hash_column="row_id",
        iterations=plan.evaluation_rules.confidence_interval_resamples,
        confidence=plan.evaluation_rules.confidence_level,
        seed=plan.evaluation_rules.confidence_interval_seed,
    )
    require_complete_confidence_intervals(
        intervals,
        context="Selected test",
    )
    coverage = profile_feature_coverage(
        test_frame,
        _coverage_specs(definition, test_frame),
    )
    explanation = _global_explanation(
        plugin,
        definition,
        candidate_spec,
        source_model,
        test_frame,
        test_frame,
        evaluation,
        config=config,
    )
    bundle = write_candidate_evidence(
        output_directory,
        candidate_id=candidate_spec.candidate_id,
        evaluation=evaluation,
        feature_coverage=coverage,
        explanation=explanation,
        confidence_intervals=intervals,
    )
    bundle.require_selectable()
    with mlflow_module.start_run(run_id=candidate_evaluation.mlflow_run_id):
        log_evidence_bundle(
            mlflow_module,
            bundle,
            artifact_path="selected_test_evidence",
            parameter_prefix="selected_test_",
        )
        mlflow_module.log_metrics(
            {
                f"test_{name}": float(value)
                for name, value in evaluation["metrics"].items()
            }
        )
    return (
        evaluation,
        intervals,
        source_model,
        test_frame,
        bundle.manifest_sha256,
    )


def _run_model_research_impl(
    spark: Any,
    *,
    definition: ModelDefinition,
    plan: ResearchPlan,
    training: TrainingSetBuildResult,
    catalog: str,
    schema: str,
    registered_model_name: str,
    experiment_path: str,
    code_sha: str,
    invocation_id: str,
    mlflow_module: Any | None = None,
    mlflow_client: Any | None = None,
    _parent_run_state: dict[str, str] | None = None,
) -> ResearchRunResult:
    """Run all candidates against one exact receipt and register only selection."""
    registered_model_name = normalize_registered_model_name(
        registered_model_name
    )
    if training.receipt.status != READY:
        raise ValueError("Model research requires a READY TrainingSetReceipt")
    if training.receipt.model_definition_checksum != definition.checksum:
        raise ValueError("Training receipt uses another model definition")
    if mlflow_module is None:
        import mlflow as mlflow_module
    if mlflow_client is None:
        from mlflow.tracking import MlflowClient

        mlflow_client = MlflowClient()
    logical_build_id = research_build_id(
        model_definition_checksum=definition.checksum,
        training_receipt_id=training.receipt.receipt_id,
        research_plan_checksum=plan.checksum,
        evaluation_schema_version=plan.evaluation_schema_version,
    )
    claim_owner_id = _claim_owner_id(invocation_id)
    existing = load_selectable_research_build(
        spark,
        catalog=catalog,
        schema=schema,
        research_build_id=logical_build_id,
    )
    if existing is not None:
        _validate_reuse(definition, plan, training, existing)
    proposed_research_attempt = (
        existing.research_attempt_id
        if existing is not None
        else attempt_id(
            logical_id=logical_build_id,
            invocation_id=invocation_id,
        )
    )
    claim = claim_research_build(
        spark,
        catalog=catalog,
        schema=schema,
        research_build_id=logical_build_id,
        research_attempt_id=proposed_research_attempt,
        model_definition_checksum=definition.checksum,
        training_receipt_id=training.receipt.receipt_id,
        research_plan_checksum=plan.checksum,
        evaluation_schema_version=plan.evaluation_schema_version,
        code_sha=code_sha,
        owner_invocation_id=claim_owner_id,
        lease_seconds=_CLAIM_LEASE_SECONDS,
    )
    if existing is not None:
        if existing.status == AWAITING_SELECTION and claim.checkpoint in {
            CLAIM_CANDIDATES_READY,
            CLAIM_SELECTION_LOCKED,
        }:
            release_research_claim(
                spark,
                catalog=catalog,
                schema=schema,
                research_build_id=logical_build_id,
                owner_invocation_id=claim_owner_id,
                lease_token=claim.lease_token,
            )
            evaluations = _load_attempt_candidates(
                spark,
                catalog=catalog,
                schema=schema,
                build=existing,
                plan=plan,
            )
            recommendation = recommend_candidate(plan, evaluations)
            if existing.automatic_candidate_id != recommendation.candidate_id:
                raise ValueError(
                    "Stored automatic recommendation no longer matches"
                )
            return ResearchRunResult(
                research_build=existing,
                candidate_evaluations=evaluations,
                recommended_candidate_id=recommendation.candidate_id,
                reused=True,
            )
        result = _reused_research_result(
            spark,
            definition=definition,
            plan=plan,
            build=existing,
            catalog=catalog,
            schema=schema,
            registered_model_name=registered_model_name,
            mlflow_client=mlflow_client,
        )
        if existing.status == AWAITING_SELECTION:
            if claim.checkpoint == CLAIM_REGISTERED:
                if (
                    result.model_build is None
                    or result.selection_decision is None
                ):
                    raise ValueError(
                        "REGISTERED reviewed research is missing selected output"
                    )
                release_research_claim(
                    spark,
                    catalog=catalog,
                    schema=schema,
                    research_build_id=logical_build_id,
                    owner_invocation_id=claim_owner_id,
                    lease_token=claim.lease_token,
                )
            elif claim.checkpoint != CLAIM_COMPLETE:
                raise ValueError(
                    "Stored reviewed research build disagrees with claim "
                    f"checkpoint {claim.checkpoint}"
                )
        elif existing.status == READY:
            if claim.checkpoint == CLAIM_REGISTERED:
                if (
                    result.model_build is None
                    or result.selection_decision is None
                ):
                    raise ValueError(
                        "REGISTERED automatic research is missing selected output"
                    )
                advance_research_claim(
                    spark,
                    catalog=catalog,
                    schema=schema,
                    research_build_id=logical_build_id,
                    owner_invocation_id=claim_owner_id,
                    lease_token=claim.lease_token,
                    expected_checkpoint=CLAIM_REGISTERED,
                    checkpoint=CLAIM_COMPLETE,
                    selection_decision_id=(
                        result.selection_decision.selection_decision_id
                    ),
                    model_build_id=result.model_build.model_build_id,
                    lease_seconds=_CLAIM_LEASE_SECONDS,
                )
            elif claim.checkpoint != CLAIM_COMPLETE:
                raise ValueError(
                    "Stored READY research build disagrees with claim "
                    f"checkpoint {claim.checkpoint}"
                )
        return result
    if claim.checkpoint == CLAIM_FAILED:
        raise ValueError(
            "This research identity has a terminal failed attempt: "
            f"{claim.failure_reason}"
        )
    if claim.checkpoint == CLAIM_COMPLETE:
        completed_build = load_selectable_research_build(
            spark,
            catalog=catalog,
            schema=schema,
            research_build_id=logical_build_id,
        )
        if completed_build is None:
            raise ValueError(
                "COMPLETE research claim has no immutable research build"
            )
        _validate_reuse(definition, plan, training, completed_build)
        return _reused_research_result(
            spark,
            definition=definition,
            plan=plan,
            build=completed_build,
            catalog=catalog,
            schema=schema,
            registered_model_name=registered_model_name,
            mlflow_client=mlflow_client,
        )
    research_attempt = claim.research_attempt_id
    terminal_attempt = load_terminal_research_build_for_attempt(
        spark,
        catalog=catalog,
        schema=schema,
        research_build_id=logical_build_id,
        research_attempt_id=research_attempt,
    )
    if terminal_attempt is not None:
        if terminal_attempt.status != FAILED:
            raise ValueError(
                "Terminal research attempt was not available through reuse"
            )
        claim = fail_research_claim(
            spark,
            catalog=catalog,
            schema=schema,
            research_build_id=logical_build_id,
            owner_invocation_id=claim_owner_id,
            lease_token=claim.lease_token,
            expected_checkpoint=claim.checkpoint,
            failure_reason=terminal_attempt.failure_reason,
            lease_seconds=_CLAIM_LEASE_SECONDS,
        )
        raise ValueError(
            "This research identity has a terminal failed attempt: "
            f"{terminal_attempt.failure_reason}"
        )
    split = plan.temporal_split
    frame_plan_identity = ResearchFramePlan(
        observation_date_column="observation_date",
        label_column=definition.label,
        raw_key_columns=definition.observation_keys,
        feature_columns=definition.model_feature_columns,
        slice_columns=tuple(
            dict.fromkeys(
                (
                    *(
                        spec.column
                        for spec in plan.slices
                        if spec.column in training.frame.columns
                    ),
                    *_audit_columns(definition, training.frame),
                )
            )
        ),
        train_dates=_date_range(split.train_start, split.train_end),
        validation_dates=_date_range(split.validate_start, split.validate_end),
        test_dates=_date_range(split.test_start, split.test_end),
    )
    logical_frame_id = research_frame_id(
        research_build_id=logical_build_id,
        frame_plan_checksum=frame_plan_identity.checksum,
    )
    frame_attempt = attempt_id(
        logical_id=logical_frame_id,
        invocation_id=research_attempt,
    )
    if claim.checkpoint == CLAIM_CLAIMED:
        packed, schemas, _resolved_frame_plan = prepare_research_frame(
            training.frame,
            definition=definition,
            plan=plan,
            logical_research_build_id=logical_build_id,
            research_attempt_id=research_attempt,
            logical_research_frame_id=logical_frame_id,
            research_frame_attempt_id=frame_attempt,
            training_receipt_id=training.receipt.receipt_id,
        )
        binding = persist_research_frame(
            spark,
            packed,
            catalog=catalog,
            schema=schema,
            research_frame_id=logical_frame_id,
            research_frame_attempt_id=frame_attempt,
            research_build_id=logical_build_id,
            research_attempt_id=research_attempt,
            training_receipt_id=training.receipt.receipt_id,
            schemas=schemas,
            git_commit=code_sha,
        )
        claim = advance_research_claim(
            spark,
            catalog=catalog,
            schema=schema,
            research_build_id=logical_build_id,
            owner_invocation_id=claim_owner_id,
            lease_token=claim.lease_token,
            expected_checkpoint=CLAIM_CLAIMED,
            checkpoint=CLAIM_FRAME_READY,
            research_frame_binding=binding,
            lease_seconds=_CLAIM_LEASE_SECONDS,
        )
    else:
        binding = claim.research_frame_binding
        if binding is None:
            raise ValueError(
                "Resumed research claim is missing its exact frame binding"
            )
        packed = read_research_frame(spark, binding=binding)
    train_frame = read_training_frame(
        spark, binding=binding
    ).withColumnRenamed("label", definition.label)
    validation_frame = read_validation_frame(
        spark, binding=binding
    ).withColumnRenamed("label", definition.label)
    summarise_binary_labels(train_frame, definition.label)
    summarise_binary_labels(validation_frame, definition.label)
    slice_specs = _reporting_slice_specs(plan, validation_frame)
    config = _evaluation_config(plan)
    experiment = mlflow_module.set_experiment(experiment_path)
    experiment_id = str(experiment.experiment_id)
    parent_tags = {
        _RESEARCH_BUILD_TAG: logical_build_id,
        _RESEARCH_ATTEMPT_TAG: research_attempt,
        _RUN_ROLE_TAG: "parent",
    }
    if claim.checkpoint == CLAIM_FRAME_READY:
        parent_identity = _find_or_create_tagged_run(
            mlflow_client,
            experiment_id=experiment_id,
            run_name=(
                f"{definition.model_name}_research_{logical_build_id[-12:]}"
            ),
            tags=parent_tags,
        )
        claim = advance_research_claim(
            spark,
            catalog=catalog,
            schema=schema,
            research_build_id=logical_build_id,
            owner_invocation_id=claim_owner_id,
            lease_token=claim.lease_token,
            expected_checkpoint=CLAIM_FRAME_READY,
            checkpoint=CLAIM_PARENT_READY,
            mlflow_experiment_id=experiment_id,
            mlflow_parent_run_id=parent_identity.info.run_id,
            lease_seconds=_CLAIM_LEASE_SECONDS,
        )
    else:
        if claim.mlflow_experiment_id != experiment_id:
            raise ValueError(
                "Resumed research claim points at another MLflow experiment"
            )
        parent_identity = mlflow_client.get_run(claim.mlflow_parent_run_id)
        changed_parent_tags = [
            key
            for key, value in parent_tags.items()
            if parent_identity.data.tags.get(key) != value
        ]
        if changed_parent_tags:
            raise ValueError(
                "Resumed parent MLflow run has inconsistent tags: "
                + ", ".join(changed_parent_tags)
            )
    started_at = _run_timestamp(
        parent_identity.info.start_time,
        datetime.now(timezone.utc),
    )
    evaluations: list[CandidateEvaluation] = []
    candidate_rows: list[dict[str, Any]] = []
    failure: Exception | None = None

    with TemporaryDirectory(prefix="nextads-model-research-") as temp:
        root = Path(temp)
        parent_root = root / "parent"
        parent_root.mkdir(parents=True)
        parent_run_id = parent_identity.info.run_id
        if _parent_run_state is not None:
            _parent_run_state["run_id"] = parent_run_id
        with mlflow_module.start_run(run_id=parent_run_id) as parent_run:
            if str(parent_run.info.experiment_id) != experiment_id:
                raise ValueError(
                    "Recovered parent run belongs to another experiment"
                )
            mlflow_module.log_params(
                {
                    "model_name": definition.model_name,
                    "model_definition_checksum": definition.checksum,
                    "research_build_id": logical_build_id,
                    "research_attempt_id": research_attempt,
                    "research_plan_checksum": plan.checksum,
                    "training_receipt_id": training.receipt.receipt_id,
                    "selection_policy": plan.selection_policy,
                    "code_sha": code_sha,
                }
            )
            split_profile = _split_profile(
                packed,
                "label",
                withheld_splits=("test",),
            )
            _safe_json(parent_root / "research_plan.json", plan.as_dict())
            _safe_json(
                parent_root / "model_definition.json",
                definition.as_dict(include_research=True),
            )
            _safe_json(
                parent_root / "training_receipt.json",
                {
                    "receipt": training.receipt.__dict__,
                    "feature_bindings": [
                        binding.__dict__
                        for binding in training.receipt.feature_bindings
                    ],
                },
            )
            _safe_json(
                parent_root / "temporal_split_profile.json", split_profile
            )
            _safe_json(
                parent_root / "research_frame_binding.json", binding.__dict__
            )
            _safe_json(
                parent_root / "feature_snapshot_coverage.json",
                [
                    {
                        "feature_id": item.feature_id,
                        "snapshot_id": item.feature_snapshot_id,
                        "snapshot_attempt_id": (
                            item.feature_snapshot_attempt_id
                        ),
                        "backing_table": item.backing_table,
                        "delta_version": item.delta_version,
                        "row_count": item.row_count,
                        "schema_checksum": item.schema_checksum,
                        "value_checksum": item.value_checksum,
                    }
                    for item in training.receipt.feature_bindings
                ],
            )

            baseline = _prevalence_baseline(
                train_frame,
                validation_frame,
                label_column=definition.label,
                slice_specs=slice_specs,
                config=config,
            )
            _safe_json(parent_root / "prevalence_baseline.json", baseline)

            for candidate in plan.candidates:
                logical_candidate_id = candidate_evaluation_id(
                    research_build_id=logical_build_id,
                    candidate_id=candidate.candidate_id,
                    candidate_spec_checksum=candidate.checksum,
                )
                candidate_attempt = attempt_id(
                    logical_id=logical_candidate_id,
                    invocation_id=research_attempt,
                )
                child_tags = {
                    _RESEARCH_BUILD_TAG: logical_build_id,
                    _RESEARCH_ATTEMPT_TAG: research_attempt,
                    _RUN_ROLE_TAG: "candidate",
                    _CANDIDATE_ID_TAG: candidate.candidate_id,
                    _CANDIDATE_EVALUATION_TAG: logical_candidate_id,
                    _CANDIDATE_ATTEMPT_TAG: candidate_attempt,
                    "mlflow.parentRunId": parent_run_id,
                }
                existing_candidate = load_terminal_candidate_evaluation(
                    spark,
                    catalog=catalog,
                    schema=schema,
                    candidate_evaluation_id=logical_candidate_id,
                    research_attempt_id=research_attempt,
                )
                if existing_candidate is not None:
                    child_identity = _find_exact_tagged_run(
                        mlflow_client,
                        experiment_id=experiment_id,
                        tags=child_tags,
                    )
                    if child_identity is None:
                        raise ValueError(
                            "Candidate Delta receipt has no matching MLflow run"
                        )
                    if (
                        existing_candidate.mlflow_run_id
                        != child_identity.info.run_id
                    ):
                        raise ValueError(
                            "Candidate Delta receipt points to another MLflow run"
                        )
                    recovered_candidate = _recover_tagged_candidate(
                        child_identity,
                        candidate=candidate,
                        logical_candidate_id=logical_candidate_id,
                        candidate_attempt=candidate_attempt,
                        logical_build_id=logical_build_id,
                        research_attempt=research_attempt,
                    )
                    if recovered_candidate is None:
                        raise ValueError(
                            "Candidate Delta receipt has no terminal MLflow tags"
                        )
                    _validate_recovered_candidate(
                        existing_candidate,
                        recovered_candidate,
                    )
                    expected_run_status = (
                        "FINISHED"
                        if existing_candidate.status == READY
                        else "FAILED"
                    )
                    if child_identity.info.status != expected_run_status:
                        mlflow_client.set_terminated(
                            child_identity.info.run_id,
                            status=expected_run_status,
                        )
                    evaluations.append(existing_candidate)
                    candidate_rows.append(
                        _candidate_comparison_row(existing_candidate)
                    )
                    continue
                child_identity = _find_or_create_tagged_run(
                    mlflow_client,
                    experiment_id=experiment_id,
                    run_name=candidate.candidate_id,
                    tags=child_tags,
                )
                child_run_id = child_identity.info.run_id
                recovered_candidate = _recover_tagged_candidate(
                    child_identity,
                    candidate=candidate,
                    logical_candidate_id=logical_candidate_id,
                    candidate_attempt=candidate_attempt,
                    logical_build_id=logical_build_id,
                    research_attempt=research_attempt,
                )
                if recovered_candidate is not None:
                    expected_run_status = (
                        "FINISHED"
                        if recovered_candidate.status == READY
                        else "FAILED"
                    )
                    if child_identity.info.status != expected_run_status:
                        mlflow_client.set_terminated(
                            child_run_id,
                            status=expected_run_status,
                        )
                    persist_candidate_evaluation(
                        spark,
                        catalog=catalog,
                        schema=schema,
                        evaluation=recovered_candidate,
                    )
                    evaluations.append(recovered_candidate)
                    candidate_rows.append(
                        _candidate_comparison_row(recovered_candidate)
                    )
                    continue
                candidate_started = _run_timestamp(
                    child_identity.info.start_time,
                    datetime.now(timezone.utc),
                )
                model_artifact_path = (
                    "model_attempts/"
                    + hashlib.sha256(
                        invocation_id.encode("utf-8")
                    ).hexdigest()[:16]
                )
                evaluation_receipt = None
                try:
                    with mlflow_module.start_run(
                        run_id=child_run_id,
                        nested=True,
                    ) as child_run:
                        if child_run.info.run_id != child_run_id:
                            raise ValueError(
                                "Recovered candidate run identity changed"
                            )
                        mlflow_module.log_params(
                            {
                                "candidate_id": candidate.candidate_id,
                                "candidate_plugin": candidate.plugin,
                                "candidate_spec_checksum": candidate.checksum,
                                "seed": candidate.seed,
                                **{
                                    f"candidate_{name}": json.dumps(value)
                                    if isinstance(value, (dict, list))
                                    else value
                                    for name, value in candidate.parameters
                                },
                            }
                        )
                        plugin = resolve_candidate_plugin(candidate)
                        fitted = plugin.trainer.fit(
                            definition, candidate, train_frame
                        )
                        predictions = plugin.prediction_adapter.predict(
                            definition,
                            candidate,
                            fitted,
                            validation_frame,
                        )
                        validate_prediction_adapter_output(
                            validation_frame,
                            predictions,
                            label_column=definition.label,
                            slice_columns=tuple(
                                spec.column for spec in slice_specs
                            ),
                            context=f"{candidate.candidate_id} validation prediction",
                        )
                        evaluation = evaluate_binary_predictions(
                            predictions,
                            label_column=definition.label,
                            row_id_hash_column="row_id",
                            slice_specs=slice_specs,
                            config=config,
                        )
                        require_complete_binary_evaluation(
                            evaluation,
                            required_metrics=(
                                plan.evaluation_rules.required_metrics
                            ),
                            context=(f"{candidate.candidate_id} validation"),
                        )
                        training_predictions = (
                            plugin.prediction_adapter.predict(
                                definition,
                                candidate,
                                fitted,
                                train_frame,
                            )
                        )
                        training_slice_specs = _reporting_slice_specs(
                            plan, train_frame
                        )
                        validate_prediction_adapter_output(
                            train_frame,
                            training_predictions,
                            label_column=definition.label,
                            slice_columns=tuple(
                                spec.column for spec in training_slice_specs
                            ),
                            context=f"{candidate.candidate_id} training prediction",
                        )
                        training_evaluation = evaluate_binary_predictions(
                            training_predictions,
                            label_column=definition.label,
                            row_id_hash_column="row_id",
                            slice_specs=training_slice_specs,
                            config=config,
                        )
                        require_complete_binary_evaluation(
                            training_evaluation,
                            required_metrics=(
                                plan.evaluation_rules.required_metrics
                            ),
                            context=f"{candidate.candidate_id} training",
                        )
                        coverage = profile_feature_coverage(
                            validation_frame,
                            _coverage_specs(definition, validation_frame),
                        )
                        explanation = _global_explanation(
                            plugin,
                            definition,
                            candidate,
                            fitted,
                            train_frame,
                            validation_frame,
                            evaluation,
                            config=config,
                        )
                        feature_mapping = _candidate_feature_mapping(
                            candidate.plugin,
                            fitted,
                            train_frame,
                            definition,
                        )
                        feature_names = tuple(
                            item.feature_name for item in feature_mapping
                        )
                        persisted_model = (
                            plugin.prediction_adapter.model_for_persistence(
                                definition,
                                candidate,
                                fitted,
                            )
                        )
                        persisted_predictions = persisted_model.transform(
                            validation_frame
                        )
                        validate_persisted_prediction_equivalence(
                            validation_frame,
                            predictions,
                            persisted_predictions,
                            label_column=definition.label,
                            slice_columns=tuple(
                                spec.column for spec in slice_specs
                            ),
                            context=(
                                f"{candidate.candidate_id} persisted model"
                            ),
                        )
                        optional = _optional_evidence(
                            plan,
                            definition,
                            candidate,
                            fitted,
                            {
                                "validation": {
                                    "profile": evaluation["profile"],
                                    "metrics": evaluation["metrics"],
                                    "precision_recall_curve": evaluation[
                                        "precision_recall_curve"
                                    ],
                                    "roc_curve": evaluation["roc_curve"],
                                    "calibration": evaluation["calibration"],
                                    "lift_gain": evaluation["lift_gain"],
                                    "score_distribution": evaluation[
                                        "score_distribution"
                                    ],
                                    "top_confusion": evaluation[
                                        "top_confusion"
                                    ],
                                    "slices": evaluation["slices"],
                                },
                                "feature_coverage": coverage,
                                "global_explanation": explanation.as_dict(),
                            },
                            feature_names,
                        )
                        optional.update(
                            {
                                "training_evaluation": {
                                    "status": COMPLETE,
                                    "evidence": {
                                        "profile": training_evaluation[
                                            "profile"
                                        ],
                                        "metrics": training_evaluation[
                                            "metrics"
                                        ],
                                    },
                                },
                                "feature_name_mapping": {
                                    "status": COMPLETE,
                                    "evidence": {
                                        "features": [
                                            item.__dict__
                                            for item in feature_mapping
                                        ]
                                    },
                                },
                            }
                        )
                        candidate_root = (
                            root / "candidates" / candidate.candidate_id
                        )
                        bundle = write_candidate_evidence(
                            candidate_root,
                            candidate_id=candidate.candidate_id,
                            evaluation=evaluation,
                            feature_coverage=coverage,
                            explanation=explanation,
                            optional_evidence=optional,
                        )
                        bundle.require_selectable()
                        log_evidence_bundle(mlflow_module, bundle)
                        log_research_model_with_signature(
                            mlflow_module,
                            persisted_model,
                            definition,
                            validation_frame,
                            artifact_path=model_artifact_path,
                        )
                        combined_digest, model_digest = (
                            _model_artifact_manifest_digest(
                                mlflow_client,
                                run_id=child_run_id,
                                artifact_path=model_artifact_path,
                                evidence_digest=bundle.manifest_sha256,
                            )
                        )
                        mlflow_module.log_text(
                            json.dumps(
                                {
                                    "candidate_id": candidate.candidate_id,
                                    "evidence_manifest_sha256": (
                                        bundle.manifest_sha256
                                    ),
                                    "model_artifact_sha256": model_digest,
                                    "manifest_sha256": combined_digest,
                                },
                                sort_keys=True,
                            ),
                            "candidate_artifact_manifest.json",
                        )
                        mlflow_module.log_metrics(
                            {
                                **{
                                    f"train_{name}": float(value)
                                    for name, value in training_evaluation[
                                        "metrics"
                                    ].items()
                                },
                                **{
                                    f"validation_{name}": float(value)
                                    for name, value in evaluation[
                                        "metrics"
                                    ].items()
                                },
                            }
                        )
                        evaluation_receipt = CandidateEvaluation(
                            candidate_evaluation_id=logical_candidate_id,
                            candidate_attempt_id=candidate_attempt,
                            research_build_id=logical_build_id,
                            research_attempt_id=research_attempt,
                            candidate_id=candidate.candidate_id,
                            candidate_spec_checksum=candidate.checksum,
                            required=not candidate.failure_allowed,
                            status=READY,
                            created_at=candidate_started,
                            completed_at=datetime.now(timezone.utc),
                            mlflow_run_id=child_run_id,
                            model_uri=(
                                f"runs:/{child_run_id}/{model_artifact_path}"
                            ),
                            metrics=_candidate_metrics(evaluation),
                            artifact_manifest_digest=combined_digest,
                            explanation_status=READY,
                        )
                        _tag_candidate_terminal(
                            mlflow_client,
                            run_id=child_run_id,
                            evaluation=evaluation_receipt,
                            model_artifact_path=model_artifact_path,
                        )
                except Exception as exc:
                    if evaluation_receipt is not None:
                        raise
                    evaluation_receipt = CandidateEvaluation(
                        candidate_evaluation_id=logical_candidate_id,
                        candidate_attempt_id=candidate_attempt,
                        research_build_id=logical_build_id,
                        research_attempt_id=research_attempt,
                        candidate_id=candidate.candidate_id,
                        candidate_spec_checksum=candidate.checksum,
                        required=not candidate.failure_allowed,
                        status=FAILED,
                        created_at=candidate_started,
                        completed_at=datetime.now(timezone.utc),
                        mlflow_run_id=child_run_id,
                        failure_reason=safe_failure_reason(
                            exc,
                            stage=f"candidate_{candidate.candidate_id}",
                        ),
                    )
                    _tag_candidate_terminal(
                        mlflow_client,
                        run_id=child_run_id,
                        evaluation=evaluation_receipt,
                    )
                persist_candidate_evaluation(
                    spark,
                    catalog=catalog,
                    schema=schema,
                    evaluation=evaluation_receipt,
                )
                evaluations.append(evaluation_receipt)
                candidate_rows.append(
                    _candidate_comparison_row(evaluation_receipt)
                )

            if claim.checkpoint == CLAIM_PARENT_READY:
                claim = advance_research_claim(
                    spark,
                    catalog=catalog,
                    schema=schema,
                    research_build_id=logical_build_id,
                    owner_invocation_id=claim_owner_id,
                    lease_token=claim.lease_token,
                    expected_checkpoint=CLAIM_PARENT_READY,
                    checkpoint=CLAIM_CANDIDATES_READY,
                    lease_seconds=_CLAIM_LEASE_SECONDS,
                )

            candidate_rows.append(
                {
                    "candidate_id": "prevalence_only_baseline",
                    "status": "COMPLETE",
                    "metrics": baseline["metrics"],
                    "selectable": False,
                }
            )
            write_candidate_comparison_evidence(parent_root, candidate_rows)
            try:
                recommendation = recommend_candidate(plan, evaluations)
                _safe_json(
                    parent_root / "automatic_recommendation.json",
                    {
                        "candidate_id": recommendation.candidate_id,
                        "policy": AUTOMATIC_SELECTION_REASON,
                        "validation_metrics": dict(recommendation.metrics),
                    },
                )
            except Exception as exc:
                failure = exc
                recommendation = None
                _safe_json(
                    parent_root / "automatic_recommendation.json",
                    {
                        "status": "FAILED",
                        "reason": safe_failure_reason(
                            exc,
                            stage="automatic_recommendation",
                        ),
                    },
                )
            _safe_json(parent_root / "candidate_statuses.json", candidate_rows)
            preliminary_digest = _manifest_parent(parent_root)
            mlflow_module.log_artifacts(
                str(parent_root), artifact_path="research"
            )

        if plan.selection_policy == AUTO and failure is None:
            mlflow_client.set_tag(
                parent_run_id,
                "nextads_model_research_status",
                "SELECTION_PENDING",
            )
            mlflow_client.update_run(parent_run_id, status="RUNNING")
        if failure is not None:
            mlflow_client.set_terminated(parent_run_id, status="FAILED")
        completed = datetime.now(timezone.utc)
        common = {
            "research_build_id": logical_build_id,
            "research_attempt_id": research_attempt,
            "model_name": definition.model_name,
            "training_receipt_id": training.receipt.receipt_id,
            "model_definition_checksum": definition.checksum,
            "research_plan_checksum": plan.checksum,
            "evaluation_schema_version": plan.evaluation_schema_version,
            "code_sha": code_sha,
            **binding.__dict__,
            "candidate_count": len(plan.candidates),
            "successful_candidate_count": sum(
                item.status == READY for item in evaluations
            ),
            "created_at": started_at,
            "completed_at": completed,
            "mlflow_experiment_id": str(experiment_id),
            "mlflow_parent_run_id": parent_run_id,
            "automatic_candidate_id": (
                None if recommendation is None else recommendation.candidate_id
            ),
            "artifact_manifest_digest": preliminary_digest,
        }
        if failure is not None or recommendation is None:
            failed_build = ModelResearchBuild(
                **common,
                status=FAILED,
                failure_reason=safe_failure_reason(
                    failure,
                    stage="research_attempt",
                ),
            )
            persist_research_build(
                spark, catalog=catalog, schema=schema, build=failed_build
            )
            claim = fail_research_claim(
                spark,
                catalog=catalog,
                schema=schema,
                research_build_id=logical_build_id,
                owner_invocation_id=claim_owner_id,
                lease_token=claim.lease_token,
                expected_checkpoint=claim.checkpoint,
                failure_reason=failed_build.failure_reason,
                lease_seconds=_CLAIM_LEASE_SECONDS,
            )
            raise failure

        provisional = ModelResearchBuild(
            **common,
            status=AWAITING_SELECTION,
        )
        if plan.selection_policy != AUTO:
            mlflow_client.set_tag(
                parent_run_id,
                "nextads_model_research_status",
                AWAITING_SELECTION,
            )
            release_research_claim(
                spark,
                catalog=catalog,
                schema=schema,
                research_build_id=logical_build_id,
                owner_invocation_id=claim_owner_id,
                lease_token=claim.lease_token,
            )
            persist_research_build(
                spark, catalog=catalog, schema=schema, build=provisional
            )
            return ResearchRunResult(
                research_build=provisional,
                candidate_evaluations=tuple(evaluations),
                recommended_candidate_id=recommendation.candidate_id,
            )

        logical_decision_id = selection_decision_id(
            research_build_id=logical_build_id,
            selection_mode=AUTO,
            recommended_candidate_id=recommendation.candidate_id,
            selected_candidate_id=recommendation.candidate_id,
            reason=AUTOMATIC_SELECTION_REASON,
        )
        candidate_spec = next(
            item
            for item in plan.candidates
            if item.candidate_id == recommendation.candidate_id
        )
        if claim.checkpoint == CLAIM_CANDIDATES_READY:
            decision_attempt = attempt_id(
                logical_id=logical_decision_id,
                invocation_id=research_attempt,
            )
            decision = ModelSelectionDecision(
                selection_decision_id=logical_decision_id,
                selection_attempt_id=decision_attempt,
                research_build_id=logical_build_id,
                research_attempt_id=research_attempt,
                selection_mode=AUTO,
                recommended_candidate_id=recommendation.candidate_id,
                selected_candidate_id=recommendation.candidate_id,
                selected_candidate_evaluation_id=(
                    recommendation.candidate_evaluation_id
                ),
                reason=AUTOMATIC_SELECTION_REASON,
                status=READY,
                created_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc),
                registered_model_name=registered_model_name,
                decision_code_sha=code_sha,
            )
            decision = replace(
                decision,
                model_build_id=selected_model_build_id(
                    definition,
                    training.receipt,
                    provisional,
                    recommendation,
                    decision,
                ),
            )
            existing_decision = load_ready_selection_decision(
                spark,
                catalog=catalog,
                schema=schema,
                selection_decision_id=logical_decision_id,
            )
            if existing_decision is None:
                persist_selection_decision(
                    spark,
                    catalog=catalog,
                    schema=schema,
                    decision=decision,
                )
            else:
                stable_fields = (
                    "research_build_id",
                    "research_attempt_id",
                    "selection_mode",
                    "recommended_candidate_id",
                    "selected_candidate_id",
                    "selected_candidate_evaluation_id",
                    "reason",
                    "model_build_id",
                    "registered_model_name",
                )
                changed = [
                    field
                    for field in stable_fields
                    if getattr(existing_decision, field)
                    != getattr(decision, field)
                ]
                if changed:
                    raise ValueError(
                        "Existing automatic selection has inconsistent lineage: "
                        + ", ".join(changed)
                    )
                decision = existing_decision
            claim = advance_research_claim(
                spark,
                catalog=catalog,
                schema=schema,
                research_build_id=logical_build_id,
                owner_invocation_id=claim_owner_id,
                lease_token=claim.lease_token,
                expected_checkpoint=CLAIM_CANDIDATES_READY,
                checkpoint=CLAIM_SELECTION_LOCKED,
                selection_decision_id=decision.selection_decision_id,
                model_build_id=decision.model_build_id,
                lease_seconds=_CLAIM_LEASE_SECONDS,
            )
        else:
            if claim.selection_decision_id != logical_decision_id:
                raise ValueError(
                    "Resumed claim has a different automatic selection"
                )
            decision = load_ready_selection_decision(
                spark,
                catalog=catalog,
                schema=schema,
                selection_decision_id=logical_decision_id,
            )
            if (
                decision is None
                or decision.model_build_id != claim.model_build_id
                or decision.registered_model_name != registered_model_name
            ):
                raise ValueError(
                    "Resumed claim is missing its locked selection decision"
                )
        test_evaluation, intervals, source_model, test_frame, test_digest = (
            _selected_test_evidence(
                mlflow_module,
                definition,
                plan,
                candidate_spec,
                recommendation,
                decision,
                binding,
                spark,
                catalog=catalog,
                schema=schema,
                output_directory=root / "selected_test",
            )
        )
        selected_metrics = {
            **{
                f"validation_{name}": value
                for name, value in recommendation.metrics
            },
            **{
                f"test_{name}": float(value)
                for name, value in test_evaluation["metrics"].items()
            },
        }
        model_build = load_ready_model_build(
            spark,
            catalog=catalog,
            schema=schema,
            model_build_id=decision.model_build_id,
        )
        if model_build is None:
            model_build = register_selected_candidate(
                definition,
                training.receipt,
                provisional,
                recommendation,
                decision,
                registered_model_name=registered_model_name,
                selection_execution_code_sha=code_sha,
                selected_metrics=selected_metrics,
                mlflow_module=mlflow_module,
                client=mlflow_client,
            )
        else:
            expected_existing_model = {
                "research_build_id": logical_build_id,
                "selection_decision_id": decision.selection_decision_id,
                "selected_candidate_id": recommendation.candidate_id,
                "selected_candidate_evaluation_id": (
                    recommendation.candidate_evaluation_id
                ),
                "registered_model_name": registered_model_name,
                "status": READY,
            }
            if any(
                getattr(model_build, field) != value
                for field, value in expected_existing_model.items()
            ):
                raise ValueError(
                    "Persisted selected model has inconsistent lineage"
                )
            validate_registered_model_build(mlflow_client, model_build)
            if model_build.registration_code_sha is None:
                raise ValueError(
                    "Persisted selected model lacks registration code provenance"
                )
        registered_model = mlflow_module.spark.load_model(
            model_build.model_uri
        )
        score_checksum = validate_score_reproduction(
            source_model,
            registered_model,
            test_frame,
        )
        if model_build.model_build_id != decision.model_build_id:
            raise ValueError(
                "Registered model build does not match the locked selection"
            )
        persist_model_build(
            spark, catalog=catalog, schema=schema, build=model_build
        )
        persist_selection_decision(
            spark, catalog=catalog, schema=schema, decision=decision
        )
        if claim.checkpoint == CLAIM_SELECTION_LOCKED:
            claim = advance_research_claim(
                spark,
                catalog=catalog,
                schema=schema,
                research_build_id=logical_build_id,
                owner_invocation_id=claim_owner_id,
                lease_token=claim.lease_token,
                expected_checkpoint=CLAIM_SELECTION_LOCKED,
                checkpoint=CLAIM_REGISTERED,
                selection_decision_id=decision.selection_decision_id,
                model_build_id=model_build.model_build_id,
                lease_seconds=_CLAIM_LEASE_SECONDS,
            )
        elif (
            claim.checkpoint != CLAIM_REGISTERED
            or claim.model_build_id != model_build.model_build_id
        ):
            raise ValueError(
                "Resumed research claim has inconsistent model registration"
            )
        _safe_json(
            parent_root / "selected_model.json",
            {
                "selection_decision_id": decision.selection_decision_id,
                "selected_candidate_id": decision.selected_candidate_id,
                "model_build_id": model_build.model_build_id,
                "model_uri": model_build.model_uri,
                "artifact_digest": model_build.artifact_digest,
                "score_reproduction_checksum": score_checksum,
                "test_evidence_manifest_sha256": test_digest,
                "confidence_intervals": intervals,
            },
        )
        final_digest = _manifest_parent(parent_root)
        with mlflow_module.start_run(run_id=parent_run_id):
            mlflow_module.log_artifacts(
                str(parent_root), artifact_path="research"
            )
            mlflow_module.log_param(
                "selected_candidate_id", decision.selected_candidate_id
            )
            mlflow_module.log_param(
                "registered_model_uri", model_build.model_uri
            )
        mlflow_client.update_run(parent_run_id, status="RUNNING")
        ready_build = replace(
            provisional,
            status=READY,
            artifact_manifest_digest=final_digest,
            completed_at=datetime.now(timezone.utc),
        )
        persist_research_build(
            spark, catalog=catalog, schema=schema, build=ready_build
        )
        claim = advance_research_claim(
            spark,
            catalog=catalog,
            schema=schema,
            research_build_id=logical_build_id,
            owner_invocation_id=claim_owner_id,
            lease_token=claim.lease_token,
            expected_checkpoint=CLAIM_REGISTERED,
            checkpoint=CLAIM_COMPLETE,
            selection_decision_id=decision.selection_decision_id,
            model_build_id=model_build.model_build_id,
            lease_seconds=_CLAIM_LEASE_SECONDS,
        )
        _finalize_parent_run(
            mlflow_client,
            ready_build,
            status=READY,
            decision=decision,
        )
        return ResearchRunResult(
            research_build=ready_build,
            candidate_evaluations=tuple(evaluations),
            recommended_candidate_id=recommendation.candidate_id,
            selection_decision=decision,
            model_build=model_build,
        )


def run_model_research(
    spark: Any,
    *,
    definition: ModelDefinition,
    plan: ResearchPlan,
    training: TrainingSetBuildResult,
    catalog: str,
    schema: str,
    registered_model_name: str,
    experiment_path: str,
    code_sha: str,
    invocation_id: str,
    mlflow_module: Any | None = None,
    mlflow_client: Any | None = None,
) -> ResearchRunResult:
    """Run research and leave its parent MLflow run terminally honest."""
    if mlflow_module is None:
        import mlflow as mlflow_module
    if mlflow_client is None:
        from mlflow.tracking import MlflowClient

        mlflow_client = MlflowClient()
    parent_run_state: dict[str, str] = {}
    try:
        return _run_model_research_impl(
            spark,
            definition=definition,
            plan=plan,
            training=training,
            catalog=catalog,
            schema=schema,
            registered_model_name=registered_model_name,
            experiment_path=experiment_path,
            code_sha=code_sha,
            invocation_id=invocation_id,
            mlflow_module=mlflow_module,
            mlflow_client=mlflow_client,
            _parent_run_state=parent_run_state,
        )
    except Exception:
        parent_run_id = parent_run_state.get("run_id")
        if parent_run_id:
            try:
                mlflow_client.set_tag(
                    parent_run_id,
                    "nextads_model_research_status",
                    FAILED,
                )
                mlflow_client.set_terminated(parent_run_id, status="FAILED")
            except Exception:
                # Preserve the original runtime failure; the claim remains
                # non-terminal and prevents a misleading READY receipt.
                pass
        raise


__all__ = [
    "AUTOMATIC_SELECTION_REASON",
    "ResearchRunResult",
    "plan_observation_dates",
    "prepare_research_frame",
    "run_model_research",
]

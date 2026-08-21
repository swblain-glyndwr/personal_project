"""Deterministic recommendation and selected-candidate registration."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from typing import Any, Iterable, Mapping

from next_ads.features.feature_builds import feature_value_checksum
from next_ads.model_development.contracts import (
    MODEL_VERSION_TAG_ARTIFACT_DIGEST,
    MODEL_VERSION_TAG_BUILD_ID,
    MODEL_VERSION_TAG_DECISION_CODE_SHA,
    MODEL_VERSION_TAG_REGISTRATION_CODE_SHA,
    MODEL_VERSION_TAG_TRAINING_RECEIPT_ID,
    ModelBuild,
    ModelDefinition,
    TrainingSetReceipt,
)
from next_ads.model_development.research_contracts import (
    CandidateEvaluation,
    ModelResearchBuild,
    ModelSelectionDecision,
    ResearchPlan,
)
from next_ads.model_development.spark_training import (
    registered_model_artifact_digest,
)


MODEL_VERSION_TAG_RESEARCH_BUILD_ID = "nextads_research_build_id"
MODEL_VERSION_TAG_SELECTION_DECISION_ID = "nextads_selection_decision_id"
MODEL_VERSION_TAG_SELECTED_CANDIDATE_ID = "nextads_selected_candidate_id"
MODEL_VERSION_TAG_CANDIDATE_EVALUATION_ID = "nextads_candidate_evaluation_id"


def normalize_registered_model_name(value: str) -> str:
    """Return the exact non-empty registration target used by MLflow."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("registered_model_name must not be empty")
    return value.strip()


def _code_sha(value: str, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must not be empty")
    return value.strip()


def _registered_version_for_child_run(
    client: Any,
    *,
    registered_model_name: str,
    candidate: CandidateEvaluation,
    expected_tags: Mapping[str, str],
) -> Any | None:
    """Recover one prior registration of the exact selected child run.

    This closes the failure window between ``register_model`` and persisting the
    READY ModelBuild.  A partially tagged version can be completed on retry,
    while a version carrying conflicting NextAds lineage is never adopted.
    """
    search = getattr(client, "search_model_versions", None)
    if not callable(search):
        return None
    versions = tuple(
        version
        for version in search(f"name='{registered_model_name}'")
        if getattr(version, "run_id", None) == candidate.mlflow_run_id
    )
    compatible = []
    for version in versions:
        tags = dict(getattr(version, "tags", None) or {})
        conflicts = [
            key
            for key, expected in expected_tags.items()
            if key in tags and str(tags[key]) != str(expected)
        ]
        if conflicts:
            raise ValueError(
                "Registered child-run version has conflicting lineage tags: "
                + ", ".join(sorted(conflicts))
            )
        compatible.append(version)
    if len(compatible) > 1:
        raise ValueError(
            "More than one registered version points at the selected child run"
        )
    return compatible[0] if compatible else None


def recommend_candidate(
    plan: ResearchPlan,
    evaluations: Iterable[CandidateEvaluation],
) -> CandidateEvaluation:
    """Choose by validation PR-AUC, log loss and candidate ID."""
    by_id: dict[str, CandidateEvaluation] = {}
    for evaluation in evaluations:
        if evaluation.candidate_id in by_id:
            raise ValueError(
                "Candidate recommendation contains duplicate candidate IDs"
            )
        by_id[evaluation.candidate_id] = evaluation
    declared = {
        candidate.candidate_id: candidate for candidate in plan.candidates
    }
    unknown = sorted(set(by_id).difference(declared))
    if unknown:
        raise ValueError(
            "Candidate recommendation contains undeclared candidates: "
            + ", ".join(unknown)
        )
    failed_required = sorted(
        candidate.candidate_id
        for candidate in plan.candidates
        if not candidate.failure_allowed
        and (
            candidate.candidate_id not in by_id
            or by_id[candidate.candidate_id].status != "READY"
        )
    )
    if failed_required:
        raise ValueError(
            "Required research candidates did not complete: "
            + ", ".join(failed_required)
        )
    ready = [
        evaluation
        for evaluation in by_id.values()
        if evaluation.status == "READY"
    ]
    if len(ready) < plan.minimum_successful_candidates:
        raise ValueError(
            "Research did not meet its minimum successful candidate count"
        )

    def ranking(evaluation: CandidateEvaluation) -> tuple[float, float, str]:
        metrics = dict(evaluation.metrics)
        try:
            auc_pr = float(metrics["auc_pr"])
            log_loss = float(metrics["log_loss"])
        except KeyError as exc:
            raise ValueError(
                f"Candidate {evaluation.candidate_id} lacks selection metrics"
            ) from exc
        return (-auc_pr, log_loss, evaluation.candidate_id)

    return min(ready, key=ranking)


def selected_model_build_id(
    definition: ModelDefinition,
    receipt: TrainingSetReceipt,
    research_build: ModelResearchBuild,
    candidate: CandidateEvaluation,
    decision: ModelSelectionDecision,
) -> str:
    """Identify a selected artifact without colliding with legacy training."""
    if decision.registered_model_name is None:
        raise ValueError(
            "Selection must lock its registration target before build identity"
        )
    payload = {
        "candidate_evaluation_id": candidate.candidate_evaluation_id,
        "candidate_id": candidate.candidate_id,
        "definition_checksum": definition.checksum,
        "research_build_id": research_build.research_build_id,
        "registered_model_name": normalize_registered_model_name(
            decision.registered_model_name
        ),
        "selection_decision_id": decision.selection_decision_id,
        "training_receipt_id": receipt.receipt_id,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _validate_selection_inputs(
    definition: ModelDefinition,
    receipt: TrainingSetReceipt,
    research_build: ModelResearchBuild,
    candidate: CandidateEvaluation,
    decision: ModelSelectionDecision,
) -> None:
    if (
        receipt.status != "READY"
        or receipt.model_name != definition.model_name
    ):
        raise ValueError("Selection requires the exact READY training receipt")
    if receipt.model_definition_checksum != definition.checksum:
        raise ValueError("Selection receipt uses a different model definition")
    if research_build.status not in {"AWAITING_SELECTION", "READY"}:
        raise ValueError("Selection requires completed research evidence")
    if (
        research_build.model_name != definition.model_name
        or research_build.training_receipt_id != receipt.receipt_id
        or research_build.model_definition_checksum != definition.checksum
    ):
        raise ValueError("Research build belongs to different model inputs")
    if candidate.status != "READY" or candidate.explanation_status != "READY":
        raise ValueError("Only a fully evidenced candidate can be selected")
    if (
        candidate.research_build_id != research_build.research_build_id
        or candidate.research_attempt_id != research_build.research_attempt_id
        or candidate.candidate_id != decision.selected_candidate_id
        or candidate.candidate_evaluation_id
        != decision.selected_candidate_evaluation_id
    ):
        raise ValueError(
            "Selection does not identify the exact candidate attempt"
        )
    if decision.status != "READY":
        raise ValueError(
            "Model registration requires a READY selection decision"
        )
    if decision.research_build_id != research_build.research_build_id:
        raise ValueError(
            "Selection decision belongs to another research build"
        )


def register_selected_candidate(
    definition: ModelDefinition,
    receipt: TrainingSetReceipt,
    research_build: ModelResearchBuild,
    candidate: CandidateEvaluation,
    decision: ModelSelectionDecision,
    *,
    registered_model_name: str,
    selection_execution_code_sha: str,
    selected_metrics: Mapping[str, float],
    mlflow_module: Any | None = None,
    client: Any | None = None,
    digest_fn: Any = registered_model_artifact_digest,
) -> ModelBuild:
    """Register only the selected child artifact and attach exact lineage."""
    _validate_selection_inputs(
        definition,
        receipt,
        research_build,
        candidate,
        decision,
    )
    if not candidate.model_uri:
        raise ValueError(
            "Selected registration needs model name and child URI"
        )
    model_name = normalize_registered_model_name(registered_model_name)
    _code_sha(
        selection_execution_code_sha,
        "selection_execution_code_sha",
    )
    if decision.registered_model_name != model_name:
        raise ValueError(
            "Selected registration target differs from the locked decision"
        )
    build_id = selected_model_build_id(
        definition,
        receipt,
        research_build,
        candidate,
        decision,
    )
    if decision.model_build_id != build_id:
        raise ValueError(
            "Selected registration build differs from the locked decision"
        )
    # The READY decision is persisted before test exposure and registration.
    # Its code SHA is therefore the durable registration intent: a retry may
    # run newer code, but it must repair or reuse the original registration
    # without rewriting provenance to the retry SHA.
    registration_code_sha = _code_sha(
        decision.decision_code_sha or "",
        "decision_code_sha",
    )
    if mlflow_module is None:
        import mlflow as mlflow_module
    if client is None:
        from mlflow.tracking import MlflowClient

        client = MlflowClient()
    started_at = datetime.now(timezone.utc)
    lineage_tags = {
        MODEL_VERSION_TAG_BUILD_ID: build_id,
        MODEL_VERSION_TAG_TRAINING_RECEIPT_ID: receipt.receipt_id,
        MODEL_VERSION_TAG_RESEARCH_BUILD_ID: research_build.research_build_id,
        MODEL_VERSION_TAG_SELECTION_DECISION_ID: decision.selection_decision_id,
        MODEL_VERSION_TAG_SELECTED_CANDIDATE_ID: candidate.candidate_id,
        MODEL_VERSION_TAG_CANDIDATE_EVALUATION_ID: (
            candidate.candidate_evaluation_id
        ),
        MODEL_VERSION_TAG_DECISION_CODE_SHA: _code_sha(
            decision.decision_code_sha or "",
            "decision_code_sha",
        ),
    }
    registered = _registered_version_for_child_run(
        client,
        registered_model_name=model_name,
        candidate=candidate,
        expected_tags=lineage_tags,
    )
    if registered is None:
        registered = mlflow_module.register_model(
            model_uri=candidate.model_uri,
            name=model_name,
        )
        existing_registration_code_sha = None
    else:
        existing_tags = dict(getattr(registered, "tags", None) or {})
        raw_registration_code_sha = existing_tags.get(
            MODEL_VERSION_TAG_REGISTRATION_CODE_SHA
        )
        existing_registration_code_sha = (
            None
            if raw_registration_code_sha is None
            else _code_sha(
                raw_registration_code_sha,
                "registered version registration_code_sha tag",
            )
        )
        if (
            existing_registration_code_sha is not None
            and existing_registration_code_sha != registration_code_sha
        ):
            raise ValueError(
                "Registered child-run version has different registration "
                "code provenance"
            )
    if existing_registration_code_sha is None:
        client.set_model_version_tag(
            name=model_name,
            version=int(registered.version),
            key=MODEL_VERSION_TAG_REGISTRATION_CODE_SHA,
            value=registration_code_sha,
        )
    version = int(registered.version)
    model_uri = f"models:/{model_name}/{version}"
    digest = digest_fn(model_uri)
    tags = {
        MODEL_VERSION_TAG_ARTIFACT_DIGEST: digest,
        **lineage_tags,
    }
    for key, value in tags.items():
        client.set_model_version_tag(
            name=model_name,
            version=version,
            key=key,
            value=value,
        )
    return ModelBuild(
        model_build_id=build_id,
        model_name=definition.model_name,
        training_receipt_id=receipt.receipt_id,
        model_definition_checksum=definition.checksum,
        runtime_profile=definition.runtime_profile,
        status="READY",
        created_at=started_at,
        mlflow_run_id=candidate.mlflow_run_id,
        registered_model_name=model_name,
        registered_model_version=version,
        model_uri=model_uri,
        artifact_digest=digest,
        metrics=tuple(
            sorted(
                (name, float(value))
                for name, value in selected_metrics.items()
            )
        ),
        completed_at=datetime.now(timezone.utc),
        research_build_id=research_build.research_build_id,
        selection_decision_id=decision.selection_decision_id,
        selected_candidate_id=candidate.candidate_id,
        selected_candidate_evaluation_id=candidate.candidate_evaluation_id,
        registration_code_sha=registration_code_sha,
    )


def validate_score_reproduction(
    source_model: Any,
    registered_model: Any,
    test_frame: Any,
    *,
    row_id_column: str = "row_id",
) -> str:
    """Prove a clean registered load reproduces selected child scores."""
    if row_id_column not in test_frame.columns:
        raise ValueError("Score reproduction requires a hashed row identity")
    expected = source_model.transform(test_frame).select(
        row_id_column, "score"
    )
    actual = registered_model.transform(test_frame).select(
        row_id_column, "score"
    )
    expected_checksum = feature_value_checksum(expected)
    actual_checksum = feature_value_checksum(actual)
    if expected_checksum != actual_checksum:
        raise ValueError(
            "Registered model scores do not reproduce child scores"
        )
    return expected_checksum


__all__ = [
    "MODEL_VERSION_TAG_CANDIDATE_EVALUATION_ID",
    "MODEL_VERSION_TAG_DECISION_CODE_SHA",
    "MODEL_VERSION_TAG_REGISTRATION_CODE_SHA",
    "MODEL_VERSION_TAG_RESEARCH_BUILD_ID",
    "MODEL_VERSION_TAG_SELECTED_CANDIDATE_ID",
    "MODEL_VERSION_TAG_SELECTION_DECISION_ID",
    "recommend_candidate",
    "normalize_registered_model_name",
    "register_selected_candidate",
    "selected_model_build_id",
    "validate_score_reproduction",
]

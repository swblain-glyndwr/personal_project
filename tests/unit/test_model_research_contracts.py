from datetime import date, datetime, timezone
import hashlib
import json

import pytest

from next_ads.model_development import (
    AUTO,
    MANDATORY_BINARY_EVIDENCE,
    READY,
    REVIEW_REQUIRED,
    AutoMLDiscoveryReceipt,
    CandidateEvaluation,
    CandidateSpec,
    EvaluationRules,
    ModelResearchBuild,
    ModelSelectionDecision,
    ResearchPlan,
    SliceSpec,
    TemporalSplitSpec,
    load_model_definition,
    load_model_research_plan,
    load_model_research_plans,
    standard_prediction_columns,
)


NOW = datetime(2026, 8, 18, tzinfo=timezone.utc)


def test_standard_prediction_columns_use_the_declared_label():
    assert standard_prediction_columns("clicked") == (
        "clicked",
        "score",
        "prediction",
        "split",
        "observation_date",
        "row_id",
    )


def _split() -> TemporalSplitSpec:
    return TemporalSplitSpec(
        train_start=date(2026, 8, 5),
        train_end=date(2026, 8, 8),
        validate_start=date(2026, 8, 9),
        validate_end=date(2026, 8, 10),
        test_start=date(2026, 8, 11),
        test_end=date(2026, 8, 11),
    )


def _plan(candidate: CandidateSpec) -> ResearchPlan:
    return ResearchPlan(
        candidates=(candidate,),
        temporal_split=_split(),
        evaluation_rules=EvaluationRules(),
        slices=(
            SliceSpec(
                slice_id="location",
                column="location",
                values=("SB1", "SB2"),
            ),
        ),
        selection_policy=AUTO,
        explanation_requirements=(
            "global_feature_importance",
            "readable_feature_names",
            "model_specific_or_permutation",
        ),
    )


def test_candidate_parameters_are_json_safe_and_order_independent():
    first = CandidateSpec(
        candidate_id="custom_candidate",
        plugin="next_ads.examples.CustomCandidate",
        parameters={"depth": 6, "nested": {"enabled": True}},
    )
    second = CandidateSpec(
        candidate_id="custom_candidate",
        plugin="next_ads.examples.CustomCandidate",
        parameters={"nested": {"enabled": True}, "depth": 6},
    )

    assert first.checksum == second.checksum
    assert first.as_dict()["parameters"] == {
        "depth": 6,
        "nested": {"enabled": True},
    }
    with pytest.raises(ValueError, match="JSON-safe"):
        CandidateSpec(
            candidate_id="bad_parameters",
            plugin="spark_logistic_regression",
            parameters={"unsupported": {1, 2}},
        )


@pytest.mark.parametrize(
    "parameter",
    [
        "seed",
        "split_column",
        "register_model",
        "model_alias",
        "publish_scores",
    ],
)
def test_candidate_cannot_override_orchestration(parameter):
    with pytest.raises(ValueError, match="controlled by orchestration"):
        CandidateSpec(
            candidate_id="unsafe_candidate",
            plugin="spark_logistic_regression",
            parameters={parameter: "unsafe"},
        )


def test_candidate_plugin_is_an_alias_or_reviewed_repository_class():
    alias = CandidateSpec(
        candidate_id="logistic_regression",
        plugin="spark_logistic_regression",
    )
    repository_class = CandidateSpec(
        candidate_id="custom_candidate",
        plugin="next_ads.model_development.custom.CustomCandidate",
    )

    assert alias.plugin == "spark_logistic_regression"
    assert repository_class.plugin.endswith("CustomCandidate")
    with pytest.raises(ValueError, match=r"plug-in alias or a next_ads\.\*"):
        CandidateSpec(
            candidate_id="external_candidate",
            plugin="external.package.CustomCandidate",
        )


def test_temporal_split_has_exact_ordered_train_validate_test_periods():
    assert _split().as_dict() == {
        "train": {"start": "2026-08-05", "end": "2026-08-08"},
        "validate": {"start": "2026-08-09", "end": "2026-08-10"},
        "test": {"start": "2026-08-11", "end": "2026-08-11"},
    }
    with pytest.raises(ValueError, match="non-overlapping"):
        TemporalSplitSpec(
            train_start=date(2026, 8, 5),
            train_end=date(2026, 8, 9),
            validate_start=date(2026, 8, 9),
            validate_end=date(2026, 8, 10),
            test_start=date(2026, 8, 11),
            test_end=date(2026, 8, 11),
        )


def test_standard_evidence_cannot_be_removed_from_a_research_plan():
    with pytest.raises(ValueError, match="omit standard binary evidence"):
        EvaluationRules(
            required_evidence=tuple(
                value
                for value in MANDATORY_BINARY_EVIDENCE
                if value != "calibration"
            )
        )


def test_research_plan_checksum_is_separate_and_canonical():
    candidate = CandidateSpec(
        candidate_id="logistic_regression",
        plugin="spark_logistic_regression",
        parameters={"regParam": 0.01, "maxIter": 50},
    )

    assert _plan(candidate).checksum == _plan(candidate).checksum
    assert _plan(candidate).selection_mode == AUTO


def test_shopping_bag_research_registry_declares_four_comparable_candidates():
    plan = load_model_research_plan("shopping_bag_pctr")

    assert plan is not None
    assert plan.selection_policy == REVIEW_REQUIRED
    assert plan.minimum_successful_candidates == 4
    assert plan.temporal_split == _split()
    assert [candidate.candidate_id for candidate in plan.candidates] == [
        "logistic_regression",
        "random_forest",
        "gradient_boosted_trees",
        "spark_xgboost",
    ]
    assert [candidate.plugin for candidate in plan.candidates] == [
        "spark_logistic_regression",
        "spark_random_forest",
        "spark_gradient_boosted_trees",
        "spark_xgboost",
    ]
    assert dict(plan.candidates[0].parameters) == {
        "elasticNetParam": 0.0,
        "maxIter": 50,
        "regParam": 0.01,
    }
    assert dict(plan.candidates[1].parameters) == {
        "maxDepth": 8,
        "minInstancesPerNode": 20,
        "numTrees": 120,
    }
    assert dict(plan.candidates[2].parameters) == {
        "maxDepth": 5,
        "maxIter": 60,
        "stepSize": 0.05,
    }
    assert dict(plan.candidates[3].parameters)["eval_metric"] == "aucpr"
    assert dict(plan.candidates[3].parameters)["num_workers"] == 4
    assert all(candidate.seed == 1729 for candidate in plan.candidates)
    assert all(not candidate.failure_allowed for candidate in plan.candidates)
    assert {slice_spec.slice_id: slice_spec for slice_spec in plan.slices}[
        "shopping_bag_location"
    ].values == ("SB1", "SB2")
    assert {slice_spec.slice_id: slice_spec for slice_spec in plan.slices}[
        "device"
    ].if_present
    assert plan.candidate_search is not None
    assert not plan.candidate_search.enabled
    assert plan.candidate_search.timeout_minutes == 30


def test_research_is_optional_for_existing_model_adopters():
    analytics = load_model_definition("analytics_pctr")
    shopping_bag = load_model_definition("shopping_bag_pctr")

    assert analytics.research is None
    assert load_model_research_plan("analytics_pctr") is None
    assert shopping_bag.research == load_model_research_plan(
        "shopping_bag_pctr"
    )
    assert [name for name, _plan in load_model_research_plans()] == [
        "shopping_bag_pctr"
    ]


def test_model_definition_exposes_research_without_changing_legacy_payload():
    definition = load_model_definition("shopping_bag_pctr")

    assert definition.research is not None
    assert "research" not in definition.as_dict()
    assert definition.as_dict(include_research=True)["research"] == (
        definition.research.as_dict()
    )


def test_model_checksums_pin_legacy_default_and_declared_logical_date():
    assert load_model_definition("analytics_pctr").checksum == (
        "f8713412c4213260d49c97d275ae0627510ea956ab1d29f25c97af09b5deeb7b"
    )
    assert load_model_definition("shopping_bag_pctr").checksum == (
        "00ed7ee0044f6b36fc117a4572463a81947478f761e84d7ec96f251fc547a1f2"
    )


def test_ready_research_receipts_pin_frame_and_mlflow_evidence():
    build = ModelResearchBuild(
        research_build_id="research-build",
        research_attempt_id="research-attempt",
        model_name="shopping_bag_pctr",
        training_receipt_id="training-receipt",
        model_definition_checksum="a" * 64,
        research_plan_checksum="b" * 64,
        evaluation_schema_version="binary_classifier_evidence/v1",
        code_sha="abc123",
        research_frame_id="research-frame",
        research_frame_attempt_id="frame-attempt",
        research_frame_table="catalog.schema.research_frames",
        research_frame_delta_version=42,
        research_frame_row_count=100,
        research_frame_schema_checksum="c" * 64,
        research_frame_data_checksum="d" * 64,
        research_frame_write_receipt_id="write-receipt",
        research_frame_feature_schema_json='{"type":"struct"}',
        research_frame_slice_schema_json='{"type":"struct"}',
        candidate_count=4,
        successful_candidate_count=4,
        status=READY,
        created_at=NOW,
        completed_at=NOW,
        mlflow_experiment_id="experiment",
        mlflow_parent_run_id="parent-run",
        automatic_candidate_id="random_forest",
        artifact_manifest_digest="e" * 64,
    )

    assert build.research_frame_delta_version == 42
    with pytest.raises(ValueError, match="MLflow and evidence identity"):
        ModelResearchBuild(
            **{
                **build.__dict__,
                "mlflow_parent_run_id": None,
            }
        )


def test_candidate_and_selection_receipts_require_complete_evidence():
    candidate = CandidateEvaluation(
        candidate_evaluation_id="candidate-evaluation",
        candidate_attempt_id="candidate-attempt",
        research_build_id="research-build",
        research_attempt_id="research-attempt",
        candidate_id="random_forest",
        candidate_spec_checksum="a" * 64,
        required=True,
        status=READY,
        created_at=NOW,
        completed_at=NOW,
        mlflow_run_id="child-run",
        model_uri="runs:/child-run/model",
        metrics=(("auc_pr", 0.12), ("log_loss", 0.4)),
        artifact_manifest_digest="b" * 64,
        explanation_status=READY,
    )
    automatic = ModelSelectionDecision(
        selection_decision_id="automatic-selection",
        selection_attempt_id="selection-attempt",
        research_build_id="research-build",
        research_attempt_id="research-attempt",
        selection_mode=AUTO,
        recommended_candidate_id="random_forest",
        selected_candidate_id="random_forest",
        selected_candidate_evaluation_id=candidate.candidate_evaluation_id,
        reason="Highest validation PR-AUC",
        status=READY,
        created_at=NOW,
        completed_at=NOW,
        registered_model_name="catalog.schema.model",
        decision_code_sha="decision-sha",
    )
    reviewed = ModelSelectionDecision(
        **{
            **automatic.__dict__,
            "selection_decision_id": "reviewed-selection",
            "selection_mode": REVIEW_REQUIRED,
            "selected_candidate_id": "logistic_regression",
            "reviewed_by": "data.scientist@next.co.uk",
            "reason": "Prefer the calibrated candidate after review",
        }
    )

    assert dict(candidate.metrics)["auc_pr"] == 0.12
    assert (
        automatic.selected_candidate_id == automatic.recommended_candidate_id
    )
    assert reviewed.selected_candidate_id != reviewed.recommended_candidate_id


def test_automl_receipt_is_bounded_and_pinned_to_the_research_frame():
    leaderboard = {
        "schema_version": "nextads_automl_leaderboard/v1",
        "research_build_id": "research-build",
        "discovery_id": "discovery",
        "research_parent_run_id": "research-parent-run",
        "experiment_id": "automl-experiment",
        "primary_metric": "roc_auc",
        "trial_count": 3,
        "best_trial_id": "trial-3",
        "trials": [
            {
                "rank": rank,
                "trial_id": trial_id,
                "primary_metric_value": score,
                "notebook_artifact_uri": f"runs:/{trial_id}/notebook",
                "notebook_path": None,
                "notebook_url": None,
                "is_best_trial": trial_id == "trial-3",
            }
            for rank, (trial_id, score) in enumerate(
                (
                    ("trial-3", 0.83),
                    ("trial-2", 0.78),
                    ("trial-1", 0.72),
                ),
                start=1,
            )
        ],
    }
    encoded = json.dumps(leaderboard, sort_keys=True, separators=(",", ":"))
    receipt = AutoMLDiscoveryReceipt(
        discovery_id="discovery",
        discovery_attempt_id="discovery-attempt",
        request_checksum="c" * 64,
        research_build_id="research-build",
        research_attempt_id="research-attempt",
        research_frame_id="research-frame",
        research_frame_attempt_id="frame-attempt",
        research_frame_table="catalog.schema.research_frames",
        research_frame_delta_version=42,
        research_frame_schema_checksum="a" * 64,
        research_frame_data_checksum="b" * 64,
        research_frame_write_receipt_id="write-receipt",
        research_frame_feature_schema_json='{"type":"struct"}',
        research_frame_slice_schema_json='{"type":"struct"}',
        status=READY,
        timeout_minutes=30,
        trial_count=3,
        created_at=NOW,
        completed_at=NOW,
        experiment_id="automl-experiment",
        best_trial_id="trial-3",
        primary_metric="roc_auc",
        trial_evidence_json=encoded,
        leaderboard_run_id="leaderboard-run",
        leaderboard_artifact_sha256=hashlib.sha256(
            encoded.encode("utf-8")
        ).hexdigest(),
        leaderboard_artifact_uri=(
            "runs:/leaderboard-run/automl_discovery/leaderboard.json"
        ),
        recipe_artifact_uri="runs:/trial-3/notebook",
    )

    assert receipt.timeout_minutes == 30
    with pytest.raises(ValueError, match="cannot exceed 120"):
        AutoMLDiscoveryReceipt(**{**receipt.__dict__, "timeout_minutes": 121})

import argparse
from contextlib import nullcontext
from dataclasses import replace
from datetime import date, datetime, timezone
import inspect
from types import SimpleNamespace

import pytest
import yaml

from jobs.model.research import select_research_candidate as selection_job
from next_ads.model_development.contracts import (
    ModelBuild,
    TrainingFeatureBinding,
    TrainingSetReceipt,
)
from next_ads.model_development.registry import (
    load_model_definition,
    load_model_research_plan,
)
from next_ads.model_development.research_contracts import (
    AWAITING_SELECTION,
    READY,
    CandidateEvaluation,
    ModelResearchBuild,
)


NOW = datetime(2026, 8, 18, 20, 0, tzinfo=timezone.utc)
DIGEST = "a" * 64
DEFINITION = load_model_definition("shopping_bag_pctr")
PLAN = load_model_research_plan("shopping_bag_pctr")
assert PLAN is not None


def _receipt() -> TrainingSetReceipt:
    return TrainingSetReceipt(
        receipt_id="training-receipt",
        model_name=DEFINITION.model_name,
        model_definition_checksum=DEFINITION.checksum,
        feature_bindings=(
            TrainingFeatureBinding(
                feature_id="feature",
                feature_snapshot_id="snapshot",
                feature_snapshot_attempt_id="snapshot-attempt",
                backing_table="catalog.schema.feature",
                delta_version=7,
                row_count=1000,
                schema_checksum="b" * 64,
                value_checksum="c" * 64,
            ),
        ),
        observation_start=date(2026, 8, 5),
        observation_end=date(2026, 8, 11),
        label_end=date(2026, 8, 11),
        schema_checksum="d" * 64,
        data_checksum="e" * 64,
        code_sha="research-sha",
        leakage_status="PASS",
        status=READY,
        created_at=NOW,
        completed_at=NOW,
    )


def _evaluation(
    candidate_id: str,
    *,
    auc_pr: float,
    log_loss: float,
) -> CandidateEvaluation:
    spec = next(
        item for item in PLAN.candidates if item.candidate_id == candidate_id
    )
    return CandidateEvaluation(
        candidate_evaluation_id=f"evaluation-{candidate_id}",
        candidate_attempt_id=f"attempt-{candidate_id}",
        research_build_id="research-build",
        research_attempt_id="research-attempt",
        candidate_id=candidate_id,
        candidate_spec_checksum=spec.checksum,
        required=not spec.failure_allowed,
        status=READY,
        created_at=NOW,
        completed_at=NOW,
        mlflow_run_id=f"run-{candidate_id}",
        model_uri=f"runs:/run-{candidate_id}/model",
        metrics=(("auc_pr", auc_pr), ("log_loss", log_loss)),
        artifact_manifest_digest="f" * 64,
        explanation_status=READY,
    )


def _evaluations() -> tuple[CandidateEvaluation, ...]:
    return (
        _evaluation("logistic_regression", auc_pr=0.10, log_loss=0.60),
        _evaluation("random_forest", auc_pr=0.20, log_loss=0.50),
        _evaluation("gradient_boosted_trees", auc_pr=0.30, log_loss=0.40),
        _evaluation("spark_xgboost", auc_pr=0.30, log_loss=0.40),
    )


def _research_build() -> ModelResearchBuild:
    return ModelResearchBuild(
        research_build_id="research-build",
        research_attempt_id="research-attempt",
        model_name=DEFINITION.model_name,
        training_receipt_id="training-receipt",
        model_definition_checksum=DEFINITION.checksum,
        research_plan_checksum=PLAN.checksum,
        evaluation_schema_version=PLAN.evaluation_schema_version,
        code_sha="research-sha",
        research_frame_id="research-frame",
        research_frame_attempt_id="research-frame-attempt",
        research_frame_table="catalog.schema.research_frame",
        research_frame_delta_version=12,
        research_frame_row_count=1000,
        research_frame_schema_checksum="1" * 64,
        research_frame_data_checksum="2" * 64,
        research_frame_write_receipt_id="frame-write-receipt",
        research_frame_feature_schema_json="{}",
        research_frame_slice_schema_json="{}",
        candidate_count=4,
        successful_candidate_count=4,
        status=AWAITING_SELECTION,
        created_at=NOW,
        completed_at=NOW,
        mlflow_experiment_id="experiment",
        mlflow_parent_run_id="parent-run",
        automatic_candidate_id="gradient_boosted_trees",
        artifact_manifest_digest="3" * 64,
    )


def _args(**updates) -> argparse.Namespace:
    values = {
        "research_build_id": "research-build",
        "candidate_id": "random_forest",
        "written_reason": "More stable lift across Shopping Bag locations.",
        "reviewed_by": "Stephen Blain",
        "model_catalog": "catalog",
        "model_schema": "schema",
        "registered_model_name": "catalog.schema.shopping_bag_pctr",
        "code_sha": "selection-sha",
        "orchestration_run_id": "job-run-1",
        "task_run_id": "task-run-1",
        "execution_count": 0,
        "log_level": "INFO",
    }
    values.update(updates)
    return argparse.Namespace(**values)


def _decision(invocation_id: str = "research-attempt"):
    return selection_job._new_reviewed_decision(
        definition=DEFINITION,
        plan=PLAN,
        build=_research_build(),
        receipt=_receipt(),
        evaluations=_evaluations(),
        candidate_id="random_forest",
        reason="More stable lift across Shopping Bag locations.",
        reviewed_by="Stephen Blain",
        registered_model_name="catalog.schema.shopping_bag_pctr",
        decision_code_sha="selection-sha",
        invocation_id=invocation_id,
        now=NOW,
    )[0]


def _model_build(decision) -> ModelBuild:
    return ModelBuild(
        model_build_id=decision.model_build_id,
        model_name=DEFINITION.model_name,
        training_receipt_id="training-receipt",
        model_definition_checksum=DEFINITION.checksum,
        runtime_profile=DEFINITION.runtime_profile,
        status=READY,
        created_at=NOW,
        mlflow_run_id="run-random_forest",
        registered_model_name="catalog.schema.shopping_bag_pctr",
        registered_model_version=8,
        model_uri="models:/catalog.schema.shopping_bag_pctr/8",
        artifact_digest=DIGEST,
        metrics=(("auc_pr", 0.25), ("log_loss", 0.45)),
        completed_at=NOW,
        research_build_id="research-build",
        selection_decision_id=decision.selection_decision_id,
        selected_candidate_id="random_forest",
        selected_candidate_evaluation_id="evaluation-random_forest",
        registration_code_sha="selection-sha",
    )


def test_cli_and_job_resource_require_review_identity_and_reason():
    parsed = selection_job.parse_args(
        [
            "--research_build_id",
            " research-build ",
            "--candidate_id",
            "random_forest",
            "--written_reason",
            "stable validation slices",
            "--reviewed_by",
            "Stephen Blain",
            "--model_catalog",
            "catalog",
            "--model_schema",
            "schema",
            "--registered_model_name",
            "catalog.schema.model",
            "--code_sha",
            "abc123",
            "--orchestration_run_id",
            "1",
            "--task_run_id",
            "2",
            "--execution_count",
            "0",
        ]
    )
    assert parsed.research_build_id == "research-build"
    assert parsed.candidate_id == "random_forest"
    assert parsed.reviewed_by == "Stephen Blain"
    with pytest.raises(SystemExit):
        selection_job.parse_args(
            [
                "--research_build_id",
                "REQUIRED",
                "--candidate_id",
                "REQUIRED",
            ]
        )

    resource = yaml.safe_load(
        (
            selection_job.PROJECT_ROOT
            / "pipelines"
            / "databricks"
            / "jobs"
            / "mktg_next_uk_nextads_model_research_selection.yml"
        ).read_text()
    )["model_research_selection_job"]
    parameters = {item["name"] for item in resource["parameters"]}
    task_parameters = resource["tasks"][0]["spark_python_task"]["parameters"]
    assert {"candidate_id", "written_reason", "reviewed_by"} <= parameters
    assert "selected_candidate_id" not in parameters
    assert "--reviewed_by" in task_parameters


def test_entrypoint_bootstraps_before_repository_runtime_imports():
    source = inspect.getsource(selection_job)
    run_source = inspect.getsource(selection_job.run_selection)
    bootstrap = source.index("sys.path.insert(0")
    project_import = source.index("from next_ads")

    assert bootstrap < project_import
    assert "promotion_mode" not in source
    assert "read_selected_test_frame" in source
    assert source.index("selection_decision_id(") < source.index(
        "read_selected_test_frame("
    )
    assert run_source.index("_reuse_decision(") < run_source.index(
        "read_selected_test_frame("
    )


def test_reviewed_decision_keeps_the_automatic_recommendation_and_override():
    first = _decision("job-1:task-1:0")
    retry = _decision("job-2:task-2:0")

    assert first.selection_mode == "REVIEW_REQUIRED"
    assert first.recommended_candidate_id == "gradient_boosted_trees"
    assert first.selected_candidate_id == "random_forest"
    assert first.reviewed_by == "Stephen Blain"
    assert first.selection_decision_id == retry.selection_decision_id
    assert first.model_build_id == retry.model_build_id
    assert first.selection_attempt_id != retry.selection_attempt_id


def test_reviewed_retry_rejects_a_different_registration_target():
    locked = _decision()
    changed, _candidate = selection_job._new_reviewed_decision(
        definition=DEFINITION,
        plan=PLAN,
        build=_research_build(),
        receipt=_receipt(),
        evaluations=_evaluations(),
        candidate_id="random_forest",
        reason="More stable lift across Shopping Bag locations.",
        reviewed_by="Stephen Blain",
        registered_model_name="catalog.schema.another_model",
        decision_code_sha="fix-sha",
        invocation_id="research-attempt",
        now=NOW,
    )

    assert changed.model_build_id != locked.model_build_id
    with pytest.raises(ValueError, match="registered_model_name"):
        selection_job._reuse_decision(changed, locked)


def test_effective_review_plan_supports_an_auto_default(monkeypatch):
    auto_default = replace(PLAN, selection_policy="AUTO")
    reviewed = replace(auto_default, selection_policy="REVIEW_REQUIRED")
    monkeypatch.setattr(
        selection_job,
        "load_model_research_plan",
        lambda _model_name: auto_default,
    )

    resolved = selection_job._effective_review_plan(
        "shopping_bag_pctr",
        reviewed.checksum,
    )

    assert resolved.selection_policy == "REVIEW_REQUIRED"
    assert resolved.checksum == reviewed.checksum


def test_reviewed_test_requires_optional_slices_present_in_source(monkeypatch):
    captured = []
    double_type = SimpleNamespace(simpleString=lambda: "double")
    predictions = SimpleNamespace(
        columns=["row_id", "label", "split", "observation_date", "location"],
        schema=SimpleNamespace(
            fields=(
                SimpleNamespace(name="prediction", dataType=double_type),
                SimpleNamespace(name="score", dataType=double_type),
            )
        ),
    )
    test_frame = SimpleNamespace(
        columns=[
            "row_id",
            "label",
            "split",
            "observation_date",
            "location",
            "device",
        ]
    )
    monkeypatch.setattr(
        selection_job,
        "validate_prediction_adapter_output",
        lambda *_args, **kwargs: captured.append(kwargs["slice_columns"]),
    )
    monkeypatch.setattr(
        selection_job,
        "evaluate_binary_predictions",
        lambda *_args, **_kwargs: {"status": "COMPLETE", "metrics": {}},
    )
    monkeypatch.setattr(
        selection_job,
        "require_complete_binary_evaluation",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        selection_job,
        "deterministic_selected_test_confidence_intervals",
        lambda *_args, **_kwargs: {"status": "COMPLETE"},
    )
    monkeypatch.setattr(
        selection_job,
        "require_complete_confidence_intervals",
        lambda *_args, **_kwargs: None,
    )

    selection_job._evaluate_selected_test(
        SimpleNamespace(transform=lambda _frame: predictions),
        test_frame,
        plan=PLAN,
    )

    assert captured == [("location", "device")]


def test_research_inputs_reject_candidate_from_another_attempt():
    values = _evaluations()[0].__dict__.copy()
    values["research_attempt_id"] = "other-attempt"
    invalid = CandidateEvaluation(**values)

    with pytest.raises(ValueError, match="different research attempt"):
        selection_job._validate_research_inputs(
            DEFINITION,
            PLAN,
            _research_build(),
            _receipt(),
            (invalid, *_evaluations()[1:]),
        )


def test_ready_decision_retry_reuses_only_the_identical_choice():
    expected = _decision()
    reused, is_reused = selection_job._reuse_decision(expected, expected)

    assert reused is expected
    assert is_reused is True
    changed = expected.__dict__.copy()
    changed["reviewed_by"] = "Different Reviewer"
    with pytest.raises(ValueError, match="different READY selection"):
        selection_job._reuse_decision(
            expected,
            type(expected)(**changed),
        )


class _FakeMlflow:
    def __init__(self):
        self.started = []
        self.dicts = []
        self.metrics = []
        self.tags = []
        self.tracking_uri = None
        self.registry_uri = None
        self.spark = SimpleNamespace(load_model=lambda uri: f"model:{uri}")

    def set_tracking_uri(self, value):
        self.tracking_uri = value

    def set_registry_uri(self, value):
        self.registry_uri = value

    def start_run(self, *, run_id):
        self.started.append(run_id)
        return nullcontext()

    def log_dict(self, value, path):
        self.dicts.append((path, value))

    def log_metrics(self, values):
        self.metrics.append(values)

    def set_tags(self, values):
        self.tags.append(values)


def test_selected_test_logging_targets_only_the_selected_child_run():
    mlflow = _FakeMlflow()
    decision = _decision()
    candidate = _evaluations()[1]
    selection_job._log_selected_test_evidence(
        mlflow,
        candidate=candidate,
        decision=decision,
        evaluation={
            "status": "COMPLETE",
            "metrics": {"auc_pr": 0.25, "log_loss": 0.45},
            "precision_recall_curve": [{"recall": 0.5, "precision": 0.2}],
        },
        confidence_intervals={
            "auc_pr": {"lower": 0.20, "estimate": 0.25, "upper": 0.30}
        },
    )

    assert mlflow.started == [candidate.mlflow_run_id]
    assert [path for path, _value in mlflow.dicts] == [
        "selected_test/evaluation.json",
        "selected_test/confidence_intervals.json",
    ]
    assert mlflow.metrics == [{"test_auc_pr": 0.25, "test_log_loss": 0.45}]
    assert mlflow.tags[0]["nextads_selection_decision_id"] == (
        decision.selection_decision_id
    )


def test_reviewed_choice_is_logged_back_to_the_parent_run_with_a_digest():
    mlflow = _FakeMlflow()
    decision = _decision()
    model_build = _model_build(decision)

    digest = selection_job._log_parent_selection(
        mlflow,
        research_build=_research_build(),
        decision=decision,
        model_build=model_build,
        test_evaluation={
            "metrics": {"auc_pr": 0.25, "log_loss": 0.45},
        },
        confidence_intervals={
            "auc_pr": {"lower": 0.20, "estimate": 0.25, "upper": 0.30}
        },
        score_reproduction_checksum="9" * 64,
        selection_execution_code_sha="fix-sha",
    )

    assert len(digest) == 64
    assert mlflow.started == ["parent-run"]
    assert [path for path, _value in mlflow.dicts] == [
        "research/reviewed_selection.json",
        "research/reviewed_selection_manifest.json",
    ]
    payload = mlflow.dicts[0][1]
    assert payload["research_code_sha"] == "research-sha"
    assert payload["decision_code_sha"] == "selection-sha"
    assert payload["registration_code_sha"] == "selection-sha"
    assert payload["selection_execution_code_sha"] == "fix-sha"
    assert mlflow.tags[0]["nextads_selected_candidate_id"] == "random_forest"
    assert mlflow.tags[0]["nextads_research_code_sha"] == "research-sha"
    assert mlflow.tags[0]["nextads_decision_code_sha"] == "selection-sha"
    assert mlflow.tags[0]["nextads_registration_code_sha"] == "selection-sha"
    assert mlflow.tags[0]["nextads_selection_execution_code_sha"] == (
        "fix-sha"
    )


@pytest.mark.parametrize("retry", [False, True])
def test_selection_is_persisted_before_test_and_retry_does_not_register(
    monkeypatch,
    retry,
):
    args = _args(code_sha="fix-sha" if retry else "selection-sha")
    research_build = _research_build()
    receipt = _receipt()
    evaluations = _evaluations()
    expected = _decision()
    ready_build = _model_build(expected)
    events = []
    register_calls = []
    mlflow = _FakeMlflow()

    monkeypatch.setattr(
        selection_job,
        "_load_research_inputs",
        lambda *_args, **_kwargs: (
            DEFINITION,
            PLAN,
            research_build,
            receipt,
            evaluations,
        ),
    )
    monkeypatch.setattr(
        selection_job,
        "_load_ready_decision_for_attempt",
        lambda *_args, **_kwargs: expected if retry else None,
    )
    claim_state = [
        SimpleNamespace(
            checkpoint=(
                selection_job.CLAIM_COMPLETE
                if retry
                else selection_job.CLAIM_CANDIDATES_READY
            ),
            lease_token="lease-token",
            selection_decision_id=(
                expected.selection_decision_id if retry else None
            ),
            model_build_id=expected.model_build_id if retry else None,
            failure_reason=None,
        )
    ]
    monkeypatch.setattr(
        selection_job,
        "claim_research_build",
        lambda *_args, **_kwargs: claim_state[0],
    )

    def advance_claim(*_args, checkpoint, **kwargs):
        events.append(("claim", checkpoint))
        claim_state[0] = SimpleNamespace(
            checkpoint=checkpoint,
            lease_token="lease-token",
            selection_decision_id=kwargs.get(
                "selection_decision_id",
                claim_state[0].selection_decision_id,
            ),
            model_build_id=kwargs.get(
                "model_build_id", claim_state[0].model_build_id
            ),
            failure_reason=None,
        )
        return claim_state[0]

    monkeypatch.setattr(
        selection_job,
        "advance_research_claim",
        advance_claim,
    )

    def read_test(*_args, selection_decision_id, **_kwargs):
        events.append(("read_test", selection_decision_id))
        return "untouched-test"

    monkeypatch.setattr(selection_job, "read_selected_test_frame", read_test)
    monkeypatch.setattr(
        selection_job,
        "_evaluate_selected_test",
        lambda *_args, **_kwargs: (
            "selected-predictions",
            {"metrics": {"auc_pr": 0.25, "log_loss": 0.45}},
            {"auc_pr": {"lower": 0.20, "upper": 0.30}},
        ),
    )
    monkeypatch.setattr(
        selection_job,
        "_log_selected_test_evidence",
        lambda *_args, **_kwargs: events.append(("log_test", None)),
    )
    monkeypatch.setattr(
        selection_job,
        "load_ready_model_build",
        lambda *_args, **_kwargs: ready_build if retry else None,
    )

    def register(*call_args, **_kwargs):
        decision = call_args[4]
        register_calls.append(decision.selection_decision_id)
        return _model_build(decision)

    monkeypatch.setattr(selection_job, "register_selected_candidate", register)
    monkeypatch.setattr(
        selection_job,
        "validate_registered_model_build",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        selection_job,
        "validate_score_reproduction",
        lambda *_args, **_kwargs: "9" * 64,
    )
    monkeypatch.setattr(
        selection_job,
        "persist_model_build",
        lambda *_args, **_kwargs: events.append(("persist_build", None)),
    )
    monkeypatch.setattr(
        selection_job,
        "persist_selection_decision",
        lambda *_args, **_kwargs: events.append(("persist_decision", None)),
    )

    evidence = selection_job.run_selection(
        args,
        spark=object(),
        mlflow_module=mlflow,
        client=object(),
        now=NOW,
    )

    assert evidence["reused"] is retry
    assert evidence["candidate_id"] == "random_forest"
    assert evidence["recommended_candidate_id"] == "gradient_boosted_trees"
    if retry:
        assert events[0] == ("read_test", expected.selection_decision_id)
    else:
        assert events[0] == ("persist_decision", None)
        assert events[1] == ("claim", selection_job.CLAIM_SELECTION_LOCKED)
        assert events.index(("persist_decision", None)) < events.index(
            ("read_test", expected.selection_decision_id)
        )
    assert events.index(("read_test", expected.selection_decision_id)) < (
        events.index(("persist_build", None))
    )
    assert len(register_calls) == (0 if retry else 1)
    assert ("set_alias", None) not in events
    assert evidence["research_code_sha"] == "research-sha"
    assert evidence["decision_code_sha"] == "selection-sha"
    assert evidence["registration_code_sha"] == "selection-sha"
    assert evidence["selection_execution_code_sha"] == (
        "fix-sha" if retry else "selection-sha"
    )

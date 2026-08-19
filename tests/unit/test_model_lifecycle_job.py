import argparse
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from jobs.model.development import run_declared_model_operation as lifecycle
from jobs.model.research import run_declared_research


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOBS_ROOT = PROJECT_ROOT / "pipelines" / "databricks" / "jobs"


def _args(**updates) -> argparse.Namespace:
    values = {
        "operation": "BUILD",
        "model_name": "shopping_bag_pctr",
        "feature_catalog": "marketingdata_dev",
        "feature_schema": "personal",
        "model_catalog": "marketingdata_dev",
        "model_schema": "personal",
        "experiment_root": "/Workspace/bundle-root",
        "observation_reference_dates": "2026-08-05,2026-08-06",
        "feature_reference_dates": "2026-08-04,2026-08-05",
        "label_end": "2026-08-19",
        "research_build_id": "",
        "candidate_id": "",
        "written_reason": "",
        "reviewed_by": "",
        "model_build_id": "",
        "run_date": "",
        "evaluation_account_limit": "",
        "evaluation_serving_slot": "",
        "evaluation_candidate_build_attempt_id": "",
        "code_sha": "abc123",
        "orchestration_run_id": 1,
        "task_run_id": 2,
        "execution_count": 0,
        "log_level": "INFO",
    }
    values.update(updates)
    return argparse.Namespace(**values)


def _handlers(calls):
    def handler(args, **kwargs):
        calls.append((args, kwargs))
        return {"status": "READY"}

    return {operation: handler for operation in lifecycle.OPERATIONS}


def test_shared_job_keeps_one_generic_saved_resource():
    config = yaml.safe_load(
        (JOBS_ROOT / "mktg_next_uk_nextads_model_development.yml").read_text()
    )
    job = config["model_development_job"]
    parameter_names = [item["name"] for item in job["parameters"]]
    task = job["tasks"][0]

    assert job["name"] == "mktg_next_uk_nextads_model_development"
    assert set(config["targets"]) == {"DEV"}
    assert parameter_names[:2] == ["operation", "model_name"]
    assert job["parameters"][0]["default"] == "REQUIRED"
    assert job["parameters"][1]["default"] == "REQUIRED"
    assert task["spark_python_task"]["python_file"].endswith(
        "run_declared_model_operation.py"
    )
    assert task["libraries"] == "${var.model_research_libraries}"
    assert (
        "promotion"
        not in (
            JOBS_ROOT / "mktg_next_uk_nextads_model_development.yml"
        ).read_text()
    )
    assert (
        "shopping_bag"
        not in (
            JOBS_ROOT / "mktg_next_uk_nextads_model_development.yml"
        ).read_text()
    )


def test_operation_validation_rejects_missing_and_irrelevant_fields_first():
    calls = []
    with pytest.raises(ValueError, match="label_end is required for BUILD"):
        lifecycle.run_operation(
            _args(label_end=""),
            spark=object(),
            handlers=_handlers(calls),
        )
    with pytest.raises(ValueError, match="Fields are not used by RESEARCH"):
        lifecycle.run_operation(
            _args(
                operation="RESEARCH",
                observation_reference_dates="",
                feature_reference_dates="",
                research_build_id="unexpected",
            ),
            spark=object(),
            handlers=_handlers(calls),
        )

    assert calls == []


def test_build_derives_registry_experiment_and_provider_names():
    calls = []
    spark = object()
    result = lifecycle.run_operation(
        _args(), spark=spark, handlers=_handlers(calls)
    )
    handler_args, kwargs = calls[0]

    assert result["operation"] == lifecycle.BUILD
    assert handler_args.registered_model_name == (
        "marketingdata_dev.personal.nextads_shopping_bag_pctr"
    )
    assert handler_args.experiment_path == (
        "/Workspace/bundle-root/shopping_bag_pctr"
    )
    assert handler_args.provider_signals_table.endswith(
        ".next_uk_nextads_score_provider_signals"
    )
    assert not hasattr(handler_args, "promotion_mode")
    assert kwargs == {"spark": spark}


def test_build_rejects_an_unimplemented_trainer_before_handler_or_writes():
    calls = []

    with pytest.raises(
        ValueError,
        match="Unknown trainer plug-in: analytics_pctr_two_stage_xgboost",
    ):
        lifecycle.run_operation(
            _args(model_name="analytics_pctr"),
            spark=object(),
            handlers=_handlers(calls),
        )

    assert calls == []


def test_research_uses_declared_dates_and_policy_without_repeating_them():
    args = _args(
        operation="RESEARCH",
        observation_reference_dates="",
        feature_reference_dates="",
    )
    calls = []
    lifecycle.run_operation(args, spark=object(), handlers=_handlers(calls))
    handler_args = calls[0][0]
    plan = lifecycle.load_model_research_plan("shopping_bag_pctr")
    train, validation, test, features = run_declared_research._declared_dates(
        plan
    )

    assert tuple(value.isoformat() for value in train) == (
        "2026-08-05",
        "2026-08-06",
        "2026-08-07",
        "2026-08-08",
    )
    assert tuple(value.isoformat() for value in validation) == (
        "2026-08-09",
        "2026-08-10",
    )
    assert tuple(value.isoformat() for value in test) == ("2026-08-11",)
    assert len(features) == 7
    assert handler_args.train_reference_dates is None
    assert handler_args.selection_mode is None
    assert handler_args.experiment_path == (
        "/Workspace/bundle-root/shopping_bag_pctr_research"
    )


def test_research_rejects_unknown_candidate_before_handler_or_writes(
    monkeypatch,
):
    plan = lifecycle.load_model_research_plan("shopping_bag_pctr")
    invalid_candidate = replace(
        plan.candidates[0],
        plugin="unknown_research_candidate",
    )
    monkeypatch.setattr(
        lifecycle,
        "load_model_research_plan",
        lambda _model_name: replace(
            plan,
            candidates=(invalid_candidate, *plan.candidates[1:]),
        ),
    )
    calls = []

    with pytest.raises(ValueError, match="Custom research plug-ins"):
        lifecycle.run_operation(
            _args(
                operation="RESEARCH",
                observation_reference_dates="",
                feature_reference_dates="",
            ),
            spark=object(),
            handlers=_handlers(calls),
        )

    assert calls == []


def test_research_rejects_unknown_evidence_producer_before_writes(
    monkeypatch,
):
    plan = lifecycle.load_model_research_plan("shopping_bag_pctr")
    monkeypatch.setattr(
        lifecycle,
        "load_model_research_plan",
        lambda _model_name: replace(
            plan,
            evidence_producers=("unknown_evidence_producer",),
        ),
    )
    calls = []

    with pytest.raises(ValueError, match="Custom research plug-ins"):
        lifecycle.run_operation(
            _args(
                operation="RESEARCH",
                observation_reference_dates="",
                feature_reference_dates="",
            ),
            spark=object(),
            handlers=_handlers(calls),
        )

    assert calls == []


def test_review_and_evaluate_are_explicit_bounded_operations():
    review_calls = []
    lifecycle.run_operation(
        _args(
            operation="REVIEW_SELECT",
            observation_reference_dates="",
            feature_reference_dates="",
            label_end="",
            research_build_id="research-1",
            candidate_id="candidate-1",
            written_reason="Reviewed evidence",
            reviewed_by="reviewer@example.com",
        ),
        spark=object(),
        handlers=_handlers(review_calls),
    )
    assert review_calls[0][0].research_build_id == "research-1"

    evaluation_calls = []
    lifecycle.run_operation(
        _args(
            operation="EVALUATE",
            observation_reference_dates="",
            feature_reference_dates="",
            label_end="",
            model_build_id="model-1",
            run_date="2026-08-19",
        ),
        spark=object(),
        handlers=_handlers(evaluation_calls),
    )
    evaluation_args = evaluation_calls[0][0]
    assert evaluation_args.feature_reference_dates == "AUTO"
    assert evaluation_args.account_limit == 10000
    assert evaluation_args.candidate_builds_table.endswith(
        ".next_uk_nextads_candidate_builds"
    )


def test_evaluate_rejects_unknown_score_provider_before_writes(monkeypatch):
    definition = lifecycle.load_model_definition("shopping_bag_pctr")
    monkeypatch.setattr(
        lifecycle,
        "load_model_definition",
        lambda _model_name: replace(
            definition,
            score_provider="unknown_score_provider",
        ),
    )
    calls = []

    with pytest.raises(
        ValueError,
        match="Unknown score provider plug-in: unknown_score_provider",
    ):
        lifecycle.run_operation(
            _args(
                operation="EVALUATE",
                observation_reference_dates="",
                feature_reference_dates="",
                label_end="",
                model_build_id="model-1",
                run_date="2026-08-19",
            ),
            spark=object(),
            handlers=_handlers(calls),
        )

    assert calls == []


def test_evaluate_requires_the_scoped_provider_contract_before_writes(
    monkeypatch,
):
    class UnscopedRegistry:
        def score_provider(self, *_args, **_kwargs):
            return SimpleNamespace(score=lambda *_args, **_kwargs: None)

    monkeypatch.setattr(lifecycle, "ModelPluginRegistry", UnscopedRegistry)
    calls = []

    with pytest.raises(
        ValueError,
        match="does not implement score_with_evaluation_scope",
    ):
        lifecycle.run_operation(
            _args(
                operation="EVALUATE",
                observation_reference_dates="",
                feature_reference_dates="",
                label_end="",
                model_build_id="model-1",
                run_date="2026-08-19",
            ),
            spark=object(),
            handlers=_handlers(calls),
        )

    assert calls == []


def test_automl_remains_separate_generic_non_registering_discovery():
    resource = JOBS_ROOT / "mktg_next_uk_nextads_model_research_automl.yml"
    config = yaml.safe_load(resource.read_text())
    job = config["model_research_automl_job"]
    task = job["tasks"][0]
    parameter_names = [item["name"] for item in job["parameters"]]

    assert job["name"] == "mktg_next_uk_nextads_model_discovery"
    assert "model_name" in parameter_names
    assert "experiment_path" not in parameter_names
    assert "libraries" not in task
    assert job["job_clusters"] == (
        "${var.model_research_automl_job_clusters_config}"
    )
    assert job["max_concurrent_runs"] == 1
    assert job["timeout_seconds"] == 9000
    assert "shopping_bag" not in resource.read_text()
    task_parameters = task["spark_python_task"]["parameters"]
    experiment_index = task_parameters.index("--experiment_path")
    assert task_parameters[experiment_index + 1] == (
        "${workspace.root_path}/{{job.parameters.model_name}}_automl"
    )
    assert (
        "register_model"
        not in (
            PROJECT_ROOT
            / "jobs"
            / "model"
            / "research"
            / "run_automl_discovery.py"
        ).read_text()
    )

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from jobs.orchestration import (
    validate_model_scoring_request as model_scoring_validator_module,
    validate_nextads_operation as nextads_operation_validator_module,
)
from jobs.orchestration.validate_model_scoring_request import (
    validate_model_scoring_request,
)
from jobs.orchestration.validate_nextads_operation import (
    CANDIDATE_BUILD,
    PREPARE_SCORING_INPUTS,
    validate_operation,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _job(path: str, config_key: str, resource_key: str):
    config = yaml.safe_load((PROJECT_ROOT / path).read_text())
    return config[config_key][resource_key]


def _tasks(job):
    return {task["task_key"]: task for task in job["tasks"]}


def test_nextads_operation_validator_fails_closed():
    assert validate_operation(CANDIDATE_BUILD) == CANDIDATE_BUILD
    assert validate_operation(PREPARE_SCORING_INPUTS) == (
        PREPARE_SCORING_INPUTS
    )

    with pytest.raises(ValueError, match="operation must be one of"):
        validate_operation("candidate_build")
    with pytest.raises(ValueError, match="operation must be one of"):
        validate_operation(f" {PREPARE_SCORING_INPUTS} ")
    with pytest.raises(ValueError, match="operation must be one of"):
        validate_operation("")


def test_model_scoring_validator_accepts_only_an_owned_implementation():
    request = validate_model_scoring_request("theme_affinity")

    assert request == {
        "model_name": "theme_affinity",
        "provider_id": "theme_affinity",
        "implementation": "theme_affinity",
        "compatibility_publisher": "theme_affinity_legacy",
        "foundation_id": "account_theme_features",
    }
    with pytest.raises(
        ValueError,
        match="No operational scoring implementation",
    ):
        validate_model_scoring_request("analytics_pctr")
    with pytest.raises(ValueError, match="scoring provider unknown"):
        validate_model_scoring_request("unknown")


def test_model_scoring_validator_resolves_databricks_workspace_path(
    monkeypatch,
):
    dbutils = MagicMock()
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils.return_value
        .notebook.return_value.getContext.return_value
        .notebookPath.return_value.get
    )
    notebook_path.return_value = (
        "/Workspace/release/files/jobs/orchestration/"
        "validate_model_scoring_request.py"
    )
    monkeypatch.setattr("dsutils.dbc.get_dbutils", lambda: dbutils)
    monkeypatch.delitem(
        model_scoring_validator_module.__dict__,
        "__file__",
    )

    assert model_scoring_validator_module.resolve_project_root() == Path(
        "/Workspace/release/files"
    )


def test_nextads_operation_validator_resolves_databricks_workspace_path(
    monkeypatch,
):
    dbutils = MagicMock()
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils.return_value
        .notebook.return_value.getContext.return_value
        .notebookPath.return_value.get
    )
    notebook_path.return_value = (
        "/Workspace/release/files/jobs/orchestration/"
        "validate_nextads_operation.py"
    )
    monkeypatch.setattr("dsutils.dbc.get_dbutils", lambda: dbutils)
    monkeypatch.delitem(
        nextads_operation_validator_module.__dict__,
        "__file__",
    )

    assert nextads_operation_validator_module.resolve_project_root() == Path(
        "/Workspace/release/files"
    )


def test_model_scoring_validator_rejects_theme_implementation_alias(tmp_path):
    config_path = tmp_path / "scoring_settings.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "default": {
                    "scoring": {
                        "providers": {
                            "theme_affinity_alias": {
                                "provider_id": "theme_affinity_alias",
                                "implementation": "theme_affinity",
                                "capability": "account_theme",
                                "compatibility_publisher": (
                                    "theme_affinity_legacy"
                                ),
                                "foundation_id": "account_theme_features",
                            }
                        }
                    }
                }
            }
        )
    )

    with pytest.raises(
        ValueError,
        match=(
            "No operational scoring implementation for "
            "theme_affinity_alias"
        ),
    ):
        validate_model_scoring_request(
            "theme_affinity_alias",
            config_path=config_path,
        )


def test_model_scoring_runs_same_day_inputs_before_scoring():
    scoring = _job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_model_scoring.yml",
        "mktg_next_uk_nextads_model_scoring_config",
        "mktg_next_uk_nextads_theme_affinity_cicd",
    )
    tasks = _tasks(scoring)
    parameters = {
        item["name"]: item["default"] for item in scoring["parameters"]
    }

    assert scoring["name"] == "mktg_next_uk_nextads_model_scoring"
    assert scoring["schedule"] == {
        "quartz_cron_expression": "0 15 12 * * ?",
        "timezone_id": "Europe/London",
    }
    assert scoring["timeout_seconds"] == 72000
    assert parameters["model_name"] == "theme_affinity"
    assert parameters["table_suffixes"] == (
        "${var.theme_affinity_publish_table_suffixes}"
    )
    child_tasks = [
        task for task in scoring["tasks"] if "run_job_task" in task
    ]
    assert [task["task_key"] for task in child_tasks] == [
        "prepare_scoring_inputs"
    ]
    assert tasks["prepare_scoring_inputs"]["run_job_task"] == {
        "job_id": "${resources.jobs.mktg_next_uk_nextads_cicd.id}",
        "job_parameters": {
            "operation": "PREPARE_SCORING_INPUTS",
            "run_date": "{{job.parameters.run_date}}",
        },
    }
    assert tasks["prepare_foundation_context"]["depends_on"] == [
        {"task_key": "prepare_scoring_inputs"},
        {"task_key": "use_theme_affinity_scoring", "outcome": "true"},
    ]


def test_compatibility_runs_only_after_the_provider_is_ready():
    scoring = _job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_model_scoring.yml",
        "mktg_next_uk_nextads_model_scoring_config",
        "mktg_next_uk_nextads_theme_affinity_cicd",
    )
    tasks = _tasks(scoring)

    for task_key in (
        "publish_provider_compatibility",
        "publish_feature_compatibility",
    ):
        assert tasks[task_key]["depends_on"] == [
            {"task_key": "publish_and_score"}
        ]
    feature_parameters = tasks["publish_feature_compatibility"][
        "spark_python_task"
    ]["parameters"]
    assert feature_parameters[
        feature_parameters.index("--table_suffixes") + 1
    ] == "{{job.parameters.table_suffixes}}"


def test_main_job_keeps_1800_candidates_and_exposes_input_only_branch():
    main = _job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_config",
        "mktg_next_uk_nextads_cicd",
    )
    tasks = _tasks(main)
    parameters = {
        item["name"]: item["default"] for item in main["parameters"]
    }

    assert main["schedule"]["quartz_cron_expression"] == "0 0 18 * * ?"
    assert parameters["operation"] == CANDIDATE_BUILD
    assert tasks["prepare_scoring_inputs_operation"]["depends_on"] == [
        {"task_key": "validate_operation"}
    ]
    child_job_ids = {
        task["run_job_task"]["job_id"]
        for task in main["tasks"]
        if "run_job_task" in task
    }
    assert (
        "${resources.jobs.mktg_next_uk_nextads_theme_affinity_cicd.id}"
        not in child_job_ids
    )
    for task_key in (
        "land_authoritative_theme_mapping",
        "refresh_item_attributes",
    ):
        assert tasks[task_key]["depends_on"] == [
            {
                "task_key": "prepare_scoring_inputs_operation",
                "outcome": "true",
            }
        ]
    for task_key in (
        "select_candidate_foundation",
        "load_control_sheet_v1",
        "load_control_sheet_v2",
        "resolve_scoring_portfolio_v1",
        "resolve_scoring_portfolio_v2",
    ):
        assert tasks[task_key]["depends_on"] == [
            {
                "task_key": "prepare_scoring_inputs_operation",
                "outcome": "false",
            }
        ]

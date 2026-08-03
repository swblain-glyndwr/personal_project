import json
from pathlib import Path

import pytest
import yaml

from tests.job_resource_helpers import load_job


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_job(path, key):
    return load_job(path, key)


def _tasks_by_key(job):
    return {task["task_key"]: task for task in job["tasks"]}


def _job_parameters(job):
    return {
        parameter["name"]: parameter["default"]
        for parameter in job.get("parameters", [])
    }


def _dependency_keys(task):
    return {
        dependency["task_key"] for dependency in task.get("depends_on", [])
    }


def _ancestors(tasks_by_key, task_key):
    ancestors = set()
    pending = list(_dependency_keys(tasks_by_key[task_key]))
    while pending:
        dependency = pending.pop()
        if dependency in ancestors:
            continue
        ancestors.add(dependency)
        pending.extend(_dependency_keys(tasks_by_key[dependency]))
    return ancestors


def _descendants(tasks_by_key, task_key):
    descendants = set()
    pending = [task_key]
    while pending:
        dependency = pending.pop()
        for candidate_key, candidate in tasks_by_key.items():
            if candidate_key in descendants:
                continue
            if dependency in _dependency_keys(candidate):
                descendants.add(candidate_key)
                pending.append(candidate_key)
    return descendants


def _selector_values(route):
    selector = f"select_score_provider_build_{route}"
    return {
        "provider_build_id": (
            f"{{{{tasks.{selector}.values.provider_build_id}}}}"
        ),
        "provider_signals_table": (
            f"{{{{tasks.{selector}.values.provider_signals_table}}}}"
        ),
        "provider_signals_delta_version": (
            f"{{{{tasks.{selector}.values.provider_signals_delta_version}}}}"
        ),
        "input_snapshot_id": (
            f"{{{{tasks.{selector}.values.input_snapshot_id}}}}"
        ),
        "scoring_foundation_build_id": (
            f"{{{{tasks.{selector}.values.scoring_foundation_build_id}}}}"
        ),
        "provider_selection_status": (
            f"{{{{tasks.{selector}.values.provider_selection_status}}}}"
        ),
        "provider_source_run_date": (
            f"{{{{tasks.{selector}.values.provider_source_run_date}}}}"
        ),
    }


def _expected_page_job_parameters(route):
    return {
        "run_date": "{{job.parameters.run_date}}",
        **_selector_values(route),
        "build_run_id": f"{route}_{{{{job.run_id}}}}",
    }


def _forwarded_page_identity_parameters():
    names = (
        "run_date",
        "provider_build_id",
        "provider_signals_table",
        "provider_signals_delta_version",
        "input_snapshot_id",
        "scoring_foundation_build_id",
        "provider_selection_status",
        "provider_source_run_date",
        "build_run_id",
    )
    return {name: f"{{{{job.parameters.{name}}}}}" for name in names}


def _assert_delivery_boundary_parameters(job):
    parameters = _job_parameters(job)
    expected = set(_forwarded_page_identity_parameters())
    assert set(parameters) == expected
    assert parameters["run_date"] == "{{job.start_time.iso_date}}"
    for name in expected - {"run_date"}:
        assert parameters[name] == ""


def _assert_cli_values(task, expected):
    parameters = task["spark_python_task"]["parameters"]
    for option, value in expected.items():
        index = parameters.index(option)
        assert parameters[index + 1] == value


def test_main_job_waits_for_native_page_build_results():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )

    tasks_by_key = _tasks_by_key(job)
    trigger_v1_task = tasks_by_key["run_page_build_v1"]
    trigger_v2_task = tasks_by_key["run_page_build_v2"]
    data_pull_task = tasks_by_key["trigger_data_pull_for_CMS_pull"]

    assert job["name"] == "mktg_next_uk_nextads_candidate_build"
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_team_notification_emails}"
    )
    assert "build_page_primary" not in tasks_by_key
    assert "build_page_v2" not in tasks_by_key
    run_job_tasks = {
        task["task_key"] for task in job["tasks"] if "run_job_task" in task
    }
    assert run_job_tasks == {
        "trigger_data_pull_for_CMS_pull",
        "run_page_build_v1",
        "run_page_build_v2",
    }
    assert data_pull_task["run_job_task"]["job_parameters"] == {
        "run_date": "{{job.parameters.run_date}}",
    }
    assert trigger_v1_task["depends_on"] == [
        {"task_key": "combine_customer_cells"},
        {"task_key": "map_theme_scores_to_ads_v1"},
    ]
    assert trigger_v2_task["depends_on"] == [
        {"task_key": "combine_customer_cells"},
        {"task_key": "map_theme_scores_to_ads_v2"},
    ]
    assert trigger_v1_task["run_job_task"] == {
        "job_id": (
            "${resources.jobs."
            "mktg_next_uk_nextads_page_build_cicd.id}"
        ),
        "job_parameters": _expected_page_job_parameters("v1"),
    }
    assert trigger_v2_task["run_job_task"] == {
        "job_id": (
            "${resources.jobs."
            "mktg_next_uk_nextads_page_build_cicd_v2.id}"
        ),
        "job_parameters": _expected_page_job_parameters("v2"),
    }
    for task in (trigger_v1_task, trigger_v2_task):
        assert "spark_python_task" not in task
        assert "run_if" not in task
    assert _job_parameters(job) == {
        "run_date": "{{job.start_time.iso_date}}",
        "v1_score_provider_id": "theme_affinity",
        "v2_score_provider_id": "theme_affinity",
    }


def test_deployed_route_jobs_do_not_reference_async_trigger_wrapper():
    resources = (
        _load_job(
            "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
            "mktg_next_uk_nextads_cicd",
        ),
        _load_job(
            "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build.yml",
            "mktg_next_uk_nextads_page_build_cicd",
        ),
        _load_job(
            "pipelines/databricks/jobs/"
            "mktg_next_uk_nextads_page_build_v2.yml",
            "mktg_next_uk_nextads_page_build_cicd_v2",
        ),
    )

    for job in resources:
        for task in job["tasks"]:
            python_file = task.get("spark_python_task", {}).get(
                "python_file", ""
            )
            assert not python_file.endswith("trigger_databricks_job.py")


def test_route_specific_provider_checks_gate_only_their_mapper():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )

    tasks_by_key = _tasks_by_key(job)

    assert tasks_by_key["map_theme_scores_to_ads_v1"]["depends_on"] == [
        {"task_key": "combine_customer_cells"},
        {"task_key": "validate_score_provider_theme_coverage_v1"},
    ]
    assert tasks_by_key["map_theme_scores_to_ads_v2"]["depends_on"] == [
        {"task_key": "combine_customer_cells"},
        {"task_key": "validate_score_provider_theme_coverage_v2"},
    ]
    assert tasks_by_key["validate_score_provider_theme_coverage_v1"][
        "depends_on"
    ] == [
        {"task_key": "audit_control_sheet_v1"},
        {"task_key": "select_score_provider_build_v1"},
    ]
    assert tasks_by_key["validate_score_provider_theme_coverage_v2"][
        "depends_on"
    ] == [
        {"task_key": "audit_control_sheet_v2"},
        {"task_key": "select_score_provider_build_v2"},
    ]
    assert tasks_by_key["map_theme_scores_to_ads_v1"]["job_cluster_key"] == (
        "next_ads_job_cluster_D32ads_v5_1_4_v1_candidates"
    )
    assert tasks_by_key["map_theme_scores_to_ads_v2"]["job_cluster_key"] == (
        "next_ads_job_cluster_D32ads_v5_1_4_v2_candidates"
    )


def test_markov_scoring_has_an_independent_scheduled_resource():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_markov_scoring.yml",
        "mktg_next_uk_nextads_markov_scoring_cicd",
    )

    tasks_by_key = _tasks_by_key(job)
    job_parameters = {
        param["name"]: param["default"] for param in job["parameters"]
    }

    assert job["name"] == "mktg_next_uk_nextads_markov_scoring"
    assert job["schedule"] == {
        "quartz_cron_expression": "0 0 18 * * ?",
        "timezone_id": "Europe/London",
    }
    assert job["timeout_seconds"] == 8100
    assert job["job_clusters"] == "${var.job_clusters_config}"
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_team_notification_emails}"
    )
    assert job["notification_settings"] == {
        "no_alert_for_skipped_runs": True,
        "no_alert_for_canceled_runs": True,
    }
    assert job["max_concurrent_runs"] == 1
    assert job_parameters == {
        "run_date": "{{job.start_time.iso_date}}",
        "input_snapshot_id": "same_day",
    }
    assert list(tasks_by_key) == [
        "prepare_provider_context",
        "score_lightweight",
        "complete_provider_context",
        "finalize_provider_context",
    ]
    prepare = tasks_by_key["prepare_provider_context"]
    assert "depends_on" not in prepare
    assert (
        prepare["spark_python_task"]["python_file"]
        == "../../../jobs/orchestration/prepare_score_provider_context.py"
    )
    prepare_parameters = prepare["spark_python_task"]["parameters"]
    assert prepare_parameters[
        prepare_parameters.index("--provider_id") + 1
    ] == "markov"
    assert prepare_parameters[
        prepare_parameters.index("--context_slot") + 1
    ] == "markov_scoring"
    assert tasks_by_key["score_lightweight"]["depends_on"] == [
        {"task_key": "prepare_provider_context"},
    ]
    score_parameters = tasks_by_key["score_lightweight"][
        "spark_python_task"
    ]["parameters"]
    assert "{{job.parameters.run_date}}" in score_parameters
    assert (
        "{{tasks.prepare_provider_context.values.input_snapshot_id}}"
        in score_parameters
    )
    assert tasks_by_key["complete_provider_context"]["depends_on"] == [
        {"task_key": "score_lightweight"},
    ]
    assert tasks_by_key["finalize_provider_context"]["depends_on"] == [
        {"task_key": "complete_provider_context"},
    ]
    assert tasks_by_key["finalize_provider_context"]["run_if"] == "ALL_DONE"
    assert {
        task_key: task["job_cluster_key"]
        for task_key, task in tasks_by_key.items()
    } == {
        "prepare_provider_context": "next_ads_job_cluster_D4ads_v5_1_1",
        "score_lightweight": "next_ads_job_cluster_D32ads_v5_1_4",
        "complete_provider_context": "next_ads_job_cluster_D4ads_v5_1_1",
        "finalize_provider_context": "next_ads_job_cluster_D4ads_v5_1_1",
    }
    assert {
        task_key: task["timeout_seconds"]
        for task_key, task in tasks_by_key.items()
    } == {
        "prepare_provider_context": 900,
        "score_lightweight": 18000,
        "complete_provider_context": 900,
        "finalize_provider_context": 900,
    }


def test_theme_affinity_foundation_and_provider_stages_are_explicit():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity.yml",
        "mktg_next_uk_nextads_theme_affinity_cicd",
    )
    tasks = {task["task_key"]: task for task in job["tasks"]}

    assert job["queue"] == {"enabled": True}
    assert job["max_concurrent_runs"] == 1
    assert tasks["predict_data_prep"]["depends_on"] == [
        {"task_key": "prepare_foundation_context"}
    ]
    assert tasks["publish_foundation"]["depends_on"] == [
        {"task_key": "predict_data_prep"}
    ]
    assert tasks["prepare_provider_context"]["depends_on"] == [
        {"task_key": "publish_foundation"}
    ]
    assert tasks["model_predict"]["depends_on"] == [
        {"task_key": "prepare_provider_context"}
    ]
    assert tasks["clean_output"]["depends_on"] == [
        {"task_key": "model_predict"}
    ]
    assert tasks["publish_compatibility_outputs"]["depends_on"] == [
        {"task_key": "predict_data_prep"}
    ]
    assert all(
        {"task_key": "publish_compatibility_outputs"}
        not in task.get("depends_on", [])
        for task in tasks.values()
        if task["task_key"] != "publish_compatibility_outputs"
    )
    for task_key in (
        "finalize_foundation_context",
        "finalize_provider_context",
    ):
        assert tasks[task_key]["run_if"] == "ALL_DONE"


@pytest.mark.parametrize(
    ("path", "resource_key"),
    [
        (
            "pipelines/databricks/jobs/"
            "mktg_next_uk_nextads_theme_affinity.yml",
            "mktg_next_uk_nextads_theme_affinity_cicd",
        ),
        (
            "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
            "mktg_next_uk_nextads_cicd",
        ),
        (
            "pipelines/databricks/jobs/"
            "mktg_next_uk_nextads_page_build.yml",
            "mktg_next_uk_nextads_page_build_cicd",
        ),
        (
            "pipelines/databricks/jobs/"
            "mktg_next_uk_nextads_page_build_v2.yml",
            "mktg_next_uk_nextads_page_build_cicd_v2",
        ),
        (
            "pipelines/databricks/jobs/"
            "mktg_next_uk_nextads_assignment_validation.yml",
            "mktg_next_uk_nextads_assignment_validation_cicd",
        ),
        (
            "pipelines/databricks/jobs/"
            "mktg_next_uk_nextads_masid_handoff.yml",
            "mktg_next_uk_nextads_masid_handoff_cicd",
        ),
        (
            "pipelines/databricks/jobs/"
            "mktg_next_uk_nextads_plp_gs_delivery.yml",
            "mktg_next_uk_nextads_plp_gs_delivery_cicd",
        ),
        (
            "pipelines/databricks/jobs/"
            "mktg_next_uk_nextads_payload_export.yml",
            "mktg_next_uk_nextads_payload_export_cicd",
        ),
    ],
)
def test_critical_nightly_jobs_queue_exactly_one_run(path, resource_key):
    job = _load_job(path, resource_key)

    assert job["queue"] == {"enabled": True}
    assert job["max_concurrent_runs"] == 1


def test_synchronous_route_timeouts_cover_complete_child_paths():
    candidate = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    page_v1 = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build.yml",
        "mktg_next_uk_nextads_page_build_cicd",
    )
    page_v2 = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml",
        "mktg_next_uk_nextads_page_build_cicd_v2",
    )
    validation = _load_job(
        "pipelines/databricks/jobs/"
        "mktg_next_uk_nextads_assignment_validation.yml",
        "mktg_next_uk_nextads_assignment_validation_cicd",
    )
    payload = _load_job(
        "pipelines/databricks/jobs/"
        "mktg_next_uk_nextads_payload_export.yml",
        "mktg_next_uk_nextads_payload_export_cicd",
    )

    candidate_tasks = {
        task["task_key"]: task for task in candidate["tasks"]
    }
    page_v1_tasks = {task["task_key"]: task for task in page_v1["tasks"]}
    page_v2_tasks = {task["task_key"]: task for task in page_v2["tasks"]}

    v1_page_critical_path = sum(
        (
            page_v1_tasks["prepare_assignment_scope_manifest"][
                "timeout_seconds"
            ],
            page_v1_tasks["build_page_primary"]["for_each_task"]["task"][
                "timeout_seconds"
            ],
            page_v1_tasks["build_page_secondary"]["for_each_task"]["task"][
                "timeout_seconds"
            ],
            page_v1_tasks["publish_assignment_build_v1"]["timeout_seconds"],
            validation["timeout_seconds"],
        )
    )
    v2_page_critical_path = sum(
        (
            page_v2_tasks["build_page_v2"]["for_each_task"]["task"][
                "timeout_seconds"
            ],
            page_v2_tasks["publish_assignment_build_v2"]["timeout_seconds"],
            payload["timeout_seconds"],
        )
    )

    assert page_v1["timeout_seconds"] == 43200
    assert page_v1["timeout_seconds"] >= v1_page_critical_path
    assert page_v2["timeout_seconds"] == 28800
    assert page_v2["timeout_seconds"] >= v2_page_critical_path

    candidate_before_page_v1 = sum(
        (
            candidate_tasks["assign_customer_cells"]["timeout_seconds"],
            candidate_tasks["combine_customer_cells"]["timeout_seconds"],
            candidate_tasks["map_theme_scores_to_ads_v1"]["timeout_seconds"],
        )
    )
    candidate_before_page_v2 = sum(
        (
            candidate_tasks["assign_customer_cells"]["timeout_seconds"],
            candidate_tasks["combine_customer_cells"]["timeout_seconds"],
            candidate_tasks["map_theme_scores_to_ads_v2"]["timeout_seconds"],
        )
    )
    assert candidate["timeout_seconds"] == 72000
    assert candidate["timeout_seconds"] >= (
        candidate_before_page_v1 + page_v1["timeout_seconds"]
    )
    assert candidate["timeout_seconds"] >= (
        candidate_before_page_v2 + page_v2["timeout_seconds"]
    )


def test_candidate_resource_excludes_legacy_markov_tasks():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    task_keys = {task["task_key"] for task in job["tasks"]}

    assert {
        "parse_attributes",
        "validate_theme_mapping_sync",
        "parse_theme_mapping",
        "score_lightweight",
    }.isdisjoint(task_keys)

    bundle_config = yaml.safe_load(
        (PROJECT_ROOT / "databricks.yml").read_text()
    )
    assert (
        "pipelines/databricks/jobs/"
        "mktg_next_uk_nextads_markov_scoring.yml"
        in bundle_config["include"]
    )


def test_provider_selection_and_coverage_pin_each_route_mapper():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )

    tasks_by_key = _tasks_by_key(job)

    for route in ("v1", "v2"):
        selector = tasks_by_key[f"select_score_provider_build_{route}"]
        coverage = tasks_by_key[
            f"validate_score_provider_theme_coverage_{route}"
        ]
        mapper = tasks_by_key[f"map_theme_scores_to_ads_{route}"]

        assert selector["spark_python_task"]["python_file"] == (
            "../../../jobs/orchestration/select_score_provider_build.py"
        )
        _assert_cli_values(
            selector,
            {
                "--client": "next_uk",
                "--job_env": "${var.job_parameter_environment_name}",
                "--run_date": "{{job.parameters.run_date}}",
                "--provider_id": (
                    f"{{{{job.parameters.{route}_score_provider_id}}}}"
                ),
                "--route": route,
                "--capability": "account_theme",
                "--use_case": "theme_ranking",
                "--readiness_wait_seconds": "1800",
                "--readiness_poll_seconds": "60",
                "--task_run_id": "{{task.run_id}}",
                "--execution_count": "{{task.execution_count}}",
            },
        )
        assert coverage["spark_python_task"]["python_file"] == (
            "../../../jobs/nextads_candidates/"
            "validate_theme_affinity_theme_coverage.py"
        )
        selector_arguments = {
            "--run_date": "{{job.parameters.run_date}}",
            **{
                f"--{name}": value
                for name, value in _selector_values(route).items()
            },
        }
        _assert_cli_values(
            coverage,
            {
                option: value
                for option, value in selector_arguments.items()
                if option
                not in {"--input_snapshot_id", "--scoring_foundation_build_id"}
            },
        )
        _assert_cli_values(
            mapper,
            {
                option: value
                for option, value in selector_arguments.items()
                if option
                not in {
                    "--input_snapshot_id",
                    "--scoring_foundation_build_id",
                    "--provider_selection_status",
                }
            },
        )


def test_control_sheet_audits_are_warning_only_but_order_route_coverage():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )

    tasks_by_key = _tasks_by_key(job)


    for route in ("v1", "v2"):
        audit_task = tasks_by_key[f"audit_control_sheet_{route}"]

        assert audit_task["depends_on"] == [
            {"task_key": f"load_control_sheet_{route}"}
        ]
        assert audit_task["spark_python_task"]["python_file"] == (
            "../../../jobs/nextads_control/audit_control_sheet.py"
        )
        assert audit_task["spark_python_task"]["parameters"] == [
            "--route",
            route,
            "--client",
            "next_uk",
            "--job_env",
            "${var.job_parameter_environment_name}",
            "--run_date",
            "{{job.parameters.run_date}}",
            "--warn-only",
        ]
        assert (
            audit_task["job_cluster_key"]
            == "next_ads_job_cluster_D4ads_v5_1_1"
        )
        assert audit_task["timeout_seconds"] == 1800
        assert tasks_by_key[
            f"validate_score_provider_theme_coverage_{route}"
        ]["depends_on"] == [
            {"task_key": f"audit_control_sheet_{route}"},
            {"task_key": f"select_score_provider_build_{route}"},
        ]


def test_v1_and_v2_candidate_routes_share_only_customer_cells():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    tasks_by_key = _tasks_by_key(job)

    v1_ancestors = _ancestors(tasks_by_key, "run_page_build_v1")
    v2_ancestors = _ancestors(tasks_by_key, "run_page_build_v2")

    assert v1_ancestors & v2_ancestors == {
        "assign_customer_cells",
        "combine_customer_cells",
    }
    assert {
        "load_control_sheet_v1",
        "audit_control_sheet_v1",
        "select_score_provider_build_v1",
        "validate_score_provider_theme_coverage_v1",
        "map_theme_scores_to_ads_v1",
    } <= v1_ancestors
    assert {
        "trigger_data_pull_for_CMS_pull",
        "load_control_sheet_v2",
        "audit_control_sheet_v2",
        "select_score_provider_build_v2",
        "validate_score_provider_theme_coverage_v2",
        "map_theme_scores_to_ads_v2",
    } <= v2_ancestors


def test_control_or_provider_failure_blocks_only_its_own_route():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    tasks_by_key = _tasks_by_key(job)

    for failed_task, blocked_child, healthy_sibling in (
        (
            "load_control_sheet_v1",
            "run_page_build_v1",
            "run_page_build_v2",
        ),
        (
            "select_score_provider_build_v1",
            "run_page_build_v1",
            "run_page_build_v2",
        ),
        (
            "load_control_sheet_v2",
            "run_page_build_v2",
            "run_page_build_v1",
        ),
        (
            "select_score_provider_build_v2",
            "run_page_build_v2",
            "run_page_build_v1",
        ),
    ):
        descendants = _descendants(tasks_by_key, failed_task)
        assert blocked_child in descendants
        assert healthy_sibling not in descendants


def test_critical_route_tasks_use_default_all_success_failure_propagation():
    resources = (
        (
            _load_job(
                "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
                "mktg_next_uk_nextads_cicd",
            ),
            {
                "select_score_provider_build_v1",
                "select_score_provider_build_v2",
                "validate_score_provider_theme_coverage_v1",
                "validate_score_provider_theme_coverage_v2",
                "map_theme_scores_to_ads_v1",
                "map_theme_scores_to_ads_v2",
                "run_page_build_v1",
                "run_page_build_v2",
            },
        ),
        (
            _load_job(
                "pipelines/databricks/jobs/"
                "mktg_next_uk_nextads_page_build.yml",
                "mktg_next_uk_nextads_page_build_cicd",
            ),
            {
                "publish_assignment_build_v1",
                "run_assignment_validation",
                "run_masid_handoff",
                "run_plp_gs_delivery",
            },
        ),
        (
            _load_job(
                "pipelines/databricks/jobs/"
                "mktg_next_uk_nextads_page_build_v2.yml",
                "mktg_next_uk_nextads_page_build_cicd_v2",
            ),
            {"publish_assignment_build_v2", "run_payload_export"},
        ),
    )

    for job, task_keys in resources:
        tasks_by_key = _tasks_by_key(job)
        for task_key in task_keys:
            assert tasks_by_key[task_key].get("run_if", "ALL_SUCCESS") == (
                "ALL_SUCCESS"
            )


def test_page_build_v1_publishes_one_complete_build_before_handoffs():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build.yml",
        "mktg_next_uk_nextads_page_build_cicd",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert job["name"] == "mktg_next_uk_nextads_page_build"
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_team_notification_emails}"
    )
    job_parameters = _job_parameters(job)
    assert set(job_parameters) == {
        "run_date",
        "build_run_id",
        "scope_manifest_json",
        "provider_build_id",
        "provider_signals_table",
        "provider_signals_delta_version",
        "input_snapshot_id",
        "scoring_foundation_build_id",
        "provider_selection_status",
        "provider_source_run_date",
    }
    assert job_parameters["run_date"] == "{{job.start_time.iso_date}}"
    assert job_parameters["build_run_id"] == "v1_{{job.run_id}}"
    for name in _selector_values("v1"):
        assert job_parameters[name] == ""

    scope_manifest = json.loads(job_parameters["scope_manifest_json"])
    primary_manifest = [
        entry for entry in scope_manifest if entry["phase"] == "primary"
    ]
    secondary_manifest = [
        entry for entry in scope_manifest if entry["phase"] == "secondary"
    ]
    assert len(primary_manifest) == 77
    assert secondary_manifest == [
        {
            "scope": "SB2",
            "phase": "secondary",
            "inherit_basic_from": "SB1",
        },
        {
            "scope": "OC2",
            "phase": "secondary",
            "inherit_basic_from": "OC1",
        },
    ]
    configured_locations = set(
        yaml.safe_load(
            (PROJECT_ROOT / "configs/clients/next_uk.yaml").read_text()
        )["default"]["locations"]
    )
    assert len({entry["scope"] for entry in scope_manifest}) == 79
    assert {entry["scope"] for entry in scope_manifest} == (
        configured_locations - {"HN1"}
    )
    assert scope_manifest == [*primary_manifest, *secondary_manifest]

    prepare = tasks_by_key["prepare_assignment_scope_manifest"]
    assert prepare["notebook_task"] == {
        "notebook_path": (
            "../../../jobs/nextads_assignment/prepare_scope_manifest.py"
        ),
        "base_parameters": {
            "scope_manifest_json": (
                "{{job.parameters.scope_manifest_json}}"
            )
        },
        "source": "WORKSPACE",
    }
    assert prepare["timeout_seconds"] == 1800

    primary = tasks_by_key["build_page_primary"]
    assert primary["depends_on"] == [
        {"task_key": "prepare_assignment_scope_manifest"}
    ]
    assert primary["for_each_task"]["inputs"] == (
        "{{tasks.prepare_assignment_scope_manifest.values."
        "primary_scope_manifest}}"
    )
    assert primary["for_each_task"]["task"]["spark_python_task"][
        "parameters"
    ] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--location",
        "{{input.scope}}",
        "--scope_manifest_json",
        "{{job.parameters.scope_manifest_json}}",
        "--run_date",
        "{{job.parameters.run_date}}",
        "--build_run_id",
        "{{job.parameters.build_run_id}}",
        "--task_run_id",
        "{{task.run_id}}",
        "--execution_count",
        "{{task.execution_count}}",
    ]

    secondary = tasks_by_key["build_page_secondary"]
    assert secondary["depends_on"] == [
        {"task_key": "build_page_primary"}
    ]
    assert secondary["for_each_task"]["inputs"] == (
        "{{tasks.prepare_assignment_scope_manifest.values."
        "secondary_scope_manifest}}"
    )
    assert secondary["for_each_task"]["task"]["spark_python_task"][
        "parameters"
    ] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--location",
        "{{input.scope}}",
        "--inherit_basic_from",
        "{{input.inherit_basic_from}}",
        "--scope_manifest_json",
        "{{job.parameters.scope_manifest_json}}",
        "--run_date",
        "{{job.parameters.run_date}}",
        "--build_run_id",
        "{{job.parameters.build_run_id}}",
        "--task_run_id",
        "{{task.run_id}}",
        "--execution_count",
        "{{task.execution_count}}",
    ]

    publisher = tasks_by_key["publish_assignment_build_v1"]
    assert publisher["depends_on"] == [
        {"task_key": "build_page_secondary"}
    ]
    assert "run_if" not in publisher
    assert publisher["spark_python_task"]["python_file"] == (
        "../../../jobs/nextads_assignment/publish_build.py"
    )
    assert publisher["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--route",
        "v1",
        "--run_date",
        "{{job.parameters.run_date}}",
        "--build_run_id",
        "{{job.parameters.build_run_id}}",
        "--scope_manifest_json",
        "{{job.parameters.scope_manifest_json}}",
    ]

    downstream_jobs = {
        "run_assignment_validation": (
            "${resources.jobs."
            "mktg_next_uk_nextads_assignment_validation_cicd.id}"
        ),
        "run_masid_handoff": (
            "${resources.jobs."
            "mktg_next_uk_nextads_masid_handoff_cicd.id}"
        ),
        "run_plp_gs_delivery": (
            "${resources.jobs."
            "mktg_next_uk_nextads_plp_gs_delivery_cicd.id}"
        ),
    }
    for task_key, job_id in downstream_jobs.items():
        task = tasks_by_key[task_key]
        assert task["depends_on"] == [
            {"task_key": "publish_assignment_build_v1"}
        ]
        assert task["run_job_task"] == {
            "job_id": job_id,
            "job_parameters": _forwarded_page_identity_parameters(),
        }
        assert "spark_python_task" not in task
        assert "run_if" not in task
    assert all(task.get("run_if") != "ALL_DONE" for task in job["tasks"])


def test_page_build_v2_publishes_complete_build_before_payload_submission():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml",
        "mktg_next_uk_nextads_page_build_cicd_v2",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert job["name"] == "mktg_next_uk_nextads_page_build_v2"
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_and_downstream_notification_emails}"
    )
    job_parameters = _job_parameters(job)
    assert set(job_parameters) == {
        "run_date",
        "build_run_id",
        "scope_manifest_json",
        "provider_build_id",
        "provider_signals_table",
        "provider_signals_delta_version",
        "input_snapshot_id",
        "scoring_foundation_build_id",
        "provider_selection_status",
        "provider_source_run_date",
    }
    assert job_parameters["run_date"] == "{{job.start_time.iso_date}}"
    assert job_parameters["build_run_id"] == "v2_{{job.run_id}}"
    for name in _selector_values("v2"):
        assert job_parameters[name] == ""

    scope_manifest = json.loads(job_parameters["scope_manifest_json"])
    assert scope_manifest == [
        {"scope": "HomePage"},
        {"scope": "ShoppingBagPage"},
        {"scope": "CheckoutPage"},
        {"scope": "ProductListingPage"},
        {"scope": "ForYouPage"},
    ]
    assert len({entry["scope"] for entry in scope_manifest}) == 5

    build_for_each = tasks_by_key["build_page_v2"]["for_each_task"]
    assert build_for_each["inputs"] == (
        "{{job.parameters.scope_manifest_json}}"
    )
    assert build_for_each["task"]["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--page_type",
        "{{input.scope}}",
        "--scope_manifest_json",
        "{{job.parameters.scope_manifest_json}}",
        "--run_date",
        "{{job.parameters.run_date}}",
        "--build_run_id",
        "{{job.parameters.build_run_id}}",
        "--task_run_id",
        "{{task.run_id}}",
        "--execution_count",
        "{{task.execution_count}}",
    ]

    publisher = tasks_by_key["publish_assignment_build_v2"]
    assert publisher["depends_on"] == [{"task_key": "build_page_v2"}]
    assert "run_if" not in publisher
    assert publisher["spark_python_task"]["python_file"] == (
        "../../../jobs/nextads_assignment/publish_build.py"
    )
    assert publisher["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--route",
        "v2",
        "--run_date",
        "{{job.parameters.run_date}}",
        "--build_run_id",
        "{{job.parameters.build_run_id}}",
        "--scope_manifest_json",
        "{{job.parameters.scope_manifest_json}}",
    ]

    payload_task = tasks_by_key["run_payload_export"]
    assert payload_task["depends_on"] == [
        {"task_key": "publish_assignment_build_v2"},
    ]
    assert payload_task["run_job_task"] == {
        "job_id": (
            "${resources.jobs."
            "mktg_next_uk_nextads_payload_export_cicd.id}"
        ),
        "job_parameters": _forwarded_page_identity_parameters(),
    }
    assert "spark_python_task" not in payload_task
    assert "run_if" not in payload_task
    assert all(task.get("run_if") != "ALL_DONE" for task in job["tasks"])


def test_payload_export_uses_forwarded_logical_run_date():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_payload_export.yml",
        "mktg_next_uk_nextads_payload_export_cicd",
    )

    _assert_delivery_boundary_parameters(job)
    assert job["tasks"][0]["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--do_export",
        "1",
        "--run_date",
        "{{job.parameters.run_date}}",
    ]


def test_data_pull_pipeline_passes_user_schema_to_python_config():
    pipeline_config = yaml.safe_load(
        (
            PROJECT_ROOT
            / "pipelines/databricks/pipelines/mktg_next_uk_nextads_data_pull.yml"
        ).read_text()
    )
    pipeline = pipeline_config["mktg_next_uk_nextads_data_pull"][
        "mktg_next_uk_nextads_data_pull"
    ]

    assert pipeline["schema"] == "${var.user_schema}"
    assert (
        pipeline["configuration"]["pipeline.user_schema"]
        == "${var.user_schema}"
    )
    assert pipeline["root_path"] == "${workspace.file_path}/src"
    assert pipeline["environment"] == "${var.pipeline_libraries}"


def test_data_pull_archive_uses_forwarded_logical_run_date():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_data_pull.yaml",
        "mktg_next_uk_nextads_data_pull",
    )

    assert job["parameters"] == [
        {
            "name": "run_date",
            "default": "{{job.start_time.iso_date}}",
        }
    ]
    archive_task = next(
        task
        for task in job["tasks"]
        if task["task_key"] == "archive_sort_order_data"
    )
    assert archive_task["spark_python_task"]["parameters"][-4:] == [
        "--log_level",
        "INFO",
        "--run_date",
        "{{job.parameters.run_date}}",
    ]


def test_assignment_validation_job_has_independent_definition_and_internal_notifications():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_assignment_validation.yml",
        "mktg_next_uk_nextads_assignment_validation_cicd",
    )

    assert job["name"] == "mktg_next_uk_nextads_assignment_validation"
    assert "schedule" not in job
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_team_notification_emails}"
    )
    _assert_delivery_boundary_parameters(job)


def test_assignment_validation_entrypoint_is_read_only():
    source = (
        PROJECT_ROOT
        / "jobs/nextads_reporting/assignment_validation.py"
    ).read_text()
    normalized = source.lower()

    assert "delete from" not in normalized
    assert "truncate table" not in normalized
    assert ".write." not in normalized
    assert ".saveastable(" not in normalized


def test_masid_handoff_uses_forwarded_logical_run_date():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_masid_handoff.yml",
        "mktg_next_uk_nextads_masid_handoff_cicd",
    )

    _assert_delivery_boundary_parameters(job)
    assert job["tasks"][0]["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--expected_rundate",
        "{{job.parameters.run_date}}",
    ]


def test_prod_data_team_notifications_are_internal_only():
    bundle_config = yaml.safe_load(
        (PROJECT_ROOT / "databricks.yml").read_text()
    )
    prod_variables = bundle_config["targets"]["PROD"]["variables"]

    assert "qa_notification_emails" not in prod_variables
    assert "core_notification_emails" not in prod_variables
    assert prod_variables["data_team_notification_emails"] == [
        "edward_taylor@next.co.uk",
        "adrienne_lowe@next.co.uk",
        "hadi_miah@next.co.uk",
        "thomas_harrop@next.co.uk",
        "stephen_blain@next.co.uk",
        "jack_douglas@next.co.uk",
        "claire_wilsonbarnes@next.co.uk",
        "thomas_lynch@next.co.uk",
    ]


def test_delivery_jobs_have_external_notifications():
    masid_job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_masid_handoff.yml",
        "mktg_next_uk_nextads_masid_handoff_cicd",
    )
    payload_job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_payload_export.yml",
        "mktg_next_uk_nextads_payload_export_cicd",
    )
    plp_job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_plp_gs_delivery.yml",
        "mktg_next_uk_nextads_plp_gs_delivery_cicd",
    )

    assert masid_job["email_notifications"]["on_failure"] == (
        "${var.data_and_downstream_notification_emails}"
    )
    assert payload_job["email_notifications"]["on_failure"] == (
        "${var.data_and_downstream_notification_emails}"
    )
    assert plp_job["email_notifications"]["on_failure"] == (
        "${var.data_and_downstream_notification_emails}"
    )
    _assert_delivery_boundary_parameters(plp_job)
    iteration_parameters = plp_job["tasks"][0]["for_each_task"]["task"][
        "spark_python_task"
    ]["parameters"]
    assert iteration_parameters == [
        "--client",
        "{{input.client}}",
        "--config_client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--territory",
        "{{input.territory}}",
        "--run_date",
        "{{job.parameters.run_date}}",
    ]


def test_prod_notifications_are_split_by_owner_group():
    bundle_config = yaml.safe_load(
        (PROJECT_ROOT / "databricks.yml").read_text()
    )
    prod_variables = bundle_config["targets"]["PROD"]["variables"]

    external_recipients = [
        "mktg_data_support@next.co.uk",
        "jane_hobday@next.co.uk",
        "james_hobday@next.co.uk",
        "sarah_galloway-grant@next.co.uk",
        "ines_bonnin-ward@next.co.uk",
        "evelyn_jones@next.co.uk",
        "dimitrios_liakouras@next.co.uk",
        "nitin_surti@next.co.uk",
        "sonal_sakaria@next.co.uk",
    ]
    reporting_recipients = [
        "stephen_blain@next.co.uk",
        "hadi_miah@next.co.uk",
        "thomas_lynch@next.co.uk",
        "thomas_harrop@next.co.uk",
    ]

    assert "masid_handoff_notification_emails" not in prod_variables
    assert "export_notification_emails" not in prod_variables
    assert "downstream_notification_emails" not in prod_variables
    assert prod_variables["data_and_downstream_notification_emails"] == (
        prod_variables["data_team_notification_emails"] + external_recipients
    )
    assert (
        prod_variables["reporting_notification_emails"] == reporting_recipients
    )
    assert set(external_recipients).isdisjoint(
        set(prod_variables["data_team_notification_emails"])
    )

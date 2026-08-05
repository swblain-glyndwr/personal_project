from pathlib import Path

from tests.job_resource_helpers import load_job


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_job(path, key):
    return load_job(path, key)


def test_main_job_uses_moved_non_v2_entrypoints():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    expected_paths = {
        "select_candidate_foundation": "../../../jobs/orchestration/select_candidate_foundation.py",
        "load_control_sheet_v1": "../../../jobs/nextads_control/load_control_sheet.py",
        "audit_control_sheet_v1": "../../../jobs/nextads_control/audit_control_sheet.py",
        "select_score_provider_build_v1": "../../../jobs/orchestration/select_score_provider_build.py",
        "validate_score_provider_theme_coverage_v1": "../../../jobs/nextads_candidates/validate_theme_affinity_theme_coverage.py",
        "map_theme_scores_to_ads_v1": "../../../jobs/nextads_candidates/build_theme_ad_candidates.py",
    }

    for task_key, expected_path in expected_paths.items():
        assert tasks_by_key[task_key]["spark_python_task"]["python_file"] == (
            expected_path
        )

    assert tasks_by_key["run_page_build_v1"]["run_job_task"]["job_id"] == (
        "${resources.jobs.mktg_next_uk_nextads_page_build_cicd.id}"
    )
    assert "spark_python_task" not in tasks_by_key["run_page_build_v1"]


def test_candidate_foundation_job_uses_moved_shared_input_entrypoints():
    job = _load_job(
        "pipelines/databricks/jobs/"
        "mktg_next_uk_nextads_candidate_foundation.yml",
        "mktg_next_uk_nextads_candidate_foundation_cicd",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}
    expected_paths = {
        "assign_customer_cells": (
            "../../../jobs/nextads_cells/assign_customer_cells.py"
        ),
        "combine_customer_cells": (
            "../../../jobs/nextads_cells/combine_customer_cells.py"
        ),
        "build_repeat_ad_exposure": (
            "../../../jobs/nextads_candidates/"
            "build_candidate_repeat_exposure.py"
        ),
        "build_ad_feedback": (
            "../../../jobs/nextads_candidates/"
            "build_candidate_ad_feedback.py"
        ),
        "publish_candidate_foundation": (
            "../../../jobs/nextads_candidates/"
            "publish_candidate_foundation.py"
        ),
    }

    for task_key, expected_path in expected_paths.items():
        assert tasks_by_key[task_key]["spark_python_task"]["python_file"] == (
            expected_path
        )


def test_markov_scoring_job_uses_moved_control_and_scoring_entrypoints():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_markov_scoring.yml",
        "mktg_next_uk_nextads_markov_scoring_cicd",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    expected_paths = {
        "prepare_provider_context": "../../../jobs/orchestration/prepare_score_provider_context.py",
        "build_markov_scores": "../../../jobs/nextads_candidates/build_theme_scores.py",
        "publish_provider_build": "../../../jobs/orchestration/publish_score_provider_build.py",
    }

    for task_key, expected_path in expected_paths.items():
        assert tasks_by_key[task_key]["spark_python_task"]["python_file"] == (
            expected_path
        )

    assert not {
        "parse_attributes",
        "validate_theme_mapping_sync",
        "parse_theme_mapping",
    }.intersection(tasks_by_key)


def test_v2_main_job_entrypoints_use_jobs_folder():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["load_control_sheet_v2"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_control/load_control_sheet_v2.py"
    assert tasks_by_key["audit_control_sheet_v2"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_control/audit_control_sheet.py"
    assert tasks_by_key["select_score_provider_build_v2"][
        "spark_python_task"
    ]["python_file"] == (
        "../../../jobs/orchestration/select_score_provider_build.py"
    )
    assert tasks_by_key["validate_score_provider_theme_coverage_v2"][
        "spark_python_task"
    ]["python_file"] == (
        "../../../jobs/nextads_candidates/"
        "validate_theme_affinity_theme_coverage.py"
    )
    assert tasks_by_key["map_theme_scores_to_ads_v2"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_candidates/build_page_type_candidates_v2.py"
    assert tasks_by_key["run_page_build_v2"]["run_job_task"]["job_id"] == (
        "${resources.jobs.mktg_next_uk_nextads_page_build_cicd_v2.id}"
    )
    assert "spark_python_task" not in tasks_by_key["run_page_build_v2"]


def test_page_build_job_uses_moved_non_v2_entrypoints():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build.yml",
        "mktg_next_uk_nextads_page_build_cicd",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["build_page_primary"]["for_each_task"]["task"][
        "spark_python_task"
    ]["python_file"] == "../../../jobs/nextads_assignment/build_page.py"
    assert tasks_by_key["build_page_secondary"]["for_each_task"]["task"][
        "spark_python_task"
    ]["python_file"] == "../../../jobs/nextads_assignment/build_page.py"

    expected_jobs = {
        "run_assignment_validation": (
            "${resources.jobs."
            "mktg_next_uk_nextads_assignment_validation_cicd.id}"
        ),
        "run_masid_handoff": (
            "${resources.jobs.mktg_next_uk_nextads_masid_handoff_cicd.id}"
        ),
        "run_plp_gs_delivery": (
            "${resources.jobs."
            "mktg_next_uk_nextads_plp_gs_delivery_cicd.id}"
        ),
    }
    for task_key, job_id in expected_jobs.items():
        assert tasks_by_key[task_key]["run_job_task"]["job_id"] == job_id
        assert "spark_python_task" not in tasks_by_key[task_key]


def test_v2_page_build_job_uses_moved_non_v2_entrypoints():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml",
        "mktg_next_uk_nextads_page_build_cicd_v2",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["run_payload_export"]["run_job_task"]["job_id"] == (
        "${resources.jobs.mktg_next_uk_nextads_payload_export_cicd.id}"
    )
    assert "spark_python_task" not in tasks_by_key["run_payload_export"]


def test_v2_page_build_entrypoint_uses_jobs_folder():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml",
        "mktg_next_uk_nextads_page_build_cicd_v2",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["build_page_v2"]["for_each_task"]["task"][
        "spark_python_task"
    ]["python_file"] == "../../../jobs/nextads_v2/build_page.py"


def test_route_oriented_entrypoint_files_exist_without_domain_wrappers():
    expected_entrypoints = [
        "jobs/nextads_cells/assign_customer_cells.py",
        "jobs/nextads_cells/combine_customer_cells.py",
        "jobs/nextads_candidates/build_theme_scores.py",
        "jobs/orchestration/select_score_provider_build.py",
        "jobs/nextads_candidates/validate_theme_affinity_theme_coverage.py",
        "jobs/nextads_candidates/build_theme_ad_candidates.py",
        "jobs/nextads_control/audit_control_sheet.py",
        "jobs/nextads_control/validate_theme_mapping_sync.py",
        "jobs/nextads_assignment/build_page.py",
    ]

    for entrypoint in expected_entrypoints:
        assert (PROJECT_ROOT / entrypoint).is_file()

    for folder in ["nextads_main", "decisioning", "ranking", "retrieval", "results"]:
        assert not (PROJECT_ROOT / "jobs" / folder).exists()


def test_obsolete_main_wrappers_are_removed():
    for entrypoint in [
        "assign_customer_cells",
        "combine_customer_cells",
        "build_markov_chain",
        "map_theme_scores_to_ads",
        "build_page",
        "trigger_databricks_job",
    ]:
        assert not (PROJECT_ROOT / "scripts" / f"{entrypoint}.py").exists()
        assert not (PROJECT_ROOT / "jobs" / "nextads_main" / f"{entrypoint}.py").exists()

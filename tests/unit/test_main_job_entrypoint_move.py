import importlib
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
        "assign_customer_cells": "../../../jobs/nextads_cells/assign_customer_cells.py",
        "combine_customer_cells": "../../../jobs/nextads_cells/combine_customer_cells.py",
        "load_control_sheet": "../../../jobs/nextads_control/load_control_sheet.py",
        "parse_attributes": "../../../jobs/nextads_control/parse_attributes.py",
        "parse_theme_mapping": "../../../jobs/nextads_control/parse_theme_mapping.py",
        "score_lightweight": "../../../jobs/nextads_candidates/build_theme_scores.py",
        "map_theme_scores_to_ads": "../../../jobs/nextads_candidates/build_theme_ad_candidates.py",
        "trigger_page_build_job": "../../../jobs/orchestration/trigger_databricks_job.py",
    }

    for task_key, expected_path in expected_paths.items():
        assert tasks_by_key[task_key]["spark_python_task"]["python_file"] == (
            expected_path
        )


def test_v2_main_job_entrypoints_stay_on_scripts():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["load_control_sheet_v2"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_control/load_control_sheet_v2.py"
    assert tasks_by_key["map_theme_scores_to_ads_v2"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_candidates/build_page_type_candidates_v2.py"


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

    trigger_tasks = [
        "trigger_assignment_validation_job",
        "trigger_masid_handoff_check_job",
        "trigger_plp_gs_delivery_job",
    ]
    for task_key in trigger_tasks:
        assert tasks_by_key[task_key]["spark_python_task"]["python_file"] == (
            "../../../jobs/orchestration/trigger_databricks_job.py"
        )


def test_v2_page_build_job_uses_moved_non_v2_entrypoints():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml",
        "mktg_next_uk_nextads_page_build_cicd_v2",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}


    trigger_tasks = [
        "trigger_payload_export_job",
    ]
    for task_key in trigger_tasks:
        assert tasks_by_key[task_key]["spark_python_task"]["python_file"] == (
            "../../../jobs/orchestration/trigger_databricks_job.py"
        )

def test_v2_page_build_entrypoint_stays_on_scripts():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml",
        "mktg_next_uk_nextads_page_build_cicd_v2",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["build_page_v2"]["for_each_task"]["task"][
        "spark_python_task"
    ]["python_file"] == "../../../jobs/nextads_v2/build_page.py"


def test_moved_entrypoint_files_exist_with_legacy_wrappers():
    entrypoints = [
        "build_markov_chain",
    ]

    for entrypoint in entrypoints:
        assert (PROJECT_ROOT / "jobs" / "nextads_main" / f"{entrypoint}.py").is_file()
        assert (PROJECT_ROOT / "scripts" / f"{entrypoint}.py").is_file()

    moved_entrypoints = [
        "jobs/nextads_cells/assign_customer_cells.py",
        "jobs/nextads_cells/combine_customer_cells.py",
        "jobs/nextads_candidates/build_theme_ad_candidates.py",
        "jobs/nextads_assignment/build_page.py",
        "jobs/orchestration/trigger_databricks_job.py",
    ]
    for entrypoint in moved_entrypoints:
        assert (PROJECT_ROOT / entrypoint).is_file()


def test_legacy_wrappers_are_importable_without_running_jobs():
    for module_name in [
        "scripts.build_markov_chain",
        "scripts.trigger_databricks_job",
    ]:
        module = importlib.import_module(module_name)
        assert hasattr(module, "main")

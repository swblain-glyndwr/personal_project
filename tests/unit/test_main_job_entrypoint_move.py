import importlib
from pathlib import Path

from tests.job_resource_helpers import load_job


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_job(path, key):
    return load_job(path, key)


def test_main_job_uses_moved_non_v2_entrypoints():
    job = _load_job(
        "resources/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    expected_paths = {
        "assign_customer_cells": "../../jobs/nextads_main/assign_customer_cells.py",
        "combine_customer_cells": "../../jobs/nextads_main/combine_customer_cells.py",
        "load_control_sheet": "../../jobs/nextads_main/load_control_sheet.py",
        "parse_attributes": "../../jobs/nextads_main/parse_attributes.py",
        "parse_theme_mapping": "../../jobs/nextads_main/parse_theme_mapping.py",
        "score_lightweight": "../../jobs/nextads_main/build_markov_chain.py",
        "map_theme_scores_to_ads": "../../jobs/nextads_main/map_theme_scores_to_ads.py",
        "trigger_page_build_job": "../../jobs/nextads_main/trigger_databricks_job.py",
    }

    for task_key, expected_path in expected_paths.items():
        assert tasks_by_key[task_key]["spark_python_task"]["python_file"] == (
            expected_path
        )


def test_v2_main_job_entrypoints_stay_on_scripts():
    job = _load_job(
        "resources/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["load_control_sheet_v2"]["spark_python_task"][
        "python_file"
    ] == "../../scripts/load_control_sheet_v2.py"
    assert tasks_by_key["map_theme_scores_to_ads_v2"]["spark_python_task"][
        "python_file"
    ] == "../../scripts/map_theme_scores_to_ads_v2.py"


def test_page_build_job_uses_moved_non_v2_entrypoints():
    job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_page_build.yml",
        "mktg_next_uk_nextads_page_build_cicd",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["build_page_primary"]["for_each_task"]["task"][
        "spark_python_task"
    ]["python_file"] == "../../jobs/nextads_main/build_page.py"
    assert tasks_by_key["build_page_secondary"]["for_each_task"]["task"][
        "spark_python_task"
    ]["python_file"] == "../../jobs/nextads_main/build_page.py"

    trigger_tasks = [
        "trigger_qa_job",
        "trigger_masid_handoff_check_job",
        "trigger_payload_export_job",
        "trigger_plp_gs_delivery_job",
    ]
    for task_key in trigger_tasks:
        assert tasks_by_key[task_key]["spark_python_task"]["python_file"] == (
            "../../jobs/nextads_main/trigger_databricks_job.py"
        )


def test_v2_page_build_entrypoint_stays_on_scripts():
    job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_page_build.yml",
        "mktg_next_uk_nextads_page_build_cicd",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["build_page_v2"]["for_each_task"]["task"][
        "spark_python_task"
    ]["python_file"] == "../../scripts/build_page_v2.py"


def test_moved_entrypoint_files_exist_with_legacy_wrappers():
    entrypoints = [
        "assign_customer_cells",
        "combine_customer_cells",
        "build_markov_chain",
        "map_theme_scores_to_ads",
        "build_page",
        "trigger_databricks_job",
    ]

    for entrypoint in entrypoints:
        assert (PROJECT_ROOT / "jobs" / "nextads_main" / f"{entrypoint}.py").is_file()
        assert (PROJECT_ROOT / "scripts" / f"{entrypoint}.py").is_file()


def test_legacy_wrappers_are_importable_without_running_jobs():
    for module_name in [
        "scripts.assign_customer_cells",
        "scripts.combine_customer_cells",
        "scripts.build_markov_chain",
        "scripts.map_theme_scores_to_ads",
        "scripts.build_page",
        "scripts.trigger_databricks_job",
    ]:
        module = importlib.import_module(module_name)
        assert hasattr(module, "main")

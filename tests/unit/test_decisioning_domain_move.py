import importlib
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (PROJECT_ROOT / path).read_text()


def _load_job(path: str, key: str) -> dict:
    return yaml.safe_load((PROJECT_ROOT / path).read_text())["resources"]["jobs"][key]


def test_decisioning_assignment_package_exposes_helpers():
    assignment = importlib.import_module("next_ads.decisioning.assignment")

    for helper in [
        "assign_random_ads",
        "assign_preranked_ads",
        "assign_random_ads_v2",
        "assign_preranked_ads_v2",
        "greedy_assignment",
        "get_algo_divisions",
    ]:
        assert hasattr(assignment, helper)


def test_legacy_assignment_wrapper_reexports_moved_helpers():
    legacy = importlib.import_module("next_ads.Assignment")
    moved = importlib.import_module("next_ads.decisioning.assignment")

    assert legacy is moved
    assert legacy.assign_random_ads is moved.assign_random_ads
    assert legacy.assign_preranked_ads_v2 is moved.assign_preranked_ads_v2
    assert legacy.greedy_assignment is moved.greedy_assignment


def test_v1_job_entrypoints_import_decisioning_package():
    for path in [
        "jobs/nextads_main/assign_customer_cells.py",
        "jobs/nextads_main/build_page.py",
        "src/next_ads/ranking/theme_score_mapping.py",
    ]:
        source = _read(path)
        assert "next_ads.decisioning.assignment" in source
        assert "next_ads.Assignment" not in source


def test_v2_build_page_route_stays_on_legacy_script_and_wrapper_import():
    job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_page_build.yml",
        "mktg_next_uk_nextads_page_build_cicd",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["build_page_v2"]["for_each_task"]["task"][
        "spark_python_task"
    ]["python_file"] == "../../scripts/build_page_v2.py"
    assert "from next_ads.Assignment import" in _read("scripts/build_page_v2.py")

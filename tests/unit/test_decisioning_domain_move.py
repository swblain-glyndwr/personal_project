import importlib
from pathlib import Path

from tests.job_resource_helpers import load_job


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (PROJECT_ROOT / path).read_text()


def _load_job(path: str, key: str) -> dict:
    return load_job(path, key)


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


def test_v1_job_entrypoints_import_decisioning_package():
    for path in [
        "jobs/nextads_cells/assign_customer_cells.py",
        "jobs/nextads_assignment/build_page.py",
        "src/next_ads/ranking/theme_score_eligibility.py",
    ]:
        source = _read(path)
        assert "next_ads.decisioning.assignment" in source


def test_v2_build_page_route_uses_jobs_folder():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml",
        "mktg_next_uk_nextads_page_build_cicd_v2",
    )
    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["build_page_v2"]["for_each_task"]["task"][
        "spark_python_task"
    ]["python_file"] == "../../../jobs/nextads_v2/build_page.py"
    assert "from next_ads.decisioning.assignment import" in _read(
        "jobs/nextads_v2/build_page.py"
    )

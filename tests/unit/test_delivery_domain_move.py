import importlib
from pathlib import Path

from next_ads.delivery import google_sheets
from tests.job_resource_helpers import load_job


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (PROJECT_ROOT / path).read_text()


def _load_job(path: str, key: str) -> dict:
    return load_job(path, key)


def test_delivery_job_entrypoints_live_under_jobs():
    entrypoints = [
        "plp_gs",
        "masid_handoff_check",
    ]

    for entrypoint in entrypoints:
        assert (
            PROJECT_ROOT / "jobs" / "nextads_delivery" / f"{entrypoint}.py"
        ).is_file()
        assert not (PROJECT_ROOT / "scripts" / f"{entrypoint}.py").exists()


def test_delivery_job_modules_are_importable_without_running_jobs():
    for module_name in [
        "jobs.nextads_delivery.plp_gs",
        "jobs.nextads_delivery.masid_handoff_check",
    ]:
        module = importlib.import_module(module_name)
        assert hasattr(module, "main")


def test_plp_gs_job_delegates_processing_to_delivery_package():
    job_module = importlib.import_module("jobs.nextads_delivery.plp_gs")

    assert job_module.run_plp_gs_delivery is google_sheets.run_plp_gs_delivery
    assert "next_ads.delivery.google_sheets" in _read("jobs/nextads_delivery/plp_gs.py")
    assert "process_control_sheet" in _read("src/next_ads/delivery/google_sheets.py")
    assert "run_plp_gs_delivery" in _read("src/next_ads/delivery/google_sheets.py")


def test_masid_handoff_job_delegates_to_delivery_package():
    moved = importlib.import_module("jobs.nextads_delivery.masid_handoff_check")

    assert hasattr(moved, "main")
    assert "next_ads.delivery.masid_handoff" in _read(
        "jobs/nextads_delivery/masid_handoff_check.py"
    )


def test_delivery_jobs_use_delivery_entrypoints():
    masid_job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_masid_handoff.yml",
        "mktg_next_uk_nextads_masid_handoff_cicd",
    )
    plp_job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_plp_gs_delivery.yml",
        "mktg_next_uk_nextads_plp_gs_delivery_cicd",
    )

    assert masid_job["tasks"][0]["spark_python_task"]["python_file"] == (
        "../../../jobs/nextads_delivery/masid_handoff_check.py"
    )
    assert plp_job["tasks"][0]["for_each_task"]["task"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_delivery/plp_gs.py"


def test_v2_payload_export_routes_use_jobs_folder():
    payload_job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_payload_export.yml",
        "mktg_next_uk_nextads_payload_export_cicd",
    )
    page_build_job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml",
        "mktg_next_uk_nextads_page_build_cicd_v2",
    )
    main_job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )

    page_tasks = {task["task_key"]: task for task in page_build_job["tasks"]}
    main_tasks = {task["task_key"]: task for task in main_job["tasks"]}

    assert payload_job["tasks"][0]["spark_python_task"]["python_file"] == (
        "../../../jobs/nextads_delivery/build_v2_payload.py"
    )
    assert page_tasks["build_page_v2"]["for_each_task"]["task"][
        "spark_python_task"
    ]["python_file"] == "../../../jobs/nextads_v2/build_page.py"
    assert main_tasks["load_control_sheet_v2"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_control/load_control_sheet_v2.py"
    assert main_tasks["map_theme_scores_to_ads_v2"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_candidates/build_page_type_candidates_v2.py"

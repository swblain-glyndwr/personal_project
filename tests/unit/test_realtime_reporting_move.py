import importlib
from pathlib import Path

from next_ads.reporting import results as reporting_results
from next_ads.realtime import unknown as realtime_unknown
from tests.job_resource_helpers import load_job


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (PROJECT_ROOT / path).read_text()


def _load_job(path: str, key: str) -> dict:
    return load_job(path, key)


def test_reporting_results_imports_work_from_new_and_legacy_paths():
    legacy_results = importlib.import_module("next_ads.Results")

    assert legacy_results.check_control_ratio is reporting_results.check_control_ratio
    assert legacy_results.summarise_sessions is reporting_results.summarise_sessions
    assert (
        legacy_results.validate_assignments_match_pf
        is reporting_results.validate_assignments_match_pf
    )


def test_realtime_unknown_imports_work_from_new_and_legacy_paths():
    legacy_unknown = importlib.import_module("real_time.real_time_unknown")

    assert legacy_unknown.set_ads is realtime_unknown.set_ads
    assert legacy_unknown.format_stream_archive is realtime_unknown.format_stream_archive
    assert legacy_unknown.main is realtime_unknown.main


def test_results_entrypoints_exist_with_legacy_wrappers():
    entrypoints = [
        "results_1",
        "results_2",
        "results_3",
        "results_agg",
        "results_performance_checks",
        "results_to_bigquery",
        "results_top_ads_by_location",
    ]

    for entrypoint in entrypoints:
        assert (PROJECT_ROOT / "jobs" / "results" / f"{entrypoint}.py").is_file()
        assert (PROJECT_ROOT / "scripts" / f"{entrypoint}.py").is_file()


def test_realtime_entrypoints_exist_with_legacy_wrappers():
    entrypoints = [
        "realtime_results",
        "viewed_bought",
    ]

    for entrypoint in entrypoints:
        assert (PROJECT_ROOT / "jobs" / "realtime" / f"{entrypoint}.py").is_file()
        assert (PROJECT_ROOT / "scripts" / f"{entrypoint}.py").is_file()

    assert (PROJECT_ROOT / "src" / "next_ads" / "realtime" / "unknown.py").is_file()
    assert (PROJECT_ROOT / "real_time" / "real_time_unknown.py").is_file()


def test_legacy_realtime_reporting_wrappers_are_importable_without_running_jobs():
    for module_name in [
        "scripts.results_1",
        "scripts.results_2",
        "scripts.results_3",
        "scripts.results_agg",
        "scripts.results_performance_checks",
        "scripts.results_to_bigquery",
        "scripts.results_top_ads_by_location",
        "scripts.realtime_results",
        "scripts.viewed_bought",
    ]:
        module = importlib.import_module(module_name)
        assert hasattr(module, "main")


def test_realtime_reporting_jobs_use_moved_entrypoints():
    results_job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_results.yml",
        "mktg_next_uk_nextads_results_cicd",
    )
    realtime_results_job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_realtime_results.yml",
        "mktg_next_uk_nextads_realtime_results_cicd",
    )
    realtime_inputs_job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_realtime_inputs.yml",
        "mktg_next_uk_nextads_realtime_inputs_cicd",
    )

    results_tasks = {task["task_key"]: task for task in results_job["tasks"]}
    expected_results_paths = {
        "results_1": "../../jobs/results/results_1.py",
        "results_2": "../../jobs/results/results_2.py",
        "results_3": "../../jobs/results/results_3.py",
        "results_agg": "../../jobs/results/results_agg.py",
        "enrich_theme_affinity_inference_log": (
            "../../jobs/results/enrich_theme_affinity_inference_log.py"
        ),
        "results_performance_check": (
            "../../jobs/results/results_performance_checks.py"
        ),
        "results_to_bigquery": "../../jobs/results/results_to_bigquery.py",
        "results_top_ads": "../../jobs/results/results_top_ads_by_location.py",
    }

    for task_key, expected_path in expected_results_paths.items():
        assert results_tasks[task_key]["spark_python_task"]["python_file"] == (
            expected_path
        )

    assert results_tasks["enrich_theme_affinity_inference_log"]["depends_on"] == [
        {"task_key": "results_3"}
    ]
    enrich_parameters = results_tasks["enrich_theme_affinity_inference_log"][
        "spark_python_task"
    ]["parameters"]
    assert "--label_window_days" in enrich_parameters
    assert "28" in enrich_parameters

    assert realtime_results_job["tasks"][0]["spark_python_task"][
        "python_file"
    ] == "../../jobs/realtime/realtime_results.py"
    assert realtime_inputs_job["tasks"][0]["spark_python_task"][
        "python_file"
    ] == "../../jobs/realtime/viewed_bought.py"


def test_realtime_reporting_move_preserves_schedule_sql_and_config_contracts():
    results_job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_results.yml",
        "mktg_next_uk_nextads_results_cicd",
    )
    realtime_results_job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_realtime_results.yml",
        "mktg_next_uk_nextads_realtime_results_cicd",
    )
    realtime_inputs_job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_realtime_inputs.yml",
        "mktg_next_uk_nextads_realtime_inputs_cicd",
    )

    assert results_job["schedule"]["quartz_cron_expression"] == "0 15 7 * * ?"
    assert (
        realtime_results_job["schedule"]["quartz_cron_expression"]
        == "0 30 7 * * ?"
    )
    assert (
        realtime_inputs_job["schedule"]["quartz_cron_expression"]
        == "0 0 18 * * ?"
    )

    root_sql_files = [
        "sql/reporting/create_table_results_topline.sql",
        "sql/reporting/create_table_results_aggregated.sql",
        "sql/reporting/create_table_results_ads.sql",
        "sql/reporting/create_table_results_ads_top_by_location.sql",
        "sql/realtime/create_table_realtime_results.sql",
        "sql/realtime/create_table_realtime_results_latest.sql",
        "sql/realtime/create_table_viewed_bought_latest.sql",
    ]
    for path in root_sql_files:
        assert (PROJECT_ROOT / path).is_file()

    assert (PROJECT_ROOT / "real_time" / "config" / "next_uk.json").is_file()
    assert "big-query-156009.Misc_eu.{client}_nextads_results_topline" in _read(
        "configs/clients/next_uk.json"
    )
    assert "temporaryGcsBucket" in _read("jobs/results/results_to_bigquery.py")

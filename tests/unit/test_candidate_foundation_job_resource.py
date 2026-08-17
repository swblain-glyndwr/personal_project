from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FOUNDATION_RESOURCE = (
    PROJECT_ROOT
    / "pipelines/databricks/jobs/mktg_next_uk_nextads_candidate_foundation.yml"
)
CANDIDATE_RESOURCE = (
    PROJECT_ROOT / "pipelines/databricks/jobs/mktg_next_uk_nextads.yml"
)


def _job(path, anchor):
    document = yaml.safe_load(path.read_text())
    return document[anchor][next(iter(document[anchor]))]


def _parameters(task):
    values = task["spark_python_task"]["parameters"]
    parsed = {}
    index = 0
    while index < len(values):
        name = values[index]
        next_index = index + 1
        if next_index == len(values) or str(values[next_index]).startswith("--"):
            parsed[name] = True
            index += 1
        else:
            parsed[name] = values[next_index]
            index += 2
    return parsed


def test_foundation_job_is_independent_queued_and_scheduled_at_1600():
    job = _job(
        FOUNDATION_RESOURCE,
        "mktg_next_uk_nextads_candidate_foundation_config",
    )
    tasks = {task["task_key"]: task for task in job["tasks"]}

    assert job["queue"] == {"enabled": True}
    assert job["max_concurrent_runs"] == 1
    assert job["schedule"] == {
        "quartz_cron_expression": "0 0 16 * * ?",
        "timezone_id": "Europe/London",
    }
    assert set(tasks) == {
        "assign_customer_cells",
        "combine_customer_cells",
        "build_repeat_ad_exposure",
        "build_ad_feedback",
        "publish_candidate_foundation",
    }
    assert tasks["publish_candidate_foundation"]["depends_on"] == [
        {"task_key": "combine_customer_cells"},
        {"task_key": "build_repeat_ad_exposure"},
        {"task_key": "build_ad_feedback"},
    ]
    assert "theme_affinity" not in FOUNDATION_RESOURCE.read_text().lower()


def test_candidate_routes_share_one_selected_foundation_not_cell_refresh():
    job = _job(CANDIDATE_RESOURCE, "mktg_next_uk_nextads_config")
    tasks = {task["task_key"]: task for task in job["tasks"]}

    assert {"assign_customer_cells", "combine_customer_cells"}.isdisjoint(tasks)
    assert job["parameters"][-1] == {
        "name": "foundation_snapshot_id",
        "default": "same_day",
    }
    for route in ("v1", "v2"):
        mapper = tasks[f"map_theme_scores_to_ads_{route}"]
        assert mapper["depends_on"] == [
            {"task_key": "select_candidate_foundation"},
            {"task_key": f"validate_score_provider_theme_coverage_{route}"},
        ]
        parameters = _parameters(mapper)
        assert parameters["--foundation_snapshot_id"] == (
            "{{tasks.select_candidate_foundation.values."
            "foundation_snapshot_id}}"
        )
        assert parameters["--customer_cells_delta_version"].endswith(
            "customer_cells_delta_version}}"
        )
        assert parameters["--repeat_ad_exposure_delta_version"].endswith(
            "repeat_ad_exposure_delta_version}}"
        )
        assert parameters["--ad_feedback_delta_version"].endswith(
            "ad_feedback_delta_version}}"
        )
        page_task = tasks[f"run_page_build_{route}"]
        assert page_task["depends_on"] == [
            {"task_key": f"map_theme_scores_to_ads_{route}"}
        ]
        page_parameters = page_task["run_job_task"]["job_parameters"]
        assert page_parameters["customer_cells_table"].endswith(
            "customer_cells_table}}"
        )
        assert page_parameters["customer_cells_delta_version"].endswith(
            "customer_cells_delta_version}}"
        )


def test_assignment_tasks_read_the_manifest_bound_customer_cell_version():
    for relative_path in (
        "jobs/nextads_assignment/build_page.py",
        "jobs/nextads_v2/build_page.py",
    ):
        source = (PROJECT_ROOT / relative_path).read_text()
        assert "read_delta_version(" in source
        assert "CUSTOMER_CELLS_DELTA_VERSION" in source
        assert "spark.table(CELLS_TABLE_LATEST)" not in source


def test_foundation_tables_have_config_and_sql_contracts():
    runtime = yaml.safe_load(
        (PROJECT_ROOT / "configs/runtime/tables_settings.yaml").read_text()
    )["default"]["tables_write"]
    for key in (
        "candidate_foundation_builds",
        "candidate_foundation_sources",
        "candidate_repeat_ad_exposure",
        "candidate_ad_feedback",
    ):
        assert key in runtime
        contract = PROJECT_ROOT / "sql/ranking" / f"create_table_{key}.sql"
        assert contract.exists()
        sql = contract.read_text().lower()
        assert "primary key" in sql
        assert "partitioned by (rundate)" in sql
        assert "check (" not in sql

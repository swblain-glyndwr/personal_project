from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_job(path, key):
    job_config = yaml.safe_load((PROJECT_ROOT / path).read_text())
    return job_config["resources"]["jobs"][key]


def test_main_job_submits_qa_without_waiting_for_qa_result():
    job = _load_job(
        "resources/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}
    trigger_task = tasks_by_key["trigger_qa_job"]

    assert "run_qa_job" not in tasks_by_key
    assert not any("run_job_task" in task for task in job["tasks"])
    assert trigger_task["depends_on"] == [
        {"task_key": "nextads_plp_gs"},
        {"task_key": "viewed_bought"},
    ]
    assert trigger_task["spark_python_task"]["python_file"] == (
        "../../scripts/trigger_databricks_job.py"
    )


def test_qa_job_has_independent_definition_and_internal_notifications():
    job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_qa.yml",
        "mktg_next_uk_nextads_qa_cicd",
    )

    assert job["name"] == "mktg_next_uk_nextads_qa"
    assert "schedule" not in job
    assert job["email_notifications"]["on_failure"] == (
        "${var.qa_notification_emails}"
    )


def test_prod_qa_notifications_are_narrower_than_main_job_notifications():
    bundle_config = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text())
    prod_variables = bundle_config["targets"]["PROD"]["variables"]

    assert "qa_notification_emails" in prod_variables
    assert set(prod_variables["qa_notification_emails"]).issubset(
        set(prod_variables["notification_emails"])
    )
    assert prod_variables["qa_notification_emails"] == [
        "adrienne_lowe@next.co.uk",
        "hadi_miah@next.co.uk",
        "stephen_blain@next.co.uk",
        "edward_taylor@next.co.uk",
    ]

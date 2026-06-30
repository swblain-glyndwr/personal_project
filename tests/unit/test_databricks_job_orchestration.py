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
    trigger_task = tasks_by_key["trigger_page_build_job"]

    assert job["name"] == "mktg_next_uk_nextads_candidate_build"
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_team_notification_emails}"
    )
    assert "build_page_primary" not in tasks_by_key
    assert "build_page_v2" not in tasks_by_key
    assert not any("run_job_task" in task for task in job["tasks"])
    assert trigger_task["depends_on"] == [
        {"task_key": "combine_customer_cells"},
        {"task_key": "map_theme_scores_to_ads_v2"},
    ]
    assert trigger_task["spark_python_task"]["python_file"] == (
        "../../jobs/nextads_main/trigger_databricks_job.py"
    )
    assert trigger_task["spark_python_task"]["parameters"] == [
        "--job-id",
        "${resources.jobs.mktg_next_uk_nextads_page_build_cicd.id}",
        "--job-name",
        "mktg_next_uk_nextads_page_build",
        "--fail-on-submit-error",
    ]


def test_page_build_triggers_downstream_jobs_without_waiting_for_results():
    job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_page_build.yml",
        "mktg_next_uk_nextads_page_build_cicd",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert job["name"] == "mktg_next_uk_nextads_page_build"
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_team_notification_emails}"
    )
    assert not any("run_job_task" in task for task in job["tasks"])
    assert tasks_by_key["trigger_qa_job"]["depends_on"] == [
        {"task_key": "build_page_secondary"},
    ]
    assert tasks_by_key["trigger_masid_handoff_check_job"]["run_if"] == "ALL_DONE"
    assert tasks_by_key["trigger_masid_handoff_check_job"]["depends_on"] == [
        {"task_key": "build_page_secondary"},
    ]
    assert tasks_by_key["trigger_payload_export_job"]["run_if"] == "ALL_DONE"
    assert tasks_by_key["trigger_payload_export_job"]["depends_on"] == [
        {"task_key": "build_page_v2"},
    ]
    assert tasks_by_key["trigger_plp_gs_delivery_job"]["run_if"] == "ALL_DONE"
    assert tasks_by_key["trigger_plp_gs_delivery_job"]["depends_on"] == [
        {"task_key": "build_page_secondary"},
    ]


def test_qa_job_has_independent_definition_and_internal_notifications():
    job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_qa.yml",
        "mktg_next_uk_nextads_qa_cicd",
    )

    assert job["name"] == "mktg_next_uk_nextads_qa"
    assert "schedule" not in job
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_team_notification_emails}"
    )


def test_prod_data_team_notifications_are_internal_only():
    bundle_config = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text())
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
        "resources/jobs/mktg_next_uk_nextads_masid_handoff.yml",
        "mktg_next_uk_nextads_masid_handoff_cicd",
    )
    payload_job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_payload_export.yml",
        "mktg_next_uk_nextads_payload_export_cicd",
    )
    plp_job = _load_job(
        "resources/jobs/mktg_next_uk_nextads_plp_gs_delivery.yml",
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


def test_prod_notifications_are_split_by_owner_group():
    bundle_config = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text())
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
    assert prod_variables["reporting_notification_emails"] == reporting_recipients
    assert set(external_recipients).isdisjoint(
        set(prod_variables["data_team_notification_emails"])
    )

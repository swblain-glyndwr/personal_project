from pathlib import Path

import yaml

from tests.job_resource_helpers import load_job


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_job(path, key):
    return load_job(path, key)


def test_main_job_submits_page_build_without_waiting_for_result():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}
    trigger_v1_task = tasks_by_key["trigger_page_build_v1_job"]
    trigger_v2_task = tasks_by_key["trigger_page_build_v2_job"]

    assert job["name"] == "mktg_next_uk_nextads_candidate_build"
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_team_notification_emails}"
    )
    assert "build_page_primary" not in tasks_by_key
    assert "build_page_v2" not in tasks_by_key
    run_job_tasks = [
        task["task_key"] for task in job["tasks"] if "run_job_task" in task
    ]
    assert run_job_tasks == ["trigger_data_pull_for_CMS_pull"]
    assert trigger_v1_task["depends_on"] == [
        {"task_key": "combine_customer_cells"},
        {"task_key": "map_theme_scores_to_ads_v1"},
    ]
    assert trigger_v1_task["spark_python_task"]["python_file"] == (
        "../../../jobs/orchestration/trigger_databricks_job.py"
    )
    assert trigger_v2_task["depends_on"] == [
        {"task_key": "combine_customer_cells"},
        {"task_key": "map_theme_scores_to_ads_v2"},
    ]
    assert trigger_v1_task["spark_python_task"]["parameters"] == [
        "--job-id",
        "${resources.jobs.mktg_next_uk_nextads_page_build_cicd.id}",
        "--job-name",
        "mktg_next_uk_nextads_page_build",
        "--fail-on-submit-error",
    ]


def test_v2_mapping_does_not_depend_on_v1_mapping():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["map_theme_scores_to_ads_v2"]["depends_on"] == [
        {"task_key": "score_lightweight"},
        {"task_key": "load_control_sheet_v2"},
    ]
    assert {"task_key": "map_theme_scores_to_ads_v1"} not in tasks_by_key[
        "map_theme_scores_to_ads_v2"
    ]["depends_on"]


def test_theme_mapping_and_lightweight_scores_remain_shared_upstream():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}
    job_parameters = {
        param["name"]: param["default"] for param in job["parameters"]
    }

    assert job_parameters["refresh_theme_mapping"] == "false"
    assert "depends_on" not in tasks_by_key["validate_theme_mapping_sync"]
    assert tasks_by_key["parse_theme_mapping"]["depends_on"] == [
        {"task_key": "parse_attributes"},
        {"task_key": "validate_theme_mapping_sync"},
    ]
    assert (
        tasks_by_key["parse_theme_mapping"]["spark_python_task"]["parameters"]
    )[-2:] == [
        "--refresh_theme_mapping",
        "{{job.parameters.refresh_theme_mapping}}",
    ]
    assert tasks_by_key["score_lightweight"]["depends_on"] == [
        {"task_key": "parse_theme_mapping"},
    ]
    assert "parse_theme_mapping_v2" not in tasks_by_key
    assert "score_lightweight_v2" not in tasks_by_key
    assert (
        tasks_by_key["validate_theme_mapping_sync"]["spark_python_task"][
            "python_file"
        ]
        == "../../../jobs/nextads_control/validate_theme_mapping_sync.py"
    )
    assert (
        "--warn-only"
        not in tasks_by_key["validate_theme_mapping_sync"][
            "spark_python_task"
        ]["parameters"]
    )


def test_theme_affinity_coverage_validation_does_not_gate_route_mappers():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["validate_theme_affinity_theme_coverage"][
        "depends_on"
    ] == [
        {"task_key": "load_control_sheet_v1"},
        {"task_key": "load_control_sheet_v2"},
    ]
    assert tasks_by_key["validate_theme_affinity_theme_coverage"][
        "spark_python_task"
    ]["python_file"] == (
        "../../../jobs/nextads_candidates/validate_theme_affinity_theme_coverage.py"
    )
    assert (
        "--warn-only"
        in tasks_by_key["validate_theme_affinity_theme_coverage"][
            "spark_python_task"
        ]["parameters"]
    )
    assert tasks_by_key["map_theme_scores_to_ads_v1"]["depends_on"] == [
        {"task_key": "score_lightweight"},
        {"task_key": "load_control_sheet_v1"},
    ]
    assert tasks_by_key["map_theme_scores_to_ads_v2"]["depends_on"] == [
        {"task_key": "score_lightweight"},
        {"task_key": "load_control_sheet_v2"},
    ]


def test_page_build_triggers_downstream_jobs_without_waiting_for_results():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build.yml",
        "mktg_next_uk_nextads_page_build_cicd",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert job["name"] == "mktg_next_uk_nextads_page_build"
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_team_notification_emails}"
    )
    assert not any("run_job_task" in task for task in job["tasks"])
    assert tasks_by_key["trigger_assignment_validation_job"]["depends_on"] == [
        {"task_key": "build_page_secondary"},
    ]
    assert (
        tasks_by_key["trigger_masid_handoff_check_job"]["run_if"] == "ALL_DONE"
    )
    assert tasks_by_key["trigger_masid_handoff_check_job"]["depends_on"] == [
        {"task_key": "build_page_secondary"},
    ]
    assert tasks_by_key["trigger_plp_gs_delivery_job"]["run_if"] == "ALL_DONE"
    assert tasks_by_key["trigger_plp_gs_delivery_job"]["depends_on"] == [
        {"task_key": "build_page_secondary"},
    ]


def test_page_build_v2_triggers_downstream_jobs_without_waiting_for_results():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml",
        "mktg_next_uk_nextads_page_build_cicd_v2",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert job["name"] == "mktg_next_uk_nextads_page_build_v2"
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_and_downstream_notification_emails}"
    )

    assert tasks_by_key["trigger_payload_export_job"]["run_if"] == "ALL_DONE"
    assert tasks_by_key["trigger_payload_export_job"]["depends_on"] == [
        {"task_key": "build_page_v2"},
    ]


def test_data_pull_pipeline_passes_user_schema_to_python_config():
    pipeline_config = yaml.safe_load(
        (
            PROJECT_ROOT
            / "pipelines/databricks/pipelines/mktg_next_uk_nextads_data_pull.yml"
        ).read_text()
    )
    pipeline = pipeline_config["mktg_next_uk_nextads_data_pull"][
        "mktg_next_uk_nextads_data_pull"
    ]

    assert pipeline["schema"] == "${var.user_schema}"
    assert (
        pipeline["configuration"]["pipeline.user_schema"]
        == "${var.user_schema}"
    )
    assert pipeline["root_path"] == "${workspace.file_path}/src"
    assert pipeline["environment"] == "${var.pipeline_libraries}"


def test_assignment_validation_job_has_independent_definition_and_internal_notifications():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_assignment_validation.yml",
        "mktg_next_uk_nextads_assignment_validation_cicd",
    )

    assert job["name"] == "mktg_next_uk_nextads_assignment_validation"
    assert "schedule" not in job
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_team_notification_emails}"
    )


def test_prod_data_team_notifications_are_internal_only():
    bundle_config = yaml.safe_load(
        (PROJECT_ROOT / "databricks.yml").read_text()
    )
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
        "pipelines/databricks/jobs/mktg_next_uk_nextads_masid_handoff.yml",
        "mktg_next_uk_nextads_masid_handoff_cicd",
    )
    payload_job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_payload_export.yml",
        "mktg_next_uk_nextads_payload_export_cicd",
    )
    plp_job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_plp_gs_delivery.yml",
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
    bundle_config = yaml.safe_load(
        (PROJECT_ROOT / "databricks.yml").read_text()
    )
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
    assert (
        prod_variables["reporting_notification_emails"] == reporting_recipients
    )
    assert set(external_recipients).isdisjoint(
        set(prod_variables["data_team_notification_emails"])
    )

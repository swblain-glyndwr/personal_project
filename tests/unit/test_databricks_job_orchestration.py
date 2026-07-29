import json
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
    data_pull_task = tasks_by_key["trigger_data_pull_for_CMS_pull"]

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
    assert data_pull_task["run_job_task"]["job_parameters"] == {
        "run_date": "{{job.parameters.run_date}}",
    }
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
        "--job-parameter",
        "run_date={{job.parameters.run_date}}",
        "--fail-on-submit-error",
    ]
    assert trigger_v2_task["spark_python_task"]["parameters"] == [
        "--job-id",
        "${resources.jobs.mktg_next_uk_nextads_page_build_cicd_v2.id}",
        "--job-name",
        "mktg_next_uk_nextads_page_build_v2",
        "--job-parameter",
        "run_date={{job.parameters.run_date}}",
        "--fail-on-submit-error",
    ]
    assert job["parameters"] == [
        {
            "name": "run_date",
            "default": "{{job.start_time.iso_date}}",
        }
    ]


def test_route_mappers_depend_only_on_shared_cells_and_their_control_sheet():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert tasks_by_key["map_theme_scores_to_ads_v1"]["depends_on"] == [
        {"task_key": "combine_customer_cells"},
        {"task_key": "load_control_sheet_v1"},
    ]
    assert tasks_by_key["map_theme_scores_to_ads_v2"]["depends_on"] == [
        {"task_key": "combine_customer_cells"},
        {"task_key": "load_control_sheet_v2"},
    ]
    assert {"task_key": "map_theme_scores_to_ads_v1"} not in tasks_by_key[
        "map_theme_scores_to_ads_v2"
    ]["depends_on"]


def test_markov_scoring_has_an_independent_scheduled_resource():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_markov_scoring.yml",
        "mktg_next_uk_nextads_markov_scoring_cicd",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}
    job_parameters = {
        param["name"]: param["default"] for param in job["parameters"]
    }

    assert job["name"] == "mktg_next_uk_nextads_markov_scoring"
    assert job["schedule"] == {
        "quartz_cron_expression": "0 0 18 * * ?",
        "timezone_id": "Europe/London",
    }
    assert job["timeout_seconds"] == 8100
    assert job["job_clusters"] == "${var.job_clusters_config}"
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_team_notification_emails}"
    )
    assert job["notification_settings"] == {
        "no_alert_for_skipped_runs": True,
        "no_alert_for_canceled_runs": True,
    }
    assert job_parameters["refresh_theme_mapping"] == "false"
    assert list(tasks_by_key) == [
        "parse_attributes",
        "validate_theme_mapping_sync",
        "parse_theme_mapping",
        "score_lightweight",
    ]
    assert "depends_on" not in tasks_by_key["parse_attributes"]
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
    assert {
        task_key: task["job_cluster_key"]
        for task_key, task in tasks_by_key.items()
    } == {
        "parse_attributes": "next_ads_job_cluster_D16ads_v5_4_4",
        "validate_theme_mapping_sync": "next_ads_job_cluster_D4ads_v5_1_1",
        "parse_theme_mapping": "next_ads_job_cluster_D16ads_v5_4_4",
        "score_lightweight": "next_ads_job_cluster_D32ads_v5_1_4",
    }
    assert {
        task_key: task["timeout_seconds"]
        for task_key, task in tasks_by_key.items()
    } == {
        "parse_attributes": 7200,
        "validate_theme_mapping_sync": 1800,
        "parse_theme_mapping": 3600,
        "score_lightweight": 18000,
    }


def test_candidate_resource_excludes_legacy_markov_tasks():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    task_keys = {task["task_key"] for task in job["tasks"]}

    assert {
        "parse_attributes",
        "validate_theme_mapping_sync",
        "parse_theme_mapping",
        "score_lightweight",
    }.isdisjoint(task_keys)

    bundle_config = yaml.safe_load(
        (PROJECT_ROOT / "databricks.yml").read_text()
    )
    assert (
        "pipelines/databricks/jobs/"
        "mktg_next_uk_nextads_markov_scoring.yml"
        in bundle_config["include"]
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
        {"task_key": "combine_customer_cells"},
        {"task_key": "load_control_sheet_v1"},
    ]
    assert tasks_by_key["map_theme_scores_to_ads_v2"]["depends_on"] == [
        {"task_key": "combine_customer_cells"},
        {"task_key": "load_control_sheet_v2"},
    ]


def test_page_build_v1_publishes_one_complete_build_before_handoffs():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build.yml",
        "mktg_next_uk_nextads_page_build_cicd",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert job["name"] == "mktg_next_uk_nextads_page_build"
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_team_notification_emails}"
    )
    job_parameters = {
        parameter["name"]: parameter["default"]
        for parameter in job["parameters"]
    }
    assert list(job_parameters) == [
        "run_date",
        "build_run_id",
        "scope_manifest_json",
    ]
    assert job_parameters["run_date"] == "{{job.start_time.iso_date}}"
    assert job_parameters["build_run_id"] == "v1_{{job.run_id}}"

    scope_manifest = json.loads(job_parameters["scope_manifest_json"])
    primary_manifest = [
        entry for entry in scope_manifest if entry["phase"] == "primary"
    ]
    secondary_manifest = [
        entry for entry in scope_manifest if entry["phase"] == "secondary"
    ]
    assert len(primary_manifest) == 77
    assert secondary_manifest == [
        {
            "scope": "SB2",
            "phase": "secondary",
            "inherit_basic_from": "SB1",
        },
        {
            "scope": "OC2",
            "phase": "secondary",
            "inherit_basic_from": "OC1",
        },
    ]
    configured_locations = set(
        yaml.safe_load(
            (PROJECT_ROOT / "configs/clients/next_uk.yaml").read_text()
        )["default"]["locations"]
    )
    assert len({entry["scope"] for entry in scope_manifest}) == 79
    assert {entry["scope"] for entry in scope_manifest} == (
        configured_locations - {"HN1"}
    )
    assert scope_manifest == [*primary_manifest, *secondary_manifest]

    assert not any("run_job_task" in task for task in job["tasks"])

    prepare = tasks_by_key["prepare_assignment_scope_manifest"]
    assert prepare["notebook_task"] == {
        "notebook_path": (
            "../../../jobs/nextads_assignment/prepare_scope_manifest.py"
        ),
        "base_parameters": {
            "scope_manifest_json": (
                "{{job.parameters.scope_manifest_json}}"
            )
        },
        "source": "WORKSPACE",
    }
    assert prepare["timeout_seconds"] == 1800

    primary = tasks_by_key["build_page_primary"]
    assert primary["depends_on"] == [
        {"task_key": "prepare_assignment_scope_manifest"}
    ]
    assert primary["for_each_task"]["inputs"] == (
        "{{tasks.prepare_assignment_scope_manifest.values."
        "primary_scope_manifest}}"
    )
    assert primary["for_each_task"]["task"]["spark_python_task"][
        "parameters"
    ] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--location",
        "{{input.scope}}",
        "--scope_manifest_json",
        "{{job.parameters.scope_manifest_json}}",
        "--run_date",
        "{{job.parameters.run_date}}",
        "--build_run_id",
        "{{job.parameters.build_run_id}}",
        "--task_run_id",
        "{{task.run_id}}",
        "--execution_count",
        "{{task.execution_count}}",
    ]

    secondary = tasks_by_key["build_page_secondary"]
    assert secondary["depends_on"] == [
        {"task_key": "build_page_primary"}
    ]
    assert secondary["for_each_task"]["inputs"] == (
        "{{tasks.prepare_assignment_scope_manifest.values."
        "secondary_scope_manifest}}"
    )
    assert secondary["for_each_task"]["task"]["spark_python_task"][
        "parameters"
    ] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--location",
        "{{input.scope}}",
        "--inherit_basic_from",
        "{{input.inherit_basic_from}}",
        "--scope_manifest_json",
        "{{job.parameters.scope_manifest_json}}",
        "--run_date",
        "{{job.parameters.run_date}}",
        "--build_run_id",
        "{{job.parameters.build_run_id}}",
        "--task_run_id",
        "{{task.run_id}}",
        "--execution_count",
        "{{task.execution_count}}",
    ]

    publisher = tasks_by_key["publish_assignment_build_v1"]
    assert publisher["depends_on"] == [
        {"task_key": "build_page_secondary"}
    ]
    assert "run_if" not in publisher
    assert publisher["spark_python_task"]["python_file"] == (
        "../../../jobs/nextads_assignment/publish_build.py"
    )
    assert publisher["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--route",
        "v1",
        "--run_date",
        "{{job.parameters.run_date}}",
        "--build_run_id",
        "{{job.parameters.build_run_id}}",
        "--scope_manifest_json",
        "{{job.parameters.scope_manifest_json}}",
    ]

    downstream_keys = [
        "trigger_assignment_validation_job",
        "trigger_masid_handoff_check_job",
        "trigger_plp_gs_delivery_job",
    ]
    for task_key in downstream_keys:
        assert tasks_by_key[task_key]["depends_on"] == [
            {"task_key": "publish_assignment_build_v1"}
        ]
        assert "run_if" not in tasks_by_key[task_key]

    assert tasks_by_key["trigger_masid_handoff_check_job"][
        "spark_python_task"
    ]["parameters"] == [
        "--job-id",
        "${resources.jobs.mktg_next_uk_nextads_masid_handoff_cicd.id}",
        "--job-name",
        "mktg_next_uk_nextads_masid_handoff",
        "--job-parameter",
        "run_date={{job.parameters.run_date}}",
        "--fail-on-submit-error",
    ]
    assert tasks_by_key["trigger_plp_gs_delivery_job"][
        "spark_python_task"
    ]["parameters"] == [
        "--job-id",
        "${resources.jobs.mktg_next_uk_nextads_plp_gs_delivery_cicd.id}",
        "--job-name",
        "mktg_next_uk_nextads_plp_gs_delivery",
        "--job-parameter",
        "run_date={{job.parameters.run_date}}",
        "--fail-on-submit-error",
    ]
    assert all(task.get("run_if") != "ALL_DONE" for task in job["tasks"])


def test_page_build_v2_publishes_complete_build_before_payload_submission():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml",
        "mktg_next_uk_nextads_page_build_cicd_v2",
    )

    tasks_by_key = {task["task_key"]: task for task in job["tasks"]}

    assert job["name"] == "mktg_next_uk_nextads_page_build_v2"
    assert job["email_notifications"]["on_failure"] == (
        "${var.data_and_downstream_notification_emails}"
    )
    job_parameters = {
        parameter["name"]: parameter["default"]
        for parameter in job["parameters"]
    }
    assert list(job_parameters) == [
        "run_date",
        "build_run_id",
        "scope_manifest_json",
    ]
    assert job_parameters["run_date"] == "{{job.start_time.iso_date}}"
    assert job_parameters["build_run_id"] == "v2_{{job.run_id}}"

    scope_manifest = json.loads(job_parameters["scope_manifest_json"])
    assert scope_manifest == [
        {"scope": "HomePage"},
        {"scope": "ShoppingBagPage"},
        {"scope": "CheckoutPage"},
        {"scope": "ProductListingPage"},
        {"scope": "ForYouPage"},
    ]
    assert len({entry["scope"] for entry in scope_manifest}) == 5

    build_for_each = tasks_by_key["build_page_v2"]["for_each_task"]
    assert build_for_each["inputs"] == (
        "{{job.parameters.scope_manifest_json}}"
    )
    assert build_for_each["task"]["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--page_type",
        "{{input.scope}}",
        "--scope_manifest_json",
        "{{job.parameters.scope_manifest_json}}",
        "--run_date",
        "{{job.parameters.run_date}}",
        "--build_run_id",
        "{{job.parameters.build_run_id}}",
        "--task_run_id",
        "{{task.run_id}}",
        "--execution_count",
        "{{task.execution_count}}",
    ]

    publisher = tasks_by_key["publish_assignment_build_v2"]
    assert publisher["depends_on"] == [{"task_key": "build_page_v2"}]
    assert "run_if" not in publisher
    assert publisher["spark_python_task"]["python_file"] == (
        "../../../jobs/nextads_assignment/publish_build.py"
    )
    assert publisher["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--route",
        "v2",
        "--run_date",
        "{{job.parameters.run_date}}",
        "--build_run_id",
        "{{job.parameters.build_run_id}}",
        "--scope_manifest_json",
        "{{job.parameters.scope_manifest_json}}",
    ]

    assert tasks_by_key["trigger_payload_export_job"]["depends_on"] == [
        {"task_key": "publish_assignment_build_v2"},
    ]
    assert "run_if" not in tasks_by_key["trigger_payload_export_job"]
    assert tasks_by_key["trigger_payload_export_job"]["spark_python_task"][
        "parameters"
    ] == [
        "--job-id",
        "${resources.jobs.mktg_next_uk_nextads_payload_export_cicd.id}",
        "--job-name",
        "mktg_next_uk_nextads_payload_export",
        "--job-parameter",
        "run_date={{job.parameters.run_date}}",
        "--fail-on-submit-error",
    ]
    assert all(task.get("run_if") != "ALL_DONE" for task in job["tasks"])


def test_payload_export_uses_forwarded_logical_run_date():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_payload_export.yml",
        "mktg_next_uk_nextads_payload_export_cicd",
    )

    assert job["parameters"] == [
        {
            "name": "run_date",
            "default": "{{job.start_time.iso_date}}",
        }
    ]
    assert job["tasks"][0]["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--do_export",
        "1",
        "--run_date",
        "{{job.parameters.run_date}}",
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


def test_data_pull_archive_uses_forwarded_logical_run_date():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_data_pull.yaml",
        "mktg_next_uk_nextads_data_pull",
    )

    assert job["parameters"] == [
        {
            "name": "run_date",
            "default": "{{job.start_time.iso_date}}",
        }
    ]
    archive_task = next(
        task
        for task in job["tasks"]
        if task["task_key"] == "archive_sort_order_data"
    )
    assert archive_task["spark_python_task"]["parameters"][-4:] == [
        "--log_level",
        "INFO",
        "--run_date",
        "{{job.parameters.run_date}}",
    ]


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


def test_assignment_validation_entrypoint_is_read_only():
    source = (
        PROJECT_ROOT
        / "jobs/nextads_reporting/assignment_validation.py"
    ).read_text()
    normalized = source.lower()

    assert "delete from" not in normalized
    assert "truncate table" not in normalized
    assert ".write." not in normalized
    assert ".saveastable(" not in normalized


def test_masid_handoff_uses_forwarded_logical_run_date():
    job = _load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_masid_handoff.yml",
        "mktg_next_uk_nextads_masid_handoff_cicd",
    )

    assert job["parameters"] == [
        {
            "name": "run_date",
            "default": "{{job.start_time.iso_date}}",
        }
    ]
    assert job["tasks"][0]["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--expected_rundate",
        "{{job.parameters.run_date}}",
    ]


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
    assert plp_job["parameters"] == [
        {
            "name": "run_date",
            "default": "{{job.start_time.iso_date}}",
        }
    ]
    iteration_parameters = plp_job["tasks"][0]["for_each_task"]["task"][
        "spark_python_task"
    ]["parameters"]
    assert iteration_parameters == [
        "--client",
        "{{input.client}}",
        "--config_client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--territory",
        "{{input.territory}}",
        "--run_date",
        "{{job.parameters.run_date}}",
    ]


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

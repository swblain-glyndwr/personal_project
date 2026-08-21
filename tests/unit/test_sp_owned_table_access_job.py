from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RESOURCE_PATH = (
    "pipelines/databricks/jobs/mktg_next_uk_nextads_prod_table_access.yml"
)


def load_yaml(path):
    return yaml.safe_load((PROJECT_ROOT / path).read_text())


def test_prod_table_access_job_is_included_and_prod_only():
    bundle = load_yaml("databricks.yml")
    resource = load_yaml(RESOURCE_PATH)

    assert RESOURCE_PATH in bundle["include"]
    assert set(resource["targets"]) == {"PROD"}
    assert (
        bundle["targets"]["PROD"]["run_as"]["service_principal_name"]
        == "${var.run_as_SPN_name}"
    )
    assert (
        bundle["targets"]["PROD"]["variables"]["run_as_SPN_name"]
        == "2be8d1c2-d35b-4438-891e-558b9b5880f6"
    )


def test_prod_table_access_job_is_manual_inert_and_fixed_scope():
    resource = load_yaml(RESOURCE_PATH)
    job = resource["targets"]["PROD"]["resources"]["jobs"][
        "mktg_next_uk_nextads_prod_table_access"
    ]
    parameters = {item["name"]: item["default"] for item in job["parameters"]}
    task = job["tasks"][0]
    task_parameters = task["spark_python_task"]["parameters"]

    assert "schedule" not in job
    assert "trigger" not in job
    assert job["max_concurrent_runs"] == 1
    assert parameters == {"confirm_mutating": "false", "dry_run": "true"}
    assert task["task_key"] == "grant_prod_sp_owned_table_access"
    assert task["spark_python_task"]["python_file"] == (
        "../../../jobs/table_operations/grant_sp_owned_table_access.py"
    )
    assert task["job_cluster_key"] == "next_ads_job_cluster_D4ads_v5_1_1"
    assert "libraries" not in task
    assert job["timeout_seconds"] == 3600
    assert task["timeout_seconds"] == 3600
    assert task_parameters == [
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--catalog",
        "${var.mktgdata_catalog}",
        "--schema",
        "${var.user_schema}",
        "--expected_owner",
        "${var.run_as_SPN_name}",
        "--confirm_mutating",
        "{{job.parameters.confirm_mutating}}",
        "--dry_run",
        "{{job.parameters.dry_run}}",
        "--log_level",
        "INFO",
    ]

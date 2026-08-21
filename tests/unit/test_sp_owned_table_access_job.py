from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RESOURCE_PATH = (
    "pipelines/databricks/jobs/mktg_next_uk_nextads_sp_owned_table_access.yml"
)
JOB_KEY = "mktg_next_uk_nextads_sp_owned_table_access"


def load_yaml(path):
    return yaml.safe_load((PROJECT_ROOT / path).read_text())


def test_sp_owned_table_access_job_is_included_for_dev_and_prod_only():
    bundle = load_yaml("databricks.yml")
    resource = load_yaml(RESOURCE_PATH)

    assert RESOURCE_PATH in bundle["include"]
    assert set(resource["targets"]) == {"DEV", "PROD"}
    expected_service_principals = {
        "DEV": "7ecc733a-4b66-4783-b984-985333d55c38",
        "PROD": "2be8d1c2-d35b-4438-891e-558b9b5880f6",
    }
    for (
        target,
        expected_service_principal,
    ) in expected_service_principals.items():
        assert (
            bundle["targets"][target]["run_as"]["service_principal_name"]
            == "${var.run_as_SPN_name}"
        )
        assert (
            bundle["targets"][target]["variables"]["run_as_SPN_name"]
            == expected_service_principal
        )


def test_sp_owned_table_access_job_is_manual_inert_and_target_scoped():
    resource = load_yaml(RESOURCE_PATH)
    for target in ("DEV", "PROD"):
        job = resource["targets"][target]["resources"]["jobs"][JOB_KEY]
        parameters = {
            item["name"]: item["default"] for item in job["parameters"]
        }
        task = job["tasks"][0]
        task_parameters = task["spark_python_task"]["parameters"]

        assert "schedule" not in job
        assert "trigger" not in job
        assert job["max_concurrent_runs"] == 1
        assert parameters == {
            "confirm_mutating": "false",
            "dry_run": "true",
        }
        assert task["task_key"] == "grant_sp_owned_table_access"
        assert task["spark_python_task"]["python_file"] == (
            "../../../jobs/table_operations/grant_sp_owned_table_access.py"
        )
        assert task["job_cluster_key"] == "next_ads_job_cluster_D4ads_v5_1_1"
        assert "libraries" not in task
        assert job["timeout_seconds"] == 14400
        assert task["timeout_seconds"] == 14400
        assert task_parameters == [
            "--job_env",
            "${var.job_parameter_environment_name}",
            "--catalog",
            "${var.mktgdata_catalog}",
            "--relation_scope",
            "${var.sp_owned_table_access_scope}",
            "--expected_owner",
            "${var.run_as_SPN_name}",
            "--confirm_mutating",
            "{{job.parameters.confirm_mutating}}",
            "--dry_run",
            "{{job.parameters.dry_run}}",
            "--log_level",
            "INFO",
        ]


def test_access_job_uses_a_dedicated_scope_variable_per_target():
    bundle = load_yaml("databricks.yml")
    resource = load_yaml(RESOURCE_PATH)

    assert bundle["targets"]["DEV"]["variables"]["mktgdata_catalog"] == (
        "marketingdata_dev"
    )
    assert (
        resource["targets"]["DEV"]["variables"]["sp_owned_table_access_scope"]
        == "ALL_SP_OWNED_SCHEMAS"
    )
    assert (
        resource["targets"]["PROD"]["variables"]["sp_owned_table_access_scope"]
        == "WAREHOUSE_AND_DS_SANDBOX"
    )
    assert (
        bundle["targets"]["DEV"]["variables"]["job_parameter_environment_name"]
        == "dev"
    )

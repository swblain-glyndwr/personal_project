from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def load_yaml(path):
    return yaml.safe_load((PROJECT_ROOT / path).read_text())


def test_validation_pipeline_is_non_deploying_dev_gate():
    pipeline = (PROJECT_ROOT / "azure-pipelines-validation.yml").read_text()
    config = yaml.safe_load(pipeline)

    assert "deploy-dab.yml" not in pipeline
    assert "destroy-dab.yml" not in pipeline
    assert "serviceConnectionName_prod" not in pipeline
    assert "agentpool_prod" not in pipeline

    jobs = config["stages"][0]["jobs"]
    test_job = jobs[0]["parameters"]
    dab_job = jobs[1]["parameters"]

    assert test_job["target"] == "DEV_INTEGRATION"
    assert dab_job["target"] == "DEV_INTEGRATION"
    assert test_job["continueOnError"] is False


def test_dev_integration_target_uses_dev_workspace_and_schema():
    bundle = load_yaml("databricks.yml")
    target = bundle["targets"]["DEV_INTEGRATION"]
    variables = target["variables"]

    assert target["workspace"]["host"] == (
        "https://adb-6694370232251359.19.azuredatabricks.net/"
    )
    assert variables["mktgdata_catalog"] == "marketingdata_dev"
    assert variables["environment_name"] == "dev"
    assert variables["job_parameter_environment_name"] == "dev"
    assert variables["user_schema"] == "nextads_integration"
    assert target["presets"]["trigger_pause_status"] == "PAUSED"


def test_preprod_and_prod_output_routes_are_separate():
    bundle = load_yaml("databricks.yml")
    settings = load_yaml("config/settings.yaml")

    preprod_vars = bundle["targets"]["PREPROD"]["variables"]
    prod_vars = bundle["targets"]["PROD"]["variables"]

    assert preprod_vars["mktgdata_catalog"] == "marketingdata_prod"
    assert preprod_vars["job_parameter_environment_name"] == "preprod"
    assert settings["preprod"]["catalog_write"] == "marketingdata_prod"
    assert settings["preprod"]["schema_write"] == "ds_sandbox"

    assert prod_vars["mktgdata_catalog"] == "marketingdata_prod"
    assert prod_vars["job_parameter_environment_name"] == "prod"
    assert settings["prod"]["catalog_write"] == "marketingdata_prod"
    assert settings["prod"]["schema_write"] == "warehouse"

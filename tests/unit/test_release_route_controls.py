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


def test_bundle_sync_explicitly_includes_transitional_package_roots():
    bundle = load_yaml("databricks.yml")
    sync_includes = bundle["sync"]["include"]

    assert "next_ads/**" in sync_includes
    assert "next_ads/data/**" in sync_includes
    assert "src/next_ads/**" in sync_includes
    assert "src/next_ads/data/**" in sync_includes


def test_gitignore_does_not_exclude_package_data_directories():
    gitignore = (PROJECT_ROOT / ".gitignore").read_text().splitlines()

    assert "/data/*" in gitignore
    assert "data/*" not in gitignore


def test_deployment_pipeline_has_develop_only_dev_integration_route():
    config = load_yaml("azure-pipelines.yml")
    stages = {stage["stage"]: stage for stage in config["stages"]}

    parameter_names = {parameter["name"] for parameter in config["parameters"]}
    assert "recreateDevIntegrationTables" in parameter_names

    deploy_stage = stages["DeployDEVIntegration"]
    destroy_stage = stages["DestroyDEVIntegration"]
    init_stage = stages["InitializeDEVIntegrationTables"]

    assert "IntegrationTests" not in stages["DeployDEV"]["dependsOn"]
    assert "IntegrationTests" not in stages["DeployDEVIntegration"]["dependsOn"]
    assert "IntegrationTests" not in stages["DestroyDEV"]["dependsOn"]
    assert "IntegrationTests" not in stages["DestroyDEVIntegration"]["dependsOn"]

    assert "refs/heads/develop" in deploy_stage["condition"]
    assert "refs/heads/develop" in destroy_stage["condition"]
    assert "refs/heads/develop" in init_stage["condition"]

    deploy_job = deploy_stage["jobs"][0]["parameters"]
    destroy_job = destroy_stage["jobs"][0]["parameters"]

    assert deploy_job["target"] == "DEV_INTEGRATION"
    assert deploy_job["AzureBuildAgentPool"] == "$(agentpool_dev)"
    assert deploy_job["azureSubscription"] == "$(serviceConnectionName_dev)"
    assert destroy_job["target"] == "DEV_INTEGRATION"

    run_setup_step = init_stage["jobs"][0]["steps"][-1]["script"]
    assert (
        "databricks bundle run mktg_next_uk_nextads_dev_integration_setup"
        in run_setup_step
    )
    assert (
        "databricks bundle run mktg_next_uk_nextads_dev_integration_alter"
        in run_setup_step
    )
    assert (
        "databricks bundle run mktg_next_uk_nextads_dev_integration_migrate"
        in run_setup_step
    )


def test_dev_integration_setup_job_is_target_specific():
    setup = load_yaml("resources/jobs/dev_integration_setup.yml")
    jobs = setup["targets"]["DEV_INTEGRATION"]["resources"]["jobs"]
    setup_job = jobs["mktg_next_uk_nextads_dev_integration_setup"]
    migrate_job = jobs["mktg_next_uk_nextads_dev_integration_migrate"]
    alter_job = jobs["mktg_next_uk_nextads_dev_integration_alter"]
    setup_task = setup_job["tasks"][0]
    migrate_task = migrate_job["tasks"][0]
    alter_task = alter_job["tasks"][0]

    assert set(setup["targets"]) == {"DEV_INTEGRATION"}
    assert setup_task["task_key"] == "create_tables"
    assert (
        setup_task["spark_python_task"]["python_file"]
        == "../../scripts/table_operations/create_tables.py"
    )
    assert setup_task["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--log_level",
        "INFO",
    ]
    assert migrate_task["task_key"] == "recreate_tables"
    assert migrate_task["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--log_level",
        "INFO",
        "--droptables",
        "True",
    ]
    assert alter_task["task_key"] == "alter_tables"
    assert alter_task["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--log_level",
        "INFO",
        "--altertables",
        "True",
    ]


def test_preprod_route_is_release_branch_only():
    config = load_yaml("azure-pipelines.yml")
    stages = {stage["stage"]: stage for stage in config["stages"]}

    deploy_stage = stages["DeployPREPROD"]
    destroy_stage = stages["DestroyPREPROD"]
    init_stage = stages["InitializePREPRODTables"]
    smoke_stage = stages["SmokePREPRODDependencies"]

    assert deploy_stage["displayName"] == "Deploy PREPROD"
    assert "refs/heads/release/" in deploy_stage["condition"]
    assert "refs/heads/release/" in destroy_stage["condition"]
    assert "refs/heads/release/" in init_stage["condition"]
    assert "refs/heads/release/" in smoke_stage["condition"]
    assert "DeployDEV" not in deploy_stage["dependsOn"]
    assert "IntegrationTests" not in deploy_stage["dependsOn"]
    assert smoke_stage["dependsOn"] == ["DeployPREPROD"]

    deploy_job = deploy_stage["jobs"][0]["parameters"]
    destroy_job = destroy_stage["jobs"][0]["parameters"]

    assert deploy_job["target"] == "PREPROD"
    assert deploy_job["AzureBuildAgentPool"] == "$(agentpool_prod)"
    assert deploy_job["azureSubscription"] == "$(serviceConnectionName_prod)"
    assert destroy_job["target"] == "PREPROD"

    run_setup_step = init_stage["jobs"][0]["steps"][-1]["script"]
    assert "databricks bundle run mktg_next_uk_nextads_preprod_setup" in run_setup_step
    assert "-t PREPROD" in run_setup_step

    run_smoke_step = smoke_stage["jobs"][0]["steps"][-1]["script"]
    assert "databricks bundle run mktg_next_uk_nextads_preprod_dependency_smoke" in run_smoke_step
    assert "-t PREPROD" in run_smoke_step


def test_preprod_setup_job_is_target_specific_and_non_destructive():
    setup = load_yaml("resources/jobs/preprod_setup.yml")
    jobs = setup["targets"]["PREPROD"]["resources"]["jobs"]
    setup_job = jobs["mktg_next_uk_nextads_preprod_setup"]
    setup_task = setup_job["tasks"][0]

    assert set(setup["targets"]) == {"PREPROD"}
    assert setup_task["task_key"] == "create_tables"
    assert (
        setup_task["spark_python_task"]["python_file"]
        == "../../scripts/table_operations/create_tables.py"
    )
    assert setup_task["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--log_level",
        "INFO",
    ]
    assert "--droptables" not in setup_task["spark_python_task"]["parameters"]


def test_preprod_dependency_smoke_job_is_metadata_only_and_target_specific():
    setup = load_yaml("resources/jobs/preprod_dependency_smoke.yml")
    jobs = setup["targets"]["PREPROD"]["resources"]["jobs"]
    smoke_job = jobs["mktg_next_uk_nextads_preprod_dependency_smoke"]
    smoke_task = smoke_job["tasks"][0]

    assert set(setup["targets"]) == {"PREPROD"}
    assert smoke_task["task_key"] == "dependency_smoke"
    assert (
        smoke_task["spark_python_task"]["python_file"]
        == "../../scripts/smoke/preprod_dependency_smoke.py"
    )
    assert smoke_task["spark_python_task"]["parameters"] == [
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--sample_read_count",
        "0",
        "--log_level",
        "INFO",
    ]

    script = (PROJECT_ROOT / "scripts/smoke/preprod_dependency_smoke.py").read_text()
    banned_write_operations = [
        "saveAsTable",
        "write.",
        "DELETE FROM",
        "INSERT INTO",
        "DROP TABLE",
        "TRUNCATE TABLE",
        "CREATE TABLE",
    ]
    for operation in banned_write_operations:
        assert operation not in script

    assert "Skipping sample reads; metadata-only smoke requested" in script


def test_prod_deployment_is_tag_only_and_hotfixes_validate_by_pr():
    deploy_pipeline = load_yaml("azure-pipelines.yml")
    validation_pipeline = load_yaml("azure-pipelines-validation.yml")
    stages = {stage["stage"]: stage for stage in deploy_pipeline["stages"]}
    deploy_condition = stages["DeployPROD"]["condition"]
    destroy_condition = stages["DestroyPROD"]["condition"]

    assert "refs/tags/" in deploy_condition
    assert "refs/tags/" in destroy_condition

    branch_refs_not_allowed_for_prod = [
        "refs/heads/develop",
        "refs/heads/main",
        "refs/heads/release/",
        "refs/heads/hotfix/",
        "refs/heads/hotfix/*",
    ]
    for branch_ref in branch_refs_not_allowed_for_prod:
        assert branch_ref not in deploy_condition
        assert branch_ref not in destroy_condition

    validation_branches = validation_pipeline["trigger"]["branches"]["include"]
    assert "main" in validation_branches
    assert "hotfix/*" in validation_branches
    assert "release/*" in validation_branches


def test_production_release_documentation_defines_tagged_route_and_evidence():
    release_route_doc = (
        PROJECT_ROOT / "docs/CICD/nextads_branch_release_route.md"
    ).read_text()
    workflow_doc = (PROJECT_ROOT / "docs/developer_workflow_guide.md").read_text()
    docs = f"{release_route_doc}\n{workflow_doc}"

    required_release_controls = [
        "nextads-vYYYY.MM.DD.N",
        "manual PROD pipeline run",
        "mktg-next-ads-ci-cd",
        "mktg-next-ads-validation",
        "NextAds main validation",
        "main PR",
        "production tag",
        "PROD pipeline run",
        "validated `release/*` branch",
        "Do not create production tags from `develop`, `release/*`, `hotfix/*`",
    ]
    for required_text in required_release_controls:
        assert required_text in docs


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

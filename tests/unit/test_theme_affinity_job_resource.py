from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_yaml(path):
    return yaml.safe_load((PROJECT_ROOT / path).read_text())


def _theme_affinity_job():
    job_config = _load_yaml(
        "resources/jobs/mktg_next_uk_nextads_theme_affinity.yml"
    )
    return job_config["resources"]["jobs"][
        "mktg_next_uk_nextads_theme_affinity_cicd"
    ]


def test_theme_affinity_resources_are_included_in_bundle():
    bundle_config = _load_yaml("databricks.yml")

    assert (
        "resources/jobs/mktg_next_uk_nextads_theme_affinity.yml"
        in bundle_config["include"]
    )
    assert (
        "resources/pipelines/mktg_next_uk_nextads_predict_data_prep.yml"
        in bundle_config["include"]
    )
    assert "hackathon_model/**" not in bundle_config["sync"]["include"]


def test_theme_affinity_job_uses_lakeflow_and_script_tasks():
    job = _theme_affinity_job()

    assert job["name"] == "mktg_next_uk_nextads_theme_affinity"
    assert "schedule" not in job
    assert job["job_clusters"] == "${var.job_clusters_config}"

    tasks = {task["task_key"]: task for task in job["tasks"]}
    assert set(tasks) == {
        "predict_data_prep",
        "model_predict",
        "sense_check_dlt_data",
        "clean_output",
        "sense_check_model_outputs",
    }
    assert "pipeline_task" in tasks["predict_data_prep"]
    assert (
        tasks["predict_data_prep"]["pipeline_task"]["pipeline_id"]
        == "${resources.pipelines.nextads_theme_affinity_predict_data_prep.id}"
    )

    for task_key in ["model_predict", "clean_output"]:
        task = tasks[task_key]
        assert "notebook_task" not in task
        assert "spark_python_task" in task
        assert task["libraries"] == "${var.theme_affinity_libraries}"
        parameters = task["spark_python_task"]["parameters"]
        assert "--job_env" in parameters
        assert "${var.job_parameter_environment_name}" in parameters
        assert "ds_sandbox" not in parameters
        assert "next_uk_nextAds_predict_prod" not in parameters

    dlt_sense_check = tasks["sense_check_dlt_data"]
    assert dlt_sense_check["depends_on"] == [{"task_key": "predict_data_prep"}]
    assert "notebook_task" not in dlt_sense_check
    assert "spark_python_task" in dlt_sense_check
    assert (
        dlt_sense_check["spark_python_task"]["python_file"]
        == "../../scripts/theme_affinity/sense_check.py"
    )
    assert dlt_sense_check["libraries"] == "${var.theme_affinity_libraries}"
    assert "data" in dlt_sense_check["spark_python_task"]["parameters"]

    model_sense_check = tasks["sense_check_model_outputs"]
    assert model_sense_check["depends_on"] == [{"task_key": "clean_output"}]
    assert "notebook_task" not in model_sense_check
    assert "spark_python_task" in model_sense_check
    assert (
        model_sense_check["spark_python_task"]["python_file"]
        == "../../scripts/theme_affinity/sense_check.py"
    )
    assert model_sense_check["libraries"] == "${var.theme_affinity_libraries}"
    assert "model_outputs" in model_sense_check["spark_python_task"]["parameters"]


def test_theme_affinity_libraries_avoid_full_runtime_requirements():
    variables = _load_yaml("resources/variables/libraries.yml")["variables"]

    assert variables["theme_affinity_libraries"]["default"] == [
        {
            "whl": (
                "/Volumes/${var.mktgdata_catalog}/ds_sandbox/ds_volume/"
                "dslib/dsutils-0.1.13-py3-none-any.whl"
            )
        },
        {"requirements": "../../requirements-theme-affinity.txt"},
    ]

    requirements = (
        PROJECT_ROOT / "requirements-theme-affinity.txt"
    ).read_text()
    assert "xgboost==3.0.0" in requirements
    assert "scikit-learn==1.6.1" in requirements
    assert "numpy>=1.26,<2.0" in requirements
    assert "protobuf" not in requirements
    assert "grpcio-status" not in requirements
    assert "databricks-connect" not in requirements
    assert "mlflow" not in requirements


def test_theme_affinity_script_bootstrap_handles_workspace_paths():
    for script_path in [
        "scripts/theme_affinity/model_predict.py",
        "scripts/theme_affinity/clean_output.py",
        "scripts/theme_affinity/sense_check.py",
        "scripts/theme_affinity/rules_rank.py",
        "scripts/theme_affinity/run_pipeline.py",
    ]:
        script = (PROJECT_ROOT / script_path).read_text()
        assert "Path(notebook_path).parents[2]" in script
        assert 'SRC_ROOT = PROJECT_ROOT / "src"' in script


def test_theme_affinity_sense_check_compares_dev_to_prod_sandbox():
    job = _theme_affinity_job()
    tasks = {task["task_key"]: task for task in job["tasks"]}
    data_parameters = tasks["sense_check_dlt_data"]["spark_python_task"][
        "parameters"
    ]
    model_parameters = tasks["sense_check_model_outputs"]["spark_python_task"][
        "parameters"
    ]
    parameters = data_parameters + model_parameters

    assert "marketingdata_prod.ds_sandbox" in parameters
    assert "next_uk_nextAds_predict_prod" in parameters
    assert (
        "marketingdata_prod.ds_sandbox.next_uk_next_ads_hackathon_model_full"
        in parameters
    )
    assert (
        "${var.mktgdata_catalog}.${var.user_schema}."
        "next_uk_nextads_theme_affinity_dlt_sense_check_summary"
        in data_parameters
    )
    assert (
        "${var.mktgdata_catalog}.${var.user_schema}."
        "next_uk_nextads_theme_affinity_model_sense_check_summary"
        in model_parameters
    )
    assert "data" in data_parameters
    assert "model_outputs" in model_parameters
    assert (
        "marketingdata_prod.ds_sandbox.next_uk_next_ads_hackathon_model_full"
        not in data_parameters
    )
    assert (
        "marketingdata_prod.ds_sandbox.next_uk_next_ads_hackathon_model_full"
        in model_parameters
    )


def test_theme_affinity_pipeline_uses_target_schema_variable():
    pipeline = _load_yaml(
        "resources/pipelines/mktg_next_uk_nextads_predict_data_prep.yml"
    )
    targets = pipeline["targets"]

    assert set(targets) == {"SANDBOX", "DEV", "DEV_INTEGRATION", "PREPROD", "PROD"}
    for target in targets.values():
        pipeline_config = target["resources"]["pipelines"][
            "nextads_theme_affinity_predict_data_prep"
        ]
        assert pipeline_config["catalog"] == "${var.mktgdata_catalog}"
        assert pipeline_config["schema"] == "${var.user_schema}"
        assert (
            pipeline_config["configuration"]["pipeline.schema"]
            == "${var.user_schema}"
        )
        assert (
            pipeline_config["configuration"]["pipeline.job_env"]
            == "${var.job_parameter_environment_name}"
        )
        assert (
            pipeline_config["configuration"]["pipeline.table_prefix"]
            == "next_uk_nextads_theme_affinity_predict"
        )
        assert (
            pipeline_config["configuration"]["pipeline.sql_path"]
            == "${workspace.file_path}/src/next_ads/ranking/theme_affinity/sql"
        )
        assert (
            pipeline_config["libraries"][0]["glob"]["include"]
            == "${workspace.file_path}/src/next_ads/ranking/theme_affinity/dlt_pipeline.py"
        )


def test_theme_affinity_runtime_files_do_not_use_legacy_write_paths():
    checked_paths = [
        PROJECT_ROOT / "resources/jobs/mktg_next_uk_nextads_theme_affinity.yml",
        PROJECT_ROOT / "resources/pipelines/mktg_next_uk_nextads_predict_data_prep.yml",
        PROJECT_ROOT / "scripts/theme_affinity",
        PROJECT_ROOT / "src/next_ads/ranking/theme_affinity",
    ]
    forbidden = [
        "/hackathon_model",
        "legacy_name: hackathon",
    ]

    for path in checked_paths:
        files = (
            [path]
            if path.is_file()
            else list(path.rglob("*.py")) + list(path.rglob("*.sql"))
        )
        for file_path in files:
            text = file_path.read_text()
            for forbidden_text in forbidden:
                assert forbidden_text not in text


def test_theme_affinity_dlt_uses_product_catalog_projection():
    data_prep = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_affinity/data_prep.py"
    ).read_text()
    product_catalog = (
        PROJECT_ROOT
        / "src/next_ads/ranking/theme_affinity/sql/0_product_catalog.sql"
    ).read_text()
    customer_segments = (
        PROJECT_ROOT
        / "src/next_ads/ranking/theme_affinity/sql/4_customer_segments.sql"
    ).read_text()

    assert "0_product_catalog.sql" in data_prep
    assert (
        "businessintelligencesystems_prod.ecommerce.bloomreach_uk_product_catalog"
        in product_catalog
    )
    assert "{schema}.{table_prefix}_product_catalog" in customer_segments


def test_theme_affinity_pipeline_helpers_are_private():
    pipeline_source = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_affinity/dlt_pipeline.py"
    ).read_text()

    assert '@dp.table(name="0_theme_mapping", private=True)' in pipeline_source
    assert '@dp.table(name="spine", private=True)' in pipeline_source


def test_theme_affinity_dlt_uses_operational_reference_date_variable():
    pipeline = _load_yaml(
        "resources/pipelines/mktg_next_uk_nextads_predict_data_prep.yml"
    )
    bundle = _load_yaml("databricks.yml")
    dlt_source = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_affinity/dlt_pipeline.py"
    ).read_text()

    assert (
        bundle["variables"]["theme_affinity_reference_date"]["default"]
        == "current"
    )
    for target in pipeline["targets"].values():
        pipeline_config = target["resources"]["pipelines"][
            "nextads_theme_affinity_predict_data_prep"
        ]
        assert (
            pipeline_config["configuration"]["pipeline.reference_date"]
            == "${var.theme_affinity_reference_date}"
        )

    assert 'REFERENCE_DATE = datetime.today().strftime("%Y-%m-%d")' not in dlt_source
    assert "pipeline.reference_date: predict" not in (
        PROJECT_ROOT / "resources/pipelines/mktg_next_uk_nextads_predict_data_prep.yml"
    ).read_text()

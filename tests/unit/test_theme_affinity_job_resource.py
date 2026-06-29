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
    assert "jobs/**" in bundle_config["sync"]["include"]
    assert (
        "resources/pipelines/mktg_next_uk_nextads_predict_data_prep.yml"
        in bundle_config["include"]
    )
    assert "hackathon_model/**" not in bundle_config["sync"]["include"]


def test_theme_affinity_job_uses_lakeflow_and_script_tasks():
    job = _theme_affinity_job()

    assert job["name"] == "mktg_next_uk_nextads_theme_affinity"
    assert job["schedule"] == {
        "quartz_cron_expression": "0 0 9 * * ?",
        "timezone_id": "Europe/London",
    }
    assert job["job_clusters"] == "${var.job_clusters_config}"
    assert job["parameters"] == [
        {
            "name": "publish_source_namespace",
            "default": (
                "${var.mktgdata_catalog}."
                "${var.theme_affinity_pipeline_schema}"
            ),
        },
        {
            "name": "publish_target_namespace",
            "default": "${var.mktgdata_catalog}.${var.user_schema}",
        },
        {
            "name": "publish_source_table_prefix",
            "default": "next_uk_nextads_theme_affinity_predict",
        },
        {
            "name": "publish_target_table_prefix",
            "default": "next_uk_nextads_theme_affinity_predict",
        },
        {
            "name": "publish_table_suffixes",
            "default": "${var.theme_affinity_publish_table_suffixes}",
        },
    ]

    tasks = {task["task_key"]: task for task in job["tasks"]}
    assert set(tasks) == {
        "predict_data_prep",
        "publish_dlt_outputs",
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

    publish_task = tasks["publish_dlt_outputs"]
    assert publish_task["depends_on"] == [{"task_key": "predict_data_prep"}]
    assert "notebook_task" not in publish_task
    assert "spark_python_task" in publish_task
    assert (
        publish_task["spark_python_task"]["python_file"]
        == "../../jobs/model/theme_affinity/publish_outputs.py"
    )
    assert publish_task["libraries"] == "${var.theme_affinity_libraries}"
    publish_parameters = publish_task["spark_python_task"]["parameters"]
    assert (
        publish_parameters[publish_parameters.index("--source_namespace") + 1]
        == "{{job.parameters.publish_source_namespace}}"
    )
    assert (
        publish_parameters[publish_parameters.index("--target_namespace") + 1]
        == "{{job.parameters.publish_target_namespace}}"
    )
    assert (
        publish_parameters[publish_parameters.index("--table_prefix") + 1]
        == "{{job.parameters.publish_source_table_prefix}}"
    )
    assert (
        publish_parameters[publish_parameters.index("--target_table_prefix") + 1]
        == "{{job.parameters.publish_target_table_prefix}}"
    )
    assert (
        publish_parameters[publish_parameters.index("--table_suffixes") + 1]
        == "{{job.parameters.publish_table_suffixes}}"
    )

    assert tasks["model_predict"]["depends_on"] == [
        {"task_key": "publish_dlt_outputs"}
    ]
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
        == "../../jobs/model/theme_affinity/sense_check.py"
    )
    assert dlt_sense_check["libraries"] == "${var.theme_affinity_libraries}"
    assert "data" in dlt_sense_check["spark_python_task"]["parameters"]

    model_sense_check = tasks["sense_check_model_outputs"]
    assert model_sense_check["depends_on"] == [{"task_key": "clean_output"}]
    assert "notebook_task" not in model_sense_check
    assert "spark_python_task" in model_sense_check
    assert (
        model_sense_check["spark_python_task"]["python_file"]
        == "../../jobs/model/theme_affinity/sense_check.py"
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
    assert "mlflow==3.11.1" in requirements
    assert "xgboost==3.0.0" in requirements
    assert "scikit-learn==1.6.1" in requirements
    assert "numpy>=1.26,<2.0" in requirements
    assert "marketingdata_utils" not in requirements
    assert "protobuf" not in requirements
    assert "grpcio-status" not in requirements
    assert "databricks-connect" not in requirements


def test_theme_affinity_mlflow_lifecycle_resources_are_included():
    bundle_config = _load_yaml("databricks.yml")

    assert (
        "resources/jobs/mktg_next_uk_nextads_theme_affinity_model_train.yml"
        in bundle_config["include"]
    )
    assert (
        "resources/jobs/mktg_next_uk_nextads_theme_affinity_model_train_spark.yml"
        in bundle_config["include"]
    )
    assert (
        "resources/jobs/mktg_next_uk_nextads_theme_affinity_model_monitor.yml"
        in bundle_config["include"]
    )
    assert (
        "resources/jobs/"
        "mktg_next_uk_nextads_theme_affinity_quality_monitor_setup.yml"
        in bundle_config["include"]
    )
    assert (
        "resources/jobs/"
        "mktg_next_uk_nextads_theme_affinity_model_import_dev.yml"
        in bundle_config["include"]
    )
    assert (
        "resources/jobs/mktg_next_uk_nextads_theme_affinity_model_promote.yml"
        in bundle_config["include"]
    )
    assert "resources/jobs/mktg_next_uk_nextads_model_train.yml" not in bundle_config[
        "include"
    ]
    assert (
        "resources/jobs/mktg_next_uk_nextads_model_predict.yml"
        not in bundle_config["include"]
    )
    assert (
        bundle_config["include"].count(
            "resources/pipelines/mktg_next_uk_nextads_predict_data_prep.yml"
        )
        == 1
    )


def test_theme_affinity_train_job_is_dev_gpu_xgboost_challenger():
    job_config = _load_yaml(
        "resources/jobs/mktg_next_uk_nextads_theme_affinity_model_train.yml"
    )
    assert set(job_config["targets"]) == {"DEV", "DEV_INTEGRATION"}

    job = job_config["theme_affinity_model_train_config"][
        "mktg_next_uk_nextads_theme_affinity_model_train"
    ]
    assert "schedule" not in job
    assert job["job_clusters"] == "${var.theme_affinity_gpu_train_job_clusters_config}"
    task = job["tasks"][0]
    assert task["task_key"] == "train_gpu_xgboost_model"
    assert (
        task["spark_python_task"]["python_file"]
        == "../../scripts/theme_affinity/train_gpu_xgboost_model.py"
    )
    assert (
        task["job_cluster_key"]
        == "next_ads_job_cluster_theme_affinity_gpu_xgboost_train"
    )
    assert task["libraries"] == "${var.theme_affinity_libraries}"
    parameters = task["spark_python_task"]["parameters"]
    assert "${var.job_parameter_environment_name}" in parameters
    assert "--input_table" in parameters
    assert "${var.theme_affinity_training_input_table}" in parameters
    assert "gpu_xgboost" in parameters


def test_theme_affinity_spark_train_job_is_dev_cpu_spark_challenger():
    job_config = _load_yaml(
        "resources/jobs/mktg_next_uk_nextads_theme_affinity_model_train_spark.yml"
    )
    assert set(job_config["targets"]) == {"DEV", "DEV_INTEGRATION"}

    job = job_config["theme_affinity_model_train_spark_config"][
        "mktg_next_uk_nextads_theme_affinity_model_train_spark"
    ]
    assert "schedule" not in job
    assert job["job_clusters"] == "${var.job_clusters_config}"
    task = job["tasks"][0]
    assert task["task_key"] == "train_spark_xgboost_model"
    assert (
        task["spark_python_task"]["python_file"]
        == "../../scripts/theme_affinity/train_model.py"
    )
    assert task["job_cluster_key"] == "next_ads_job_cluster_D32ads_v5_1_4"
    assert task["libraries"] == "${var.theme_affinity_libraries}"
    parameters = task["spark_python_task"]["parameters"]
    assert "${var.job_parameter_environment_name}" in parameters
    assert "--input_table" in parameters
    assert "${var.theme_affinity_training_input_table}" in parameters


def test_theme_affinity_spark_train_job_uses_existing_cpu_spark_cluster():
    variables = _load_yaml("resources/variables/clusters.yml")["variables"]

    shared_clusters = variables["job_clusters_config"]["default"]
    cluster = next(
        cluster
        for cluster in shared_clusters
        if cluster["job_cluster_key"] == "next_ads_job_cluster_D32ads_v5_1_4"
    )
    new_cluster = cluster["new_cluster"]
    assert new_cluster["spark_version"] == "15.4.x-scala2.12"
    assert new_cluster["node_type_id"] == "Standard_D32ads_v5"
    assert new_cluster["driver_node_type_id"] == "Standard_D32ads_v5"
    assert new_cluster["is_single_node"] is False
    assert new_cluster["autoscale"] == {"min_workers": 1, "max_workers": 4}


def test_theme_affinity_gpu_train_job_uses_requested_gpu_ml_cluster():
    variables = _load_yaml("resources/variables/clusters.yml")["variables"]
    train_clusters = variables["theme_affinity_gpu_train_job_clusters_config"][
        "default"
    ]

    assert len(train_clusters) == 1
    cluster = train_clusters[0]
    assert cluster["job_cluster_key"] == (
        "next_ads_job_cluster_theme_affinity_gpu_xgboost_train"
    )
    new_cluster = cluster["new_cluster"]
    assert new_cluster["policy_id"] == "${var.job_cluster_policy_id}"
    assert new_cluster["kind"] == "CLASSIC_PREVIEW"
    assert new_cluster["spark_version"] == "18.1.x-scala2.13"
    assert new_cluster["runtime_engine"] == "STANDARD"
    assert new_cluster["use_ml_runtime"] is True
    assert new_cluster["is_single_node"] is False
    assert new_cluster["node_type_id"] == "Standard_NV36ads_A10_v5"
    assert "driver_node_type_id" not in new_cluster
    assert new_cluster["autoscale"] == {"min_workers": 1, "max_workers": 1}
    assert (
        "spark.databricks.cluster.profile"
        not in new_cluster["spark_conf"]
    )
    assert "spark.master" not in new_cluster["spark_conf"]
    assert "custom_tags" not in new_cluster
    assert "num_workers" not in new_cluster
    assert new_cluster["spark_env_vars"] == {
        "CURRENT_ENV": "${var.environment_name}",
        "GOOGLE_CLOUD_PROJECT": "big-query-156009",
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
    }


def test_theme_affinity_promote_job_is_prod_only_and_parameterised():
    job_config = _load_yaml(
        "resources/jobs/mktg_next_uk_nextads_theme_affinity_model_promote.yml"
    )
    assert set(job_config["targets"]) == {"PROD"}

    job = job_config["theme_affinity_model_promote_config"][
        "mktg_next_uk_nextads_theme_affinity_model_promote"
    ]
    assert "schedule" not in job
    assert job["parameters"] == [
        {
            "name": "source_model_name",
            "default": (
                "marketingdata_prod.ds_sandbox."
                "nextads_theme_affinity_ranker"
            ),
        },
        {"name": "source_model_version", "default": ""},
        {"name": "source_alias", "default": "preprod"},
        {
            "name": "target_model_name",
            "default": (
                "marketingdata_prod.warehouse.nextads_theme_affinity_ranker"
            ),
        },
        {"name": "target_alias", "default": "prod"},
    ]
    task = job["tasks"][0]
    assert task["task_key"] == "promote_model"
    assert (
        task["spark_python_task"]["python_file"]
        == "../../jobs/model/lifecycle/promote_model.py"
    )
    assert task["libraries"] == "${var.theme_affinity_libraries}"
    parameters = task["spark_python_task"]["parameters"]
    assert "{{job.parameters.source_model_name}}" in parameters
    assert "{{job.parameters.source_model_version}}" in parameters
    assert "{{job.parameters.source_alias}}" in parameters
    assert "{{job.parameters.target_model_name}}" in parameters
    assert "{{job.parameters.target_alias}}" in parameters
    assert "marketingdata_prod.ds_sandbox." in parameters
    assert "marketingdata_prod.warehouse." in parameters


def test_theme_affinity_import_dev_model_job_is_preprod_only_and_version_based():
    job_config = _load_yaml(
        "resources/jobs/"
        "mktg_next_uk_nextads_theme_affinity_model_import_dev.yml"
    )
    assert set(job_config["targets"]) == {"PREPROD"}

    job = job_config["theme_affinity_model_import_dev_config"][
        "mktg_next_uk_nextads_theme_affinity_model_import_dev"
    ]
    assert "schedule" not in job
    assert job["parameters"] == [
        {
            "name": "source_model_name",
            "default": (
                "marketingdata_dev.nextads_integration."
                "nextads_theme_affinity_ranker"
            ),
        },
        {"name": "source_model_version", "default": ""},
        {
            "name": "target_model_name",
            "default": "marketingdata_prod.ds_sandbox.nextads_theme_affinity_ranker",
        },
        {"name": "source_alias", "default": ""},
        {"name": "target_alias", "default": "preprod_gpu_xgboost"},
    ]
    task = job["tasks"][0]
    assert task["task_key"] == "promote_model"
    assert (
        task["spark_python_task"]["python_file"]
        == "../../jobs/model/lifecycle/promote_model.py"
    )
    assert task["libraries"] == "${var.theme_affinity_libraries}"
    parameters = task["spark_python_task"]["parameters"]
    assert "{{job.parameters.source_model_name}}" in parameters
    assert "{{job.parameters.source_model_version}}" in parameters
    assert "{{job.parameters.source_alias}}" in parameters
    assert "{{job.parameters.target_model_name}}" in parameters
    assert "{{job.parameters.target_alias}}" in parameters
    assert "marketingdata_dev.nextads_integration." in parameters
    assert "marketingdata_prod.ds_sandbox." in parameters


def test_theme_affinity_monitor_job_is_unscheduled_and_parameterised():
    job_config = _load_yaml(
        "resources/jobs/mktg_next_uk_nextads_theme_affinity_model_monitor.yml"
    )
    assert set(job_config["targets"]) == {"PROD"}

    job = job_config["theme_affinity_model_monitor_config"][
        "mktg_next_uk_nextads_theme_affinity_model_monitor"
    ]
    assert "schedule" not in job
    assert job["parameters"] == [
        {
            "name": "baseline_table",
            "default": "${var.theme_affinity_monitor_baseline_table}",
        },
        {
            "name": "candidate_table",
            "default": "${var.theme_affinity_monitor_candidate_table}",
        },
        {"name": "sample_limit", "default": "100000"},
    ]
    task = job["tasks"][0]
    assert task["task_key"] == "monitor_model"
    assert (
        task["spark_python_task"]["python_file"]
        == "../../scripts/theme_affinity/monitor_model.py"
    )
    assert task["libraries"] == "${var.theme_affinity_libraries}"
    parameters = task["spark_python_task"]["parameters"]
    assert "{{job.parameters.baseline_table}}" in parameters
    assert "{{job.parameters.candidate_table}}" in parameters
    assert "{{job.parameters.sample_limit}}" in parameters


def test_theme_affinity_quality_monitor_setup_job_is_native_and_prod_only():
    job_config = _load_yaml(
        "resources/jobs/"
        "mktg_next_uk_nextads_theme_affinity_quality_monitor_setup.yml"
    )
    assert set(job_config["targets"]) == {"PROD"}

    job = job_config["theme_affinity_quality_monitor_setup_config"][
        "mktg_next_uk_nextads_theme_affinity_quality_monitor_setup"
    ]
    assert "schedule" not in job
    assert job["parameters"] == [
        {"name": "action", "default": "setup"},
        {"name": "monitor_type", "default": "time_series"},
        {
            "name": "table_name",
            "default": (
                "${var.mktgdata_catalog}.${var.user_schema}."
                "next_uk_nextads_theme_affinity_predict_ranked"
            ),
        },
        {
            "name": "output_schema_name",
            "default": "${var.mktgdata_catalog}.${var.user_schema}",
        },
        {
            "name": "assets_dir",
            "default": (
                "${workspace.root_path}/quality_monitors/"
                "theme_affinity_predict_ranked"
            ),
        },
        {"name": "timestamp_col", "default": "reference_date"},
        {"name": "granularities", "default": "1 day"},
        {"name": "slicing_exprs", "default": "repurchase_stage,GmaName,theme_clean"},
        {"name": "run_refresh", "default": "false"},
        {"name": "problem_type", "default": "classification"},
        {"name": "prediction_col", "default": "prediction"},
        {"name": "model_id_col", "default": "model_id"},
        {"name": "label_col", "default": "label"},
        {"name": "prediction_proba_col", "default": ""},
    ]
    task = job["tasks"][0]
    assert task["task_key"] == "setup_quality_monitor"
    assert (
        task["spark_python_task"]["python_file"]
        == "../../scripts/theme_affinity/setup_quality_monitor.py"
    )
    assert task["libraries"] == "${var.theme_affinity_libraries}"
    parameters = task["spark_python_task"]["parameters"]
    assert "{{job.parameters.action}}" in parameters
    assert "{{job.parameters.monitor_type}}" in parameters
    assert "{{job.parameters.table_name}}" in parameters
    assert "{{job.parameters.timestamp_col}}" in parameters
    assert "{{job.parameters.run_refresh}}" in parameters
    assert "{{job.parameters.problem_type}}" in parameters
    assert "{{job.parameters.prediction_col}}" in parameters
    assert "{{job.parameters.model_id_col}}" in parameters


def test_theme_affinity_mlflow_lifecycle_excludes_old_branch_artifacts():
    bundle_text = (PROJECT_ROOT / "databricks.yml").read_text()
    assert "scripts/ranking_model" not in bundle_text
    assert "mktg_next_uk_nextads_model_train" not in bundle_text
    assert "mktg_next_uk_nextads_model_predict" not in bundle_text
    assert "wheels/marketingdata_utils" not in bundle_text

    checked_paths = [
        PROJECT_ROOT / "pyproject.toml",
        PROJECT_ROOT / "requirements-theme-affinity.txt",
        PROJECT_ROOT / "resources/variables/libraries.yml",
        PROJECT_ROOT / "scripts/theme_affinity",
        PROJECT_ROOT / "src/next_ads/ranking/theme_affinity",
    ]
    for path in checked_paths:
        files = [path] if path.is_file() else list(path.rglob("*.py"))
        for file_path in files:
            assert "marketingdata_utils" not in file_path.read_text()


def test_theme_affinity_script_bootstrap_handles_workspace_paths():
    script_paths = {
        "jobs/model/theme_affinity/model_predict.py": "Path(notebook_path).parents[3]",
        "jobs/model/theme_affinity/clean_output.py": "Path(notebook_path).parents[3]",
        "jobs/model/theme_affinity/publish_outputs.py": (
            "Path(notebook_path).parents[3]"
        ),
        "jobs/model/theme_affinity/sense_check.py": "Path(notebook_path).parents[3]",
        "scripts/theme_affinity/rules_rank.py": "Path(notebook_path).parents[2]",
        "scripts/theme_affinity/run_pipeline.py": "Path(notebook_path).parents[2]",
        "scripts/theme_affinity/monitor_model.py": "Path(notebook_path).parents[2]",
        "scripts/theme_affinity/setup_quality_monitor.py": (
            "Path(notebook_path).parents[2]"
        ),
        "scripts/theme_affinity/train_gpu_xgboost_model.py": (
            "Path(notebook_path).parents[2]"
        ),
        "scripts/theme_affinity/train_model.py": "Path(notebook_path).parents[2]",
        "jobs/model/lifecycle/promote_model.py": "Path(notebook_path).parents[3]",
    }
    for script_path, project_root_expression in script_paths.items():
        script = (PROJECT_ROOT / script_path).read_text()
        assert project_root_expression in script
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
    assert "--candidate_intermediate_namespace" in data_parameters
    assert (
        data_parameters[
            data_parameters.index("--candidate_intermediate_namespace") + 1
        ]
        == "${var.mktgdata_catalog}.${var.theme_affinity_pipeline_schema}"
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


def test_theme_affinity_pipeline_uses_target_pipeline_schema_variable():
    pipeline = _load_yaml(
        "resources/pipelines/mktg_next_uk_nextads_predict_data_prep.yml"
    )
    bundle = _load_yaml("databricks.yml")
    targets = pipeline["targets"]

    assert (
        bundle["variables"]["theme_affinity_pipeline_schema"]["default"]
        == "${var.user_schema}"
    )
    assert (
        bundle["variables"]["theme_affinity_publish_table_suffixes"]["default"]
        == (
            "ranked,complete,advanced_features,customer_features,"
            "customer_segments,popularity_metrics"
        )
    )
    assert (
        bundle["targets"]["PROD"]["variables"]["theme_affinity_pipeline_schema"]
        == "ds_sandbox"
    )
    assert bundle["targets"]["PROD"]["variables"]["user_schema"] == "warehouse"

    assert set(targets) == {"SANDBOX", "DEV", "DEV_INTEGRATION", "PREPROD", "PROD"}
    for target in targets.values():
        pipeline_config = target["resources"]["pipelines"][
            "nextads_theme_affinity_predict_data_prep"
        ]
        assert pipeline_config["catalog"] == "${var.mktgdata_catalog}"
        assert pipeline_config["schema"] == "${var.theme_affinity_pipeline_schema}"
        assert (
            pipeline_config["configuration"]["pipeline.schema"]
            == "${var.theme_affinity_pipeline_schema}"
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
        PROJECT_ROOT / "jobs/model/theme_affinity",
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


def test_theme_affinity_assignment_sources_use_new_model_output():
    settings = _load_yaml("configs/runtime/tables_settings.yaml")
    assignment_sources = settings["default"]["theme_affinity_assignment_sources"]

    assert assignment_sources["champion"] == (
        "@format {this.catalog_write}.{this.schema_write}."
        "{this.client}_nextads_theme_affinity_model_latest"
    )
    assert assignment_sources["challenger"] == assignment_sources["champion"]

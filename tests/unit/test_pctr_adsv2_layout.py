import ast
from pathlib import Path

from next_ads.common import paths as common_paths
from tests.job_resource_helpers import load_job, load_yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_analytics_pctr_is_retained_as_experiment():
    assert not (PROJECT_ROOT / "analytics_pctr").exists()
    assert (PROJECT_ROOT / "experiments" / "analytics_pctr").is_dir()
    assert (
        PROJECT_ROOT / "experiments" / "analytics_pctr" / "SQL"
    ).is_dir()
    assert (
        PROJECT_ROOT / "experiments" / "analytics_pctr" / "run_predictions.py"
    ).is_file()


def test_analytics_pctr_job_points_to_experiment_and_stays_dev_only_paused():
    bundle_config = load_yaml("databricks.yml")
    config = load_yaml(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_analytics_pctr.yml"
    )
    assert "experiments/analytics_pctr/**" in bundle_config["sync"]["include"]
    assert set(config["targets"]) == {"DEV"}

    job = load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_analytics_pctr.yml",
        "mktg_next_uk_nextads_analytics_pctr_model",
    )
    assert job["schedule"]["pause_status"] == "PAUSED"

    notebook_paths = [
        task["notebook_task"]["notebook_path"]
        for task in job["tasks"]
        if "notebook_task" in task
    ]
    python_paths = [
        task["spark_python_task"]["python_file"]
        for task in job["tasks"]
        if "spark_python_task" in task
    ]
    assert notebook_paths
    assert all(
        path.startswith("${workspace.file_path}/experiments/analytics_pctr/")
        for path in notebook_paths
    )
    assert python_paths == [
        "${workspace.file_path}/experiments/analytics_pctr/run_predictions.py"
    ]


def test_prediction_proof_records_the_exact_output_needed_for_adoption():
    source = (
        PROJECT_ROOT / "experiments/analytics_pctr/run_predictions.py"
    ).read_text()
    job_source = (
        PROJECT_ROOT
        / "pipelines/databricks/jobs"
        / "mktg_next_uk_nextads_analytics_pctr_prediction_verification.yml"
    ).read_text()

    assert "ANALYTICS_PCTR_PREDICTION_RECEIPT=" in source
    assert "DESCRIBE HISTORY {TARGET_TABLE_LATEST} LIMIT 1" in source
    assert 'spark.conf.get("spark.databricks.job.runId")' in source
    assert "activation_mode: EVALUATE" in job_source
    assert "schedule:" not in job_source
    assert "mktg_next_uk_nextads.yml" not in job_source
    assert "PROD:" not in job_source


def test_prediction_route_handles_empty_impression_history():
    source_path = (
        PROJECT_ROOT / "experiments/analytics_pctr/run_predictions.py"
    )
    source = source_path.read_text()
    tree = ast.parse(source)
    helper = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "optional_first_value"
    )
    namespace = {}
    exec(
        compile(
            ast.Module(body=[helper], type_ignores=[]),
            str(source_path),
            "exec",
        ),
        namespace,
    )

    assert namespace["optional_first_value"]([]) is None
    assert namespace["optional_first_value"]([(12.5,)]) == 12.5
    assert "median_impressions = optional_first_value" in source


def test_adsv2_entrypoints_remain_in_current_route_folders():
    main_job = load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    page_build_v2_job = load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml",
        "mktg_next_uk_nextads_page_build_cicd_v2",
    )
    payload_job = load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads_payload_export.yml",
        "mktg_next_uk_nextads_payload_export_cicd",
    )

    main_tasks = {task["task_key"]: task for task in main_job["tasks"]}
    page_v2_tasks = {task["task_key"]: task for task in page_build_v2_job["tasks"]}

    assert main_tasks["load_control_sheet_v2"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_control/load_control_sheet_v2.py"
    assert main_tasks["map_theme_scores_to_ads_v2"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_candidates/build_page_type_candidates_v2.py"
    assert page_v2_tasks["build_and_publish_v2"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_assignment/bulk_build.py"
    bulk_build_source = (
        PROJECT_ROOT / "jobs/nextads_assignment/bulk_build.py"
    ).read_text()
    assert "build_v2_assignments(" in bulk_build_source
    assert payload_job["tasks"][0]["spark_python_task"]["python_file"] == (
        "../../../jobs/nextads_delivery/build_v2_payload.py"
    )


def test_moved_sql_contracts_resolve_from_domain_folders():
    assert common_paths.resolve_sql_contract_path(
        "nextads_analytics_pctr_predictions"
    ).as_posix().endswith(
        "sql/ranking/pctr/create_table_nextads_analytics_pctr_predictions.sql"
    )
    assert common_paths.resolve_sql_contract_path(
        "nextads_analytics_pctr_predictions_latest"
    ).as_posix().endswith(
        "sql/ranking/pctr/create_table_nextads_analytics_pctr_predictions_latest.sql"
    )
    assert common_paths.resolve_sql_contract_path(
        "sort_order_v2"
    ).as_posix().endswith("sql/adsv2/create_table_sort_order_v2.sql")
    assert common_paths.resolve_sql_contract_path(
        "sort_order_v2_latest"
    ).as_posix().endswith("sql/adsv2/create_table_sort_order_v2_latest.sql")


def test_sort_order_latest_projects_declared_schema_after_control_sheet_join():
    data_pull = (
        PROJECT_ROOT / "src" / "next_ads" / "data" / "sort_order" / "data_pull.py"
    ).read_text()

    assert 'StructField("item_pos", LongType(), True)' in data_pull
    assert "for field in sort_order_schema.fields" in data_pull
    assert "F.col(field.name).cast(field.dataType).alias(field.name)" in data_pull
    assert ").select(\n        *[" in data_pull


def test_sort_order_contract_retains_cluster_id_hotfix():
    data_pull = (
        PROJECT_ROOT / "src" / "next_ads" / "data" / "sort_order" / "data_pull.py"
    ).read_text()
    sort_order_ddl = (
        PROJECT_ROOT / "sql" / "adsv2" / "create_table_sort_order_v2.sql"
    ).read_text()
    latest_ddl = (
        PROJECT_ROOT
        / "sql"
        / "adsv2"
        / "create_table_sort_order_v2_latest.sql"
    ).read_text()

    assert 'StructField("ClusterID", StringType(), True)' in data_pull
    assert "AdVariant STRING,\n    ClusterID STRING,\n    rundate DATE" in sort_order_ddl
    assert "AdVariant STRING,\n    ClusterID STRING,\n    rundate DATE" in latest_ddl

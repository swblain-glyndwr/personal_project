from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOB_PATH = (
    PROJECT_ROOT
    / "pipelines"
    / "databricks"
    / "jobs"
    / "mktg_next_uk_nextads_shopping_bag_feature_preparation.yml"
)


def test_job_publishes_only_the_missing_shopping_bag_feature_groups():
    job = yaml.safe_load(JOB_PATH.read_text())[
        "shopping_bag_feature_preparation_job"
    ]
    tasks = {task["task_key"]: task for task in job["tasks"]}

    assert set(tasks) == {
        "resolve_reference_date",
        "build_shopping_bag_account_activity",
        "publish_advert_features",
        "create_required_feature_tables",
        "publish_click_labels",
    }
    assert tasks["build_shopping_bag_account_activity"][
        "spark_python_task"
    ][
        "python_file"
    ].endswith("/build_shopping_bag_account_activity.py")
    assert tasks["publish_advert_features"]["spark_python_task"][
        "python_file"
    ].endswith("/build_advert_features.py")
    assert tasks["publish_click_labels"]["spark_python_task"][
        "python_file"
    ].endswith("/build_shopping_bag_click_labels.py")
    setup = tasks["create_required_feature_tables"]["spark_python_task"]
    assert setup["python_file"].endswith("/create_feature_store_tables.py")
    assert "next_uk_nextads_fs_shopping_bag_account_activity_90d" in setup[
        "parameters"
    ]
    for table_name in (
        "next_uk_nextads_fs_item_attributes_latest",
        "next_uk_nextads_fs_advert_core_daily",
        "next_uk_nextads_fs_advert_attribute_profile_daily",
    ):
        assert table_name in setup["parameters"]
    assert "next_uk_nextads_fs_shopping_bag_click_labels" in setup[
        "parameters"
    ]
    assert "true" not in [
        value.lower()
        for value in setup["parameters"]
        if isinstance(value, str)
    ]
    assert tasks["publish_advert_features"]["depends_on"] == [
        {"task_key": "create_required_feature_tables"}
    ]


def test_job_is_manual_personal_dev_evidence_only():
    source = JOB_PATH.read_text()
    job = yaml.safe_load(source)["shopping_bag_feature_preparation_job"]
    parameters = {
        parameter["name"]: parameter["default"]
        for parameter in job["parameters"]
    }
    bundle = (PROJECT_ROOT / "databricks.yml").read_text()

    assert "default: REQUIRED" in source
    assert parameters["reference_date"] == "REQUIRED"
    assert parameters["label_end"] == "REQUIRED"
    assert parameters["feature_reference_date"] == "REQUIRED"
    assert parameters["source_catalog"] == "${var.feature_store_source_catalog}"
    assert parameters["source_schema"] == "${var.feature_store_source_schema}"
    tasks = {task["task_key"]: task for task in job["tasks"]}
    activity_parameters = tasks["build_shopping_bag_account_activity"][
        "spark_python_task"
    ]["parameters"]
    advert_parameters = tasks["publish_advert_features"]["spark_python_task"][
        "parameters"
    ]
    label_parameters = tasks["publish_click_labels"]["spark_python_task"][
        "parameters"
    ]
    feature_date = "{{tasks.resolve_reference_date.values.reference_date}}"
    assert feature_date in activity_parameters
    assert feature_date in advert_parameters
    assert "{{job.parameters.reference_date}}" in label_parameters
    assert feature_date not in label_parameters
    assert "activation_mode: EVALUATE" in source
    assert "schedule:" not in source
    assert "DEV_FEATURE_STORE:" not in source
    assert "PREPROD:" not in source
    assert "PROD:" not in source
    assert JOB_PATH.name in bundle

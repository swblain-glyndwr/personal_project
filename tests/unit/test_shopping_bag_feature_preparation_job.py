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
        "publish_account_features",
        "publish_advert_features",
        "publish_click_labels",
    }
    assert tasks["publish_account_features"]["spark_python_task"][
        "python_file"
    ].endswith("/build_account_features.py")
    assert tasks["publish_advert_features"]["spark_python_task"][
        "python_file"
    ].endswith("/build_advert_features.py")
    assert tasks["publish_click_labels"]["spark_python_task"][
        "python_file"
    ].endswith("/build_model_inputs.py")


def test_job_is_manual_personal_dev_evidence_only():
    source = JOB_PATH.read_text()
    bundle = (PROJECT_ROOT / "databricks.yml").read_text()

    assert "default: REQUIRED" in source
    assert "activation_mode: EVALUATE" in source
    assert "schedule:" not in source
    assert "DEV_FEATURE_STORE:" not in source
    assert "PREPROD:" not in source
    assert "PROD:" not in source
    assert JOB_PATH.name in bundle

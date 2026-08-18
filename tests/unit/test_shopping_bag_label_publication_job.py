from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOB_PATH = (
    PROJECT_ROOT
    / "pipelines"
    / "databricks"
    / "jobs"
    / "mktg_next_uk_nextads_shopping_bag_label_publication.yml"
)
BUILDER_PATH = (
    PROJECT_ROOT
    / "jobs"
    / "features"
    / "nextads"
    / "build_shopping_bag_click_labels.py"
)


def test_manual_label_job_sets_up_and_publishes_only_the_new_contract():
    source = JOB_PATH.read_text()
    job = yaml.safe_load(source)["shopping_bag_label_publication_job"]
    tasks = {task["task_key"]: task for task in job["tasks"]}

    assert set(tasks) == {
        "create_observed_label_table",
        "publish_observed_labels",
    }
    setup = tasks["create_observed_label_table"]["spark_python_task"]
    assert setup["python_file"].endswith("/create_feature_store_tables.py")
    assert "next_uk_nextads_fs_shopping_bag_click_labels" in setup[
        "parameters"
    ]
    assert "--recreate_tables" in setup["parameters"]
    assert setup["parameters"][
        setup["parameters"].index("--recreate_tables") + 1
    ] == "false"

    publication = tasks["publish_observed_labels"]["spark_python_task"]
    assert publication["python_file"].endswith(
        "/build_shopping_bag_click_labels.py"
    )
    assert tasks["publish_observed_labels"]["depends_on"] == [
        {"task_key": "create_observed_label_table"}
    ]
    assert "build_account_features.py" not in source
    assert "build_advert_features.py" not in source
    assert "build_model_inputs.py" not in source


def test_manual_label_job_is_dev_only_and_unscheduled():
    source = JOB_PATH.read_text()
    job = yaml.safe_load(source)["shopping_bag_label_publication_job"]
    parameters = {
        parameter["name"]: parameter["default"]
        for parameter in job["parameters"]
    }

    assert parameters["reference_date"] == "REQUIRED"
    assert parameters["label_end"] == "REQUIRED"
    assert "schedule:" not in source
    assert "PREPROD:" not in source
    assert "PROD:" not in source
    assert "activation_mode: EVALUATE" in source
    assert JOB_PATH.name in (PROJECT_ROOT / "databricks.yml").read_text()


def test_label_publication_replaces_the_session_date_partition():
    source = BUILDER_PATH.read_text()

    assert '"reference_date_column": "session_date"' in source

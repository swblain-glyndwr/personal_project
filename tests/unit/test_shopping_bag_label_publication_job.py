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
        "build_shopping_bag_click_labels",
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

    publication = tasks["build_shopping_bag_click_labels"][
        "spark_python_task"
    ]
    assert publication["python_file"].endswith(
        "/build_shopping_bag_click_labels.py"
    )
    assert tasks["build_shopping_bag_click_labels"]["depends_on"] == [
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


def test_manual_label_job_logs_a_bounded_non_gating_evidence_funnel():
    job_source = (
        PROJECT_ROOT
        / "jobs"
        / "features"
        / "nextads"
        / "build_shopping_bag_click_labels.py"
    ).read_text()
    evidence_source = (
        PROJECT_ROOT
        / "src"
        / "next_ads"
        / "features"
        / "shopping_bag_label_evidence.py"
    ).read_text()

    assert "SHOPPING_BAG_LABEL_FUNNEL=" in job_source
    assert "next_uk_nextads_results_ads_location" in job_source
    assert "pinned_spark.table(reporting_table)" not in job_source
    assert '"is_gate": False' in evidence_source
    assert "AMBIGUOUS_ACCOUNT" in evidence_source
    assert "AMBIGUOUS_MATCH" in evidence_source
    assert "PRE_REFRESH" in evidence_source
    assert "UNKNOWN_TREATMENT" in evidence_source
    assert "event_cms_page_id" in evidence_source
    assert "assignment_cms_page_id" in evidence_source
    assert "label_horizon_days" in evidence_source
    assert '"quality_checks"' in evidence_source


def test_label_publication_uses_the_registry_snapshot_date_scope():
    source = BUILDER_PATH.read_text()
    registry = yaml.safe_load(
        (
            PROJECT_ROOT
            / "configs"
            / "features"
            / "nextads_feature_store.yaml"
        ).read_text()
    )
    observed_labels = next(
        table
        for table in registry["feature_store"]["physical_tables"]
        if table["name"]
        == "next_uk_nextads_fs_shopping_bag_click_labels"
    )

    assert "write_options=" not in source
    assert observed_labels["timestamp_key"] == "exposure_timestamp"
    assert observed_labels["snapshot_date_key"] == "session_date"

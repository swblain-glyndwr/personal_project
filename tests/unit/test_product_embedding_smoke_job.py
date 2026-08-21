from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOB_RESOURCE = (
    "pipelines/databricks/jobs/"
    "mktg_next_uk_nextads_product_embedding_runtime_smoke.yml"
)


def _load_yaml(path: str):
    return yaml.safe_load((PROJECT_ROOT / path).read_text())


def test_embedding_smoke_job_is_manual_personal_dev_only():
    bundle = _load_yaml("databricks.yml")
    resource = _load_yaml(JOB_RESOURCE)
    job = resource["targets"]["DEV"]["resources"]["jobs"][
        "mktg_next_uk_nextads_product_embedding_runtime_smoke"
    ]

    assert JOB_RESOURCE in bundle["include"]
    assert set(resource["targets"]) == {"DEV"}
    assert job["max_concurrent_runs"] == 1
    assert "schedule" not in job
    assert "trigger" not in job
    assert "continuous" not in job
    assert "parameters" not in job


def test_embedding_smoke_uses_isolated_pinned_runtime_profile():
    libraries = _load_yaml("pipelines/databricks/variables/libraries.yml")
    resource = _load_yaml(JOB_RESOURCE)
    tasks = resource["targets"]["DEV"]["resources"]["jobs"][
        "mktg_next_uk_nextads_product_embedding_runtime_smoke"
    ]["tasks"]
    task = next(
        task
        for task in tasks
        if task["task_key"] == "product_embedding_runtime_smoke"
    )

    assert task["job_cluster_key"] == "next_ads_job_cluster_D4ads_v5_1_1"
    assert task["libraries"] == "${var.feature_store_embedding_libraries}"
    assert libraries["variables"]["feature_store_embedding_libraries"][
        "default"
    ][1] == {
        "requirements": "../../../requirements-feature-store-embeddings.txt"
    }

    requirements = (
        PROJECT_ROOT / "requirements-feature-store-embeddings.txt"
    ).read_text().splitlines()
    assert requirements == [
        "--extra-index-url https://download.pytorch.org/whl/cpu",
        "mlflow==3.11.1",
        "numpy==1.26.4",
        "protobuf==4.24.1",
        "sentence-transformers==2.4.0",
        "threadpoolctl==3.6.0",
        "torch==2.10.0+cpu",
        "transformers==4.41.2",
    ]


def test_embedding_smoke_is_read_only_and_requires_exact_model_binding():
    resource = _load_yaml(JOB_RESOURCE)
    tasks = resource["targets"]["DEV"]["resources"]["jobs"][
        "mktg_next_uk_nextads_product_embedding_runtime_smoke"
    ]["tasks"]
    task = next(
        task
        for task in tasks
        if task["task_key"] == "product_embedding_runtime_smoke"
    )
    script = (
        PROJECT_ROOT
        / "jobs/features/nextads/smoke_product_embedding_runtime.py"
    ).read_text()
    runtime_source = (
        PROJECT_ROOT / "src/next_ads/features/embedding_runtime.py"
    ).read_text()
    runtime_sources = script + runtime_source

    assert task["spark_python_task"]["parameters"] == [
        "--log_level",
        "INFO",
    ]
    assert "next_ads.features.embedding_runtime" in script
    assert "mlflow.sentence_transformers.load_model" not in runtime_sources
    assert "trust_remote_code=True" not in runtime_sources
    assert "load_approved_dev_smoke_binding" in script
    for operation in [
        "saveAsTable",
        "write.",
        "DELETE FROM",
        "INSERT INTO",
        "DROP TABLE",
        "TRUNCATE TABLE",
        "CREATE TABLE",
        "ALTER TABLE",
        "set_registered_model_alias",
        "register_model",
    ]:
        assert operation not in runtime_sources


def test_advert_item_bridge_smoke_is_read_only_and_runs_first():
    resource = _load_yaml(JOB_RESOURCE)
    tasks = resource["targets"]["DEV"]["resources"]["jobs"][
        "mktg_next_uk_nextads_product_embedding_runtime_smoke"
    ]["tasks"]
    bridge_task = next(
        task for task in tasks if task["task_key"] == "advert_item_bridge_smoke"
    )
    embedding_task = next(
        task
        for task in tasks
        if task["task_key"] == "product_embedding_runtime_smoke"
    )

    assert bridge_task["job_cluster_key"] == (
        "next_ads_job_cluster_D8ads_v5_2_2"
    )
    assert bridge_task["libraries"] == "${var.feature_store_libraries}"
    assert bridge_task["job_cluster_key"] != embedding_task["job_cluster_key"]
    assert bridge_task["spark_python_task"]["parameters"] == [
        "--log_level",
        "INFO",
    ]
    assert embedding_task["depends_on"] == [
        {"task_key": "advert_item_bridge_smoke"}
    ]

    script = (
        PROJECT_ROOT / "jobs/features/nextads/smoke_advert_item_bridge.py"
    ).read_text()
    for operation in [
        "saveAsTable",
        "write.",
        "DELETE FROM",
        "INSERT INTO",
        "DROP TABLE",
        "TRUNCATE TABLE",
        "CREATE TABLE",
        "ALTER TABLE",
    ]:
        assert operation not in script

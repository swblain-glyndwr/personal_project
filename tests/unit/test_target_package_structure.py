import importlib
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_target_package_structure_exists_and_imports():
    package_root = PROJECT_ROOT / "src" / "next_ads"
    expected_subpackages = [
        "common",
        "features",
        "data",
        "control",
        "retrieval",
        "ranking",
        "decisioning",
        "delivery",
        "reporting",
        "realtime",
    ]

    assert package_root.is_dir()
    assert (package_root / "__init__.py").is_file()

    for subpackage in expected_subpackages:
        assert (package_root / subpackage / "__init__.py").is_file()
        importlib.import_module(f"next_ads.{subpackage}")


def test_existing_databricks_job_entrypoints_stay_on_scripts_or_jobs():
    job_config = yaml.safe_load(
        (PROJECT_ROOT / "resources/jobs/mktg_next_uk_nextads.yml").read_text()
    )
    job = job_config["resources"]["jobs"]["mktg_next_uk_nextads_cicd"]

    python_files_by_task = {}
    for task in job["tasks"]:
        if "spark_python_task" in task:
            python_files_by_task[task["task_key"]] = task["spark_python_task"][
                "python_file"
            ]
        if "for_each_task" in task:
            nested_task = task["for_each_task"]["task"]
            python_files_by_task[nested_task["task_key"]] = nested_task[
                "spark_python_task"
            ]["python_file"]

    python_files = list(python_files_by_task.values())

    assert python_files
    assert all(
        path.startswith("../../scripts/")
        or path.startswith("../../jobs/nextads_main/")
        for path in python_files
    )
    assert not any(path.startswith("../../src/") for path in python_files)


def test_feature_layer_target_directories_exist_without_active_jobs():
    target_dirs = [
        PROJECT_ROOT / "jobs" / "features",
        PROJECT_ROOT / "jobs" / "model",
        PROJECT_ROOT / "jobs" / "nextads_main",
        PROJECT_ROOT / "jobs" / "nextads_v2",
        PROJECT_ROOT / "configs" / "features",
        PROJECT_ROOT / "pipelines" / "databricks",
        PROJECT_ROOT / "sql" / "features",
    ]

    for target_dir in target_dirs:
        assert target_dir.is_dir()
        assert (target_dir / "README.md").is_file()


def test_repo_structure_documentation_describes_transition_rules():
    doc = (PROJECT_ROOT / "docs/repo_structure.md").read_text()

    assert "src/next_ads" in doc
    assert "src/next_ads/features" in doc
    assert "Existing Databricks job entry points remain in `scripts/`" in doc
    assert "When a story explicitly scopes a domain move" in doc
    assert "Existing Databricks job definitions remain in `resources/jobs/`" in doc
    assert "Decision-affecting logic should move only in follow-up stories" in doc

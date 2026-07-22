import importlib
import re
from pathlib import Path

from tests.job_resource_helpers import load_job


PROJECT_ROOT = Path(__file__).resolve().parents[2]

PROJECT_ROOT_FROM_FILE_RE = re.compile(
    r"PROJECT_ROOT = Path\(__file__\)\.resolve\(\)\.parents\[(\d+)\]"
)
PROJECT_ROOT_FROM_NOTEBOOK_RE = re.compile(
    r"PROJECT_ROOT = Path\(notebook_path\)\.parents\[(\d+)\]"
)
PROJECT_ROOT_PARENT_CHAIN_RE = re.compile(
    r"PROJECT_ROOT = Path\((?:__file__|notebook_path)\)"
    r"(?:\.resolve\(\))?\.parent(?:\b|\.)"
)


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
    job = load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )

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
    allowed_job_roots = (
        "../../../jobs/nextads_control/",
        "../../../jobs/nextads_cells/",
        "../../../jobs/nextads_candidates/",
        "../../../jobs/nextads_assignment/",
        "../../../jobs/nextads_control/",
        "../../../jobs/nextads_delivery/",
        "../../../jobs/orchestration/",
    )
    assert all(path.startswith(allowed_job_roots) for path in python_files)
    assert not any(path.startswith("../../src/") for path in python_files)


def test_feature_layer_target_directories_exist_without_active_jobs():
    target_dirs = [
        PROJECT_ROOT / "jobs" / "features",
        PROJECT_ROOT / "jobs" / "model",
        PROJECT_ROOT / "jobs" / "nextads_v2",
        PROJECT_ROOT / "configs" / "features",
        PROJECT_ROOT / "pipelines" / "databricks",
        PROJECT_ROOT / "sql" / "features",
    ]

    for target_dir in target_dirs:
        assert target_dir.is_dir()
        assert (target_dir / "README.md").is_file()


def test_job_project_root_bootstrap_depths_match_file_location():
    job_files = sorted((PROJECT_ROOT / "jobs").rglob("*.py"))

    assert job_files

    for job_file in job_files:
        relative_path = job_file.relative_to(PROJECT_ROOT)
        expected_depth = len(relative_path.parts) - 1
        source = job_file.read_text(encoding="utf-8", errors="ignore")

        assert not PROJECT_ROOT_PARENT_CHAIN_RE.search(source), relative_path

        file_match = PROJECT_ROOT_FROM_FILE_RE.search(source)
        if file_match:
            assert int(file_match.group(1)) == expected_depth, relative_path

        notebook_match = PROJECT_ROOT_FROM_NOTEBOOK_RE.search(source)
        if notebook_match:
            assert int(notebook_match.group(1)) == expected_depth, relative_path


def test_repo_structure_documentation_describes_transition_rules():
    doc = (PROJECT_ROOT / "docs/repo_structure.md").read_text()

    assert "src/next_ads" in doc
    assert "src/next_ads/features" in doc
    assert "New or moved Databricks job entry points should live under route-oriented" in doc
    assert "When a story explicitly scopes a domain move" in doc
    assert "Existing Databricks job definitions remain in `pipelines/databricks/jobs/`" in doc
    assert "Decision-affecting logic should move only in follow-up stories" in doc

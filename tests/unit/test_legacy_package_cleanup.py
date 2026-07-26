import ast
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]

LEGACY_IMPORT_ROOTS = {
    "next_ads.Assignment",
    "next_ads.Attributes",
    "next_ads.Export",
    "next_ads.Plotting",
    "next_ads.Results",
    "next_ads.Scoring",
    "next_ads.data_validation",
    "next_ads.utils",
}


def _python_files(*roots: str):
    for root in roots:
        yield from (PROJECT_ROOT / root).rglob("*.py")


def _imports(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            yield node.module
        elif isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)


def test_legacy_package_roots_are_removed():
    assert not (PROJECT_ROOT / "next_ads").exists()
    assert not (PROJECT_ROOT / "src" / "next_ads" / "data_validation").exists()


def test_python_sources_use_only_canonical_next_ads_imports():
    violations = []

    for path in _python_files("jobs", "src", "tests"):
        for imported_module in _imports(path):
            if any(
                imported_module == legacy_root
                or imported_module.startswith(f"{legacy_root}.")
                for legacy_root in LEGACY_IMPORT_ROOTS
            ):
                violations.append(
                    f"{path.relative_to(PROJECT_ROOT)}: {imported_module}"
                )

    assert not violations, "\n".join(violations)


def test_job_entrypoints_resolve_src_before_importing_package_code():
    violations = []

    for path in _python_files("jobs"):
        source = path.read_text(encoding="utf-8")
        imports_next_ads = any(
            imported_module == "next_ads"
            or imported_module.startswith("next_ads.")
            for imported_module in _imports(path)
        )
        if not imports_next_ads:
            continue

        has_src_bootstrap = 'PROJECT_ROOT / "src"' in source
        uses_feature_job_bootstrap = "from _registry_job import" in source
        if not (has_src_bootstrap or uses_feature_job_bootstrap):
            violations.append(str(path.relative_to(PROJECT_ROOT)))

    assert not violations, "\n".join(violations)


def test_jobs_root_contains_no_python_entrypoints():
    root_python_files = sorted(
        path.name for path in (PROJECT_ROOT / "jobs").glob("*.py")
    )

    assert root_python_files == ["__init__.py"]


def test_bundle_sync_and_data_pull_use_canonical_routes():
    bundle = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text())
    sync_includes = bundle["sync"]["include"]

    assert "src/next_ads/**" in sync_includes
    assert "next_ads/**" not in sync_includes
    assert "next_ads/data/**" not in sync_includes

    data_pull_job = yaml.safe_load(
        (
            PROJECT_ROOT
            / "pipelines/databricks/jobs/mktg_next_uk_nextads_data_pull.yaml"
        ).read_text()
    )
    tasks = data_pull_job["mktg_next_uk_nextads_data_pull_config"][
        "mktg_next_uk_nextads_data_pull"
    ]["tasks"]
    archive_task = next(
        task for task in tasks if task["task_key"] == "archive_sort_order_data"
    )

    assert archive_task["spark_python_task"]["python_file"] == (
        "../../../jobs/nextads_data/archive_sort_order_data.py"
    )

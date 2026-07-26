import ast
from pathlib import Path
from types import SimpleNamespace

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PYTHON_SOURCE_ROOTS = ("jobs", "src", "tests", "experiments", "deployment")

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
        root_path = PROJECT_ROOT / root
        if root_path.exists():
            yield from root_path.rglob("*.py")


def _imports(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            yield node.module
        elif isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)


def _imports_next_ads(node):
    if isinstance(node, ast.ImportFrom):
        return bool(
            node.module
            and (
                node.module == "next_ads"
                or node.module.startswith("next_ads.")
            )
        )
    if isinstance(node, ast.Import):
        return any(
            alias.name == "next_ads" or alias.name.startswith("next_ads.")
            for alias in node.names
        )
    return False


def _is_src_path_insert(node):
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Attribute)
        and isinstance(node.func.value.value, ast.Name)
        and node.func.value.value.id == "sys"
        and node.func.value.attr == "path"
        and node.func.attr == "insert"
        and len(node.args) >= 2
        and isinstance(node.args[0], ast.Constant)
        and node.args[0].value == 0
        and isinstance(node.args[1], ast.Call)
        and isinstance(node.args[1].func, ast.Name)
        and node.args[1].func.id == "str"
        and len(node.args[1].args) == 1
        and isinstance(node.args[1].args[0], ast.Name)
        and node.args[1].args[0].id == "SRC_ROOT"
    )


def _canonical_module_exists(module_name: str) -> bool:
    if module_name == "next_ads":
        return (PROJECT_ROOT / "src" / "next_ads" / "__init__.py").is_file()

    module_path = (
        PROJECT_ROOT / "src" / Path(*module_name.split("."))
    )
    return (
        module_path.with_suffix(".py").is_file()
        or (module_path / "__init__.py").is_file()
    )


def _walk_mappings(value):
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from _walk_mappings(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _walk_mappings(nested)


def _resolve_bundle_source_path(config_path: Path, configured_path: str) -> Path:
    workspace_prefix = "${workspace.file_path}/"
    if configured_path.startswith(workspace_prefix):
        return PROJECT_ROOT / configured_path.removeprefix(workspace_prefix)
    return (config_path.parent / configured_path).resolve()


def _bundle_source_exists(source_path: Path) -> bool:
    if source_path.is_file():
        return True
    if source_path.suffix:
        return False
    return any(
        source_path.with_suffix(suffix).is_file()
        for suffix in (".py", ".sql", ".ipynb")
    )


def _pipeline_python_sources():
    for config_path in sorted(
        (PROJECT_ROOT / "pipelines" / "databricks" / "pipelines").glob(
            "*.y*ml"
        )
    ):
        config = yaml.safe_load(config_path.read_text())
        for mapping in _walk_mappings(config):
            configuration = mapping.get("configuration")
            libraries = mapping.get("libraries")
            if not isinstance(configuration, dict) or not isinstance(
                libraries, list
            ):
                continue
            for library in libraries:
                source_path = library.get("glob", {}).get("include")
                if isinstance(source_path, str) and source_path.endswith(".py"):
                    yield config_path, configuration, source_path


def _execute_pipeline_bootstrap_without_file(
    source_path: Path,
    configured_src_root: Path,
):
    tree = ast.parse(source_path.read_text(), filename=str(source_path))
    bootstrap_nodes = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_bootstrap_repo_paths"
    ]
    module = ast.fix_missing_locations(
        ast.Module(body=bootstrap_nodes, type_ignores=[])
    )

    class _PipelineConfig:
        def get(self, key, default=None):
            if key == "pipeline.source_path":
                return str(configured_src_root)
            return default

    fake_sys = SimpleNamespace(path=["legacy-repository-root"])
    namespace = {
        "Path": Path,
        "spark": SimpleNamespace(conf=_PipelineConfig()),
        "sys": fake_sys,
    }
    exec(compile(module, str(source_path), "exec"), namespace)
    namespace["_bootstrap_repo_paths"]()
    return fake_sys.path


def test_legacy_package_roots_are_removed():
    assert not (PROJECT_ROOT / "next_ads").exists()
    assert not (PROJECT_ROOT / "src" / "next_ads" / "data_validation").exists()


def test_python_sources_use_only_canonical_next_ads_imports():
    violations = []

    for path in _python_files(*PYTHON_SOURCE_ROOTS):
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


def test_every_next_ads_import_module_resolves_under_src():
    violations = []

    for path in _python_files(*PYTHON_SOURCE_ROOTS):
        for imported_module in _imports(path):
            if (
                imported_module == "next_ads"
                or imported_module.startswith("next_ads.")
            ) and not _canonical_module_exists(imported_module):
                violations.append(
                    f"{path.relative_to(PROJECT_ROOT)}: {imported_module}"
                )

    assert not violations, "\n".join(violations)


def test_job_entrypoints_resolve_src_before_importing_package_code():
    violations = []

    for path in _python_files("jobs"):
        source = path.read_text(encoding="utf-8-sig")
        tree = ast.parse(
            source,
            filename=str(path),
        )
        package_import_lines = [
            node.lineno for node in ast.walk(tree) if _imports_next_ads(node)
        ]
        if not package_import_lines:
            continue

        feature_bootstrap_lines = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.module == "_registry_job"
        ]
        if feature_bootstrap_lines:
            bootstrap_lines = feature_bootstrap_lines
        else:
            has_declared_src_root = 'SRC_ROOT = PROJECT_ROOT / "src"' in source
            bootstrap_lines = [
                node.lineno
                for node in ast.walk(tree)
                if _is_src_path_insert(node)
            ]
            if not has_declared_src_root:
                bootstrap_lines = []

        if (
            not bootstrap_lines
            or min(bootstrap_lines) >= min(package_import_lines)
        ):
            violations.append(
                f"{path.relative_to(PROJECT_ROOT)}: "
                "canonical package imported before src bootstrap"
            )

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


def test_all_databricks_resource_configs_are_included():
    bundle = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text())
    configured_includes = set(bundle["include"])
    resource_configs = {
        str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
        for resource_dir in ("jobs", "pipelines")
        for path in (
            PROJECT_ROOT / "pipelines" / "databricks" / resource_dir
        ).glob("*.y*ml")
    }

    assert resource_configs <= configured_includes, "\n".join(
        sorted(resource_configs - configured_includes)
    )


def test_all_databricks_source_routes_exist():
    violations = []
    config_root = PROJECT_ROOT / "pipelines" / "databricks"

    for config_path in sorted(config_root.rglob("*.y*ml")):
        config = yaml.safe_load(config_path.read_text())
        for mapping in _walk_mappings(config):
            configured_paths = []
            python_file = mapping.get("python_file")
            if isinstance(python_file, str):
                configured_paths.append(python_file)

            notebook_path = mapping.get("notebook_path")
            if (
                isinstance(notebook_path, str)
                and notebook_path.startswith("${workspace.file_path}/")
            ):
                configured_paths.append(notebook_path)

            source_include = mapping.get("include")
            if isinstance(source_include, str) and source_include.endswith(".py"):
                configured_paths.append(source_include)

            for configured_path in configured_paths:
                resolved_path = _resolve_bundle_source_path(
                    config_path, configured_path
                )
                if not _bundle_source_exists(resolved_path):
                    violations.append(
                        f"{config_path.relative_to(PROJECT_ROOT)}: "
                        f"{configured_path}"
                    )

    assert not violations, "\n".join(violations)


def test_lakeflow_sources_resolve_src_without_file(tmp_path):
    deployed_src_root = tmp_path / "src"
    (deployed_src_root / "next_ads").mkdir(parents=True)

    pipeline_sources = {
        (
            config_path,
            source_path,
        ): configuration.get("pipeline.source_path")
        for config_path, configuration, source_path in _pipeline_python_sources()
    }

    assert pipeline_sources
    for (
        config_path,
        configured_source,
    ), configured_src in pipeline_sources.items():
        assert configured_src == "${workspace.file_path}/src", config_path

        source_path = _resolve_bundle_source_path(
            config_path, configured_source
        )
        tree = ast.parse(source_path.read_text(), filename=str(source_path))
        bootstrap_call_line = min(
            node.lineno
            for node in tree.body
            if isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id == "_bootstrap_repo_paths"
        )
        package_import_line = min(
            node.lineno for node in tree.body if _imports_next_ads(node)
        )

        assert bootstrap_call_line < package_import_line, source_path
        resolved_paths = _execute_pipeline_bootstrap_without_file(
            source_path,
            deployed_src_root,
        )
        assert resolved_paths[:2] == [
            str(deployed_src_root),
            str(tmp_path),
        ]


def test_feature_job_bootstrap_uses_its_declared_file_location():
    helper_path = (
        PROJECT_ROOT / "jobs" / "features" / "nextads" / "_registry_job.py"
    )
    source = helper_path.read_text()

    assert "PROJECT_ROOT = Path(__file__).resolve().parents[3]" in source
    assert 'globals().get("__file__")' not in source
    assert "get_dbutils" not in source

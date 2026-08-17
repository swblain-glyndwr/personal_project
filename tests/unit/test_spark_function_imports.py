import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PRODUCTION_ROOTS = (PROJECT_ROOT / "jobs", PROJECT_ROOT / "src")


def _uses_spark_functions_alias(tree):
    return any(
        isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "F"
        for node in ast.walk(tree)
    )


def _imports_spark_functions_alias(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "pyspark.sql":
            if any(
                alias.name == "functions" and alias.asname == "F"
                for alias in node.names
            ):
                return True
        if isinstance(node, ast.Import) and any(
            alias.name == "pyspark.sql.functions" and alias.asname == "F"
            for alias in node.names
        ):
            return True
    return False


def test_production_files_using_f_import_spark_functions_explicitly():
    missing_imports = []
    for root in PRODUCTION_ROOTS:
        for path in root.rglob("*.py"):
            source = path.read_bytes()
            if b"F." not in source:
                continue
            tree = ast.parse(source, filename=str(path))
            if _uses_spark_functions_alias(
                tree
            ) and not _imports_spark_functions_alias(tree):
                missing_imports.append(path.relative_to(PROJECT_ROOT).as_posix())

    assert missing_imports == []

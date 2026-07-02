import json
from pathlib import Path


def find_project_root() -> Path:
    """Find the repo root from package, job, script, or test locations."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    return Path(__file__).resolve().parents[3]


PROJECT_ROOT = find_project_root()


SQL_SEARCH_DIRS = [
    "sql/control",
    "sql/retrieval",
    "sql/retrieval/conditional_probability",
    "sql/ranking",
    "sql/ranking/theme_affinity",
    "sql/decisioning",
    "sql/adsv2",
    "sql/delivery",
    "sql/reporting",
    "sql/realtime",
    "sql/features",
    "sql",
]


def _first_existing_path(candidates: list[str]) -> Path:
    for candidate in candidates:
        path = PROJECT_ROOT / candidate
        if path.exists():
            return path
    return PROJECT_ROOT / candidates[0]


def resolve_client_config_path(client: str) -> Path:
    return _first_existing_path(
        [
            f"configs/clients/{client}.json",
            f"config/{client}.json",
        ]
    )


def iter_client_config_paths() -> list[Path]:
    paths = sorted((PROJECT_ROOT / "configs" / "clients").glob("*.json"))
    if paths:
        return paths
    return sorted((PROJECT_ROOT / "config").glob("*.json"))


def load_client_config(client: str) -> dict:
    return json.loads(resolve_client_config_path(client).read_text())


def resolve_sql_path(file_name: str) -> Path:
    candidates = [PROJECT_ROOT / directory / file_name for directory in SQL_SEARCH_DIRS]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return PROJECT_ROOT / "sql" / file_name


def resolve_sql_contract_path(table_ref: str) -> Path:
    file_name = f"create_table_{table_ref.replace('.', '_')}.sql"
    return resolve_sql_path(file_name)

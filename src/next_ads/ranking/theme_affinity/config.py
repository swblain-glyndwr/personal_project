from dataclasses import dataclass
from pathlib import Path

from next_ads.common import config_manager


@dataclass(frozen=True)
class ThemeAffinityRuntime:
    config: object
    job_env: str
    client: str
    namespace: str
    table_prefix: str
    model_uri: str
    project_root: Path
    sql_path: Path


def _project_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    return Path(__file__).resolve().parents[4]


def resolve_runtime(
    job_env: str,
    client: str,
    model_uri: str | None = None,
) -> ThemeAffinityRuntime:
    config = config_manager.load_config(job_env)
    project_root = _project_root()
    namespace = f"{config.catalog_write}.{config.schema_write}"
    table_prefix = config.ranking_model_table_prefix
    resolved_model_uri = model_uri or config.ranking_model.model_uri
    sql_path = project_root / "src" / "next_ads" / "ranking" / "theme_affinity" / "sql"

    return ThemeAffinityRuntime(
        config=config,
        job_env=job_env,
        client=client,
        namespace=namespace,
        table_prefix=table_prefix,
        model_uri=resolved_model_uri,
        project_root=project_root,
        sql_path=sql_path,
    )

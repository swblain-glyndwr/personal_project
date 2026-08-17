from dataclasses import dataclass
from datetime import date
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
    run_date: date | None = None
    input_snapshot_id: str | None = None
    provider_build_id: str | None = None
    provider_build_attempt_id: str | None = None
    item_themes_table: str | None = None
    context_slot: str | None = None
    provider_context: object | None = None
    git_commit: str | None = None


def _project_root() -> Path:
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").exists():
            return parent
    return Path(__file__).resolve().parents[4]


def resolve_runtime(
    job_env: str,
    client: str,
    model_uri: str | None = None,
    run_date: date | None = None,
    input_snapshot_id: str | None = None,
    provider_build_id: str | None = None,
    provider_build_attempt_id: str | None = None,
    item_themes_table: str | None = None,
    context_slot: str | None = None,
    provider_context: object | None = None,
    git_commit: str | None = None,
) -> ThemeAffinityRuntime:
    config = config_manager.load_config(job_env, client=client)
    project_root = _project_root()
    namespace = f"{config.catalog_write}.{config.schema_write}"
    table_prefix = config.ranking_model_table_prefix
    resolved_model_uri = model_uri or config.ranking_model.model_uri
    sql_path = (
        project_root
        / "src"
        / "next_ads"
        / "ranking"
        / "theme_affinity"
        / "sql"
    )

    return ThemeAffinityRuntime(
        config=config,
        job_env=job_env,
        client=client,
        namespace=namespace,
        table_prefix=table_prefix,
        model_uri=resolved_model_uri,
        project_root=project_root,
        sql_path=sql_path,
        run_date=run_date,
        input_snapshot_id=input_snapshot_id,
        provider_build_id=provider_build_id,
        provider_build_attempt_id=provider_build_attempt_id,
        item_themes_table=item_themes_table,
        context_slot=context_slot,
        provider_context=provider_context,
        git_commit=git_commit,
    )


def read_runtime_foundation_output(
    spark,
    runtime: ThemeAffinityRuntime,
    output_name: str,
):
    """Read a foundation output only through its provider-bound Delta version."""
    if runtime.provider_context is None:
        raise ValueError("Theme Affinity provider context is incomplete")
    from next_ads.ranking.provider_context import (
        read_bound_foundation_output,
    )

    return read_bound_foundation_output(
        spark,
        runtime.provider_context,
        output_name,
    )


def resolve_context_runtime(
    spark,
    *,
    job_env: str,
    client: str,
    context_slot: str,
    expected_run_date: str,
    expected_input_snapshot_id: str,
    expected_provider_build_id: str,
    expected_provider_build_attempt_id: str,
    git_commit: str,
) -> tuple[ThemeAffinityRuntime, object]:
    from datetime import date

    from next_ads.ranking.provider_context import (
        foundation_output_binding,
        load_active_provider_context,
    )

    config = config_manager.load_config(job_env, client=client)
    context = load_active_provider_context(
        spark,
        context_table=config.tables_write.score_provider_run_contexts,
        context_slot=context_slot,
    )
    expected = {
        "run_date": date.fromisoformat(expected_run_date),
        "input_snapshot_id": expected_input_snapshot_id,
        "provider_build_id": expected_provider_build_id,
        "provider_build_attempt_id": expected_provider_build_attempt_id,
    }
    mismatched = [
        field
        for field, value in expected.items()
        if getattr(context, field) != value
    ]
    if mismatched:
        raise ValueError(
            "Active provider context does not match task parameters: "
            + ", ".join(mismatched)
        )
    foundation_output_binding(context, "ranked")
    return (
        resolve_runtime(
            job_env,
            client,
            model_uri=context.model_uri,
            run_date=context.run_date,
            input_snapshot_id=context.input_snapshot_id,
            provider_build_id=context.provider_build_id,
            provider_build_attempt_id=context.provider_build_attempt_id,
            item_themes_table=config.tables_write.scoring_input_item_themes,
            context_slot=context.context_slot,
            provider_context=context,
            git_commit=git_commit,
        ),
        context,
    )

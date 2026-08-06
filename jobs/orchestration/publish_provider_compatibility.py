"""Publish legacy provider outputs from one exact canonical READY build."""

from __future__ import annotations

import sys
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    from dsutils.dbc import get_dbutils

    notebook_path = (
        get_dbutils()
        .notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    SRC_ROOT = PROJECT_ROOT / "src"
    if SRC_ROOT.exists():
        sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging
from pyspark.sql import functions as F

from next_ads.common import config_manager
from next_ads.ranking.provider_compatibility import (
    configured_compatibility_publisher,
)
from next_ads.ranking.provider_selection import (
    load_score_provider_builds,
    select_score_provider_build,
)
from next_ads.ranking.scoring_inputs import read_delta_version


def main(
    job_env: str,
    client: str,
    run_date: str,
    provider_id: str,
    log_level: str | None,
) -> None:
    configure_logging(
        log_level=log_level
    ) if log_level else configure_logging()
    spark = configure_spark()
    config = config_manager.load_config(job_env, client=client)
    resolved_date = date.fromisoformat(run_date)
    cutoff = datetime.now(timezone.utc)
    provider = config.scoring.providers[provider_id]
    builds = load_score_provider_builds(
        spark,
        table=config.tables_write.score_provider_builds,
        run_date=resolved_date,
        selection_cutoff=cutoff,
        provider_id=provider_id,
        capability=provider.capability,
        use_case="theme_ranking",
    )
    selection = select_score_provider_build(
        builds,
        run_date=resolved_date,
        selection_cutoff=cutoff,
        provider_id=provider_id,
        capability=provider.capability,
        use_case="theme_ranking",
        allow_fallback=False,
    )
    build = next(
        item
        for item in builds
        if item.provider_build_attempt_id
        == selection.provider_build_attempt_id
    )
    signals = read_delta_version(
        spark,
        selection.provider_signals_table,
        selection.provider_signals_delta_version,
    ).where(F.col("ProviderBuildID") == selection.provider_build_id)
    context = SimpleNamespace(
        run_date=build.run_date,
        model_uri=build.model_uri,
        provider_id=build.provider_id,
    )
    publisher = configured_compatibility_publisher(
        spark,
        config=config,
        context=context,
        provider_config=provider,
    )
    publisher(signals, build.completed_at)


if __name__ == "__main__":
    parser = get_job_parser()
    parser._parse_args()
    main(
        parser.get_arg("--job_env"),
        parser.get_arg("--client") or "next_uk",
        parser.get_arg("--run_date"),
        parser.get_arg("--provider_id") or "theme_affinity",
        parser.get_arg("--log_level"),
    )

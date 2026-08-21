"""Run allowlisted NextAds retention and Delta maintenance."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession


def resolve_project_root() -> Path:
    """Resolve the synced repository root in local and Databricks runs."""
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        from dsutils.dbc import get_dbutils

        dbutils = get_dbutils()
        notebook_path = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        if not notebook_path.startswith("/Workspace"):
            notebook_path = "/Workspace" + notebook_path
        return Path(notebook_path).parents[2]


PROJECT_ROOT = resolve_project_root()
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(1, str(PROJECT_ROOT))

from next_ads.common import config_manager
from next_ads.common.job_logging import configure_job_logging
from next_ads.decisioning.table_maintenance import (
    build_maintenance_plan,
    execute_maintenance_plan,
)


VALID_JOB_ENVS = frozenset({"dev", "preprod", "prod"})


def parse_run_date(value: str) -> date:
    """Parse a required logical ISO run date."""
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            "--run_date must use ISO format YYYY-MM-DD"
        ) from exc
    if value != parsed.isoformat():
        raise argparse.ArgumentTypeError(
            "--run_date must use ISO format YYYY-MM-DD"
        )
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the fixed maintenance job contract."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--client", default="next_uk", choices=["next_uk"])
    parser.add_argument("--job_env", required=True, choices=sorted(VALID_JOB_ENVS))
    parser.add_argument("--run_date", required=True, type=parse_run_date)
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Resolve the allowlist and execute its logical-date maintenance plan."""
    args = parse_args(argv)
    configure_job_logging(
        getattr(logging, args.log_level.upper(), logging.INFO),
        log_format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger(__name__)
    config = config_manager.load_config(args.job_env, client=args.client)
    statements = build_maintenance_plan(config, args.run_date)
    logger.info(
        "Prepared %s maintenance operations for %s",
        len(statements),
        args.run_date.isoformat(),
    )
    execute_maintenance_plan(
        SparkSession.builder.getOrCreate(),
        statements,
        run_date=args.run_date,
        logger=logger,
    )
    logger.info("Table maintenance complete")


if __name__ == "__main__":
    main()

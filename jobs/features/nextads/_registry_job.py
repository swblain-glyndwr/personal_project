"""Shared helpers for Next Ads feature-store Databricks jobs."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path


try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
except NameError:
    PROJECT_ROOT = Path("/Workspace")
finally:
    sys.path.insert(0, str(PROJECT_ROOT))


from next_ads.features import load_feature_store_registry


LOGGER = logging.getLogger(__name__)


def configure_job_logging(log_level: str) -> None:
    """Configure job logging while keeping dependency internals quiet."""
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)


def parse_common_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference_date", default=None)
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--source_catalog", default="marketingdata_prod")
    parser.add_argument("--source_schema", default="warehouse")
    parser.add_argument("--theme_source_catalog", default=None)
    parser.add_argument("--theme_source_schema", default="ds_sandbox")
    parser.add_argument(
        "--theme_table_prefix",
        default="next_uk_nextads_theme_affinity_predict",
    )
    parser.add_argument("--replace_reference_date", default="true")
    parser.add_argument("--job_env", default="dev")
    parser.add_argument("--client", default="next_uk")
    parser.add_argument("--theme_training_reference_date", default="skip")
    parser.add_argument(
        "--theme_training_table_prefix",
        default="next_uk_nextads_theme_affinity_training",
    )
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def log_owned_tables(source_job: str, args: argparse.Namespace) -> list[str]:
    configure_job_logging(args.log_level)
    registry = load_feature_store_registry()
    catalog = args.catalog or registry.default_catalog
    schema = args.schema or registry.default_schema
    owned_tables = [
        registry.resolved_table_path(table.name, catalog=catalog, schema=schema)
        for table in registry.physical_tables
        if table.source_job == source_job
    ]

    LOGGER.info(
        "Feature-store job %s reference_date=%s target=%s.%s",
        source_job,
        args.reference_date,
        catalog,
        schema,
    )
    for table_path in owned_tables:
        LOGGER.info("Registered output table: %s", table_path)
    return owned_tables


def metadata_only_main(source_job: str) -> None:
    args = parse_common_args()
    owned_tables = log_owned_tables(source_job, args)
    LOGGER.info(
        "%s completed metadata-only scaffold for %s tables",
        source_job,
        len(owned_tables),
    )

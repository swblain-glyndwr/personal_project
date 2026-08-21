"""Resolve one Feature Store reference date for a multi-task source build."""

from __future__ import annotations

import argparse
import logging

from _registry_job import configure_job_logging
from dsutils.dbc import configure_spark, get_dbutils
from next_ads.features.theme_affinity import resolve_theme_reference_date


LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference_date", default="predict")
    parser.add_argument("--source_catalog", default="marketingdata_prod")
    parser.add_argument("--source_schema", default="warehouse")
    parser.add_argument("--theme_source_catalog", default=None)
    parser.add_argument("--theme_source_schema", default="warehouse")
    parser.add_argument(
        "--theme_table_prefix",
        default="next_uk_nextads_account_theme_foundation",
    )
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_job_logging(args.log_level)
    spark = configure_spark()
    source_catalog = args.theme_source_catalog or args.source_catalog
    reference_date = resolve_theme_reference_date(
        spark,
        source_catalog,
        args.theme_source_schema,
        args.theme_table_prefix,
        args.reference_date,
    )
    get_dbutils().jobs.taskValues.set(
        key="reference_date",
        value=reference_date,
    )
    LOGGER.info("FEATURE_STORE_REFERENCE_DATE=%s", reference_date)


if __name__ == "__main__":
    main()

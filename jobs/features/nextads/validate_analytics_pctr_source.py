"""Validate and receipt the repository-owned Analytics pCTR feature output."""

from __future__ import annotations

import argparse
import logging

from dsutils.dbc import configure_spark, get_dbutils
from next_ads.features.analytics_pctr_source import (
    bind_analytics_pctr_source,
    load_analytics_pctr_source_definition,
    persist_analytics_pctr_source_binding,
    serialise_source_binding,
    with_producing_run_id,
)

from _registry_job import configure_job_logging


LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference_date", required=True)
    parser.add_argument("--source_binding", required=True)
    parser.add_argument("--source_catalog", required=True)
    parser.add_argument("--source_schema", required=True)
    parser.add_argument("--producing_job_run_id", required=True)
    parser.add_argument("--receipt_correlation_id", required=True)
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_job_logging(args.log_level)
    spark = configure_spark()
    definition = load_analytics_pctr_source_definition(
        args.source_binding,
        catalog=args.source_catalog,
        schema=args.source_schema,
    )
    resolved_receipt, _ = bind_analytics_pctr_source(
        spark,
        definition=definition,
        reference_date=args.reference_date,
    )
    receipt = with_producing_run_id(
        resolved_receipt,
        args.producing_job_run_id,
    )
    receipt_table = persist_analytics_pctr_source_binding(
        spark,
        definition=definition,
        binding=receipt,
        receipt_correlation_id=args.receipt_correlation_id,
    )
    serialised = serialise_source_binding(receipt)
    task_values = get_dbutils().jobs.taskValues
    task_values.set(key="source_receipt", value=serialised)
    task_values.set(key="source_delta_version", value=receipt.delta_version)
    task_values.set(key="source_table_id", value=receipt.table_id)
    task_values.set(
        key="source_reference_date_row_count",
        value=receipt.reference_date_row_count,
    )
    task_values.set(key="source_schema_sha256", value=receipt.schema_sha256)
    task_values.set(key="source_receipt_table", value=receipt_table)
    LOGGER.info("ANALYTICS_PCTR_SOURCE_RECEIPT=%s", serialised)


if __name__ == "__main__":
    main()

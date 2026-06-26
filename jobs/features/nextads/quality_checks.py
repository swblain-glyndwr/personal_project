"""Run feature-store quality checks."""

import logging
from datetime import datetime, timezone
from functools import reduce

from _registry_job import configure_job_logging, log_owned_tables, parse_common_args
from dsutils.dbc import configure_spark
from next_ads.features import load_feature_store_registry
from next_ads.features.materialization import (
    create_feature_engineering_client,
    feature_table_path,
    write_feature_table,
)
from next_ads.features.theme_affinity import resolve_theme_reference_date
from pyspark.sql import functions as F


LOGGER = logging.getLogger(__name__)

FEATURE_STORE_QUALITY_TABLES = [
    "next_uk_nextads_fs_account_profile",
    "next_uk_nextads_fs_account_web_activity_90d",
    "next_uk_nextads_fs_advert_core_daily",
    "next_uk_nextads_fs_advert_attribute_profile_daily",
    "next_uk_nextads_fs_account_theme_interactions_daily",
    "next_uk_nextads_fs_account_theme_affinity_daily",
    "next_uk_nextads_fs_theme_popularity_daily",
    "next_uk_nextads_fs_labels_theme_response",
    "next_uk_nextads_fs_theme_affinity_model_input",
    "next_uk_nextads_fs_labels_clicks",
]


def _null_key_condition(primary_keys: tuple[str, ...]):
    return reduce(
        lambda left, right: left | right,
        (F.col(column).isNull() for column in primary_keys),
    )


def _quality_event(
    table_name: str,
    reference_date: str,
    run_timestamp: datetime,
    row_count: int,
    distinct_key_count: int,
    null_key_count: int,
) -> dict[str, object]:
    duplicate_key_count = max(row_count - distinct_key_count, 0)
    status = (
        "pass"
        if row_count > 0 and null_key_count == 0 and duplicate_key_count == 0
        else "fail"
    )
    return {
        "table_name": table_name,
        "check_name": "primary_key_quality",
        "run_timestamp": run_timestamp,
        "reference_date": reference_date,
        "status": status,
        "row_count": row_count,
        "distinct_key_count": distinct_key_count,
        "null_key_count": null_key_count,
        "duplicate_key_count": duplicate_key_count,
        "freshness_status": status,
        "metric_value": float(row_count),
        "details": (
            "row_count must be non-zero and primary keys must be unique "
            "and non-null for the target reference date"
        ),
    }


def main() -> None:
    args = parse_common_args()
    configure_job_logging(args.log_level)
    log_owned_tables("quality_checks", args)

    spark = configure_spark()
    registry = load_feature_store_registry()
    target_catalog = args.catalog or registry.default_catalog
    target_schema = args.schema or registry.default_schema
    source_catalog = args.theme_source_catalog or target_catalog
    reference_date = resolve_theme_reference_date(
        spark,
        source_catalog,
        args.theme_source_schema,
        args.theme_table_prefix,
        args.reference_date,
    )
    run_timestamp = datetime.now(timezone.utc)
    quality_events = []

    for table_name in FEATURE_STORE_QUALITY_TABLES:
        table = registry.table_spec(table_name)
        table_path = feature_table_path(
            table_name,
            target_catalog,
            target_schema,
            registry,
        )
        dataframe = spark.table(table_path).where(
            F.col(table.timestamp_key) == F.lit(reference_date).cast("date")
        )
        row_count = dataframe.count()
        distinct_key_count = dataframe.select(*table.primary_keys).distinct().count()
        null_key_count = dataframe.where(
            _null_key_condition(table.primary_keys)
        ).count()
        quality_events.append(
            _quality_event(
                table_name,
                reference_date,
                run_timestamp,
                row_count,
                distinct_key_count,
                null_key_count,
            )
        )

    quality_df = spark.createDataFrame(quality_events)
    table_path = write_feature_table(
        spark,
        "next_uk_nextads_fs_feature_quality_events",
        quality_df,
        catalog=target_catalog,
        schema=target_schema,
        reference_date=reference_date,
        replace_reference_date=False,
        feature_engineering_client=create_feature_engineering_client(),
    )
    LOGGER.info("Wrote feature-store quality events: %s", table_path)

    failed_events = [event for event in quality_events if event["status"] != "pass"]
    if failed_events:
        raise ValueError(f"Feature-store quality checks failed: {failed_events}")


if __name__ == "__main__":
    main()

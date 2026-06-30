"""Validate feature-store source frames before writing feature tables."""

from __future__ import annotations

import logging
from functools import reduce
from operator import or_

from _registry_job import configure_job_logging, parse_common_args
from dsutils.dbc import configure_spark
from next_ads.features import load_feature_store_registry
from next_ads.features.materialization import (
    align_to_feature_table_contract,
    validate_required_column_values,
    validate_required_columns,
)
from next_ads.features.nextads_core import (
    build_advert_attribute_profile_df,
    build_advert_core_df,
    build_click_labels_df,
    build_item_attributes_df,
)
from next_ads.features.theme_affinity import (
    build_account_theme_affinity_df,
    build_account_theme_interactions_df,
    build_theme_affinity_account_profile_df,
    build_theme_affinity_account_web_activity_df,
    build_theme_affinity_model_input_df,
    build_theme_popularity_df,
    build_theme_response_labels_df,
    read_theme_account_source_tables,
    read_theme_source_tables,
    resolve_theme_reference_date,
)
from pyspark.sql import functions as F


LOGGER = logging.getLogger(__name__)


def _null_key_condition(primary_keys: tuple[str, ...]):
    return reduce(or_, (F.col(column).isNull() for column in primary_keys))


def _validate_frame(table_name: str, df, registry) -> dict[str, object]:
    table = registry.table_spec(table_name)
    validate_required_columns(df, table.primary_keys, table_name)
    aligned_df = align_to_feature_table_contract(df, table_name, registry)
    validate_required_column_values(aligned_df, table.primary_keys, table_name)

    row_count = aligned_df.count()
    distinct_key_count = aligned_df.select(*table.primary_keys).distinct().count()
    duplicate_key_count = max(row_count - distinct_key_count, 0)
    if row_count == 0:
        raise ValueError(f"{table_name} would write zero rows")
    if duplicate_key_count:
        raise ValueError(
            f"{table_name} would write duplicate feature-store keys: "
            f"rows={row_count}, distinct_keys={distinct_key_count}, "
            f"duplicate_keys={duplicate_key_count}"
        )
    null_key_count = aligned_df.where(_null_key_condition(table.primary_keys)).count()
    return {
        "table_name": table_name,
        "row_count": row_count,
        "distinct_key_count": distinct_key_count,
        "null_key_count": null_key_count,
        "duplicate_key_count": duplicate_key_count,
    }


def _planned_feature_frames(spark, args, reference_date: str) -> dict[str, object]:
    theme_catalog = args.theme_source_catalog or args.source_catalog
    theme_tables = read_theme_source_tables(
        spark,
        theme_catalog,
        args.theme_source_schema,
        args.theme_table_prefix,
        reference_date,
    )
    account_tables = read_theme_account_source_tables(
        spark,
        theme_catalog,
        args.theme_source_schema,
        args.theme_table_prefix,
        reference_date,
    )
    return {
        "next_uk_nextads_fs_account_profile": (
            build_theme_affinity_account_profile_df(
                account_tables["customer_features"],
                account_tables["customer_segments"],
                reference_date,
            )
        ),
        "next_uk_nextads_fs_account_web_activity_90d": (
            build_theme_affinity_account_web_activity_df(
                account_tables["ranked"],
                account_tables["advanced"],
                reference_date,
            )
        ),
        "next_uk_nextads_fs_advert_core_daily": build_advert_core_df(
            spark,
            args.source_catalog,
            args.source_schema,
            reference_date,
        ),
        "next_uk_nextads_fs_item_attributes_latest": build_item_attributes_df(
            spark,
            args.source_catalog,
            args.source_schema,
        ),
        "next_uk_nextads_fs_advert_attribute_profile_daily": (
            build_advert_attribute_profile_df(
                spark,
                args.source_catalog,
                args.source_schema,
                reference_date,
            )
        ),
        "next_uk_nextads_fs_account_theme_interactions_daily": (
            build_account_theme_interactions_df(
                theme_tables["ranked"],
                reference_date,
            )
        ),
        "next_uk_nextads_fs_account_theme_affinity_daily": (
            build_account_theme_affinity_df(
                theme_tables["ranked"],
                theme_tables["prediction"],
                reference_date,
            )
        ),
        "next_uk_nextads_fs_theme_popularity_daily": build_theme_popularity_df(
            theme_tables["popularity"],
            reference_date,
        ),
        "next_uk_nextads_fs_labels_theme_response": (
            build_theme_response_labels_df(
                theme_tables["ranked"],
                reference_date,
            )
        ),
        "next_uk_nextads_fs_theme_affinity_model_input": (
            build_theme_affinity_model_input_df(
                theme_tables["ranked"],
                theme_tables["prediction"],
                reference_date,
            )
        ),
        "next_uk_nextads_fs_labels_clicks": build_click_labels_df(
            spark,
            args.source_catalog,
            args.source_schema,
            reference_date,
        ),
    }


def main() -> None:
    args = parse_common_args()
    configure_job_logging(args.log_level)

    spark = configure_spark()
    registry = load_feature_store_registry()
    theme_catalog = args.theme_source_catalog or args.source_catalog
    reference_date = resolve_theme_reference_date(
        spark,
        theme_catalog,
        args.theme_source_schema,
        args.theme_table_prefix,
        args.reference_date,
    )
    LOGGER.info(
        "Running feature-store source preflight for reference_date=%s",
        reference_date,
    )

    results = []
    for table_name, dataframe in _planned_feature_frames(
        spark,
        args,
        reference_date,
    ).items():
        result = _validate_frame(table_name, dataframe, registry)
        LOGGER.info("Preflight passed: %s", result)
        results.append(result)

    LOGGER.info("Feature-store source preflight passed for %s tables", len(results))


if __name__ == "__main__":
    main()

"""Build Theme Affinity feature-store tables."""

import logging

from _registry_job import configure_job_logging, log_owned_tables, parse_common_args
from dsutils.dbc import configure_spark
from next_ads.features import load_feature_store_registry
from next_ads.features.materialization import (
    create_feature_engineering_client,
    write_feature_table,
)
from next_ads.features.theme_affinity import (
    build_account_theme_affinity_df,
    build_account_theme_interactions_df,
    build_theme_popularity_df,
    build_theme_response_labels_df,
    read_theme_source_tables,
    resolve_theme_reference_date,
)


LOGGER = logging.getLogger(__name__)


def main() -> None:
    args = parse_common_args()
    configure_job_logging(args.log_level)
    log_owned_tables("build_theme_affinity_features", args)

    spark = configure_spark()
    registry = load_feature_store_registry()
    target_catalog = args.catalog or registry.default_catalog
    target_schema = args.schema or registry.default_schema
    source_catalog = args.theme_source_catalog or target_catalog
    replace_reference_date = args.replace_reference_date.lower() == "true"
    reference_date = resolve_theme_reference_date(
        spark,
        source_catalog,
        args.theme_source_schema,
        args.theme_table_prefix,
        args.reference_date,
    )
    source_tables = read_theme_source_tables(
        spark,
        source_catalog,
        args.theme_source_schema,
        args.theme_table_prefix,
        reference_date,
    )
    feature_engineering_client = create_feature_engineering_client()

    writes = {
        "next_uk_nextads_fs_account_theme_interactions_daily": (
            build_account_theme_interactions_df(
                source_tables["ranked"],
                reference_date,
            )
        ),
        "next_uk_nextads_fs_account_theme_affinity_daily": (
            build_account_theme_affinity_df(
                source_tables["ranked"],
                source_tables["prediction"],
                reference_date,
            )
        ),
        "next_uk_nextads_fs_theme_popularity_daily": build_theme_popularity_df(
            source_tables["popularity"],
            reference_date,
        ),
        "next_uk_nextads_fs_labels_theme_response": (
            build_theme_response_labels_df(
                source_tables["ranked"],
                reference_date,
            )
        ),
    }

    for table_name, dataframe in writes.items():
        table_path = write_feature_table(
            spark,
            table_name,
            dataframe,
            catalog=target_catalog,
            schema=target_schema,
            reference_date=reference_date,
            replace_reference_date=replace_reference_date,
            feature_engineering_client=feature_engineering_client,
        )
        LOGGER.info("Wrote Theme Affinity feature table: %s", table_path)


if __name__ == "__main__":
    main()

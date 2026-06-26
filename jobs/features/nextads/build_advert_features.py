"""Build advert, item, product, and seasonal feature-store tables."""

import logging

from _registry_job import configure_job_logging, log_owned_tables, parse_common_args
from dsutils.dbc import configure_spark
from next_ads.features import load_feature_store_registry
from next_ads.features.materialization import (
    create_feature_engineering_client,
    write_feature_table,
)
from next_ads.features.nextads_core import (
    build_advert_attribute_profile_df,
    build_advert_core_df,
    build_item_attributes_df,
    resolve_reference_date_from_theme,
)


LOGGER = logging.getLogger(__name__)


def main() -> None:
    args = parse_common_args()
    configure_job_logging(args.log_level)
    log_owned_tables("build_advert_features", args)

    spark = configure_spark()
    registry = load_feature_store_registry()
    target_catalog = args.catalog or registry.default_catalog
    target_schema = args.schema or registry.default_schema
    reference_date = resolve_reference_date_from_theme(spark, args)
    replace_reference_date = args.replace_reference_date.lower() == "true"
    feature_engineering_client = create_feature_engineering_client()

    writes = {
        "next_uk_nextads_fs_advert_core_daily": (
            build_advert_core_df(
                spark,
                args.source_catalog,
                args.source_schema,
                reference_date,
            )
        ),
        "next_uk_nextads_fs_item_attributes_latest": (
            build_item_attributes_df(
                spark,
                args.source_catalog,
                args.source_schema,
            )
        ),
        "next_uk_nextads_fs_advert_attribute_profile_daily": (
            build_advert_attribute_profile_df(
                spark,
                args.source_catalog,
                args.source_schema,
                reference_date,
            )
        ),
    }

    for table_name, dataframe in writes.items():
        table = registry.table_spec(table_name)
        table_path = write_feature_table(
            spark,
            table_name,
            dataframe,
            catalog=target_catalog,
            schema=target_schema,
            reference_date=reference_date if table.timestamp_key else None,
            reference_date_column=table.timestamp_key or "reference_date",
            replace_reference_date=replace_reference_date,
            feature_engineering_client=feature_engineering_client,
        )
        LOGGER.info("Wrote advert feature table: %s", table_path)

    LOGGER.info(
        "Product embeddings, advert semantic profiles, advert product profiles "
        "and seasonal demand remain scaffolded until their model/source "
        "contracts are promoted into this route."
    )


if __name__ == "__main__":
    main()

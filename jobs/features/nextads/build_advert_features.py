"""Build advert, item, product, and seasonal feature-store tables."""

import logging

from _registry_job import (
    configure_job_logging,
    feature_group_identity,
    log_owned_tables,
    parse_common_args,
    validate_builder_output_tables,
)
from dsutils.dbc import configure_spark
from next_ads.features import load_feature_store_registry
from next_ads.features.analytics_pctr_source import parse_reference_date
from next_ads.features.nextads_core import (
    build_advert_attribute_profile_df,
    build_advert_core_df,
    build_item_attributes_df,
    resolve_reference_date_from_theme,
)
from next_ads.features.snapshot_publication import (
    write_and_publish_feature_group,
)
from next_ads.features.source_pinning import PinnedSourceSession


LOGGER = logging.getLogger(__name__)
BUILDER = "build_advert_features"


def main() -> None:
    args = parse_common_args()
    configure_job_logging(args.log_level)
    log_owned_tables(BUILDER, args)

    spark = configure_spark()
    registry = load_feature_store_registry()
    target_catalog = args.catalog or registry.default_catalog
    target_schema = args.schema or registry.default_schema
    reference_date = resolve_reference_date_from_theme(spark, args)
    identity = feature_group_identity(args, BUILDER)
    pinned_spark = PinnedSourceSession(
        spark,
        feature_build_id=identity["feature_build_id"],
        feature_build_attempt_id=identity["feature_build_attempt_id"],
        reference_date=parse_reference_date(reference_date),
        target_catalog=target_catalog,
        target_schema=target_schema,
    )
    replace_reference_date = args.replace_reference_date.lower() == "true"
    writes = {
        "next_uk_nextads_fs_advert_core_daily": (
            build_advert_core_df(
                pinned_spark,
                args.source_catalog,
                args.source_schema,
                reference_date,
            )
        ),
        "next_uk_nextads_fs_item_attributes_latest": (
            build_item_attributes_df(
                pinned_spark,
                args.source_catalog,
                args.source_schema,
            )
        ),
        "next_uk_nextads_fs_advert_attribute_profile_daily": (
            build_advert_attribute_profile_df(
                pinned_spark,
                args.source_catalog,
                args.source_schema,
                reference_date,
            )
        ),
    }
    validate_builder_output_tables(
        BUILDER,
        writes,
        registry,
    )

    _ready_build, snapshot = write_and_publish_feature_group(
        spark,
        catalog=target_catalog,
        schema=target_schema,
        group_id=BUILDER,
        reference_date=parse_reference_date(reference_date),
        frames=writes,
        sources=pinned_spark.source_bindings,
        registry=registry,
        replace_reference_date=replace_reference_date,
        **identity,
    )
    LOGGER.info(
        "Published READY advert feature snapshot: %s attempt %s",
        snapshot.feature_snapshot_id,
        snapshot.feature_snapshot_attempt_id,
    )


if __name__ == "__main__":
    main()

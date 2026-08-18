"""Publish current-date account activity for the Shopping Bag pCTR example."""

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
from next_ads.features.shopping_bag_account_activity import (
    build_shopping_bag_account_activity_df,
    read_shopping_bag_account_activity_sources,
)
from next_ads.features.snapshot_publication import (
    write_and_publish_feature_group,
)
from next_ads.features.source_pinning import PinnedSourceSession


LOGGER = logging.getLogger(__name__)
BUILDER = "build_shopping_bag_account_activity"
OUTPUT_TABLE = "next_uk_nextads_fs_shopping_bag_account_activity_90d"


def main() -> None:
    args = parse_common_args()
    configure_job_logging(args.log_level)
    log_owned_tables(BUILDER, args)
    if not args.reference_date:
        raise ValueError("reference_date is required for account activity")

    spark = configure_spark()
    registry = load_feature_store_registry()
    target_catalog = args.catalog or registry.default_catalog
    target_schema = args.schema or registry.default_schema
    reference_date = parse_reference_date(args.reference_date)
    identity = feature_group_identity(args, BUILDER)
    pinned_spark = PinnedSourceSession(
        spark,
        feature_build_id=identity["feature_build_id"],
        feature_build_attempt_id=identity["feature_build_attempt_id"],
        reference_date=reference_date,
        target_catalog=target_catalog,
        target_schema=target_schema,
        registry=registry,
    )
    sources = read_shopping_bag_account_activity_sources(
        pinned_spark,
        args.source_catalog,
        args.source_schema,
    )
    activity = build_shopping_bag_account_activity_df(
        **sources,
        reference_date=reference_date,
    )
    frames = {OUTPUT_TABLE: activity}
    validate_builder_output_tables(BUILDER, frames, registry)

    _ready_build, snapshot = write_and_publish_feature_group(
        spark,
        catalog=target_catalog,
        schema=target_schema,
        group_id=BUILDER,
        reference_date=reference_date,
        frames=frames,
        sources=pinned_spark.source_bindings,
        registry=registry,
        replace_reference_date=args.replace_reference_date.lower() == "true",
        **identity,
    )
    LOGGER.info(
        "Published READY Shopping Bag account activity snapshot: %s attempt %s",
        snapshot.feature_snapshot_id,
        snapshot.feature_snapshot_attempt_id,
    )


if __name__ == "__main__":
    main()

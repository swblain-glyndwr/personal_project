"""Build Theme Affinity feature-store tables."""

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
from next_ads.features.snapshot_publication import (
    write_and_publish_feature_group,
)
from next_ads.features.source_pinning import PinnedSourceSession
from next_ads.features.theme_affinity import (
    build_account_theme_affinity_df,
    build_account_theme_interactions_df,
    build_theme_popularity_df,
    build_theme_response_labels_df,
    read_theme_source_tables,
    resolve_theme_reference_date,
)


LOGGER = logging.getLogger(__name__)
BUILDER = "build_theme_affinity_features"


def main() -> None:
    args = parse_common_args()
    configure_job_logging(args.log_level)
    log_owned_tables(BUILDER, args)

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
    identity = feature_group_identity(args, BUILDER)
    pinned_spark = PinnedSourceSession(
        spark,
        feature_build_id=identity["feature_build_id"],
        feature_build_attempt_id=identity["feature_build_attempt_id"],
        reference_date=parse_reference_date(reference_date),
        target_catalog=target_catalog,
        target_schema=target_schema,
        registry=registry,
    )
    source_tables = read_theme_source_tables(
        pinned_spark,
        source_catalog,
        args.theme_source_schema,
        args.theme_table_prefix,
        reference_date,
    )
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
        "Published READY Theme Affinity snapshot: %s attempt %s",
        snapshot.feature_snapshot_id,
        snapshot.feature_snapshot_attempt_id,
    )


if __name__ == "__main__":
    main()

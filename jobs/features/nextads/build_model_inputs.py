"""Build model-input and label feature-store tables."""

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
from next_ads.features.nextads_core import build_click_labels_df
from next_ads.features.theme_affinity import (
    build_theme_affinity_model_input_df,
    read_theme_source_tables,
    resolve_theme_reference_date,
)
from next_ads.features.snapshot_publication import (
    write_and_publish_feature_group,
)
from next_ads.features.source_pinning import PinnedSourceSession


LOGGER = logging.getLogger(__name__)
BUILDER = "build_model_inputs"


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

    writes = (
        "next_uk_nextads_fs_theme_affinity_model_input",
        "next_uk_nextads_fs_labels_clicks",
    )
    validate_builder_output_tables(BUILDER, writes, registry)

    model_input_df = build_theme_affinity_model_input_df(
        source_tables["ranked"],
        source_tables["prediction"],
        reference_date,
    )
    click_labels_df = build_click_labels_df(
        pinned_spark,
        args.source_catalog,
        args.source_schema,
        reference_date,
    )
    _ready_build, snapshot = write_and_publish_feature_group(
        spark,
        catalog=target_catalog,
        schema=target_schema,
        group_id=BUILDER,
        reference_date=parse_reference_date(reference_date),
        frames={
            "next_uk_nextads_fs_theme_affinity_model_input": model_input_df,
            "next_uk_nextads_fs_labels_clicks": click_labels_df,
        },
        sources=pinned_spark.source_bindings,
        registry=registry,
        replace_reference_date=replace_reference_date,
        **identity,
    )
    LOGGER.info(
        "Published READY model-input snapshot: %s attempt %s",
        snapshot.feature_snapshot_id,
        snapshot.feature_snapshot_attempt_id,
    )


if __name__ == "__main__":
    main()

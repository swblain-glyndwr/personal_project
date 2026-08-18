"""Publish observed Shopping Bag impression and click labels."""

import json
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
    build_observed_shopping_bag_click_labels_df,
    build_raw_shopping_bag_events_df,
    read_shopping_bag_click_label_sources,
)
from next_ads.features.snapshot_publication import (
    write_and_publish_feature_group,
)
from next_ads.features.source_pinning import PinnedSourceSession


LOGGER = logging.getLogger(__name__)
BUILDER = "build_shopping_bag_click_labels"
OBSERVED_TABLE = "next_uk_nextads_fs_shopping_bag_click_labels"


def _log_label_evidence(raw_events, observed_labels) -> None:
    """Log bounded audit totals that reviewers can read from the task output."""
    from pyspark.sql import functions as F

    raw_evidence = (
        raw_events.groupBy("event_route", "platform", "action")
        .agg(F.countDistinct("raw_event_id").alias("tagged_events"))
        .orderBy("event_route", "platform", "action")
        .collect()
    )
    evidence = (
        observed_labels.groupBy(
            "route",
            "platform",
            "exposure_match_type",
            "label_horizon_days",
        )
        .agg(
            F.countDistinct("exposure_id").alias("accepted_exposures"),
            F.sum("click_count").alias("attributed_clicks"),
            F.sum("clicked").alias("positive_labels"),
        )
        .orderBy(
            "route",
            "platform",
            "exposure_match_type",
            "label_horizon_days",
        )
        .collect()
    )
    LOGGER.info(
        "SHOPPING_BAG_RAW_EVENT_EVIDENCE=%s",
        json.dumps(
            [row.asDict(recursive=True) for row in raw_evidence]
        ),
    )
    LOGGER.info(
        "SHOPPING_BAG_LABEL_EVIDENCE=%s",
        json.dumps([row.asDict(recursive=True) for row in evidence]),
    )
    quality = observed_labels.agg(
        F.count(F.lit(1)).alias("label_rows"),
        F.countDistinct(
            "exposure_id",
            "label_horizon_days",
            "exposure_timestamp",
        ).alias("distinct_label_keys"),
        F.sum(
            F.when(
                F.lower(F.trim("treatment")).isin(
                    "best",
                    "bestprem",
                    "bestchallenger",
                    "bestchallengerprem",
                ),
                0,
            ).otherwise(1)
        ).alias("invalid_treatment_rows"),
        F.sum(
            F.when(
                F.col("first_click_timestamp").isNotNull()
                & (
                    F.col("first_click_timestamp")
                    <= F.col("exposure_timestamp")
                ),
                1,
            ).otherwise(0)
        ).alias("click_not_after_exposure_rows"),
        F.sum(
            F.when(
                (F.col("platform") == "APP")
                & (
                    F.col("event_cms_page_id").isNull()
                    | F.col("assignment_cms_page_id").isNull()
                ),
                1,
            ).otherwise(0)
        ).alias("app_rows_without_cms_match"),
    ).first()
    LOGGER.info(
        "SHOPPING_BAG_LABEL_QUALITY=%s",
        json.dumps(quality.asDict(recursive=True)),
    )


def _validate_action_watermarks(sources, label_end) -> None:
    """Reject labels when either telemetry source stops before the cutoff."""
    from pyspark.sql import functions as F

    for source_name in ("web_actions", "app_actions"):
        row = sources[source_name].agg(
            F.max(F.to_date("date")).alias("max_event_date")
        ).first()
        max_event_date = row["max_event_date"] if row else None
        if max_event_date is None or max_event_date < label_end:
            raise ValueError(
                f"{source_name} is incomplete for label_end={label_end}: "
                f"max_event_date={max_event_date}"
            )
        LOGGER.info(
            "Shopping Bag label source watermark: %s=%s",
            source_name,
            max_event_date,
        )


def main() -> None:
    args = parse_common_args()
    configure_job_logging(args.log_level)
    log_owned_tables(BUILDER, args)
    if not args.reference_date:
        raise ValueError("reference_date is required for Shopping Bag labels")
    if not args.label_end:
        raise ValueError("label_end is required for Shopping Bag labels")

    spark = configure_spark()
    registry = load_feature_store_registry()
    target_catalog = args.catalog or registry.default_catalog
    target_schema = args.schema or registry.default_schema
    reference_date = parse_reference_date(args.reference_date)
    label_end = parse_reference_date(args.label_end)
    if label_end <= reference_date:
        raise ValueError("label_end must be after reference_date")
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
    sources = read_shopping_bag_click_label_sources(
        pinned_spark,
        args.source_catalog,
        args.source_schema,
    )
    _validate_action_watermarks(sources, label_end)
    raw_events = build_raw_shopping_bag_events_df(
        sources["web_actions"],
        sources["app_actions"],
        reference_date=reference_date.isoformat(),
        label_end=label_end.isoformat(),
    ).cache()
    observed = build_observed_shopping_bag_click_labels_df(
        spark=spark,
        reference_date=reference_date.isoformat(),
        as_of_date=label_end.isoformat(),
        **sources,
    ).cache()
    frames = {OBSERVED_TABLE: observed}
    validate_builder_output_tables(BUILDER, frames, registry)
    _log_label_evidence(raw_events, observed)

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
        write_options={
            OBSERVED_TABLE: {"reference_date_column": "session_date"}
        },
        **identity,
    )
    LOGGER.info(
        "Published READY Shopping Bag label snapshot: %s attempt %s",
        snapshot.feature_snapshot_id,
        snapshot.feature_snapshot_attempt_id,
    )


if __name__ == "__main__":
    main()

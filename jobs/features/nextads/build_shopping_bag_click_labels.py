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
    source_table,
)
from next_ads.features.shopping_bag_label_evidence import (
    collect_reporting_sanity,
    collect_shopping_bag_label_evidence,
)
from next_ads.features.snapshot_publication import (
    write_and_publish_feature_group,
)
from next_ads.features.source_pinning import PinnedSourceSession


LOGGER = logging.getLogger(__name__)
BUILDER = "build_shopping_bag_click_labels"
OBSERVED_TABLE = "next_uk_nextads_fs_shopping_bag_click_labels"


def _log_label_evidence(
    *,
    spark,
    sources,
    raw_events,
    observed_labels,
    reference_date: str,
    label_end: str,
    source_catalog: str,
    source_schema: str,
    source_watermarks,
) -> None:
    """Log one bounded funnel and a non-gating reporting comparison."""
    evidence = collect_shopping_bag_label_evidence(
        sources=sources,
        raw_events=raw_events,
        observed_labels=observed_labels,
        reference_date=reference_date,
        label_end=label_end,
        source_watermarks=source_watermarks,
    )
    reporting_table = source_table(
        source_catalog,
        source_schema,
        "next_uk_nextads_results_ads_location",
    )
    try:
        # This comparison is evidence only, so it must not become a recorded
        # input to the reproducible label snapshot.
        reporting_results = (
            spark.table(reporting_table)
            if spark.catalog.tableExists(reporting_table)
            else None
        )
        reporting_sanity = collect_reporting_sanity(
            observed_labels,
            reporting_results,
            reference_date=reference_date,
            reporting_table=reporting_table,
        )
    except Exception as exc:
        LOGGER.warning(
            "Reporting sanity was unavailable and did not block labels: %s",
            exc,
        )
        reporting_sanity = collect_reporting_sanity(
            observed_labels,
            None,
            reference_date=reference_date,
            reporting_table=reporting_table,
        )
        reporting_sanity["detail"] = (
            "Reporting sanity was unavailable and did not block labels: "
            f"{type(exc).__name__}"
        )
    evidence["reporting_sanity"] = reporting_sanity
    LOGGER.info(
        "SHOPPING_BAG_LABEL_FUNNEL=%s",
        json.dumps(evidence, sort_keys=True),
    )


def _validate_action_watermarks(sources, label_end) -> dict[str, str]:
    """Reject labels when either telemetry source stops before the cutoff."""
    from pyspark.sql import functions as F

    watermarks = {}
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
        watermarks[source_name] = max_event_date.isoformat()
    return watermarks


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
    source_watermarks = _validate_action_watermarks(sources, label_end)
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
    _log_label_evidence(
        spark=spark,
        sources=sources,
        raw_events=raw_events,
        observed_labels=observed,
        reference_date=reference_date.isoformat(),
        label_end=label_end.isoformat(),
        source_catalog=args.source_catalog,
        source_schema=args.source_schema,
        source_watermarks=source_watermarks,
    )

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
        "Published READY Shopping Bag label snapshot: %s attempt %s",
        snapshot.feature_snapshot_id,
        snapshot.feature_snapshot_attempt_id,
    )


if __name__ == "__main__":
    main()

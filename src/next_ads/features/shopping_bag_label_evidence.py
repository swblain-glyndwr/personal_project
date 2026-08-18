"""Bounded operational evidence for observed Shopping Bag click labels."""

from __future__ import annotations

from datetime import date, datetime
import re
from typing import Any, Mapping, Sequence

from next_ads.features.nextads_core import (
    SHOPPING_BAG_SERVED_TREATMENTS,
    _campaign_key,
    _normalise_label_sessions,
    _normalise_observed_advert_id,
    _shopping_bag_v1_assignments,
    _shopping_bag_v2_assignments,
)


EVIDENCE_CONTRACT = "nextads_shopping_bag_label_evidence/v1"
DEFAULT_MAX_EVIDENCE_GROUPS = 200


def _json_safe(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def bounded_evidence_section(
    rows: Sequence[Mapping[str, Any]],
    *,
    total_groups: int | None = None,
    max_groups: int = DEFAULT_MAX_EVIDENCE_GROUPS,
) -> dict[str, Any]:
    """Return a JSON-safe section with an explicit truncation marker."""
    if max_groups < 1:
        raise ValueError("max_groups must be positive")
    total = len(rows) if total_groups is None else int(total_groups)
    selected = list(rows[:max_groups])
    return {
        "total_groups": total,
        "returned_groups": len(selected),
        "truncated": total > len(selected),
        "rows": [_json_safe(dict(row)) for row in selected],
    }


def reporting_sanity_payload(
    *,
    reference_date: str,
    label_impressions: int,
    label_positive_exposures: int,
    reporting_soft_impressions: int | None,
    reporting_soft_clicks: int | None,
    reporting_table: str,
    status: str,
    detail: str | None = None,
) -> dict[str, Any]:
    """Build a directional reporting comparison that can never be a gate."""

    def _rate(numerator: int | None, denominator: int | None) -> float | None:
        if numerator is None or denominator in {None, 0}:
            return None
        return float(numerator) / float(denominator)

    label_rate = _rate(label_positive_exposures, label_impressions)
    reporting_rate = _rate(reporting_soft_clicks, reporting_soft_impressions)
    return {
        "status": status,
        "is_gate": False,
        "reference_date": reference_date,
        "reporting_table": reporting_table,
        "detail": detail,
        "comparison_rule": (
            "Directional sanity only: observed Banner events and reporting "
            "SoftImpressions/SoftClicks have different denominators and must "
            "not be asserted equal."
        ),
        "observed_event_labels": {
            "impressions": int(label_impressions),
            "positive_exposures": int(label_positive_exposures),
            "click_rate": label_rate,
        },
        "reporting_soft_metrics": {
            "impressions": reporting_soft_impressions,
            "clicks": reporting_soft_clicks,
            "click_rate": reporting_rate,
        },
        "directional_click_rate_difference": (
            None
            if label_rate is None or reporting_rate is None
            else label_rate - reporting_rate
        ),
    }


def _bounded_group_rows(
    grouped,
    *,
    count_column: str,
    dimensions: Sequence[str],
    max_groups: int,
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    total_groups = grouped.count()
    ordering = [F.desc(count_column)] + [F.asc(column) for column in dimensions]
    rows = [
        row.asDict(recursive=True)
        for row in grouped.orderBy(*ordering).limit(max_groups).collect()
    ]
    return bounded_evidence_section(
        rows,
        total_groups=total_groups,
        max_groups=max_groups,
    )


def _assignment_exclusion_expr(
    measurement_column: str,
    assigned_column: str,
    treatment_column: str,
):
    from pyspark.sql import functions as F

    measurement = F.trim(F.col(measurement_column).cast("string"))
    assigned = F.trim(F.col(assigned_column).cast("string"))
    treatment = F.lower(F.trim(F.col(treatment_column).cast("string")))
    no_ad = (measurement.isin("NoAd", "NoAds", "NoAdFound")) | (
        assigned.isin("NoAd", "NoAds", "NoAdFound")
    )
    suppressed = (measurement == "AdSuppressed") | (
        assigned == "AdSuppressed"
    ) | (treatment == "adsuppressed")
    unresolved = (
        measurement.isNull()
        | assigned.isNull()
        | (~measurement.rlike(r"^P\d+_C\d+"))
        | (~assigned.rlike(r"^P\d+_C\d+"))
    )
    return (
        F.when(no_ad, F.lit("NO_AD"))
        .when(suppressed, F.lit("SUPPRESSED"))
        .when(treatment == "control", F.lit("CONTROL"))
        .when(
            treatment.isNull()
            | (~treatment.isin(*SHOPPING_BAG_SERVED_TREATMENTS)),
            F.lit("UNKNOWN_TREATMENT"),
        )
        .when(unresolved, F.lit("UNRESOLVED_ADVERT"))
    )


def _assignment_match_priority(
    observed_column: str,
    measurement_column: str,
    assigned_column: str,
):
    from pyspark.sql import functions as F

    observed = F.col(observed_column)
    measurement = F.col(measurement_column)
    assigned = F.col(assigned_column)
    return (
        F.when(
            observed == _normalise_observed_advert_id(measurement),
            F.lit(0),
        )
        .when(
            observed == _normalise_observed_advert_id(assigned),
            F.lit(1),
        )
        .when(
            (_campaign_key(observed) != "")
            & (
                (_campaign_key(observed) == _campaign_key(measurement))
                | (_campaign_key(observed) == _campaign_key(assigned))
            ),
            F.lit(2),
        )
    )


def _session_outcomes(sources: Mapping[str, Any], reference_date: str, label_end: str):
    from pyspark.sql import functions as F

    start = F.lit(reference_date).cast("date")
    end = F.lit(label_end).cast("date")
    rpid_lookup = (
        sources["rpid_accounts"]
        .select(
            F.col("roamingprofileid").cast("string").alias("rpid"),
            F.col("account_number").cast("string").alias(
                "mapped_account_number"
            ),
        )
        .where(F.col("rpid").isNotNull())
        .dropDuplicates(["rpid", "mapped_account_number"])
    )

    def _dated(frame):
        return frame.where(F.col("date").cast("date").between(start, end))

    candidates = _normalise_label_sessions(
        _dated(sources["web_sessions"]),
        rpid_lookup,
        "WEB",
        include_excluded=True,
    ).unionByName(
        _normalise_label_sessions(
            _dated(sources["app_sessions"]),
            rpid_lookup,
            "APP",
            include_excluded=True,
        )
    )
    return (
        candidates.where(F.col("session_id").isNotNull())
        .groupBy("platform", "session_date", "session_id")
        .agg(
            F.countDistinct(
                F.when(
                    F.col("session_exclusion_reason").isNull(),
                    F.col("account_number"),
                )
            ).alias("eligible_account_count"),
            F.min(
                F.when(
                    F.col("session_exclusion_reason").isNull(),
                    F.col("account_number"),
                )
            ).alias("account_number"),
            F.collect_set(
                F.coalesce("session_exclusion_reason", F.lit(""))
            ).alias("candidate_reasons"),
        )
        .withColumn(
            "session_outcome",
            F.when(
                F.col("eligible_account_count") > 1,
                F.lit("AMBIGUOUS_ACCOUNT"),
            )
            .when(F.col("eligible_account_count") == 1, F.lit("MAPPED"))
            .when(
                F.array_contains("candidate_reasons", "PRE_REFRESH"),
                F.lit("PRE_REFRESH"),
            )
            .when(
                F.array_contains("candidate_reasons", "UNMAPPED_ACCOUNT"),
                F.lit("UNMAPPED_ACCOUNT"),
            )
            .when(
                F.array_contains("candidate_reasons", "MISSING_SESSION_START"),
                F.lit("MISSING_SESSION_START"),
            )
            .otherwise(F.lit("UNMAPPED_SESSION")),
        )
        .select(
            "platform",
            "session_date",
            "session_id",
            "account_number",
            "session_outcome",
        )
    )


def _assignment_candidates(sources: Mapping[str, Any], reference_date: str):
    return _shopping_bag_v1_assignments(
        sources["v1_assignments"],
        sources["v1_control_sheet"],
        sources["v1_multipage_locations"],
        reference_date,
        include_ineligible=True,
    ).unionByName(
        _shopping_bag_v2_assignments(
            sources["v2_assignments"],
            sources["v2_control_sheet"],
            reference_date,
            include_ineligible=True,
        )
    )


def collect_shopping_bag_label_evidence(
    *,
    sources: Mapping[str, Any],
    raw_events,
    observed_labels,
    reference_date: str,
    label_end: str,
    source_watermarks: Mapping[str, Any],
    max_groups: int = DEFAULT_MAX_EVIDENCE_GROUPS,
) -> dict[str, Any]:
    """Collect a bounded funnel from telemetry through accepted labels."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    reference_date_lit = F.lit(reference_date).cast("date")
    session_outcomes = _session_outcomes(
        sources,
        reference_date,
        label_end,
    ).cache()
    events_with_session = (
        raw_events.alias("event")
        .join(
            session_outcomes.alias("session"),
            (F.col("event.platform") == F.col("session.platform"))
            & (F.col("event.event_date") == F.col("session.session_date"))
            & (F.col("event.session_id") == F.col("session.session_id")),
            "left",
        )
        .select(
            "event.*",
            F.col("session.account_number"),
            F.coalesce(
                F.col("session.session_outcome"),
                F.lit("UNMAPPED_SESSION"),
            ).alias("session_outcome"),
        )
        .cache()
    )
    mapped_events = events_with_session.where(
        F.col("session_outcome") == "MAPPED"
    )
    mapped_impressions = mapped_events.where(
        (F.col("action") == "Banner Impression - Next Ads")
        & (F.col("event_date") == reference_date_lit)
    ).cache()

    assignments = (
        _assignment_candidates(sources, reference_date)
        .withColumn(
            "assignment_exclusion_reason",
            _assignment_exclusion_expr(
                "measurement_advert_id",
                "assigned_advert_id",
                "treatment",
            ),
        )
        .cache()
    )
    joined = (
        mapped_impressions.alias("imp")
        .join(
            assignments.alias("asg"),
            (F.col("imp.platform") == F.col("asg.platform"))
            & (F.col("imp.event_route") == F.col("asg.route"))
            & (F.col("imp.account_number") == F.col("asg.account_number"))
            & (F.col("imp.event_date") == F.col("asg.session_date"))
            & (
                (F.col("imp.platform") == "WEB")
                | (
                    F.col("imp.event_cms_page_id")
                    == F.col("asg.cms_page_id")
                )
            ),
            "left",
        )
        .select(
            "imp.*",
            F.col("asg.account_number").alias("assignment_account_number"),
            F.col("asg.location").alias("assignment_location"),
            F.col("asg.cms_page_id").alias("assignment_cms_page_id"),
            F.col("asg.treatment").alias("assignment_treatment"),
            F.col("asg.measurement_advert_id").alias(
                "assignment_measurement_advert_id"
            ),
            F.col("asg.assigned_advert_id").alias(
                "assignment_assigned_advert_id"
            ),
            F.col("asg.assignment_exclusion_reason"),
        )
        .withColumn(
            "assignment_match_priority",
            F.when(
                F.col("assignment_exclusion_reason").isNull(),
                _assignment_match_priority(
                    "normalized_observed_advert_id",
                    "assignment_measurement_advert_id",
                    "assignment_assigned_advert_id",
                ),
            ),
        )
        .cache()
    )
    exclusion_priority = (
        F.when(F.col("assignment_exclusion_reason") == "CONTROL", 0)
        .when(F.col("assignment_exclusion_reason") == "NO_AD", 1)
        .when(F.col("assignment_exclusion_reason") == "SUPPRESSED", 2)
        .when(F.col("assignment_exclusion_reason") == "UNKNOWN_TREATMENT", 3)
        .when(F.col("assignment_exclusion_reason") == "UNRESOLVED_ADVERT", 4)
        .otherwise(99)
    )
    candidate_summary = joined.groupBy("raw_event_id").agg(
        F.sum(
            F.when(F.col("assignment_account_number").isNotNull(), 1).otherwise(0)
        ).alias("assignment_candidate_count"),
        F.sum(
            F.when(
                F.col("assignment_account_number").isNotNull()
                & F.col("assignment_exclusion_reason").isNull(),
                1,
            ).otherwise(0)
        ).alias("eligible_assignment_count"),
        F.sum(
            F.when(F.col("assignment_match_priority").isNotNull(), 1).otherwise(0)
        ).alias("advert_match_count"),
        F.min(
            F.struct(
                exclusion_priority.alias("priority"),
                F.coalesce(
                    "assignment_exclusion_reason",
                    F.lit("INELIGIBLE_ASSIGNMENT"),
                ).alias("reason"),
            )
        ).alias("first_exclusion"),
    )
    best_matches = (
        joined.where(F.col("assignment_match_priority").isNotNull())
        .withColumn(
            "best_priority",
            F.min("assignment_match_priority").over(
                Window.partitionBy("raw_event_id")
            ),
        )
        .where(F.col("assignment_match_priority") == F.col("best_priority"))
        .groupBy("raw_event_id")
        .agg(F.count(F.lit(1)).alias("best_match_count"))
    )
    accepted = observed_labels.select(
        F.col("impression_event_id").alias("accepted_event_id")
    ).distinct()
    assignment_outcomes = (
        mapped_impressions.join(candidate_summary, "raw_event_id", "left")
        .join(best_matches, "raw_event_id", "left")
        .join(
            accepted,
            F.col("raw_event_id") == F.col("accepted_event_id"),
            "left",
        )
        .withColumn(
            "assignment_outcome",
            F.when(F.col("accepted_event_id").isNotNull(), F.lit("ACCEPTED"))
            .when(
                F.coalesce("assignment_candidate_count", F.lit(0)) == 0,
                F.lit("NO_ROUTE_ASSIGNMENT"),
            )
            .when(
                F.coalesce("eligible_assignment_count", F.lit(0)) == 0,
                F.col("first_exclusion.reason"),
            )
            .when(
                F.coalesce("advert_match_count", F.lit(0)) == 0,
                F.lit("ADVERT_NOT_MATCHED"),
            )
            .when(
                F.coalesce("best_match_count", F.lit(0)) > 1,
                F.lit("AMBIGUOUS_MATCH"),
            )
            .otherwise(F.lit("UNRESOLVED_MATCH")),
        )
        .cache()
    )

    raw_totals = raw_events.agg(
        F.countDistinct(
            F.when(
                (F.col("action") == "Banner Impression - Next Ads")
                & (F.col("event_date") == reference_date_lit),
                F.col("raw_event_id"),
            )
        ).alias("raw_impressions"),
        F.countDistinct(
            F.when(
                F.col("action") == "Banner Click - Next Ads",
                F.col("raw_event_id"),
            )
        ).alias("raw_clicks"),
    ).first()
    session_totals = events_with_session.agg(
        F.countDistinct(
            F.when(
                (F.col("session_outcome") == "MAPPED")
                & (F.col("action") == "Banner Impression - Next Ads")
                & (F.col("event_date") == reference_date_lit),
                F.col("raw_event_id"),
            )
        ).alias("mapped_impressions"),
        F.countDistinct(
            F.when(
                (F.col("session_outcome") == "MAPPED")
                & (F.col("action") == "Banner Click - Next Ads"),
                F.col("raw_event_id"),
            )
        ).alias("mapped_clicks"),
    ).first()
    assignment_totals = assignment_outcomes.agg(
        F.countDistinct(
            F.when(
                F.col("assignment_outcome") == "ACCEPTED",
                F.col("raw_event_id"),
            )
        ).alias("accepted_exposures")
    ).first()
    label_totals = observed_labels.agg(
        F.count(F.lit(1)).alias("mature_label_rows"),
        F.countDistinct(
            "exposure_id",
            "label_horizon_days",
            "exposure_timestamp",
        ).alias("distinct_label_keys"),
        F.sum("clicked").alias("positive_labels"),
        F.sum("click_count").alias("attributed_clicks"),
        F.sum(
            F.when(F.col("label_horizon_days") == 0, F.col("clicked")).otherwise(0)
        ).alias("same_session_positive_labels"),
        F.sum(
            F.when(
                ~F.lower(F.trim("treatment")).isin(
                    *SHOPPING_BAG_SERVED_TREATMENTS
                ),
                1,
            ).otherwise(0)
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
                    | (
                        F.lower(F.trim("event_cms_page_id"))
                        != F.lower(F.trim("assignment_cms_page_id"))
                    )
                ),
                1,
            ).otherwise(0)
        ).alias("app_rows_without_exact_cms_match"),
        F.sum(
            F.when(~F.col("label_is_mature"), 1).otherwise(0)
        ).alias("immature_label_rows"),
    ).first()
    funnel = [
        {"stage": "RAW_TAGGED_IMPRESSIONS", "count": int(raw_totals["raw_impressions"] or 0)},
        {"stage": "RAW_TAGGED_CLICKS", "count": int(raw_totals["raw_clicks"] or 0)},
        {"stage": "SESSION_MAPPED_IMPRESSIONS", "count": int(session_totals["mapped_impressions"] or 0)},
        {"stage": "SESSION_MAPPED_CLICKS", "count": int(session_totals["mapped_clicks"] or 0)},
        {"stage": "ACCEPTED_EXPOSURES", "count": int(assignment_totals["accepted_exposures"] or 0)},
        {"stage": "MATURE_LABEL_ROWS", "count": int(label_totals["mature_label_rows"] or 0)},
        {"stage": "ATTRIBUTED_CLICKS", "count": int(label_totals["attributed_clicks"] or 0)},
        {"stage": "POSITIVE_LABELS", "count": int(label_totals["positive_labels"] or 0)},
        {"stage": "SAME_SESSION_POSITIVE_LABELS", "count": int(label_totals["same_session_positive_labels"] or 0)},
    ]

    raw_breakdown_grouped = raw_events.groupBy(
        "event_route", "platform", "action"
    ).agg(F.countDistinct("raw_event_id").alias("tagged_events"))
    session_breakdown_grouped = events_with_session.groupBy(
        "event_route", "platform", "action", "session_outcome"
    ).agg(F.countDistinct("raw_event_id").alias("tagged_events"))
    assignment_breakdown_grouped = assignment_outcomes.groupBy(
        "event_route", "platform", "assignment_outcome"
    ).agg(F.countDistinct("raw_event_id").alias("impressions"))
    assignment_source_exclusions = (
        assignments.where(F.col("assignment_exclusion_reason").isNotNull())
        .groupBy(
            "route",
            "platform",
            "location",
            "cms_page_id",
            "treatment",
            "assignment_exclusion_reason",
        )
        .agg(F.count(F.lit(1)).alias("assignment_rows"))
    )
    accepted_breakdown_grouped = observed_labels.groupBy(
        "route",
        "platform",
        "location",
        "event_cms_page_id",
        "assignment_cms_page_id",
        "treatment",
        "exposure_match_type",
        "label_horizon_days",
    ).agg(
        F.countDistinct("exposure_id").alias("accepted_exposures"),
        F.sum("click_count").alias("attributed_clicks"),
        F.sum("clicked").alias("positive_labels"),
    )

    try:
        return {
            "contract": EVIDENCE_CONTRACT,
            "reference_date": reference_date,
            "label_end": label_end,
            "source_watermarks": _json_safe(dict(source_watermarks)),
            "funnel": funnel,
            "quality_checks": {
                "label_rows": int(label_totals["mature_label_rows"] or 0),
                "distinct_label_keys": int(
                    label_totals["distinct_label_keys"] or 0
                ),
                "duplicate_label_key_rows": int(
                    (label_totals["mature_label_rows"] or 0)
                    - (label_totals["distinct_label_keys"] or 0)
                ),
                "invalid_treatment_rows": int(
                    label_totals["invalid_treatment_rows"] or 0
                ),
                "click_not_after_exposure_rows": int(
                    label_totals["click_not_after_exposure_rows"] or 0
                ),
                "app_rows_without_exact_cms_match": int(
                    label_totals["app_rows_without_exact_cms_match"] or 0
                ),
                "immature_label_rows": int(
                    label_totals["immature_label_rows"] or 0
                ),
            },
            "raw_event_breakdown": _bounded_group_rows(
                raw_breakdown_grouped,
                count_column="tagged_events",
                dimensions=("event_route", "platform", "action"),
                max_groups=max_groups,
            ),
            "session_mapping_breakdown": _bounded_group_rows(
                session_breakdown_grouped,
                count_column="tagged_events",
                dimensions=(
                    "event_route",
                    "platform",
                    "action",
                    "session_outcome",
                ),
                max_groups=max_groups,
            ),
            "assignment_match_breakdown": _bounded_group_rows(
                assignment_breakdown_grouped,
                count_column="impressions",
                dimensions=(
                    "event_route",
                    "platform",
                    "assignment_outcome",
                ),
                max_groups=max_groups,
            ),
            "assignment_source_exclusions": _bounded_group_rows(
                assignment_source_exclusions,
                count_column="assignment_rows",
                dimensions=(
                    "route",
                    "platform",
                    "location",
                    "cms_page_id",
                    "treatment",
                    "assignment_exclusion_reason",
                ),
                max_groups=max_groups,
            ),
            "accepted_label_breakdown": _bounded_group_rows(
                accepted_breakdown_grouped,
                count_column="accepted_exposures",
                dimensions=(
                    "route",
                    "platform",
                    "location",
                    "event_cms_page_id",
                    "assignment_cms_page_id",
                    "treatment",
                    "exposure_match_type",
                    "label_horizon_days",
                ),
                max_groups=max_groups,
            ),
        }
    finally:
        assignment_outcomes.unpersist()
        joined.unpersist()
        assignments.unpersist()
        mapped_impressions.unpersist()
        events_with_session.unpersist()
        session_outcomes.unpersist()


def collect_reporting_sanity(
    observed_labels,
    reporting_results,
    *,
    reference_date: str,
    reporting_table: str,
) -> dict[str, Any]:
    """Compare event labels with reporting soft metrics without gating output."""
    from pyspark.sql import functions as F

    label_row = (
        observed_labels.where(F.col("label_horizon_days") == 0)
        .agg(
            F.sum("impression_count").alias("impressions"),
            F.sum("clicked").alias("positive_exposures"),
        )
        .first()
    )
    label_impressions = int(label_row["impressions"] or 0)
    label_positives = int(label_row["positive_exposures"] or 0)
    if reporting_results is None:
        return reporting_sanity_payload(
            reference_date=reference_date,
            label_impressions=label_impressions,
            label_positive_exposures=label_positives,
            reporting_soft_impressions=None,
            reporting_soft_clicks=None,
            reporting_table=reporting_table,
            status="UNAVAILABLE",
            detail="Reporting table was not available to the manual DEV run.",
        )

    required = {"SessionDate", "LocationSet", "SoftImpressions", "SoftClicks"}
    missing = sorted(required.difference(reporting_results.columns))
    if missing:
        return reporting_sanity_payload(
            reference_date=reference_date,
            label_impressions=label_impressions,
            label_positive_exposures=label_positives,
            reporting_soft_impressions=None,
            reporting_soft_clicks=None,
            reporting_table=reporting_table,
            status="INCOMPATIBLE",
            detail="Missing reporting columns: " + ", ".join(missing),
        )

    locations = [
        str(row["location"])
        for row in observed_labels.where(F.col("label_horizon_days") == 0)
        .select("location")
        .where(F.col("location").isNotNull())
        .distinct()
        .limit(50)
        .collect()
    ]
    if not locations:
        location_pattern = r"a^"
    else:
        alternatives = "|".join(re.escape(value) for value in locations)
        location_pattern = rf"(^|\+)({alternatives})(\+|$)"
    reporting_row = (
        reporting_results.where(
            F.col("SessionDate").cast("date")
            == F.lit(reference_date).cast("date")
        )
        .where(F.col("LocationSet").rlike(location_pattern))
        .agg(
            F.sum("SoftImpressions").alias("soft_impressions"),
            F.sum("SoftClicks").alias("soft_clicks"),
        )
        .first()
    )
    soft_impressions = int(reporting_row["soft_impressions"] or 0)
    soft_clicks = int(reporting_row["soft_clicks"] or 0)
    return reporting_sanity_payload(
        reference_date=reference_date,
        label_impressions=label_impressions,
        label_positive_exposures=label_positives,
        reporting_soft_impressions=soft_impressions,
        reporting_soft_clicks=soft_clicks,
        reporting_table=reporting_table,
        status="AVAILABLE" if soft_impressions else "EMPTY",
        detail=(
            "Reporting rows were filtered to label locations; differences are "
            "expected because reporting uses soft page/URL measurement."
        ),
    )


__all__ = [
    "DEFAULT_MAX_EVIDENCE_GROUPS",
    "EVIDENCE_CONTRACT",
    "bounded_evidence_section",
    "collect_reporting_sanity",
    "collect_shopping_bag_label_evidence",
    "reporting_sanity_payload",
]

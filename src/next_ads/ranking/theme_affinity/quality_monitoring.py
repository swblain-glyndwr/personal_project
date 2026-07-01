from __future__ import annotations

from databricks.sdk.service.catalog import MonitorMetric, MonitorMetricType


_AGGREGATE = MonitorMetricType.CUSTOM_METRIC_TYPE_AGGREGATE


THEME_AFFINITY_RANKED_PROFILE = "theme_affinity_ranked"
NO_CUSTOM_METRICS_PROFILE = "none"


def custom_metrics_for_profile(profile: str | None) -> tuple[MonitorMetric, ...]:
    normalized = (profile or THEME_AFFINITY_RANKED_PROFILE).strip().lower()
    if normalized in {"", THEME_AFFINITY_RANKED_PROFILE}:
        return theme_affinity_ranked_custom_metrics()
    if normalized == NO_CUSTOM_METRICS_PROFILE:
        return ()
    raise ValueError(
        "custom_metrics_profile must be one of "
        f"{THEME_AFFINITY_RANKED_PROFILE!r} or {NO_CUSTOM_METRICS_PROFILE!r}, "
        f"got {profile!r}"
    )


def theme_affinity_ranked_custom_metrics() -> tuple[MonitorMetric, ...]:
    return (
        _aggregate_metric(
            name="account_count",
            input_column="account_number",
            definition=(
                "CAST(approx_count_distinct({{input_column}}) AS DOUBLE)"
            ),
        ),
        _aggregate_metric(
            name="avg_candidates_per_account",
            input_column="account_number",
            definition=(
                "count(*) / "
                "CAST(approx_count_distinct({{input_column}}) AS DOUBLE)"
            ),
        ),
        _aggregate_metric(
            name="avg_simple_rules_rank",
            input_column="simple_rules_rank",
            definition="avg(CAST({{input_column}} AS DOUBLE))",
        ),
        _aggregate_metric(
            name="p95_simple_rules_rank",
            input_column="simple_rules_rank",
            definition=(
                "percentile_approx(CAST({{input_column}} AS DOUBLE), 0.95)"
            ),
        ),
        _aggregate_metric(
            name="top_10_candidate_share",
            input_column="simple_rules_rank",
            definition=(
                "avg(CASE WHEN {{input_column}} <= 10 "
                "THEN 1.0 ELSE 0.0 END)"
            ),
        ),
        _aggregate_metric(
            name="retrieval_present_rate",
            input_column="num_retrieval_methods",
            definition=(
                "avg(CASE WHEN {{input_column}} > 0 "
                "THEN 1.0 ELSE 0.0 END)"
            ),
        ),
        _aggregate_metric(
            name="avg_retrieval_methods",
            input_column="num_retrieval_methods",
            definition="avg(CAST({{input_column}} AS DOUBLE))",
        ),
        _aggregate_metric(
            name="theme_fallback_rate",
            input_column="rules_rank_source",
            definition=(
                "avg(CASE WHEN {{input_column}} = 'theme' "
                "THEN 1.0 ELSE 0.0 END)"
            ),
        ),
        _aggregate_metric(
            name="unknown_repurchase_stage_rate",
            input_column="repurchase_stage",
            definition=(
                "avg(CASE WHEN {{input_column}} = 'unknown' "
                "THEN 1.0 ELSE 0.0 END)"
            ),
        ),
        _aggregate_metric(
            name="unknown_gma_rate",
            input_column="GmaName",
            definition=(
                "avg(CASE WHEN {{input_column}} = 'unknown' "
                "THEN 1.0 ELSE 0.0 END)"
            ),
        ),
        _aggregate_metric(
            name="views_recency_missing_rate",
            input_column="views_behavior__recency",
            definition=(
                "avg(CASE WHEN {{input_column}} >= 9999 "
                "THEN 1.0 ELSE 0.0 END)"
            ),
        ),
    )


def _aggregate_metric(
    *,
    name: str,
    input_column: str,
    definition: str,
) -> MonitorMetric:
    return MonitorMetric(
        name=name,
        type=_AGGREGATE,
        input_columns=[input_column],
        output_data_type="double",
        definition=definition,
    )

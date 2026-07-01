import pytest

from next_ads.ranking.theme_affinity.quality_monitoring import (
    custom_metrics_for_profile,
)


def test_theme_affinity_ranked_custom_metrics_profile_is_default():
    metrics = custom_metrics_for_profile(None)

    assert [metric.name for metric in metrics] == [
        "account_count",
        "avg_candidates_per_account",
        "avg_simple_rules_rank",
        "p95_simple_rules_rank",
        "top_10_candidate_share",
        "retrieval_present_rate",
        "avg_retrieval_methods",
        "theme_fallback_rate",
        "unknown_repurchase_stage_rate",
        "unknown_gma_rate",
        "views_recency_missing_rate",
    ]
    assert all(metric.output_data_type == "double" for metric in metrics)
    assert all(len(metric.input_columns) == 1 for metric in metrics)


def test_theme_affinity_ranked_custom_metrics_cover_expected_columns():
    metrics = custom_metrics_for_profile("theme_affinity_ranked")

    assert {metric.input_columns[0] for metric in metrics} == {
        "account_number",
        "simple_rules_rank",
        "num_retrieval_methods",
        "rules_rank_source",
        "repurchase_stage",
        "GmaName",
        "views_behavior__recency",
    }
    retrieval_metric = next(
        metric for metric in metrics if metric.name == "retrieval_present_rate"
    )
    assert "CASE WHEN {{input_column}} > 0" in retrieval_metric.definition


def test_none_custom_metrics_profile_disables_repo_metrics():
    assert custom_metrics_for_profile("none") == ()


def test_unknown_custom_metrics_profile_is_rejected():
    with pytest.raises(ValueError, match="custom_metrics_profile"):
        custom_metrics_for_profile("ad_hoc")

import pandas as pd

from next_ads.ml.lifecycle.drift import (
    DriftMetric,
    assess_drift,
    categorical_total_variation,
    drift_metrics,
    numeric_psi,
    to_mlflow_metrics,
)


def test_numeric_psi_detects_distribution_shift():
    expected = pd.Series([1, 1, 2, 2, 3, 3, 4, 4])
    actual = pd.Series([1, 4, 4, 4, 5, 5, 6, 6])

    assert numeric_psi(expected, actual, bins=4) > 0


def test_categorical_total_variation_detects_category_shift():
    expected = pd.Series(["a", "a", "b", "b"])
    actual = pd.Series(["a", "c", "c", "c"])

    assert categorical_total_variation(expected, actual) > 0


def test_drift_metrics_include_row_counts_missing_rates_and_feature_metrics():
    baseline = pd.DataFrame(
        {
            "score": [0.1, 0.2, None, 0.4],
            "segment": ["new", "new", "returning", None],
        }
    )
    candidate = pd.DataFrame(
        {
            "score": [0.9, 0.8, 0.7, None],
            "segment": ["new", "lapsed", "lapsed", "lapsed"],
        }
    )

    metrics = drift_metrics(
        baseline=baseline,
        candidate=candidate,
        numeric_cols=["score"],
        categorical_cols=["segment"],
    )
    metric_names = {metric.name for metric in metrics}

    assert "baseline_rows" in metric_names
    assert "candidate_rows" in metric_names
    assert "feature.score.missing_rate.baseline" in metric_names
    assert "feature.score.psi" in metric_names
    assert "feature.segment.total_variation" in metric_names
    assert "feature.segment.js_divergence" in metric_names


def test_drift_assessment_warns_and_blocks_promotion_at_thresholds():
    warning = assess_drift(
        [DriftMetric("feature.score.psi", 0.11)],
        numeric_psi_warn_threshold=0.1,
        numeric_psi_fail_threshold=0.25,
    )
    failure = assess_drift(
        [DriftMetric("feature.score.psi", 0.3)],
        numeric_psi_warn_threshold=0.1,
        numeric_psi_fail_threshold=0.25,
    )

    assert warning.status == "warn"
    assert warning.retrain_recommended is True
    assert warning.promotion_blocked is False
    assert failure.status == "fail"
    assert failure.retrain_recommended is True
    assert failure.promotion_blocked is True


def test_to_mlflow_metrics_prefixes_metric_names():
    metrics = to_mlflow_metrics([DriftMetric("feature.score.psi", 0.12)])

    assert metrics == {"drift.feature.score.psi": 0.12}

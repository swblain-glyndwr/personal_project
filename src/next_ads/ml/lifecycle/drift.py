from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


EPSILON = 1e-9


@dataclass(frozen=True)
class DriftMetric:
    name: str
    value: float


@dataclass(frozen=True)
class DriftAssessment:
    status: str
    retrain_recommended: bool
    promotion_blocked: bool
    reasons: tuple[str, ...]


def numeric_psi(expected, actual, bins: int = 10) -> float:
    expected = _clean_numeric(expected)
    actual = _clean_numeric(actual)
    if expected.size == 0 or actual.size == 0:
        return 0.0

    quantiles = np.linspace(0, 1, bins + 1)
    edges = np.unique(np.quantile(expected, quantiles))
    if edges.size < 2:
        return 0.0
    edges[0] = -np.inf
    edges[-1] = np.inf

    expected_hist, _ = np.histogram(expected, bins=edges)
    actual_hist, _ = np.histogram(actual, bins=edges)
    expected_pct = _safe_distribution(expected_hist)
    actual_pct = _safe_distribution(actual_hist)
    return float(
        np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
    )


def categorical_total_variation(expected, actual) -> float:
    expected_dist, actual_dist = _aligned_categorical_distributions(
        expected,
        actual,
    )
    return float(0.5 * np.sum(np.abs(actual_dist - expected_dist)))


def categorical_js_divergence(expected, actual) -> float:
    expected_dist, actual_dist = _aligned_categorical_distributions(
        expected,
        actual,
    )
    midpoint = (expected_dist + actual_dist) / 2
    return float(
        0.5 * _kl_divergence(expected_dist, midpoint)
        + 0.5 * _kl_divergence(actual_dist, midpoint)
    )


def drift_metrics(
    baseline: pd.DataFrame,
    candidate: pd.DataFrame,
    numeric_cols: list[str] | tuple[str, ...] = (),
    categorical_cols: list[str] | tuple[str, ...] = (),
    prediction_col: str | None = None,
) -> list[DriftMetric]:
    metrics = [
        DriftMetric("baseline_rows", float(len(baseline))),
        DriftMetric("candidate_rows", float(len(candidate))),
    ]

    for column in numeric_cols:
        if column not in baseline or column not in candidate:
            continue
        metrics.extend(
            [
                DriftMetric(
                    f"feature.{column}.missing_rate.baseline",
                    _missing_rate(baseline[column]),
                ),
                DriftMetric(
                    f"feature.{column}.missing_rate.candidate",
                    _missing_rate(candidate[column]),
                ),
                DriftMetric(
                    f"feature.{column}.psi",
                    numeric_psi(baseline[column], candidate[column]),
                ),
            ]
        )

    for column in categorical_cols:
        if column not in baseline or column not in candidate:
            continue
        metrics.extend(
            [
                DriftMetric(
                    f"feature.{column}.missing_rate.baseline",
                    _missing_rate(baseline[column]),
                ),
                DriftMetric(
                    f"feature.{column}.missing_rate.candidate",
                    _missing_rate(candidate[column]),
                ),
                DriftMetric(
                    f"feature.{column}.total_variation",
                    categorical_total_variation(
                        baseline[column],
                        candidate[column],
                    ),
                ),
                DriftMetric(
                    f"feature.{column}.js_divergence",
                    categorical_js_divergence(
                        baseline[column],
                        candidate[column],
                    ),
                ),
            ]
        )

    if prediction_col and prediction_col in baseline and prediction_col in candidate:
        metrics.append(
            DriftMetric(
                f"prediction.{prediction_col}.psi",
                numeric_psi(baseline[prediction_col], candidate[prediction_col]),
            )
        )

    return metrics


def assess_drift(
    metrics: list[DriftMetric],
    numeric_psi_warn_threshold: float = 0.1,
    numeric_psi_fail_threshold: float = 0.25,
    categorical_warn_threshold: float = 0.1,
    categorical_fail_threshold: float = 0.25,
) -> DriftAssessment:
    warn_reasons = []
    fail_reasons = []

    for metric in metrics:
        if metric.name.endswith(".psi"):
            _append_threshold_reason(
                metric,
                warn_reasons,
                fail_reasons,
                numeric_psi_warn_threshold,
                numeric_psi_fail_threshold,
            )
        elif metric.name.endswith(".total_variation") or metric.name.endswith(
            ".js_divergence"
        ):
            _append_threshold_reason(
                metric,
                warn_reasons,
                fail_reasons,
                categorical_warn_threshold,
                categorical_fail_threshold,
            )

    if fail_reasons:
        return DriftAssessment(
            status="fail",
            retrain_recommended=True,
            promotion_blocked=True,
            reasons=tuple(fail_reasons),
        )
    if warn_reasons:
        return DriftAssessment(
            status="warn",
            retrain_recommended=True,
            promotion_blocked=False,
            reasons=tuple(warn_reasons),
        )
    return DriftAssessment(
        status="pass",
        retrain_recommended=False,
        promotion_blocked=False,
        reasons=(),
    )


def to_mlflow_metrics(
    metrics: list[DriftMetric],
    prefix: str = "drift",
) -> dict[str, float]:
    return {f"{prefix}.{metric.name}": float(metric.value) for metric in metrics}


def _clean_numeric(values) -> np.ndarray:
    series = pd.Series(values)
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    return numeric.to_numpy(dtype=float)


def _safe_distribution(counts) -> np.ndarray:
    counts = np.asarray(counts, dtype=float) + EPSILON
    return counts / counts.sum()


def _aligned_categorical_distributions(expected, actual):
    expected_series = pd.Series(expected).fillna("<missing>").astype(str)
    actual_series = pd.Series(actual).fillna("<missing>").astype(str)
    categories = sorted(set(expected_series.unique()) | set(actual_series.unique()))
    expected_counts = expected_series.value_counts().reindex(categories, fill_value=0)
    actual_counts = actual_series.value_counts().reindex(categories, fill_value=0)
    return _safe_distribution(expected_counts), _safe_distribution(actual_counts)


def _kl_divergence(left, right) -> float:
    left = np.asarray(left, dtype=float) + EPSILON
    right = np.asarray(right, dtype=float) + EPSILON
    return float(np.sum(left * np.log(left / right)))


def _missing_rate(values) -> float:
    series = pd.Series(values)
    if len(series) == 0:
        return 0.0
    return float(series.isna().mean())


def _append_threshold_reason(
    metric: DriftMetric,
    warn_reasons: list[str],
    fail_reasons: list[str],
    warn_threshold: float,
    fail_threshold: float,
) -> None:
    reason = f"{metric.name}={metric.value:.6g}"
    if metric.value >= fail_threshold:
        fail_reasons.append(reason)
    elif metric.value >= warn_threshold:
        warn_reasons.append(reason)

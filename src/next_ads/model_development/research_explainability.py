"""Readable global explanations for supported research candidates."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import math
import re
from statistics import fmean, pstdev
from typing import Any, Callable, Iterable, Mapping, Sequence

from next_ads.model_development.research_failures import safe_failure_reason


COMPLETE = "COMPLETE"
FAILED = "FAILED"
_GENERIC_FEATURE = re.compile(r"^feature_[0-9]+$", re.IGNORECASE)
_XGBOOST_INDEX = re.compile(r"^f([0-9]+)$", re.IGNORECASE)


@dataclass(frozen=True)
class FeatureNameMapping:
    """Map one fitted vector position back to a Feature Store value."""

    vector_index: int
    source_column: str
    category: str | None = None

    def __post_init__(self) -> None:
        """Validate that this position has a readable source name."""
        source = self.source_column.strip()
        if self.vector_index < 0:
            raise ValueError("Feature vector index must not be negative")
        if not source or _GENERIC_FEATURE.fullmatch(source):
            raise ValueError(
                "Feature explanations require readable Feature Store names"
            )
        category = None if self.category is None else self.category.strip()
        if category is not None and not category:
            raise ValueError("Feature category must not be blank")
        if category is not None and len(category) > 128:
            raise ValueError("Feature category must not exceed 128 characters")
        object.__setattr__(self, "source_column", source)
        object.__setattr__(self, "category", category)

    @property
    def feature_name(self) -> str:
        """Return the human-readable encoded feature name."""
        if self.category is None:
            return self.source_column
        return f"{self.source_column}={self.category}"


@dataclass(frozen=True)
class GlobalExplanation:
    """One candidate's bounded, machine-readable global explanation."""

    status: str
    method: str
    features: tuple[dict[str, Any], ...] = ()
    reason: str | None = None

    def __post_init__(self) -> None:
        """Validate status-dependent explanation evidence."""
        if self.status not in {COMPLETE, FAILED}:
            raise ValueError("Explanation status must be COMPLETE or FAILED")
        if self.status == COMPLETE and not self.features:
            raise ValueError("Completed explanation has no feature evidence")
        if self.status == FAILED and not self.reason:
            raise ValueError("Failed explanation must retain its reason")
        validate_readable_explanation(self.features)

    def as_dict(self) -> dict[str, Any]:
        """Return a stable serialisable explanation payload."""
        return {
            "status": self.status,
            "method": self.method,
            "reason": self.reason,
            "features": [dict(row) for row in self.features],
        }


def explain_logistic_regression(
    fitted_model: Any,
    feature_mapping: Sequence[FeatureNameMapping],
) -> GlobalExplanation:
    """Explain a binary logistic model with sign, magnitude and odds ratio."""
    classifier = _classifier_stage(fitted_model)
    if not hasattr(classifier, "coefficients"):
        raise ValueError("Logistic model does not expose coefficients")
    values = _vector_values(classifier.coefficients)
    mappings = _ordered_mapping(feature_mapping, expected_size=len(values))
    rows = tuple(
        {
            "feature": mapping.feature_name,
            "source_column": mapping.source_column,
            "category": mapping.category,
            "vector_index": mapping.vector_index,
            "coefficient": float(coefficient),
            "signed_importance": float(coefficient),
            "absolute_importance": abs(float(coefficient)),
            "odds_ratio": math.exp(max(-50.0, min(50.0, float(coefficient)))),
        }
        for mapping, coefficient in zip(mappings, values)
    )
    return GlobalExplanation(
        status=COMPLETE,
        method="logistic_regression_coefficients",
        features=_sort_importance(rows),
    )


def explain_tree_importance(
    fitted_model: Any,
    feature_mapping: Sequence[FeatureNameMapping],
    *,
    method: str = "native_tree_importance",
) -> GlobalExplanation:
    """Explain a Spark random forest or GBT using native importance."""
    classifier = _classifier_stage(fitted_model)
    if not hasattr(classifier, "featureImportances"):
        raise ValueError("Tree model does not expose featureImportances")
    values = _vector_values(classifier.featureImportances)
    mappings = _ordered_mapping(feature_mapping, expected_size=len(values))
    rows = tuple(
        {
            "feature": mapping.feature_name,
            "source_column": mapping.source_column,
            "category": mapping.category,
            "vector_index": mapping.vector_index,
            "importance": float(importance),
            "absolute_importance": abs(float(importance)),
        }
        for mapping, importance in zip(mappings, values)
    )
    return GlobalExplanation(
        status=COMPLETE,
        method=method,
        features=_sort_importance(rows),
    )


def explain_xgboost(
    fitted_model: Any,
    feature_mapping: Sequence[FeatureNameMapping],
    *,
    contribution_frame: Any | None = None,
    contribution_summary: Sequence[Mapping[str, Any]] | None = None,
    contribution_column: str = "contributions",
    row_id_hash_column: str = "row_id_hash",
    max_contribution_rows: int = 1000,
) -> GlobalExplanation:
    """Explain XGBoost with gain and required bounded contributions."""
    classifier = _classifier_stage(fitted_model)
    mappings = _ordered_mapping(feature_mapping)
    gain_by_index = _xgboost_gain_by_index(classifier, mappings)
    if contribution_frame is not None and contribution_summary is not None:
        raise ValueError(
            "Provide either contribution_frame or contribution_summary"
        )
    if contribution_frame is not None:
        contribution_summary = aggregate_bounded_contributions(
            contribution_frame,
            mappings,
            contribution_column=contribution_column,
            row_id_hash_column=row_id_hash_column,
            max_rows=max_contribution_rows,
        )
    if not contribution_summary:
        raise ValueError(
            "XGBoost explanation requires bounded contribution evidence"
        )
    contributions = {
        int(row["vector_index"]): dict(row) for row in contribution_summary
    }
    expected_indexes = set(range(len(mappings)))
    actual_indexes = set(contributions)
    if actual_indexes != expected_indexes:
        raise ValueError(
            "XGBoost contribution indexes must cover every mapped feature: "
            f"missing={sorted(expected_indexes - actual_indexes)}, "
            f"unexpected={sorted(actual_indexes - expected_indexes)}"
        )
    contribution_rows = {
        int(row.get("rows", 0)) for row in contributions.values()
    }
    if len(contribution_rows) != 1 or next(iter(contribution_rows)) < 1:
        raise ValueError(
            "XGBoost contribution rows must be positive and consistent"
        )
    rows = []
    for mapping in mappings:
        contribution = contributions.get(mapping.vector_index, {})
        gain = float(gain_by_index.get(mapping.vector_index, 0.0))
        rows.append(
            {
                "feature": mapping.feature_name,
                "source_column": mapping.source_column,
                "category": mapping.category,
                "vector_index": mapping.vector_index,
                "gain_importance": gain,
                "mean_contribution": contribution.get("mean_contribution"),
                "mean_absolute_contribution": contribution.get(
                    "mean_absolute_contribution"
                ),
                "contribution_rows": contribution.get("rows", 0),
                "absolute_importance": contribution.get(
                    "mean_absolute_contribution",
                    gain,
                ),
            }
        )
    return GlobalExplanation(
        status=COMPLETE,
        method="xgboost_gain_and_bounded_contributions",
        features=_sort_importance(tuple(rows)),
    )


def aggregate_bounded_contributions(
    frame: Any,
    feature_mapping: Sequence[FeatureNameMapping],
    *,
    contribution_column: str = "contributions",
    row_id_hash_column: str = "row_id_hash",
    max_rows: int = 1000,
) -> list[dict[str, Any]]:
    """Aggregate contribution vectors without returning observation rows."""
    from pyspark.ml.functions import vector_to_array
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType

    if not 1 <= max_rows <= 10_000:
        raise ValueError("max_rows must be between 1 and 10000")
    missing = sorted(
        {contribution_column, row_id_hash_column}.difference(frame.columns)
    )
    if missing:
        raise ValueError(
            "Contribution evidence is missing: " + ", ".join(missing)
        )
    mappings = _ordered_mapping(feature_mapping)
    contribution_type = frame.schema[contribution_column].dataType
    if isinstance(contribution_type, ArrayType):
        contribution_values = F.col(contribution_column)
        if isinstance(contribution_type.elementType, ArrayType):
            invalid_outer_size = (
                frame.where(F.size(F.col(contribution_column)) != F.lit(1))
                .limit(1)
                .count()
            )
            if invalid_outer_size:
                raise ValueError(
                    "Binary XGBoost contribution output must contain one class"
                )
            contribution_values = F.col(contribution_column).getItem(0)
    else:
        contribution_values = vector_to_array(F.col(contribution_column))
    sampled = (
        frame.select(contribution_column, row_id_hash_column)
        .where(F.col(contribution_column).isNotNull())
        .orderBy(F.col(row_id_hash_column).cast("string").asc())
        .limit(max_rows)
        .select(
            F.posexplode(contribution_values).alias(
                "vector_index", "contribution"
            )
        )
        .where(F.col("vector_index") < F.lit(len(mappings)))
        .groupBy("vector_index")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.avg("contribution").alias("mean_contribution"),
            F.avg(F.abs(F.col("contribution"))).alias(
                "mean_absolute_contribution"
            ),
        )
        .orderBy("vector_index")
        .collect()
    )
    if len(sampled) > len(mappings):
        raise ValueError("Contribution aggregation exceeded feature count")
    result = [
        {
            "vector_index": int(row["vector_index"]),
            "feature": mappings[int(row["vector_index"])].feature_name,
            "rows": int(row["rows"]),
            "mean_contribution": float(row["mean_contribution"]),
            "mean_absolute_contribution": float(
                row["mean_absolute_contribution"]
            ),
        }
        for row in sampled
    ]
    expected_indexes = set(range(len(mappings)))
    actual_indexes = {row["vector_index"] for row in result}
    if actual_indexes != expected_indexes:
        raise ValueError(
            "Contribution aggregation did not cover every mapped feature"
        )
    row_counts = {row["rows"] for row in result}
    if len(row_counts) != 1 or next(iter(row_counts)) < 1:
        raise ValueError(
            "Contribution aggregation produced inconsistent sample counts"
        )
    return result


def deterministic_permutation_importance(
    feature_mapping: Sequence[FeatureNameMapping],
    *,
    baseline_metric: float,
    evaluate_permuted: Callable[[str, int], float],
    repeats: int = 3,
    seed: int = 1729,
    higher_is_better: bool = True,
) -> GlobalExplanation:
    """Provide a deterministic fallback for any other candidate family.

    ``evaluate_permuted`` owns the model-specific Spark scoring operation.  It
    receives a source feature column and a stable seed.  Only the resulting
    aggregate metric is retained here.
    """
    if not 1 <= repeats <= 20:
        raise ValueError("repeats must be between 1 and 20")
    mappings = _ordered_mapping(feature_mapping)
    by_source: dict[str, list[FeatureNameMapping]] = {}
    for mapping in mappings:
        by_source.setdefault(mapping.source_column, []).append(mapping)
    rows = []
    for source_column, encoded in sorted(by_source.items()):
        permuted_metrics = []
        for repeat in range(repeats):
            repeat_seed = _stable_seed(seed, source_column, repeat)
            permuted_metrics.append(
                float(evaluate_permuted(source_column, repeat_seed))
            )
        differences = [
            baseline_metric - value
            if higher_is_better
            else value - baseline_metric
            for value in permuted_metrics
        ]
        importance = fmean(differences)
        rows.append(
            {
                "feature": source_column,
                "source_column": source_column,
                "category": None,
                "encoded_positions": len(encoded),
                "repeats": repeats,
                "importance": importance,
                "absolute_importance": abs(importance),
                "importance_standard_deviation": pstdev(differences),
            }
        )
    return GlobalExplanation(
        status=COMPLETE,
        method="deterministic_permutation_importance",
        features=_sort_importance(tuple(rows)),
    )


def produce_global_explanation(
    candidate_plugin: str,
    fitted_model: Any,
    feature_mapping: Sequence[FeatureNameMapping],
    *,
    contribution_frame: Any | None = None,
    permutation_baseline_metric: float | None = None,
    permutation_evaluator: Callable[[str, int], float] | None = None,
    seed: int = 1729,
) -> GlobalExplanation:
    """Route a supported model family to its readable explanation."""
    plugin = candidate_plugin.casefold()
    if plugin == "spark_logistic_regression":
        return explain_logistic_regression(fitted_model, feature_mapping)
    if plugin == "spark_random_forest":
        return explain_tree_importance(
            fitted_model,
            feature_mapping,
            method="random_forest_native_importance",
        )
    if plugin == "spark_gradient_boosted_trees":
        return explain_tree_importance(
            fitted_model,
            feature_mapping,
            method="gradient_boosted_tree_native_importance",
        )
    if plugin == "spark_xgboost":
        return explain_xgboost(
            fitted_model,
            feature_mapping,
            contribution_frame=contribution_frame,
        )
    if permutation_evaluator is None or permutation_baseline_metric is None:
        raise ValueError(
            "Custom candidates require a deterministic permutation evaluator"
        )
    return deterministic_permutation_importance(
        feature_mapping,
        baseline_metric=permutation_baseline_metric,
        evaluate_permuted=permutation_evaluator,
        seed=seed,
    )


def failed_explanation(
    method: str, error: Exception | str
) -> GlobalExplanation:
    """Retain an explanation failure for selection gating and diagnosis."""
    reason = safe_failure_reason(error, stage=f"explanation_{method}")
    return GlobalExplanation(
        status=FAILED,
        method=method,
        reason=reason,
    )


def validate_readable_explanation(
    rows: Iterable[Mapping[str, Any]],
) -> None:
    """Reject anonymous fitted-vector names such as ``feature_42``."""
    for row in rows:
        name = str(row.get("feature", "")).strip()
        if not name or _GENERIC_FEATURE.fullmatch(name):
            raise ValueError(
                "Explanation output contains an unreadable feature name"
            )


def _ordered_mapping(
    mappings: Sequence[FeatureNameMapping],
    *,
    expected_size: int | None = None,
) -> tuple[FeatureNameMapping, ...]:
    ordered = tuple(sorted(mappings, key=lambda item: item.vector_index))
    if not ordered:
        raise ValueError("Feature explanation mapping must not be empty")
    expected_indexes = tuple(range(len(ordered)))
    found_indexes = tuple(mapping.vector_index for mapping in ordered)
    if found_indexes != expected_indexes:
        raise ValueError(
            "Feature explanation mapping must cover consecutive vector "
            f"positions: found={found_indexes}"
        )
    if expected_size is not None and len(ordered) != expected_size:
        raise ValueError(
            "Feature explanation mapping size does not match fitted vector: "
            f"mapping={len(ordered)}, fitted={expected_size}"
        )
    names = [mapping.feature_name for mapping in ordered]
    if len(names) != len(set(names)):
        raise ValueError("Feature explanation names must be unique")
    return ordered


def _classifier_stage(model: Any) -> Any:
    stages = tuple(getattr(model, "stages", ()))
    if not stages:
        return model
    for stage in reversed(stages):
        if any(
            hasattr(stage, attribute)
            for attribute in (
                "coefficients",
                "featureImportances",
                "get_feature_importances",
                "get_booster",
            )
        ):
            return stage
    return stages[-1]


def _vector_values(vector: Any) -> tuple[float, ...]:
    values = vector.toArray() if hasattr(vector, "toArray") else vector
    return tuple(float(value) for value in values)


def _xgboost_gain_by_index(
    classifier: Any,
    mappings: Sequence[FeatureNameMapping],
) -> dict[int, float]:
    importance: Mapping[Any, Any] | None = None
    if hasattr(classifier, "get_feature_importances"):
        try:
            importance = classifier.get_feature_importances(
                importance_type="gain"
            )
        except TypeError:
            importance = classifier.get_feature_importances("gain")
    if importance is None and hasattr(classifier, "get_booster"):
        importance = classifier.get_booster().get_score(importance_type="gain")
    if importance is None:
        raise ValueError("XGBoost model does not expose gain importance")
    by_name = {
        mapping.feature_name: mapping.vector_index for mapping in mappings
    }
    by_source = {
        mapping.source_column: mapping.vector_index for mapping in mappings
    }
    resolved: dict[int, float] = {}
    for raw_key, raw_value in importance.items():
        key = str(raw_key)
        index_match = _XGBOOST_INDEX.fullmatch(key)
        if isinstance(raw_key, int) or key.isdigit():
            index = int(raw_key)
        elif index_match:
            index = int(index_match.group(1))
        elif key in by_name:
            index = by_name[key]
        elif key in by_source:
            index = by_source[key]
        else:
            raise ValueError(f"Unmapped XGBoost importance feature: {key}")
        if not 0 <= index < len(mappings):
            raise ValueError(
                f"XGBoost importance index is out of range: {index}"
            )
        resolved[index] = float(raw_value)
    return resolved


def _stable_seed(seed: int, feature: str, repeat: int) -> int:
    digest = hashlib.sha256(
        f"{seed}:{feature}:{repeat}".encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:4], byteorder="big", signed=False)


def _sort_importance(
    rows: tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any], ...]:
    return tuple(
        sorted(
            rows,
            key=lambda row: (
                -float(row.get("absolute_importance") or 0.0),
                str(row["feature"]),
            ),
        )
    )


__all__ = [
    "COMPLETE",
    "FAILED",
    "FeatureNameMapping",
    "GlobalExplanation",
    "aggregate_bounded_contributions",
    "deterministic_permutation_importance",
    "explain_logistic_regression",
    "explain_tree_importance",
    "explain_xgboost",
    "failed_explanation",
    "produce_global_explanation",
    "validate_readable_explanation",
]

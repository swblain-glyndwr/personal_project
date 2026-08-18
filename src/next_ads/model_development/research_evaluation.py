"""Bounded, consistent evidence for binary model research candidates."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, replace
import math
import random
import re
from typing import Any, Iterable, Mapping, Sequence


COMPLETE = "COMPLETE"
INSUFFICIENT = "INSUFFICIENT"
TOP_PERCENTAGES = (1, 5, 10)
_GENERIC_FEATURE = re.compile(r"^feature_[0-9]+$", re.IGNORECASE)
_UNSAFE_SLICE_PARTS = (
    "account",
    "customer",
    "email",
    "exposure_id",
    "row_id",
)


def require_complete_binary_evaluation(
    evaluation: Mapping[str, Any],
    *,
    required_metrics: Iterable[str],
    context: str,
) -> None:
    """Reject incomplete aggregate evidence before a candidate can advance."""
    if evaluation.get("status") != COMPLETE:
        reason = str(evaluation.get("reason") or "no reason recorded")
        raise ValueError(f"{context} evidence is not COMPLETE: {reason}")
    metrics = evaluation.get("metrics")
    if not isinstance(metrics, Mapping):
        raise ValueError(f"{context} evidence has no metric mapping")
    missing = sorted(set(required_metrics).difference(metrics))
    if missing:
        raise ValueError(
            f"{context} evidence is missing mandatory metrics: "
            + ", ".join(missing)
        )


def require_complete_confidence_intervals(
    intervals: Mapping[str, Any],
    *,
    context: str,
) -> None:
    """Require deterministic final-test PR-AUC and lift confidence bounds."""
    if intervals.get("status") != COMPLETE:
        reason = str(intervals.get("reason") or "no reason recorded")
        raise ValueError(
            f"{context} confidence intervals are not COMPLETE: {reason}"
        )
    metrics = intervals.get("metrics")
    required = {"auc_pr", "lift_at_5_percent"}
    if not isinstance(metrics, Mapping):
        raise ValueError(f"{context} confidence intervals have no metrics")
    missing = sorted(required.difference(metrics))
    if missing:
        raise ValueError(
            f"{context} confidence intervals are missing: "
            + ", ".join(missing)
        )
    for metric in sorted(required):
        bounds = metrics[metric]
        if not isinstance(bounds, Mapping) or not {
            "lower",
            "median",
            "upper",
        }.issubset(bounds):
            raise ValueError(
                f"{context} confidence interval is incomplete for {metric}"
            )


@dataclass(frozen=True)
class EvaluationConfig:
    """Limits and evidence gates shared by every binary candidate."""

    curve_bins: int = 100
    calibration_bins: int = 10
    distribution_bins: int = 20
    rank_bins: int = 100
    min_rows: int = 100
    min_positive_rows: int = 5
    min_negative_rows: int = 5
    max_slice_values: int = 25
    top_percentages: tuple[int, ...] = TOP_PERCENTAGES

    def __post_init__(self) -> None:
        """Validate limits before any Spark work starts."""
        for name, value, lower, upper in (
            ("curve_bins", self.curve_bins, 10, 1000),
            ("calibration_bins", self.calibration_bins, 2, 100),
            ("distribution_bins", self.distribution_bins, 2, 100),
            ("rank_bins", self.rank_bins, 10, 1000),
            ("min_rows", self.min_rows, 1, 10_000_000),
            (
                "min_positive_rows",
                self.min_positive_rows,
                1,
                10_000_000,
            ),
            (
                "min_negative_rows",
                self.min_negative_rows,
                1,
                10_000_000,
            ),
            ("max_slice_values", self.max_slice_values, 1, 100),
        ):
            if not lower <= int(value) <= upper:
                raise ValueError(f"{name} must be between {lower} and {upper}")
        if tuple(sorted(set(self.top_percentages))) != (self.top_percentages):
            raise ValueError("top_percentages must be sorted and unique")
        if any(not 1 <= value <= 50 for value in self.top_percentages):
            raise ValueError("top_percentages must be between 1 and 50")


@dataclass(frozen=True)
class FeatureCoverageSpec:
    """Declare how missing and default feature values must be reported."""

    column: str
    display_name: str | None = None
    default_values: tuple[Any, ...] = ()
    missing_indicator_column: str | None = None
    default_indicator_column: str | None = None

    def __post_init__(self) -> None:
        """Require a readable, non-empty feature name."""
        if not self.column.strip():
            raise ValueError("Feature coverage column must not be empty")
        display_name = (self.display_name or self.column).strip()
        if not display_name or _GENERIC_FEATURE.fullmatch(display_name):
            raise ValueError(
                "Feature coverage requires a readable Feature Store name"
            )
        object.__setattr__(self, "column", self.column.strip())
        object.__setattr__(self, "display_name", display_name)
        for field_name in (
            "missing_indicator_column",
            "default_indicator_column",
        ):
            value = getattr(self, field_name)
            if value is not None:
                value = value.strip()
                if not value:
                    raise ValueError(f"{field_name} must not be blank")
                object.__setattr__(self, field_name, value)


@dataclass(frozen=True)
class SliceEvaluationSpec:
    """Bound one declared reporting slice and its suppression threshold."""

    slice_id: str
    column: str
    values: tuple[Any, ...] = ()
    minimum_rows: int = 100

    def __post_init__(self) -> None:
        """Reject ambiguous or unbounded slice declarations."""
        slice_id = self.slice_id.strip()
        column = self.column.strip()
        if not slice_id:
            raise ValueError("slice_id must not be blank")
        if not column:
            raise ValueError("Slice column must not be blank")
        if (
            isinstance(self.minimum_rows, bool)
            or not isinstance(self.minimum_rows, int)
            or self.minimum_rows < 1
        ):
            raise ValueError("Slice minimum_rows must be an integer >= 1")
        values = tuple(self.values)
        if len(values) != len(set(values)):
            raise ValueError("Declared slice values must be unique")
        if any(
            not isinstance(value, (str, int, float, bool, type(None)))
            or isinstance(value, float)
            and not math.isfinite(value)
            for value in values
        ):
            raise ValueError("Declared slice values must be finite scalars")
        object.__setattr__(self, "slice_id", slice_id)
        object.__setattr__(self, "column", column)
        object.__setattr__(self, "values", values)


def evaluate_binary_predictions(
    predictions: Any,
    *,
    label_column: str,
    score_column: str = "score",
    row_id_hash_column: str = "row_id",
    slice_columns: tuple[str, ...] = (),
    slice_specs: tuple[SliceEvaluationSpec, ...] = (),
    config: EvaluationConfig | None = None,
) -> dict[str, Any]:
    """Evaluate one candidate without collecting unrestricted scored rows."""
    from pyspark.sql import functions as F

    config = config or EvaluationConfig()
    if slice_columns and slice_specs:
        raise ValueError("Use either slice_columns or slice_specs, not both")
    resolved_slices = tuple(slice_specs) or tuple(
        SliceEvaluationSpec(
            slice_id=column,
            column=column,
            minimum_rows=config.min_rows,
        )
        for column in slice_columns
    )
    slice_ids = [spec.slice_id for spec in resolved_slices]
    if len(slice_ids) != len(set(slice_ids)):
        raise ValueError("Reporting slice IDs must be unique")
    slice_spec_columns = tuple(
        dict.fromkeys(spec.column for spec in resolved_slices)
    )
    _require_columns(
        predictions,
        {
            label_column,
            score_column,
            row_id_hash_column,
            *slice_spec_columns,
        },
        context="Candidate evaluation",
    )
    for column in slice_spec_columns:
        _validate_slice_column(column)
    prepared = predictions.select(
        F.col(label_column).cast("double").alias("__research_label"),
        F.col(score_column).cast("double").alias("__research_score"),
        F.col(row_id_hash_column).cast("string").alias("__research_row_hash"),
        *[F.col(column) for column in slice_spec_columns],
    )
    invalid = prepared.agg(
        F.sum(
            F.when(
                F.col("__research_label").isNull()
                | (~F.col("__research_label").isin(0.0, 1.0)),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("invalid_labels"),
        F.sum(
            F.when(
                F.col("__research_score").isNull()
                | F.isnan(F.col("__research_score"))
                | (F.col("__research_score") < F.lit(0.0))
                | (F.col("__research_score") > F.lit(1.0)),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("invalid_scores"),
        F.sum(
            F.when(
                F.col("__research_row_hash").isNull()
                | (F.length(F.col("__research_row_hash")) == F.lit(0)),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("invalid_row_hashes"),
    ).first()
    if invalid is None:
        raise ValueError("Candidate predictions are empty")
    failures = {
        key: int(invalid[key] or 0)
        for key in (
            "invalid_labels",
            "invalid_scores",
            "invalid_row_hashes",
        )
        if int(invalid[key] or 0)
    }
    if failures:
        raise ValueError(f"Candidate predictions are invalid: {failures}")
    result = _evaluate_prepared(prepared, config=config)
    result["slices"] = _evaluate_slices(
        prepared,
        slice_specs=resolved_slices,
        config=config,
    )
    return result


def profile_feature_coverage(
    frame: Any,
    specs: Sequence[FeatureCoverageSpec],
) -> list[dict[str, Any]]:
    """Return bounded missingness and declared-default coverage statistics."""
    from pyspark.sql import functions as F

    if not specs:
        raise ValueError("Feature coverage requires at least one feature")
    duplicate_columns = [
        column
        for column, count in Counter(spec.column for spec in specs).items()
        if count > 1
    ]
    if duplicate_columns:
        raise ValueError(
            "Feature coverage columns are duplicated: "
            + ", ".join(sorted(duplicate_columns))
        )
    required_columns = {spec.column for spec in specs}
    required_columns.update(
        indicator
        for spec in specs
        for indicator in (
            spec.missing_indicator_column,
            spec.default_indicator_column,
        )
        if indicator is not None
    )
    _require_columns(frame, required_columns, context="Feature coverage")
    expressions = [F.count(F.lit(1)).alias("__coverage_rows")]
    for index, spec in enumerate(specs):
        missing_condition = (
            F.coalesce(
                F.col(spec.missing_indicator_column).cast("boolean"),
                F.lit(False),
            )
            if spec.missing_indicator_column is not None
            else F.col(spec.column).isNull()
        )
        expressions.append(
            F.sum(missing_condition.cast("long")).alias(f"__missing_{index}")
        )
        default_values = tuple(
            value for value in spec.default_values if value is not None
        )
        if spec.default_indicator_column is not None:
            default_condition = F.coalesce(
                F.col(spec.default_indicator_column).cast("boolean"),
                F.lit(False),
            )
        else:
            default_condition = (
                F.col(spec.column).isin(*default_values)
                if default_values
                else F.lit(False)
            )
            if None in spec.default_values:
                default_condition = (
                    default_condition | F.col(spec.column).isNull()
                )
        expressions.append(
            F.sum(default_condition.cast("long")).alias(f"__default_{index}")
        )
    row = frame.agg(*expressions).first()
    if row is None or int(row["__coverage_rows"] or 0) == 0:
        raise ValueError("Feature coverage frame is empty")
    rows = int(row["__coverage_rows"])
    return [
        {
            "feature": spec.display_name,
            "rows": rows,
            "missing_rows": int(row[f"__missing_{index}"] or 0),
            "missing_rate": int(row[f"__missing_{index}"] or 0) / rows,
            "default_rows": int(row[f"__default_{index}"] or 0),
            "default_rate": int(row[f"__default_{index}"] or 0) / rows,
            "missing_source": (
                f"indicator:{spec.missing_indicator_column}"
                if spec.missing_indicator_column is not None
                else "null_value"
            ),
            "default_source": (
                f"indicator:{spec.default_indicator_column}"
                if spec.default_indicator_column is not None
                else "declared_values"
            ),
        }
        for index, spec in enumerate(specs)
    ]


def deterministic_selected_test_confidence_intervals(
    predictions: Any,
    *,
    label_column: str,
    score_column: str = "score",
    split_column: str = "split",
    block_column: str | None = None,
    row_id_hash_column: str = "row_id",
    derived_block_count: int = 100,
    iterations: int = 400,
    confidence: float = 0.95,
    seed: int = 1729,
    curve_bins: int = 100,
    max_blocks: int = 366,
) -> dict[str, Any]:
    """Bootstrap final-test PR-AUC and lift from bounded block/bin counts.

    Only the selected candidate should call this function.  It refuses a frame
    containing any split other than ``test``.  Spark first reduces rows into a
    bounded block-by-score-bin table.  A declared block can be supplied; by
    default the already-hashed row identity is deterministically assigned to
    100 blocks.  Resampling never handles account-level predictions or exposes
    block identifiers in the returned evidence.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if not 20 <= iterations <= 2000:
        raise ValueError("iterations must be between 20 and 2000")
    if not 0.8 <= confidence < 1.0:
        raise ValueError("confidence must be between 0.8 and 1.0")
    if not 10 <= curve_bins <= 1000:
        raise ValueError("curve_bins must be between 10 and 1000")
    if not 10 <= derived_block_count <= max_blocks:
        raise ValueError(
            "derived_block_count must be between 10 and max_blocks"
        )
    _require_columns(
        predictions,
        {
            label_column,
            score_column,
            split_column,
            block_column or row_id_hash_column,
        },
        context="Selected test confidence intervals",
    )
    splits = [
        row[split_column]
        for row in predictions.select(split_column)
        .distinct()
        .limit(2)
        .collect()
    ]
    if splits != ["test"]:
        raise ValueError(
            "Confidence intervals require only the selected candidate's "
            f"untouched test split: found={sorted(str(v) for v in splits)}"
        )
    block = (
        F.col(block_column).cast("string")
        if block_column is not None
        else F.pmod(
            F.xxhash64(F.col(row_id_hash_column).cast("string")),
            F.lit(derived_block_count),
        ).cast("string")
    )
    invalid_block_source = (
        F.col(block_column).isNull()
        if block_column is not None
        else F.col(row_id_hash_column).isNull()
        | (F.length(F.col(row_id_hash_column).cast("string")) == F.lit(0))
    )
    invalid = predictions.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(
            F.when(
                F.col(label_column).cast("double").isNull()
                | (~F.col(label_column).cast("double").isin(0.0, 1.0)),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("invalid_labels"),
        F.sum(
            F.when(
                F.col(score_column).cast("double").isNull()
                | F.isnan(F.col(score_column).cast("double"))
                | (F.col(score_column).cast("double") < F.lit(0.0))
                | (F.col(score_column).cast("double") > F.lit(1.0)),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("invalid_scores"),
        F.sum(invalid_block_source.cast("long")).alias("invalid_blocks"),
    ).first()
    if invalid is None:
        raise ValueError("Selected test predictions are empty")
    total_rows = int(invalid["rows"] or 0)
    if total_rows < 1:
        raise ValueError("Selected test predictions are empty")
    invalid_counts = {
        key: int(invalid[key] or 0)
        for key in ("invalid_labels", "invalid_scores", "invalid_blocks")
        if int(invalid[key] or 0)
    }
    if invalid_counts:
        raise ValueError(
            f"Selected test predictions are invalid: {invalid_counts}"
        )
    base = predictions.select(
        block.alias("__block_source"),
        F.col(label_column).cast("double").alias("__label"),
        F.col(score_column).cast("double").alias("__score"),
    )
    score_groups = base.groupBy("__score").agg(
        F.count(F.lit(1)).alias("__score_rows")
    )
    score_window = Window.orderBy(F.col("__score").desc()).rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
    score_mapping = score_groups.withColumn(
        "__cumulative_rows",
        F.sum("__score_rows").over(score_window),
    ).withColumn(
        "__score_bin",
        F.lit(curve_bins)
        - F.least(
            F.lit(curve_bins),
            F.ceil(
                F.col("__cumulative_rows")
                * F.lit(curve_bins)
                / F.lit(total_rows)
            ).cast("int"),
        ),
    )
    bounded = (
        base.join(
            score_mapping.select("__score", "__score_bin"),
            on="__score",
            how="inner",
        )
        .select(
            F.sha2(
                F.col("__block_source"),
                256,
            ).alias("__block"),
            "__label",
            "__score_bin",
        )
        .groupBy("__block", "__score_bin")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum("__label").alias("positives"),
        )
    )
    block_count = bounded.select("__block").distinct().count()
    if block_count < 2:
        return {
            "status": INSUFFICIENT,
            "reason": "At least two independent test blocks are required",
            "block_strategy": block_column or "deterministic_row_hash_buckets",
            "block_count": block_count,
            "iterations": 0,
            "metrics": {},
        }
    if block_count > max_blocks:
        raise ValueError(
            f"Bootstrap block count {block_count} exceeds limit {max_blocks}"
        )
    rows = bounded.collect()
    if len(rows) > max_blocks * curve_bins:
        raise ValueError("Bootstrap aggregation exceeded its bounded contract")
    by_block: dict[str, list[dict[str, int]]] = defaultdict(list)
    for row in rows:
        by_block[str(row["__block"])].append(
            {
                "score_bin": int(row["__score_bin"]),
                "rows": int(row["rows"]),
                "positives": int(row["positives"] or 0),
            }
        )
    blocks = sorted(by_block)
    rng = random.Random(seed)
    samples: dict[str, list[float]] = {
        "auc_pr": [],
        "lift_at_5_percent": [],
    }
    for _iteration in range(iterations):
        weights = Counter(rng.choice(blocks) for _ in blocks)
        aggregate: dict[int, dict[str, int]] = defaultdict(
            lambda: {"rows": 0, "positives": 0}
        )
        for block, weight in weights.items():
            for row in by_block[block]:
                target = aggregate[row["score_bin"]]
                target["rows"] += row["rows"] * weight
                target["positives"] += row["positives"] * weight
        bins = [
            {"score_bin": score_bin_value, **counts}
            for score_bin_value, counts in aggregate.items()
        ]
        metrics = binary_metrics_from_score_bins(bins)
        if metrics["status"] != COMPLETE:
            continue
        samples["auc_pr"].append(float(metrics["auc_pr"]))
        samples["lift_at_5_percent"].append(
            _lift_from_score_bins(bins, percentage=5)
        )
    if len(samples["auc_pr"]) < max(20, iterations // 2):
        return {
            "status": INSUFFICIENT,
            "reason": "Too few valid deterministic bootstrap samples",
            "block_strategy": block_column or "deterministic_row_hash_buckets",
            "block_count": block_count,
            "iterations": len(samples["auc_pr"]),
            "metrics": {},
        }
    tail = (1.0 - confidence) / 2.0
    return {
        "status": COMPLETE,
        "block_strategy": block_column or "deterministic_row_hash_buckets",
        "block_count": block_count,
        "iterations": len(samples["auc_pr"]),
        "confidence": confidence,
        "seed": seed,
        "metrics": {
            metric: {
                "lower": _percentile(values, tail),
                "median": _percentile(values, 0.5),
                "upper": _percentile(values, 1.0 - tail),
            }
            for metric, values in samples.items()
        },
    }


def binary_metrics_from_score_bins(
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Calculate bounded approximate PR and ROC curves from score bins."""
    bins = sorted(
        (
            {
                "score_bin": int(row["score_bin"]),
                "rows": int(row["rows"]),
                "positives": int(row["positives"]),
            }
            for row in rows
        ),
        key=lambda row: row["score_bin"],
        reverse=True,
    )
    total_rows = sum(row["rows"] for row in bins)
    positives = sum(row["positives"] for row in bins)
    negatives = total_rows - positives
    if not total_rows or not positives or not negatives:
        return {
            "status": INSUFFICIENT,
            "reason": "Both binary label classes are required",
            "rows": total_rows,
            "positives": positives,
            "negatives": negatives,
            "precision_recall_curve": [],
            "roc_curve": [],
        }
    precision_recall = [{"score_bin": None, "precision": 1.0, "recall": 0.0}]
    roc = [{"score_bin": None, "false_positive_rate": 0.0, "recall": 0.0}]
    true_positive = 0
    false_positive = 0
    for row in bins:
        true_positive += row["positives"]
        false_positive += row["rows"] - row["positives"]
        precision_recall.append(
            {
                "score_bin": row["score_bin"],
                "precision": true_positive / (true_positive + false_positive),
                "recall": true_positive / positives,
            }
        )
        roc.append(
            {
                "score_bin": row["score_bin"],
                "false_positive_rate": false_positive / negatives,
                "recall": true_positive / positives,
            }
        )
    auc_pr = _precision_recall_area(precision_recall)
    auc_roc = _curve_area(
        roc,
        x="false_positive_rate",
        y="recall",
    )
    return {
        "status": COMPLETE,
        "rows": total_rows,
        "positives": positives,
        "negatives": negatives,
        "prevalence": positives / total_rows,
        "auc_pr": auc_pr,
        "auc_roc": auc_roc,
        "precision_recall_curve": precision_recall,
        "roc_curve": roc,
    }


def _evaluate_prepared(
    prepared: Any,
    *,
    config: EvaluationConfig,
) -> dict[str, Any]:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    probability = F.least(
        F.lit(1.0 - 1e-15),
        F.greatest(F.lit(1e-15), F.col("__research_score")),
    )
    label = F.col("__research_label")
    profile = prepared.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(label).alias("positives"),
        F.avg(label).alias("observed_click_rate"),
        F.avg(F.col("__research_score")).alias("predicted_click_rate"),
        F.avg(
            -(label * F.log(probability))
            - ((F.lit(1.0) - label) * F.log(F.lit(1.0) - probability))
        ).alias("log_loss"),
    ).first()
    if profile is None or int(profile["rows"] or 0) == 0:
        raise ValueError("Candidate predictions are empty")
    rows = int(profile["rows"])
    positives = int(profile["positives"] or 0)
    negatives = rows - positives
    summary = {
        "rows": rows,
        "positives": positives,
        "negatives": negatives,
        "observed_click_rate": float(profile["observed_click_rate"] or 0.0),
        "predicted_click_rate": float(profile["predicted_click_rate"] or 0.0),
        "log_loss": float(profile["log_loss"] or 0.0),
    }
    summary["calibration_gap"] = abs(
        summary["predicted_click_rate"] - summary["observed_click_rate"]
    )
    insufficient_reason = _insufficient_reason(
        rows,
        positives,
        negatives,
        config=config,
    )
    if insufficient_reason:
        return {
            "status": INSUFFICIENT,
            "reason": insufficient_reason,
            "profile": {"rows": rows},
            "metrics": {},
            "precision_recall_curve": [],
            "roc_curve": [],
            "calibration": [],
            "lift_gain": [],
            "score_distribution": [],
            "top_confusion": [],
        }
    ranked = prepared.withColumn(
        "__research_rank",
        F.row_number().over(
            Window.orderBy(
                F.col("__research_score").desc(),
                F.col("__research_row_hash").asc(),
            )
        ),
    )
    score_bins = _rank_score_bin_rows(
        prepared,
        rows=rows,
        bins=config.curve_bins,
    )
    curves = binary_metrics_from_score_bins(score_bins)
    assert curves["status"] == COMPLETE
    auc_pr, auc_roc = _exact_tie_aware_auc(
        prepared,
        positives=positives,
        negatives=negatives,
    )
    rank_bin = F.least(
        F.lit(config.rank_bins),
        F.ceil(
            F.col("__research_rank") * F.lit(config.rank_bins) / F.lit(rows)
        ).cast("int"),
    )
    rank_rows = [
        row.asDict(recursive=True)
        for row in (
            ranked.withColumn("__rank_bin", rank_bin)
            .groupBy("__rank_bin")
            .agg(
                F.count(F.lit(1)).alias("rows"),
                F.sum("__research_label").alias("positives"),
                F.min("__research_score").alias("minimum_score"),
                F.max("__research_score").alias("maximum_score"),
            )
            .orderBy("__rank_bin")
            .collect()
        )
    ]
    if len(rank_rows) > config.rank_bins:
        raise ValueError("Rank aggregation exceeded its bounded contract")
    top_confusion = _top_confusion_rows(
        ranked,
        rows=rows,
        positives=positives,
        percentages=config.top_percentages,
    )
    top_metrics = {
        key: value
        for row in top_confusion
        for key, value in (
            (f"precision_at_{row['percentage']}_percent", row["precision"]),
            (f"recall_at_{row['percentage']}_percent", row["recall"]),
            (f"lift_at_{row['percentage']}_percent", row["lift"]),
        )
    }
    return {
        "status": COMPLETE,
        "profile": summary,
        "metrics": {
            "auc_pr": auc_pr,
            "prevalence": curves["prevalence"],
            "auc_roc": auc_roc,
            "log_loss": summary["log_loss"],
            "observed_click_rate": summary["observed_click_rate"],
            "predicted_click_rate": summary["predicted_click_rate"],
            "calibration_gap": summary["calibration_gap"],
            **top_metrics,
        },
        "precision_recall_curve": curves["precision_recall_curve"],
        "roc_curve": curves["roc_curve"],
        "calibration": _calibration_rows(
            prepared,
            bins=config.calibration_bins,
        ),
        "lift_gain": _lift_gain_rows(
            rank_rows,
            total_rows=rows,
            total_positives=positives,
        ),
        "score_distribution": _score_distribution_rows(
            prepared,
            bins=config.distribution_bins,
        ),
        "top_confusion": top_confusion,
    }


def _exact_tie_aware_auc(
    prepared: Any,
    *,
    positives: int,
    negatives: int,
) -> tuple[float, float]:
    """Calculate distributed average precision and ROC AUC by score threshold."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if positives <= 0 or negatives <= 0:
        raise ValueError("Tie-aware AUC requires both binary label classes")
    grouped = prepared.groupBy("__research_score").agg(
        F.count(F.lit(1)).cast("double").alias("__score_rows"),
        F.sum("__research_label").cast("double").alias("__score_positives"),
    )
    ranked_window = Window.orderBy(
        F.col("__research_score").desc()
    ).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    ranked = (
        grouped.withColumn(
            "__score_negatives",
            F.col("__score_rows") - F.col("__score_positives"),
        )
        .withColumn(
            "__cumulative_rows",
            F.sum("__score_rows").over(ranked_window),
        )
        .withColumn(
            "__cumulative_positives",
            F.sum("__score_positives").over(ranked_window),
        )
        .withColumn(
            "__cumulative_negatives",
            F.sum("__score_negatives").over(ranked_window),
        )
    )
    row = ranked.agg(
        F.sum(
            (F.col("__score_positives") / F.lit(float(positives)))
            * (F.col("__cumulative_positives") / F.col("__cumulative_rows"))
        ).alias("auc_pr"),
        (
            F.sum(
                F.col("__score_positives")
                * (
                    F.lit(float(negatives))
                    - F.col("__cumulative_negatives")
                    + F.lit(0.5) * F.col("__score_negatives")
                )
            )
            / F.lit(float(positives * negatives))
        ).alias("auc_roc"),
    ).first()
    if row is None or row["auc_pr"] is None or row["auc_roc"] is None:
        raise ValueError("Tie-aware AUC aggregation produced no result")
    return float(row["auc_pr"]), float(row["auc_roc"])


def _rank_score_bin_rows(
    prepared: Any,
    *,
    rows: int,
    bins: int,
) -> list[dict[str, int]]:
    """Build bounded score bins without separating rows tied on score."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    grouped = prepared.groupBy("__research_score").agg(
        F.count(F.lit(1)).alias("__score_rows"),
        F.sum("__research_label").alias("__score_positives"),
    )
    window = Window.orderBy(F.col("__research_score").desc()).rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
    score_bin = F.lit(bins) - F.least(
        F.lit(bins),
        F.ceil(
            F.sum("__score_rows").over(window) * F.lit(bins) / F.lit(rows)
        ).cast("int"),
    )
    aggregated = (
        grouped.withColumn("__score_bin", score_bin)
        .groupBy("__score_bin")
        .agg(
            F.sum("__score_rows").alias("rows"),
            F.sum("__score_positives").alias("positives"),
        )
        .collect()
    )
    if len(aggregated) > bins:
        raise ValueError("Score-bin aggregation exceeded its bounded contract")
    return [
        {
            "score_bin": int(row["__score_bin"]),
            "rows": int(row["rows"]),
            "positives": int(row["positives"] or 0),
        }
        for row in aggregated
    ]


def _calibration_rows(frame: Any, *, bins: int) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    score_bin = F.least(
        F.lit(bins - 1),
        F.floor(F.col("__research_score") * F.lit(bins)).cast("int"),
    )
    rows = (
        frame.withColumn("__calibration_bin", score_bin)
        .groupBy("__calibration_bin")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.avg("__research_score").alias("mean_score"),
            F.avg("__research_label").alias("observed_rate"),
        )
        .orderBy("__calibration_bin")
        .collect()
    )
    if len(rows) > bins:
        raise ValueError(
            "Calibration aggregation exceeded its bounded contract"
        )
    return [
        {
            "score_bin": int(row["__calibration_bin"]),
            "rows": int(row["rows"]),
            "mean_score": float(row["mean_score"]),
            "observed_rate": float(row["observed_rate"]),
        }
        for row in rows
    ]


def _score_distribution_rows(
    frame: Any,
    *,
    bins: int,
) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    score_bin = F.least(
        F.lit(bins - 1),
        F.floor(F.col("__research_score") * F.lit(bins)).cast("int"),
    )
    rows = (
        frame.withColumn("__distribution_bin", score_bin)
        .groupBy("__research_label", "__distribution_bin")
        .agg(F.count(F.lit(1)).alias("rows"))
        .orderBy("__research_label", "__distribution_bin")
        .collect()
    )
    if len(rows) > bins * 2:
        raise ValueError(
            "Score-distribution aggregation exceeded its bounded contract"
        )
    return [
        {
            "label": int(row["__research_label"]),
            "score_bin": int(row["__distribution_bin"]),
            "rows": int(row["rows"]),
        }
        for row in rows
    ]


def _top_confusion_rows(
    ranked: Any,
    *,
    rows: int,
    positives: int,
    percentages: tuple[int, ...],
) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    expressions = []
    cutoffs = {}
    for percentage in percentages:
        cutoff = max(1, math.ceil(rows * percentage / 100.0))
        cutoffs[percentage] = cutoff
        expressions.extend(
            (
                F.sum(
                    F.when(
                        F.col("__research_rank") <= F.lit(cutoff),
                        F.col("__research_label"),
                    ).otherwise(F.lit(0.0))
                ).alias(f"tp_{percentage}"),
                F.min(
                    F.when(
                        F.col("__research_rank") <= F.lit(cutoff),
                        F.col("__research_score"),
                    )
                ).alias(f"threshold_{percentage}"),
            )
        )
    result = ranked.agg(*expressions).first()
    assert result is not None
    prevalence = positives / rows
    output = []
    for percentage in percentages:
        selected = cutoffs[percentage]
        true_positive = int(result[f"tp_{percentage}"] or 0)
        false_positive = selected - true_positive
        false_negative = positives - true_positive
        true_negative = (rows - positives) - false_positive
        precision = true_positive / selected
        recall = true_positive / positives
        output.append(
            {
                "percentage": percentage,
                "selected_rows": selected,
                "threshold": float(result[f"threshold_{percentage}"]),
                "tp": true_positive,
                "fp": false_positive,
                "fn": false_negative,
                "tn": true_negative,
                "precision": precision,
                "recall": recall,
                "lift": precision / prevalence,
            }
        )
    return output


def _lift_gain_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    total_rows: int,
    total_positives: int,
) -> list[dict[str, Any]]:
    cumulative_rows = 0
    cumulative_positives = 0
    output = []
    prevalence = total_positives / total_rows
    for row in rows:
        cumulative_rows += int(row["rows"])
        cumulative_positives += int(row["positives"] or 0)
        precision = cumulative_positives / cumulative_rows
        output.append(
            {
                "rank_bin": int(row["__rank_bin"]),
                "population_fraction": cumulative_rows / total_rows,
                "cumulative_gain": cumulative_positives / total_positives,
                "cumulative_lift": precision / prevalence,
                "minimum_score": float(row["minimum_score"]),
                "maximum_score": float(row["maximum_score"]),
            }
        )
    return output


def _evaluate_slices(
    prepared: Any,
    *,
    slice_specs: tuple[SliceEvaluationSpec, ...],
    config: EvaluationConfig,
) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    output = []
    for spec in slice_specs:
        if spec.values:
            discovered_values: tuple[Any, ...] = ()
        else:
            value_rows = (
                prepared.groupBy(spec.column)
                .agg(F.count(F.lit(1)).alias("__slice_rows"))
                .orderBy(
                    F.col("__slice_rows").desc(),
                    F.col(spec.column).asc_nulls_last(),
                )
                .limit(config.max_slice_values + 1)
                .collect()
            )
            discovered_values = tuple(row[spec.column] for row in value_rows)
        values = _bounded_slice_values(
            spec,
            discovered_values=discovered_values,
            max_values=config.max_slice_values,
        )
        slice_config = _config_for_slice(config, spec)
        for value in values:
            condition = (
                F.col(spec.column).isNull()
                if value is None
                else F.col(spec.column).eqNullSafe(F.lit(value))
            )
            slice_frame = prepared.where(condition)
            if slice_frame.limit(1).count() == 0:
                evaluation = _empty_slice_evaluation(slice_config)
            else:
                evaluation = _evaluate_prepared(
                    slice_frame,
                    config=slice_config,
                )
            output.append(
                {
                    "slice_id": spec.slice_id,
                    "slice_column": spec.column,
                    "slice_value": "<NULL>"
                    if value is None
                    else str(value)[:128],
                    "minimum_rows": spec.minimum_rows,
                    "status": evaluation["status"],
                    "reason": evaluation.get("reason"),
                    "profile": evaluation["profile"],
                    "metrics": evaluation["metrics"],
                }
            )
    return output


def _bounded_slice_values(
    spec: SliceEvaluationSpec,
    *,
    discovered_values: tuple[Any, ...],
    max_values: int,
) -> tuple[Any, ...]:
    values = spec.values or discovered_values
    if len(values) > max_values:
        raise ValueError(
            f"Slice {spec.slice_id} exceeds the bounded reporting limit of "
            f"{max_values} values"
        )
    return values


def _config_for_slice(
    config: EvaluationConfig,
    spec: SliceEvaluationSpec,
) -> EvaluationConfig:
    return replace(config, min_rows=spec.minimum_rows)


def _empty_slice_evaluation(config: EvaluationConfig) -> dict[str, Any]:
    return {
        "status": INSUFFICIENT,
        "reason": f"Row volume is below minimum {config.min_rows}",
        "profile": {"rows": 0},
        "metrics": {},
        "precision_recall_curve": [],
        "roc_curve": [],
        "calibration": [],
        "lift_gain": [],
        "score_distribution": [],
        "top_confusion": [],
    }


def _lift_from_score_bins(
    rows: Iterable[Mapping[str, Any]],
    *,
    percentage: int,
) -> float:
    bins = sorted(rows, key=lambda row: int(row["score_bin"]), reverse=True)
    total_rows = sum(int(row["rows"]) for row in bins)
    total_positives = sum(int(row["positives"]) for row in bins)
    if not total_rows or not total_positives:
        return 0.0
    target = max(1, math.ceil(total_rows * percentage / 100.0))
    selected = 0
    selected_positives = 0.0
    for row in bins:
        available = int(row["rows"])
        take = min(available, target - selected)
        if take <= 0:
            break
        selected_positives += float(row["positives"]) * take / available
        selected += take
    precision = selected_positives / selected
    return precision / (total_positives / total_rows)


def _curve_area(
    rows: Sequence[Mapping[str, Any]],
    *,
    x: str,
    y: str,
) -> float:
    return sum(
        (float(right[x]) - float(left[x]))
        * (float(right[y]) + float(left[y]))
        / 2.0
        for left, right in zip(rows, rows[1:])
    )


def _precision_recall_area(
    rows: Sequence[Mapping[str, Any]],
) -> float:
    """Return average-precision area so a constant score equals prevalence."""
    return sum(
        (float(right["recall"]) - float(left["recall"]))
        * float(right["precision"])
        for left, right in zip(rows, rows[1:])
    )


def _percentile(values: Sequence[float], quantile: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise ValueError("Cannot calculate a percentile from no values")
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _insufficient_reason(
    rows: int,
    positives: int,
    negatives: int,
    *,
    config: EvaluationConfig,
) -> str | None:
    if rows < config.min_rows:
        return f"Row volume is below minimum {config.min_rows}"
    if positives < config.min_positive_rows:
        return f"Positive-class volume is below minimum {config.min_positive_rows}"
    if negatives < config.min_negative_rows:
        return f"Negative-class volume is below minimum {config.min_negative_rows}"
    return None


def _require_columns(
    frame: Any,
    required: set[str],
    *,
    context: str,
) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{context} is missing: " + ", ".join(missing))


def _validate_slice_column(column: str) -> None:
    normalized = column.casefold()
    if any(part in normalized for part in _UNSAFE_SLICE_PARTS):
        raise ValueError(
            f"Reporting slice cannot contain an identity column: {column}"
        )


__all__ = [
    "COMPLETE",
    "INSUFFICIENT",
    "EvaluationConfig",
    "FeatureCoverageSpec",
    "SliceEvaluationSpec",
    "binary_metrics_from_score_bins",
    "deterministic_selected_test_confidence_intervals",
    "evaluate_binary_predictions",
    "profile_feature_coverage",
    "require_complete_binary_evaluation",
    "require_complete_confidence_intervals",
]

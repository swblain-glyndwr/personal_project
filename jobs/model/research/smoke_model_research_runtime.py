"""Prove the standard model-research runtime can fit Spark XGBoost."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import logging
from pathlib import Path
import platform
import sys


def resolve_project_root(
    script_file: str | None,
    notebook_path: str | None = None,
) -> Path:
    """Resolve the bundle root for Python and Databricks execution."""
    if script_file:
        return Path(script_file).resolve().parents[3]
    if notebook_path is None:
        from dsutils.dbc import get_dbutils

        notebook_path = (
            get_dbutils()
            .notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    return Path(notebook_path).parents[3]


PROJECT_ROOT = resolve_project_root(globals().get("__file__"))
SRC_ROOT = PROJECT_ROOT / "src"
if not (SRC_ROOT / "next_ads").is_dir():
    raise RuntimeError(f"Canonical NextAds package not found under {SRC_ROOT}")
sys.path.insert(0, str(SRC_ROOT))
sys.path.insert(1, str(PROJECT_ROOT))


from dsutils.dbc import configure_spark
from next_ads.common.job_logging import configure_job_logging
from next_ads.model_development.research_explainability import (
    FeatureNameMapping,
    aggregate_bounded_contributions,
)
from next_ads.model_development.research_evaluation import (
    EvaluationConfig,
    evaluate_binary_predictions,
)


LOGGER = logging.getLogger(__name__)
EVIDENCE_PREFIX = "MODEL_RESEARCH_RUNTIME_SMOKE="
EXPECTED_PACKAGES = {
    "databricks-feature-engineering": "0.12.1",
    "dynaconf": "3.2.12",
    "matplotlib": "3.11.1",
    "mlflow": "3.11.1",
    "numpy": "1.26.4",
    "xgboost": "3.0.0",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def _package_versions() -> dict[str, str]:
    versions = {
        package: importlib.metadata.version(package)
        for package in EXPECTED_PACKAGES
    }
    for package, expected in EXPECTED_PACKAGES.items():
        if versions[package] != expected:
            raise ValueError(
                f"{package} must be {expected}; found {versions[package]}"
            )
    import matplotlib

    if matplotlib.__version__ != EXPECTED_PACKAGES["matplotlib"]:
        raise ValueError("Imported Matplotlib version differs from its package")
    return versions


def _tiny_xgboost_fit(spark) -> tuple[int, int]:
    from pyspark.ml.linalg import Vectors
    from xgboost.spark import SparkXGBClassifier

    rows = [
        (
            f"row-{index:02d}",
            float(index % 2),
            Vectors.dense(float(index), float(index % 3)),
        )
        for index in range(32)
    ]
    frame = spark.createDataFrame(
        rows, ("row_id_hash", "label", "features")
    ).repartition(4)
    model = SparkXGBClassifier(
        features_col="features",
        label_col="label",
        prediction_col="prediction",
        probability_col="probability",
        eval_metric="aucpr",
        n_estimators=2,
        max_depth=2,
        num_workers=4,
        seed=1729,
        pred_contrib_col="contributions",
    ).fit(frame)
    predictions = model.transform(frame)
    count = predictions.where("probability IS NOT NULL").count()
    if count != len(rows):
        raise ValueError("Spark XGBoost smoke did not score every input row")
    contributions = aggregate_bounded_contributions(
        predictions,
        (
            FeatureNameMapping(0, "first_feature"),
            FeatureNameMapping(1, "second_feature"),
        ),
    )
    if len(contributions) != 2:
        raise ValueError("Spark XGBoost smoke did not explain both features")
    return count, len(contributions)


def _constant_score_metrics(spark) -> tuple[float, float, float]:
    """Prove tied scores produce prevalence PR-AUC and chance ROC-AUC."""
    rows = [
        (f"constant-{index:03d}", float(index < 20), 0.1)
        for index in range(200)
    ]
    predictions = spark.createDataFrame(rows, ("row_id", "label", "score"))
    evaluation = evaluate_binary_predictions(
        predictions,
        label_column="label",
        score_column="score",
        row_id_hash_column="row_id",
        config=EvaluationConfig(
            min_rows=100,
            min_positive_rows=5,
            min_negative_rows=5,
        ),
    )
    metrics = evaluation["metrics"]
    prevalence = float(metrics["prevalence"])
    auc_pr = float(metrics["auc_pr"])
    auc_roc = float(metrics["auc_roc"])
    if abs(auc_pr - prevalence) > 1e-12 or abs(auc_roc - 0.5) > 1e-12:
        raise ValueError(
            "Tie-aware metric smoke failed: "
            f"prevalence={prevalence}, auc_pr={auc_pr}, auc_roc={auc_roc}"
        )
    return prevalence, auc_pr, auc_roc


def main() -> None:
    args = parse_args()
    configure_job_logging(args.log_level)
    spark = configure_spark()
    versions = _package_versions()
    if sys.version_info[:2] != (3, 11):
        raise ValueError(
            "Model research requires Python 3.11; "
            f"found {platform.python_version()}"
        )
    scored_rows, explained_features = _tiny_xgboost_fit(spark)
    prevalence, constant_auc_pr, constant_auc_roc = _constant_score_metrics(
        spark
    )
    evidence = {
        "constant_score_auc_pr": constant_auc_pr,
        "constant_score_auc_roc": constant_auc_roc,
        "constant_score_prevalence": prevalence,
        "package_versions": versions,
        "python_version": platform.python_version(),
        "runtime_version": spark.conf.get(
            "spark.databricks.clusterUsageTags.sparkVersion", "unknown"
        ),
        "spark_xgboost_rows": scored_rows,
        "spark_xgboost_explained_features": explained_features,
        "spark_xgboost_workers": 4,
        "status": "PASS",
        "writes_performed": False,
    }
    LOGGER.info(
        "%s%s",
        EVIDENCE_PREFIX,
        json.dumps(evidence, sort_keys=True, separators=(",", ":")),
    )


if __name__ == "__main__":
    main()

from __future__ import annotations

from next_ads.ml.lifecycle.drift import (
    assess_drift,
    drift_metrics,
    to_mlflow_metrics,
)
from next_ads.ml.lifecycle.registry import configure_mlflow
from next_ads.ml.lifecycle.spec import DriftThresholds


def with_monitoring_derived_columns(df):
    columns = set(df.columns)

    if "theme_clean" not in columns and "theme" in columns:
        df = df.withColumn("theme_clean", df["theme"].cast("string"))
        columns.add("theme_clean")

    if "simple_rules_rank" in columns and "simple_rules_rank_band" not in columns:
        from pyspark.sql import functions as F

        df = df.withColumn(
            "simple_rules_rank_band",
            F.when(F.col("simple_rules_rank").isNull(), F.lit("rank_missing"))
            .when(F.col("simple_rules_rank") <= F.lit(20), F.lit("rank_001_020"))
            .when(F.col("simple_rules_rank") <= F.lit(100), F.lit("rank_021_100"))
            .when(F.col("simple_rules_rank") <= F.lit(256), F.lit("rank_101_256"))
            .otherwise(F.lit("rank_gt_256")),
        )
        columns.add("simple_rules_rank_band")

    if "user_total_views" in columns and "user_total_views_bucket" not in columns:
        from pyspark.sql import functions as F

        df = df.withColumn(
            "user_total_views_bucket",
            F.when(F.col("user_total_views").isNull(), F.lit("views_missing"))
            .when(F.col("user_total_views") <= F.lit(0), F.lit("views_000"))
            .when(F.col("user_total_views") <= F.lit(5), F.lit("views_001_005"))
            .when(F.col("user_total_views") <= F.lit(20), F.lit("views_006_020"))
            .when(F.col("user_total_views") <= F.lit(100), F.lit("views_021_100"))
            .otherwise(F.lit("views_gt_100")),
        )
        columns.add("user_total_views_bucket")

    if (
        "num_retrieval_methods" in columns
        and "num_retrieval_methods_bucket" not in columns
    ):
        from pyspark.sql import functions as F

        df = df.withColumn(
            "num_retrieval_methods_bucket",
            F.when(F.col("num_retrieval_methods").isNull(), F.lit("retrieval_missing"))
            .when(F.col("num_retrieval_methods") <= F.lit(0), F.lit("retrieval_0"))
            .when(F.col("num_retrieval_methods") == F.lit(1), F.lit("retrieval_1"))
            .when(F.col("num_retrieval_methods") == F.lit(2), F.lit("retrieval_2"))
            .when(F.col("num_retrieval_methods") == F.lit(3), F.lit("retrieval_3"))
            .otherwise(F.lit("retrieval_4_plus")),
        )

    return df


def table_drift_metrics(
    spark,
    baseline_table: str,
    candidate_table: str,
    feature_cols: list[str],
    categorical_cols: list[str] | None = None,
    prediction_col: str | None = None,
    sample_limit: int = 100000,
):
    categorical_cols = categorical_cols or []
    numeric_cols = [column for column in feature_cols if column not in categorical_cols]

    baseline_df = with_monitoring_derived_columns(spark.table(baseline_table))
    candidate_df = with_monitoring_derived_columns(spark.table(candidate_table))
    selected_cols = _common_selected_columns(
        baseline_df,
        candidate_df,
        feature_cols + categorical_cols + _optional(prediction_col),
    )

    baseline = baseline_df.select(*selected_cols).limit(sample_limit).toPandas()
    candidate = candidate_df.select(*selected_cols).limit(sample_limit).toPandas()
    return drift_metrics(
        baseline=baseline,
        candidate=candidate,
        numeric_cols=numeric_cols,
        categorical_cols=categorical_cols,
        prediction_col=prediction_col,
    )


def log_table_drift_to_mlflow(
    spark,
    mlflow_module,
    experiment_path: str,
    baseline_table: str,
    candidate_table: str,
    feature_cols: list[str],
    categorical_cols: list[str] | None = None,
    prediction_col: str | None = None,
    sample_limit: int = 100000,
    thresholds: DriftThresholds | None = None,
    tags: dict[str, str] | None = None,
):
    thresholds = thresholds or DriftThresholds()
    configure_mlflow(mlflow_module)
    mlflow_module.set_experiment(experiment_path)
    metrics = table_drift_metrics(
        spark=spark,
        baseline_table=baseline_table,
        candidate_table=candidate_table,
        feature_cols=feature_cols,
        categorical_cols=categorical_cols,
        prediction_col=prediction_col,
        sample_limit=sample_limit,
    )
    assessment = assess_drift(
        metrics,
        numeric_psi_warn_threshold=thresholds.numeric_psi_warn,
        numeric_psi_fail_threshold=thresholds.numeric_psi_fail,
        categorical_warn_threshold=thresholds.categorical_warn,
        categorical_fail_threshold=thresholds.categorical_fail,
    )
    with mlflow_module.start_run(run_name="model_drift_monitor") as run:
        mlflow_module.log_param("baseline_table", baseline_table)
        mlflow_module.log_param("candidate_table", candidate_table)
        mlflow_module.log_param("sample_limit", sample_limit)
        mlflow_module.log_param("numeric_psi_warn", thresholds.numeric_psi_warn)
        mlflow_module.log_param("numeric_psi_fail", thresholds.numeric_psi_fail)
        mlflow_module.log_param("categorical_warn", thresholds.categorical_warn)
        mlflow_module.log_param("categorical_fail", thresholds.categorical_fail)
        if tags:
            for key, value in tags.items():
                mlflow_module.set_tag(key, value)
        mlflow_module.set_tag("drift.status", assessment.status)
        mlflow_module.set_tag(
            "drift.retrain_recommended",
            str(assessment.retrain_recommended).lower(),
        )
        mlflow_module.set_tag(
            "drift.promotion_blocked",
            str(assessment.promotion_blocked).lower(),
        )
        if assessment.reasons:
            mlflow_module.set_tag("drift.reasons", "; ".join(assessment.reasons))
        mlflow_module.log_metrics(to_mlflow_metrics(metrics))
        return {
            "run_id": run.info.run_id,
            "metrics": metrics,
            "assessment": assessment,
        }


def _optional(value: str | None) -> list[str]:
    return [value] if value else []


def _common_selected_columns(baseline_df, candidate_df, requested_cols):
    baseline_cols = set(baseline_df.columns)
    candidate_cols = set(candidate_df.columns)
    return sorted(
        column
        for column in set(requested_cols)
        if column in baseline_cols and column in candidate_cols
    )

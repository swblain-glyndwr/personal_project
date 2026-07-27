# Databricks notebook source
# MAGIC %md
# MAGIC # Shopping Bag pCTR Spark Model Training
# MAGIC
# MAGIC This notebook trains a first experimental pCTR model for Shopping Bag advert
# MAGIC exposures. The objective is to estimate the probability that a known,
# MAGIC linkable customer clicks a tagged Next Ads banner within seven days of an
# MAGIC observed Shopping Bag exposure.
# MAGIC
# MAGIC The training data is the full unsampled output from
# MAGIC pctr_tagged_click_training. That table has one row per observed
# MAGIC account-ad exposure, click labels for several attribution windows, advert
# MAGIC context, and customer behaviour features.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports
# MAGIC

# COMMAND ----------

# MAGIC %pip install "xgboost==3.2.0" "protobuf==4.24.1" "sentence-transformers>=2.2.2,<=2.4.0" "transformers" "torch<2.12"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from datetime import datetime
import json
import math

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, NumericType, StringType

from xgboost.spark import SparkXGBClassifier

spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC This cell keeps the core experiment choices visible. The model uses the full
# MAGIC unsampled training table because the dataset is small enough for Databricks
# MAGIC and because downsampling negatives can distort probability calibration
# MAGIC unless we take extra care with weights and post-training correction.
# MAGIC
# MAGIC The target is `label_7d`, which means a row is positive if the customer clicked
# MAGIC the same campaign within seven days of the observed exposure. This gives the
# MAGIC model more positive examples than same-session or 24-hour labels, which is
# MAGIC useful for a first pCTR experiment where clicks are expected to be sparse.
# MAGIC
# MAGIC The default input table is the multi-snapshot training table. Each
# MAGIC `reference_date` in that table is a point-in-time build produced by the
# MAGIC feature notebooks. The model uses those monthly reference dates for a
# MAGIC time-based split: older months for training, then a later validation month,
# MAGIC then the newest test months.
# MAGIC
# MAGIC The snapshot table exists so model evaluation looks like production: train on
# MAGIC older point-in-time builds and test on later ones. The unsuffixed/latest
# MAGIC table is useful for debugging one run, but it is not enough for a seasonal
# MAGIC model because it only represents one cut of time.
# MAGIC
# MAGIC `snapshot_table_suffix` lets the same notebook read either the real backfill
# MAGIC table, usually `_snapshots`, or a throwaway test table such as `_smoke`.

# COMMAND ----------

def get_widget_value(name, default):
    try:
        dbutils.widgets.text(name, str(default))
        value = dbutils.widgets.get(name)
        return value if value not in (None, "") else default
    except NameError:
        return default


reference_date = get_widget_value("reference_date", "2026-04-15")
snapshot_table_suffix = get_widget_value("snapshot_table_suffix", "snapshots")
seed = 616

train_month_count = 15
validation_month_count = 1
test_month_count = 2

target_col = "label_7d"
label_col = "label"
weight_col = "weight"
features_col = "features"
prediction_col = "prediction"
probability_col = "probability"
raw_prediction_col = "rawPrediction"
score_col = "predicted_pctr"

dev_schema = spark.sql("SELECT current_user()").first()[0].split("@")[0].replace(".", "_")
training_input_tbl = get_widget_value(
    "training_input_tbl",
    f"marketingdata_dev.{dev_schema}.next_uk_pctr_sb_tagged_click_training_{snapshot_table_suffix}",
)

print(
    "pCTR Spark model training run config: "
    f"reference_date={reference_date}, "
    f"snapshot_table_suffix={snapshot_table_suffix}, "
    f"training_input_tbl={training_input_tbl}. "
    "Widget options: reference_date='YYYY-MM-DD'; snapshot_table_suffix is free text, "
    "for example 'snapshots' or 'smoke'; training_input_tbl is optional and overrides "
    "the default snapshot table. Model training expects the snapshot table to contain "
    "the configured train/validation/test months. Meaning: reference_date labels the "
    "experiment run, while the actual model split is driven by all reference_date "
    "partitions present in training_input_tbl."
)

experiment_path = "/Shared/mlflow/nextads/dev/experiments/pctr_spark_model"
registered_model_name = f"marketingdata_dev.{dev_schema}.nextads_pctr_spark_model"
register_best_model = True

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment(experiment_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load
# MAGIC
# MAGIC The input table is already at the modelling row grain: one row represents one
# MAGIC observed Shopping Bag advert exposure for one known account.
# MAGIC

# COMMAND ----------

df_raw = spark.table(training_input_tbl)

row_count = df_raw.count()

df_label_distribution = (
    df_raw
    .groupBy(F.col(target_col).cast("int").alias(target_col))
    .agg(F.count("*").alias("rows"))
    .orderBy(target_col)
)

display(df_label_distribution)

label_counts = {
    int(row[target_col]): int(row["rows"])
    for row in df_label_distribution.collect()
    if row[target_col] is not None
}

print(f"Loaded {row_count:,} rows from {training_input_tbl}")

# COMMAND ----------

df_snapshot_coverage = (
    df_raw
    .groupBy("reference_date")
    .agg(
        F.count("*").alias("rows"),
        F.countDistinct("account_number").alias("accounts"),
        F.sum(F.col(target_col).cast("int")).alias("positive_rows"),
        F.avg(F.col(target_col).cast("double")).alias("positive_rate"),
    )
    .orderBy("reference_date")
)

display(df_snapshot_coverage)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modelling label
# MAGIC

# COMMAND ----------

df_model_base = (
    df_raw
    .withColumn(label_col, F.col(target_col).cast("double"))
    .where(F.col(label_col).isin(0.0, 1.0))
)

display(df_model_base.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time-Based Snapshot Split
# MAGIC
# MAGIC The model is trained on older reference-date snapshots and evaluated on newer
# MAGIC snapshots. This is a closer proxy for production than random account splits
# MAGIC when the model needs to handle seasonal behaviour and future campaigns.

# COMMAND ----------

snapshot_dates = [row["reference_date"] for row in df_snapshot_coverage.select("reference_date").collect()]
snapshot_count = len(snapshot_dates)

if snapshot_count < train_month_count + validation_month_count + test_month_count:
    raise ValueError(
        "The seasonal pCTR model expects at least "
        f"{train_month_count + validation_month_count + test_month_count} reference-date snapshots. "
        f"Found {snapshot_count} in {training_input_tbl}."
    )

snapshot_split_window = Window.orderBy("reference_date")

snapshot_splits = (
    df_snapshot_coverage
    .select("reference_date")
    .withColumn("snapshot_index", F.row_number().over(snapshot_split_window))
    .withColumn("snapshot_count", F.lit(snapshot_count))
    .withColumn(
        "split",
        F.when(F.col("snapshot_index") <= F.col("snapshot_count") - validation_month_count - test_month_count, F.lit("train"))
        .when(F.col("snapshot_index") <= F.col("snapshot_count") - test_month_count, F.lit("validation"))
        .otherwise(F.lit("test")),
    )
)

df_model_split = (
    df_model_base
    .join(snapshot_splits.select("reference_date", "split"), on="reference_date", how="inner")
    .cache()
)

df_split_summary = (
    df_model_split
    .groupBy("split")
    .agg(
        F.count("*").alias("rows"),
        F.countDistinct("account_number").alias("accounts"),
        F.sum(label_col).alias("positive_rows"),
        F.avg(label_col).alias("positive_rate"),
    )
    .orderBy("split")
)

display(df_split_summary)

display(snapshot_splits)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature inclusion/leakage control
# MAGIC
# MAGIC This cell decides which columns are allowed into the model. The most important
# MAGIC rule is that a feature must be available at scoring time. Anything that is
# MAGIC only known after the click, such as `first_click_ts_7d`, is excluded because
# MAGIC it would leak the answer.
# MAGIC
# MAGIC We also remove raw identifiers and timestamps that are not suitable as direct
# MAGIC predictive features. Some contextual IDs such as advert and campaign fields
# MAGIC are kept because the model is being trained to score observed/current advert
# MAGIC candidates. The control-sheet theme and category fields are excluded because
# MAGIC they are low-information here; the advert-side signal should come from item,
# MAGIC catalogue, creative text, and semantic features.

# COMMAND ----------

leakage_and_non_feature_cols = {
    # Labels and post-click fields are outcomes.
    "label_same_session",
    "label_24h",
    "label_7d",
    "label",
    "first_click_ts_7d",
    "hours_to_first_click",
    # Raw identifiers and event timestamps define the row but shouldmn't be learned directly.
    "account_number",
    "unique_visit_id",
    "reference_date",
    "session_date",
    "exposure_ts",
    "advert_url",
    "split",
    "unique_ad_id",
    "assigned_unique_ad_id",
    "campaign_key",
    "assigned_campaign_key",
    "placement_id",
    "campaign_id",
    "treatment", "fallow_control", "exposure_source", "exposure_confidence", "accountnumberkey",
}

low_information_advert_cols = {
    # Useful for audit, but currently too sparse/generic to feed as model signal.
    "advert_theme",
    "advert_category",
}

excluded_feature_cols = leakage_and_non_feature_cols | low_information_advert_cols

available_cols = df_model_split.columns
candidate_feature_cols = [
    col_name for col_name in available_cols
    if col_name not in excluded_feature_cols
]

schema_by_name = {field.name: field.dataType for field in df_model_split.schema.fields}

numeric_feature_cols = [
    col_name for col_name in candidate_feature_cols
    if isinstance(schema_by_name[col_name], NumericType)
]

categorical_feature_cols = [
    col_name for col_name in candidate_feature_cols
    if isinstance(schema_by_name[col_name], (StringType, BooleanType))
]

unsupported_feature_cols = [
    col_name for col_name in candidate_feature_cols
    if col_name not in numeric_feature_cols and col_name not in categorical_feature_cols
]

final_feature_cols = numeric_feature_cols + categorical_feature_cols
leakage_overlap = sorted(set(final_feature_cols).intersection(excluded_feature_cols))

if leakage_overlap:
    raise ValueError(f"Excluded columns reached the feature list: {leakage_overlap}")

if not final_feature_cols:
    raise ValueError("No usable feature columns were found.")

df_feature_summary = spark.createDataFrame(
    [
        ("numeric_features", len(numeric_feature_cols), ", ".join(numeric_feature_cols[:25])),
        ("categorical_features", len(categorical_feature_cols), ", ".join(categorical_feature_cols[:25])),
        ("excluded_leakage_or_ids", len(leakage_and_non_feature_cols), ", ".join(sorted(leakage_and_non_feature_cols))),
        ("excluded_low_information_advert_cols", len(low_information_advert_cols), ", ".join(sorted(low_information_advert_cols))),
        ("unsupported_excluded", len(unsupported_feature_cols), ", ".join(unsupported_feature_cols[:25])),
    ],
    ["feature_group", "column_count", "example_columns"],
)

display(df_feature_summary)

print(f"Final feature count: {len(final_feature_cols):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Null handling and class weights
# MAGIC
# MAGIC Spark ML estimators require numeric feature vectors and do not handle arbitrary
# MAGIC null values. Numeric nulls are replaced with `0`, which means "no observed
# MAGIC value" for the count-style and recency-style features in this table.
# MAGIC Categorical nulls are replaced with the explicit string `__missing__`, so the
# MAGIC model can learn whether missingness itself carries signal.
# MAGIC
# MAGIC Click data is usually imbalanced: most observed adverts are not clicked. The
# MAGIC `weight` column gives positive and negative rows balanced total influence
# MAGIC during training. This helps the model pay attention to rare clicks without
# MAGIC having to downsample the full dataset.

# COMMAND ----------

positive_count = label_counts[1]
negative_count = label_counts[0]
total_labelled_count = positive_count + negative_count

positive_weight = total_labelled_count / (2.0 * positive_count)
negative_weight = total_labelled_count / (2.0 * negative_count)

df_prepared = df_model_split

for col_name in numeric_feature_cols:
    df_prepared = df_prepared.withColumn(
        col_name,
        F.coalesce(F.col(col_name).cast("double"), F.lit(0.0)),
    )

for col_name in categorical_feature_cols:
    df_prepared = df_prepared.withColumn(
        col_name,
        F.coalesce(F.col(col_name).cast("string"), F.lit("__missing__")),
    )

df_prepared = df_prepared.withColumn(
    weight_col,
    # Balance the total training influence of click and non-click rows without downsampling.
    F.when(F.col(label_col) == 1.0, F.lit(positive_weight)).otherwise(F.lit(negative_weight)),
)

train_df = df_prepared.where(F.col("split") == "train").cache()
validation_df = df_prepared.where(F.col("split") == "validation").cache()
test_df = df_prepared.where(F.col("split") == "test").cache()

display(
    df_prepared
    .groupBy("split", label_col)
    .agg(
        F.count("*").alias("rows"),
        F.avg(weight_col).alias("avg_weight"),
        F.sum(weight_col).alias("total_weight"),
    )
    .orderBy("split", label_col)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Shared feature pipeline
# MAGIC
# MAGIC All candidate models use the same preprocessing so that the comparison is
# MAGIC fair. Categorical columns are converted into numeric category indices using
# MAGIC `StringIndexer`, then expanded using `OneHotEncoder`. Numeric columns are
# MAGIC already cast to doubles. `VectorAssembler` then combines everything into the
# MAGIC single `features` vector expected by Spark ML classifiers.
# MAGIC
# MAGIC By separating preprocessing from the classifier, the logged MLflow artefact
# MAGIC contains the full transformation path: raw columns in, pCTR score out. That
# MAGIC makes the fitted model easier to reproduce and safer to use later.

# COMMAND ----------

categorical_index_cols = [f"{col_name}__idx" for col_name in categorical_feature_cols]
categorical_encoded_cols = [f"{col_name}__ohe" for col_name in categorical_feature_cols]

preprocessing_stages = []

if categorical_feature_cols:
    preprocessing_stages.append(
        StringIndexer(
            inputCols=categorical_feature_cols,
            outputCols=categorical_index_cols,
            handleInvalid="keep",
        )
    )
    preprocessing_stages.append(
        OneHotEncoder(
            inputCols=categorical_index_cols,
            outputCols=categorical_encoded_cols,
            handleInvalid="keep",
        )
    )

assembler_inputs = numeric_feature_cols + categorical_encoded_cols
preprocessing_stages.append(
    VectorAssembler(
        inputCols=assembler_inputs,
        outputCol=features_col,
        handleInvalid="keep",
    )
)

def build_pipeline(classifier):
    """Attach the shared preprocessing stages to one candidate classifier."""
    return Pipeline(stages=preprocessing_stages + [classifier])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluation helpers
# MAGIC PR-AUC is sensitive to performance on the positive class, which is what matters when clicks are
# MAGIC sparse. The helpers also calculate ROC-AUC, log loss, lift at the top scored
# MAGIC percentiles, and calibration deciles. Lift tells us whether the model
# MAGIC concentrates clicks near the top of the ranked list; calibration tells us
# MAGIC whether predicted probabilities are numerically close to observed click
# MAGIC rates.

# COMMAND ----------

def add_score_column(predictions):
    """Extract the positive-class probability from Spark's probability vector."""
    return predictions.withColumn(score_col, vector_to_array(F.col(probability_col)).getItem(1))


def binary_metrics(predictions):
    """Calculate ranking and probability metrics for a scored Spark DataFrame."""
    scored = add_score_column(predictions).cache()

    null_prediction_count = scored.where(F.col(score_col).isNull()).count()
    score_bounds = scored.agg(
        F.min(score_col).alias("min_score"),
        F.max(score_col).alias("max_score"),
    ).first()

    if null_prediction_count != 0:
        raise ValueError(f"Found {null_prediction_count:,} null pCTR predictions.")

    if score_bounds["min_score"] < 0.0 or score_bounds["max_score"] > 1.0:
        raise ValueError(
            "Predicted pCTR values must be between 0 and 1. "
            f"Observed min={score_bounds['min_score']}, max={score_bounds['max_score']}."
        )

    evaluator_pr = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol=raw_prediction_col,
        metricName="areaUnderPR",
    )
    evaluator_roc = BinaryClassificationEvaluator(
        labelCol=label_col,
        rawPredictionCol=raw_prediction_col,
        metricName="areaUnderROC",
    )

    eps = 1e-15
    aggregate_row = (
        scored
        .withColumn("score_clipped", F.least(F.greatest(F.col(score_col), F.lit(eps)), F.lit(1.0 - eps)))
        .agg(
            F.count("*").alias("rows"),
            F.avg(label_col).alias("positive_rate"),
            F.avg(
                -(
                    F.col(label_col) * F.log(F.col("score_clipped"))
                    + (1.0 - F.col(label_col)) * F.log(1.0 - F.col("score_clipped"))
                )
            ).alias("log_loss"),
        )
        .first()
    )

    metrics = {
        "rows": float(aggregate_row["rows"]),
        "positive_rate": float(aggregate_row["positive_rate"]),
        "areaUnderPR": float(evaluator_pr.evaluate(scored)),
        "areaUnderROC": float(evaluator_roc.evaluate(scored)),
        "log_loss": float(aggregate_row["log_loss"]),
        "min_score": float(score_bounds["min_score"]),
        "max_score": float(score_bounds["max_score"]),
    }

    total_rows = int(aggregate_row["rows"])
    baseline_rate = float(aggregate_row["positive_rate"])
    ranking_window = Window.orderBy(F.col(score_col).desc())
    ranked = scored.withColumn("score_rank", F.row_number().over(ranking_window))

    for pct in [0.01, 0.05, 0.10]:
        # Lift asks whether the highest-scored rows click more often than the average row.
        cutoff = max(1, int(math.ceil(total_rows * pct)))
        top_rows = ranked.where(F.col("score_rank") <= cutoff)
        top_metrics = top_rows.agg(
            F.avg(label_col).alias("precision_at_pct"),
            F.sum(label_col).alias("positive_rows_at_pct"),
        ).first()
        precision_at_pct = float(top_metrics["precision_at_pct"])
        positive_rows_at_pct = float(top_metrics["positive_rows_at_pct"])
        metrics[f"precision_at_top_{int(pct * 100)}pct"] = precision_at_pct
        metrics[f"recall_at_top_{int(pct * 100)}pct"] = positive_rows_at_pct / (baseline_rate * total_rows)
        metrics[f"lift_at_top_{int(pct * 100)}pct"] = precision_at_pct / baseline_rate if baseline_rate else 0.0

    scored.unpersist()
    return metrics


def calibration_deciles(predictions):
    """Summarise predicted versus actual click rate by descending score decile."""
    scored = add_score_column(predictions)
    decile_window = Window.orderBy(F.col(score_col).desc())
    return (
        scored
        # Deciles make calibration readable: predicted probability versus actual click rate.
        .withColumn("score_decile", F.ntile(10).over(decile_window))
        .groupBy("score_decile")
        .agg(
            F.count("*").alias("rows"),
            F.avg(score_col).alias("avg_predicted_pctr"),
            F.avg(label_col).alias("actual_click_rate"),
            F.sum(label_col).alias("clicked_rows"),
        )
        .orderBy("score_decile")
    )


def top_percentile_lift_table(predictions):
    """Create a display table for the best model's lift at top score cut-offs."""
    scored = add_score_column(predictions).cache()
    total_rows = scored.count()
    baseline_rate = scored.agg(F.avg(label_col).alias("positive_rate")).first()["positive_rate"]
    ranking_window = Window.orderBy(F.col(score_col).desc())
    ranked = scored.withColumn("score_rank", F.row_number().over(ranking_window))

    rows = []
    for pct in [0.01, 0.05, 0.10]:
        cutoff = max(1, int(math.ceil(total_rows * pct)))
        row = (
            ranked
            .where(F.col("score_rank") <= cutoff)
            .agg(
                F.count("*").alias("rows"),
                F.avg(label_col).alias("click_rate"),
                F.sum(label_col).alias("clicked_rows"),
            )
            .first()
        )
        rows.append(
            (
                f"top_{int(pct * 100)}pct",
                int(row["rows"]),
                float(row["click_rate"]),
                float(row["clicked_rows"]),
                float(row["click_rate"] / baseline_rate) if baseline_rate else 0.0,
            )
        )

    scored.unpersist()
    return spark.createDataFrame(
        rows,
        ["segment", "rows", "click_rate", "clicked_rows", "lift_vs_average"],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Candidate Models
# MAGIC
# MAGIC The experiment compares simple and more flexible models. Logistic Regression
# MAGIC is the baseline: if complex models cannot beat it, that tells us either the
# MAGIC signal is mostly linear or the feature set needs more work. Random Forest is
# MAGIC a non-linear benchmark. GBT is Spark ML's built-in boosted tree model.
# MAGIC
# MAGIC XGBoost is included because gradient-boosted trees are often strong on click
# MAGIC prediction problems and handle non-linear interactions between advert,
# MAGIC customer, calendar, and product-match features.

# COMMAND ----------

xgb_num_round = 150

model_specs = [
    {
        "model_name": "logistic_regression",
        "estimator": LogisticRegression(
            featuresCol=features_col,
            labelCol=label_col,
            weightCol=weight_col,
            predictionCol=prediction_col,
            probabilityCol=probability_col,
            rawPredictionCol=raw_prediction_col,
            maxIter=50,
            regParam=0.01,
            elasticNetParam=0.0,
        ),
        "params": {
            "maxIter": 50,
            "regParam": 0.01,
            "elasticNetParam": 0.0,
        },
    },
    {
        "model_name": "random_forest",
        "estimator": RandomForestClassifier(
            featuresCol=features_col,
            labelCol=label_col,
            weightCol=weight_col,
            predictionCol=prediction_col,
            probabilityCol=probability_col,
            rawPredictionCol=raw_prediction_col,
            numTrees=120,
            maxDepth=8,
            minInstancesPerNode=20,
            seed=seed,
        ),
        "params": {
            "numTrees": 120,
            "maxDepth": 8,
            "minInstancesPerNode": 20,
            "seed": seed,
        },
    },
    {
        "model_name": "gbt",
        "estimator": GBTClassifier(
            featuresCol=features_col,
            labelCol=label_col,
            weightCol=weight_col,
            predictionCol=prediction_col,
            maxIter=80,
            maxDepth=5,
            stepSize=0.05,
            minInstancesPerNode=20,
            seed=seed,
        ),
        "params": {
            "maxIter": 80,
            "maxDepth": 5,
            "stepSize": 0.05,
            "minInstancesPerNode": 20,
            "seed": seed,
        },
    },
    {
        "model_name": "spark_xgboost",
        "estimator": SparkXGBClassifier(
            features_col=features_col,
            label_col=label_col,
            weight_col=weight_col,
            prediction_col=prediction_col,
            probability_col=probability_col,
            raw_prediction_col=raw_prediction_col,
            eval_metric="aucpr",
            max_depth=6,
            eta=0.05,
            n_estimators=xgb_num_round,
            subsample=0.8,
            colsample_bytree=0.8,
            seed=seed,
            num_workers=4,
        ),
        "params": {
            "eval_metric": "aucpr",
            "max_depth": 6,
            "eta": 0.05,
            "num_round": xgb_num_round,
            "n_estimators": xgb_num_round,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "seed": seed,
            "num_workers": 4,
        },
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train, validate, and log models with MLflow
# MAGIC
# MAGIC MLflow acts as the experiment ledger. Each run records the input table,
# MAGIC target, feature list, excluded leakage columns, model parameters, validation
# MAGIC metrics, and fitted model artefact. This matters because modelling work is
# MAGIC comparative: a useful experiment is one where we can later explain exactly
# MAGIC which data, features, and parameters produced each result.
# MAGIC
# MAGIC Every candidate is trained on the training split and evaluated on the
# MAGIC validation split. The test split is intentionally not used in this loop. It
# MAGIC remains untouched until the best validation model has been selected.

# COMMAND ----------

from datetime import timezone

run_timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
validation_results = []
models_by_name = {}

split_counts = {
    row["split"]: {
        "rows": row["rows"],
        "accounts": row["accounts"],
        "positive_rows": row["positive_rows"],
        "positive_rate": row["positive_rate"],
    }
    for row in df_split_summary.collect()
}

for spec in model_specs:
    model_name = spec["model_name"]
    pipeline = build_pipeline(spec["estimator"])
    run_name = f"pctr_7d_{model_name}_{run_timestamp}"

    with mlflow.start_run(run_name=run_name) as run:
        mlflow.log_param("model_name", model_name)
        mlflow.log_param("input_table", training_input_tbl)
        mlflow.log_param("target_col", target_col)
        mlflow.log_param("label_col", label_col)
        mlflow.log_param("reference_date", reference_date)
        mlflow.log_param("seed", seed)
        mlflow.log_param("train_month_count", train_month_count)
        mlflow.log_param("validation_month_count", validation_month_count)
        mlflow.log_param("test_month_count", test_month_count)
        mlflow.log_param("snapshot_count", snapshot_count)
        mlflow.log_param("feature_count", len(final_feature_cols))
        mlflow.log_param("numeric_feature_count", len(numeric_feature_cols))
        mlflow.log_param("categorical_feature_count", len(categorical_feature_cols))
        mlflow.log_param("positive_weight", positive_weight)
        mlflow.log_param("negative_weight", negative_weight)
        mlflow.log_params({f"model__{key}": value for key, value in spec["params"].items()})

        for split_name, counts in split_counts.items():
            mlflow.log_metric(f"{split_name}_rows", counts["rows"])
            mlflow.log_metric(f"{split_name}_accounts", counts["accounts"])
            mlflow.log_metric(f"{split_name}_positive_rows", counts["positive_rows"])
            mlflow.log_metric(f"{split_name}_positive_rate", counts["positive_rate"])

        mlflow.log_text(
            json.dumps(final_feature_cols, indent=2),
            "feature_columns.json",
        )
        mlflow.log_text(
            json.dumps(numeric_feature_cols, indent=2),
            "numeric_feature_columns.json",
        )
        mlflow.log_text(
            json.dumps(categorical_feature_cols, indent=2),
            "categorical_feature_columns.json",
        )
        mlflow.log_text(
            json.dumps(sorted(leakage_and_non_feature_cols), indent=2),
            "excluded_columns.json",
        )
        mlflow.log_text(
            json.dumps(sorted(low_information_advert_cols), indent=2),
            "excluded_low_information_advert_columns.json",
        )
        mlflow.log_table(
            df_snapshot_coverage.toPandas(),
            artifact_file="snapshot_coverage.json",
        )
        mlflow.log_table(
            snapshot_splits.toPandas(),
            artifact_file="snapshot_splits.json",
        )

        print(f"Training {model_name}...")
        fitted_pipeline = pipeline.fit(train_df)
        models_by_name[model_name] = fitted_pipeline

        validation_predictions = fitted_pipeline.transform(validation_df)
        validation_metrics = binary_metrics(validation_predictions)
        validation_calibration = calibration_deciles(validation_predictions)
        mlflow.log_metrics({f"validation_{key}": value for key, value in validation_metrics.items()})
        mlflow.log_table(
            validation_calibration.toPandas(),
            artifact_file="validation_calibration_deciles.json",
        )

        mlflow.spark.log_model(fitted_pipeline, artifact_path="model")

        validation_results.append(
            {
                "model_name": model_name,
                "run_id": run.info.run_id,
                **{f"validation_{key}": value for key, value in validation_metrics.items()},
            }
        )

df_validation_results = spark.createDataFrame(validation_results)
display(df_validation_results.orderBy(F.col("validation_areaUnderPR").desc()))

if df_validation_results.count() != len(model_specs):
    raise ValueError("Expected MLflow runs and validation results for every configured candidate model.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select the best validation model
# MAGIC
# MAGIC The best candidate is selected using validation PR-AUC. PR-AUC is the primary
# MAGIC because the positive class is sparse and the modelling goal is to
# MAGIC identify rows with higher click probability.
# MAGIC
# MAGIC The final test metrics are calculated only after model selection. This keeps
# MAGIC the test set as a fair final check rather than a dataset that influences
# MAGIC modelling choices.

# COMMAND ----------

best_validation_row = (
    df_validation_results
    .orderBy(F.col("validation_areaUnderPR").desc())
    .first()
)

best_model_name = best_validation_row["model_name"]
best_run_id = best_validation_row["run_id"]
best_model = models_by_name[best_model_name]

print(f"Best validation model: {best_model_name}")
print(f"Best validation run ID: {best_run_id}")

test_predictions = best_model.transform(test_df).cache()
test_metrics = binary_metrics(test_predictions)

with mlflow.start_run(run_id=best_run_id):
    mlflow.log_metrics({f"test_{key}": value for key, value in test_metrics.items()})

df_test_metrics = spark.createDataFrame(
    [(key, float(value)) for key, value in sorted(test_metrics.items())],
    ["metric", "value"],
)

display(df_test_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calibration and Lift for the best model
# MAGIC
# MAGIC Calibration compares predicted probabilities with observed click rates. If a
# MAGIC decile has an average predicted pCTR of 0.02, a well-calibrated model should
# MAGIC have an actual click rate near 2% in that decile. Early experimental models
# MAGIC do not need perfect calibration, but the relationship should be directionally
# MAGIC sensible.
# MAGIC
# MAGIC Lift shows whether the model concentrates clicks near the top of the scored
# MAGIC population. For example, lift of `3.0` in the top 5% means the top-scored 5%
# MAGIC clicks at three times the average rate. This is often more useful than raw
# MAGIC accuracy for advertising decisions, because we usually care about ranking and
# MAGIC prioritisation.

# COMMAND ----------

df_best_calibration_deciles = calibration_deciles(test_predictions)
display(df_best_calibration_deciles)

df_best_lift = top_percentile_lift_table(test_predictions)
display(df_best_lift)

with mlflow.start_run(run_id=best_run_id):
    mlflow.log_table(
        df_best_calibration_deciles.toPandas(),
        artifact_file="test_calibration_deciles.json",
    )
    mlflow.log_table(
        df_best_lift.toPandas(),
        artifact_file="test_top_percentile_lift.json",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the best candidate
# MAGIC
# MAGIC Registering the best model creates a versioned artefact in Unity Catalog model
# MAGIC registry. This does not make the experiment production-ready on its own, but
# MAGIC it gives the selected candidate a durable name and version so it can be
# MAGIC inspected, promoted, or used by a later scoring notebook.

# COMMAND ----------

if register_best_model:
    model_uri = f"runs:/{best_run_id}/model"
    registered_model = mlflow.register_model(
        model_uri=model_uri,
        name=registered_model_name,
    )

    client = MlflowClient()
    client.set_registered_model_alias(
        name=registered_model_name,
        alias="dev_candidate",
        version=registered_model.version,
    )

    print(
        f"Registered {best_model_name} as {registered_model_name} "
        f"version {registered_model.version} with alias dev_candidate"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final experiment summary
# MAGIC
# MAGIC The tables below are the headline outputs of the notebook:
# MAGIC
# MAGIC - split counts and positive rates show whether the training, validation, and
# MAGIC   test populations look sensible
# MAGIC - validation metrics compare all configured candidate models on the same
# MAGIC   holdout month
# MAGIC - test metrics describe the selected model on data that was not used for
# MAGIC   model choice
# MAGIC - calibration and lift tables show whether the best model's scores are useful
# MAGIC   for pCTR ranking and probability interpretation

# COMMAND ----------

display(df_split_summary)
display(df_validation_results.orderBy(F.col("validation_areaUnderPR").desc()))
display(df_test_metrics)
display(df_best_calibration_deciles)
display(df_best_lift)

print(f"Best model: {best_model_name}")
print(f"MLflow run: {best_run_id}")
print(f"Experiment: {experiment_path}")
print(f"Registered model: {registered_model_name if register_best_model else 'registration disabled'}")

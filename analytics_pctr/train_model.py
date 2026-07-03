# COMMAND ---------- [markdown]
# # Two Stage Model
# 
# * Primary Classification model is a high level 'popularity' model based on advert and customer features
# * Secondary Regression Model is a customer-advert 'affinity' model minimising the residuals from the first model using only affinity features 
# 
# 
# 

# COMMAND ----------
import json
import pandas as pd
import mlflow
import mlflow.spark
from mlflow.models import infer_signature

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import Window
import matplotlib.pyplot as plt
import seaborn as sns

# Pipelining
from pyspark.ml.feature import VectorAssembler, Imputer
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.functions import vector_to_array

# Models
from xgboost.spark import SparkXGBClassifier, SparkXGBRegressor

# ## Evaluation
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    RegressionEvaluator,
)

# COMMAND ----------
# Spark Performance Settings
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# COMMAND ---------- [markdown]
# ## General Functions

# COMMAND ----------
def get_widget_value(name, default):
    try:
        dbutils.widgets.text(name, str(default))
        value = dbutils.widgets.get(name)
        return value if value not in (None, "") else default
    except NameError:
        return default


def validate_positive_int(name, value):
    parsed_value = int(value)
    if parsed_value <= 0:
        raise ValueError(
            f"Invalid widget value for {name}: {value}. Value must be a positive integer."
        )
    return parsed_value

# COMMAND ----------
dbutils.widgets.text(
    name="catalog_schema_prefix",
    defaultValue="marketingdata_dev.claire_wilsonbarnes",
    label="catalog_schema_prefix",
)
dbutils.widgets.text(
    name="table_prefix",
    defaultValue="next_uk_nextAds_analytics_pctr",
    label="table_prefix",
)

dbutils.widgets.text(
    name="lookback_period", defaultValue="30", label="lookback_period"
)

catalog_schema_prefix = get_widget_value(
    "catalog_schema_prefix", "marketingdata_dev.claire_wilsonbarnes"
)
lookback_period = int(get_widget_value("lookback_period", "30"))
table_prefix = get_widget_value(
    "table_prefix", "next_uk_nextAds_analytics_pctr"
)

## mlflow
experiment_path = "/Workspace/Users/claire_wilsonbarnes@next.co.uk/mlflow/nextads/dev/experiments/analytics_pctr_model"
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(experiment_path)

# COMMAND ---------- [markdown]
# ## Variables

# COMMAND ----------
## Training Data parameters
training_data_table = (
    f"{catalog_schema_prefix}.{table_prefix}_training_history"
)
final_validation_data_table = (
    f"{catalog_schema_prefix}.{table_prefix}_validation_history"
)

classifier_registered_model_name = "marketingdata_dev.ds_sandbox.nextads_analytics_pctr_popularity_classification_model"
regresssor_registered_model_name = "marketingdata_dev.ds_sandbox.nextads_analytics_pctr_affinity_regression_model"

# Due to missing click data can ONLY use data from '2026-04-29' onwards currently ( OR use data prior to 19/02/2026) to ensure no skew in the metrics being utlised over the 30 day time window
start_date_filter = "2026-04-29"
# Sample via timeseries
# split into 3 datasets- training, validation and test
# Final Validation = 1st-7th June ( with ALL adverts for all customers)
train_cutoff_date = "2026-05-23"
validation_cutoff_date = "2026-05-29"

# Sample weighting parameters
# Add more weight to a Basic reccomendation that the Best reccomendations
# Approx 10% of ads get basic targeting
advert_proportions = {"Basic": 0.1, "Best": 0.9}
# To dampen the impact & reduce variance - might need to tune this!
alpha = 0.5
random_seed = 553


primary_keys = ["account_number", "uniqueAdID", "rundate"]

fill_zeros_columns = {
    "day_impressions": 0,
    "prior_day_impressions": 0,
    "week_impressions": 0,
    "prior_week_impressions": 0,
    "view_highest_catid_weight": 0,
    "view_lift_adjusted": 0,
    "view_cs": 0,
    "purchase_highest_catid_weight": 0,
    "purchase_lift_adjusted": 0,
    "purchase_cs": 0,
}

imputation_columns = {"age": "age_imputed"}

non_feature_cols = [
    "potnumber",
    "campaignnumber",
    # Included for potential  pre-filtering for low visibility ads
    "total_ad_impressions",
    "total_ad_perc_impressions",
    "total_ad_cumulativeperc_impresssions",
    "gender",
    "postcodearea",
    "primary_advert_location",
    "view_support12",
    "view_support1",
    "view_lift",
    "purchase_support12",
    "purchase_support1",
    "purchase_support2",
    "pruchase_lift",
    "treatment_typelocation",
]
removed_feature_cols = [
    "mailoptout",
    "staff_indicator",
    "channel_ctr",
    "dayofweek_ctr",
    "perc_viewtimedow",
    "customer_lifespan",
    "total_time_on_site",
    "total_sessions",
    "avg_site_time",
    "med_time_onsite",
    "number_departments_viewed",
    "number_pages_viewed_last_week",
    "total_order_value",
    "total_number_items",
    "customer_total_impressions",
    "customer_total_unique_adverts",
    "customer_advert_previous_impression_number",
    "number_algodivisions_clicked",
    "number_algodivisions_impressions",
    "number_impressions_same_algodivision",
    "number_unique_adverts_same_algodivision",
    "number_unique_adverts_clicked_same_algodivision",
    "channel_impressions",
    "dayofweek_impressions",
    "week_impressions",
    "prior_week_impressions",
    "highest_spend_cat_alignment",
    "view_cs",
    "purchase_cs",
]

popularity_feature_columns = [
    "cash_acc",
    "advert_ctr",
    "device_ctr",
    "geo_ctr",
    "gender_ctr",
    "dod_ctr_change",
    "wow_ctr_change",
    "age_imputed",
    "number_pages_viewed",
    "prior_30_day_order_value",
    "customer_total_clicks",
    "customer_total_unique_adverts_clicked",
    "customer_advert_previous_click_number",
    "number_clicks_same_algodivision",
    "advert_impressions",
    "device_impressions",
    "geo_impressions",
    "gender_impressions",
    "day_impressions",
    "prior_day_impressions",
]


affinity_feature_columns = [
    "view_theme_score",
    "perc_order_value_cat_affinity",
    "perc_30_day_order_value_cat_affinity",
    "perc_order_qty_cat_affinity",
    "view_highest_catid_weight",
    "view_lift_adjusted",
    "purchase_highest_catid_weight",
    "purchase_lift_adjusted",
    "purchase_theme_affinity",
]

### Column naming parameters
## Popularity Classifier
popularity_target_col = "ad_clicked"
weighting_column = "sample_weight"
popularity_prediction_col = "prediction"
popularity_probability_col = "probability"
popularity_raw_prediction_col = "rawPrediction"
popularity_vector_assembler_features = "features"
popularity_click_prob_col = "popularity_prob_click"

# Affinity Regressor
regressor_vector_assembler_features = "regressor_features"
regressor_target_col = "residuals"
regressor_predictions_col = "residual_predictions"

## Ranking
popularity_smoothed_score_col = "popularity_smoothed_score"
regression_weighted_score_col = "regression_weighted_score"
combined_score_col = "combined_weighted_score"
weighted_ranking_col = "weighted_ranking"

# COMMAND ----------
## Pipeline components
# Preprocessing Popularity Model
# Inmputing age
imputer = Imputer(
    strategy="median",
    inputCols=list(imputation_columns.keys()),
    outputCols=list(imputation_columns.values()),
)
popularity_vector_assembler = VectorAssembler(
    inputCols=popularity_feature_columns,
    outputCol=popularity_vector_assembler_features,
)

## Affinity Regressor Model
regressor_vector_assembler = VectorAssembler(
    inputCols=affinity_feature_columns,
    outputCol=regressor_vector_assembler_features,
)
models_dict = {
    "popularity_classifier": [
        {
            "model_name": "spark_xgboost_classifier",
            "estimator": SparkXGBClassifier(
                features_col=popularity_vector_assembler_features,
                label_col=popularity_target_col,
                prediction=popularity_prediction_col,
                raw_prediction=popularity_raw_prediction_col,
                probability=popularity_probability_col,
                weight_col=weighting_column,
                seed=random_seed,
            ),
            "params": {
                "max_depth": 6,
                "n_estimators": 200,
                "learning_rate": 0.05,
                "eval_metric": "aucpr",
                "colsample_bytree": 0.4,
                "min_child_weight": 1,
            },
        }
    ],
    "affinity_regressor": [
        {
            "model_name": "spark_xgboost_regressor",
            "estimator": SparkXGBRegressor(
                features_col=regressor_vector_assembler_features,
                label_col=regressor_target_col,
                prediction_col=regressor_predictions_col,
                weight_col=weighting_column,
                seed=random_seed,
            ),
            "params": {
                "max_depth": 4,
                "n_estimators": 200,
                "learning_rate": 0.05,
                "eval_metric": "rmse",
                "colsample_bytree": 1,
                "min_child_weight": 1,
            },
        }
    ],
}

# COMMAND ----------
### Popularity Classifier Model Evaluators
# Training Evaluators build
areaunderPR_eval = BinaryClassificationEvaluator(
    labelCol=popularity_target_col,
    metricName="areaUnderPR",
    weightCol=weighting_column,
    rawPredictionCol=popularity_raw_prediction_col,
)
areaunderroc_eval = BinaryClassificationEvaluator(
    labelCol=popularity_target_col,
    metricName="areaUnderROC",
    rawPredictionCol=popularity_raw_prediction_col,
    weightCol=weighting_column,
)
## Validation Evaluators Build
unweighted_areaunderPR_eval = BinaryClassificationEvaluator(
    labelCol=popularity_target_col,
    metricName="areaUnderPR",
    rawPredictionCol=popularity_raw_prediction_col,
)
unweighted_areaunderroc_eval = BinaryClassificationEvaluator(
    labelCol=popularity_target_col,
    metricName="areaUnderROC",
    rawPredictionCol=popularity_raw_prediction_col,
)

### Affinity Regressor Model Evaluators
rmse_eval = RegressionEvaluator(
    labelCol=regressor_target_col,
    predictionCol=regressor_predictions_col,
    metricName="rmse",
)

# COMMAND ---------- [markdown]
# ## Model Functions

# COMMAND ----------
def create_sample_weight(df, input_col, output_col, percentile_split, alpha):

    if len(percentile_split) != 2:
        print(
            f"Error expected 2 values for the percentile split but recieved {len(percentile_split)}"
        )
        df = df.withColumn(output_col, F.lit(1))
        return df

    percentile_keys = list(percentile_split.keys())
    percentile_values = list(percentile_split.values())
    df = df.withColumn(
        "raw_weight",
        F.when(
            F.col(input_col) == percentile_keys[0], 1 / percentile_values[0]
        ).otherwise(1 / percentile_values[1]),
    ).withColumn("weight", F.pow(F.col("raw_weight"), F.lit(alpha)))
    weight_sum, weight_count = df.agg(
        F.sum("weight"), F.count("weight")
    ).collect()[0]
    df = df.withColumn(
        "sample_weight", F.col("weight") * F.lit(weight_count / weight_sum)
    )
    return df

# COMMAND ----------
def classification_eval(df, include_weighting=True):

    auPR = unweighted_areaunderPR_eval.evaluate(df)
    auroc = unweighted_areaunderroc_eval.evaluate(df)
    results = {
        "auPR": auPR,
        "auROC": auroc,
    }
    if include_weighting:
        auPRweighted = areaunderPR_eval.evaluate(df)
        aurocweighted = areaunderroc_eval.evaluate(df)
        results["auPR_weighted"] = auPRweighted
        results["auROC_weighted"] = aurocweighted
    return results


def regression_eval(df):
    rmse = rmse_eval.evaluate(df)
    return {"rmse": rmse}

# COMMAND ----------
def run_model_training(
    model_type: str,
    model_specs: list,
    train_df,
    val_df,
    preprocessing_steps: list,
    target_col,
    label_col,
    final_feature_cols,
    random_seed,
    split_df_summary,
    weighted_model: bool = False,
    targeting_details={},
):

    print("Starting Model Training")
    best_model_run_id = None
    best_model_estimator_score = 0

    for spec in model_specs:
        current_result = None
        run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_name = spec.get("model_name")
        estimator = spec.get("estimator")
        estimator.setParams(**spec.get("params"))
        pipeline = Pipeline(stages=preprocessing_steps + [estimator])
        run_name = f"analytics_pctr_{model_name}_{run_timestamp}"
        print(f"Running Model {run_name}")
        try:
            with mlflow.start_run(run_name=run_name) as run:
                print(f"Training Model {model_name}")
                mlflow.log_param("model_name", model_name)
                mlflow.log_param("input_table", train_df)
                mlflow.log_param("target_col", target_col)
                mlflow.log_param("label_col", label_col)
                mlflow.log_param("seed", random_seed)
                mlflow.log_param("feature_count", len(final_feature_cols))
                if weighted_model:
                    mlflow.log_params(
                        {
                            f"weighting__{key}": value
                            for key, value in targeting_details.items()
                        }
                    )
                mlflow.log_params(
                    {
                        f"model__{key}": value
                        for key, value in spec.get("params").items()
                    }
                )
                mlflow.log_text(
                    json.dumps(final_feature_cols, indent=2),
                    "feature_columns.json",
                )

                mlflow.log_table(
                    split_df_summary.toPandas(),
                    artifact_file="train_val_test_splits.json",
                )
                # Train pipeline
                print(f"Training pipeline for {model_type} {model_name}")
                trained_model = pipeline.fit(train_df)
                # Get training metrics
                pred_train = trained_model.transform(train_df)
                pred_val = trained_model.transform(val_df)

                if model_type == "popularity_classifier":
                    train_eval = classification_eval(pred_train)
                    for metric, value in train_eval.items():
                        mlflow.log_metric(f"train_{metric}", value)
                    # Get validation metrics
                    val_eval = classification_eval(pred_val)
                    for metric, value in val_eval.items():
                        mlflow.log_metric(f"val_{metric}", value)
                    current_result = (
                        val_eval["auPR_weighted"]
                        if weighted_model
                        else val_eval["auPR"]
                    )
                    if current_result > best_model_estimator_score:
                        best_model_estimator_score = current_result
                        best_model_run_id = run.info.run_id

                elif model_type == "affinity_regressor":
                    # Training metrics:
                    train_eval = regression_eval(pred_train)
                    for metric, value in train_eval.items():
                        mlflow.log_metric(f"train_{metric}", value)
                    # Validation metrics:
                    val_eval = regression_eval(pred_val)
                    for metric, value in val_eval.items():
                        mlflow.log_metric(f"val_{metric}", value)

                    current_result = val_eval["rmse"]

                    if (
                        current_result < best_model_estimator_score
                        or best_model_estimator_score == 0
                    ):
                        best_model_estimator_score = current_result
                        best_model_run_id = run.info.run_id
                else:
                    print(f"Error - unknown model type {model_type}")
                signature = infer_signature(
                    model_input=train_df.sample(
                        fraction=0.01, seed=random_seed
                    ),
                    model_output=pred_train.sample(
                        fraction=0.01, seed=random_seed
                    ),
                )
                mlflow.spark.log_model(
                    trained_model, artifact_path="model", signature=signature
                )
        except Exception as e:
            print(f"Error training model {model_name}: {e}")
            mlflow.end_run()
    return best_model_run_id

# COMMAND ---------- [markdown]
# ## Training
# 

# COMMAND ----------
# Core training dataset

df = spark.table(training_data_table)
# Preprocessing
# Setting a hard limit of start date due to data issues - will be a non-issue for future
df = df.filter(F.col("rundate") >= start_date_filter)
df = df.fillna(fill_zeros_columns)

# COMMAND ----------
df = df.withColumn(
    "split_type",
    F.when(F.col("rundate") < train_cutoff_date, "train")
    .when(F.col("rundate") >= validation_cutoff_date, "test")
    .otherwise("validation"),
)

# Split of datasets for training
train_df = df.filter(F.col("split_type") == "train")
validation_df = df.filter((F.col("split_type") == "validation"))
test_df = df.filter(F.col("split_type") == "test")

# View of splits
split_df_summary = (
    df.groupBy("split_type")
    .agg(
        F.count("*").alias("records"),
        F.countDistinct("account_number").alias("accounts"),
        F.sum("ad_clicked").alias("positive_rows"),
    )
    .withColumn(
        "percent_positive",
        F.round(F.col("positive_rows") / F.col("records"), 4),
    )
)
display(split_df_summary)

# COMMAND ----------
# Use Treatment type as sample weighting for training (addition to sampel sets for validation metrics)
train_df = create_sample_weight(
    train_df, "treatment_type", "sample_weight", advert_proportions, alpha
)
validation_df = create_sample_weight(
    validation_df, "treatment_type", "sample_weight", advert_proportions, alpha
)
test_df = create_sample_weight(
    test_df, "treatment_type", "sample_weight", advert_proportions, alpha
)

# COMMAND ----------
# Train the Classifier:

classifier_model_run_id = run_model_training(
    "popularity_classifier",
    models_dict.get("popularity_classifier", []),
    train_df,
    validation_df,
    [imputer, popularity_vector_assembler],
    popularity_target_col,
    popularity_probability_col,
    popularity_feature_columns,
    random_seed,
    split_df_summary,
    True,
    advert_proportions | {"alpha": alpha},
)

# COMMAND ----------
## Create the training set for the Regressor:
popularity_model = mlflow.spark.load_model(
    f"runs:/{classifier_model_run_id}/model"
)
train_df_popularity_scored = popularity_model.transform(train_df)
val_df_popularity_scored = popularity_model.transform(validation_df)

# Calculate the residuals for the Regressor model
train_df_popularity_scored = train_df_popularity_scored.withColumn(
    popularity_click_prob_col,
    vector_to_array(F.col(popularity_probability_col))[1],
).withColumn(
    regressor_target_col,
    F.col(popularity_target_col) - F.col(popularity_click_prob_col),
)
val_df_popularity_scored = val_df_popularity_scored.withColumn(
    popularity_click_prob_col,
    vector_to_array(F.col(popularity_probability_col))[1],
).withColumn(
    regressor_target_col,
    F.col(popularity_target_col) - F.col(popularity_click_prob_col),
)

# COMMAND ----------
regressor_model_run_id = run_model_training(
    "affinity_regressor",
    models_dict.get("affinity_regressor", []),
    train_df_popularity_scored,
    val_df_popularity_scored,
    [regressor_vector_assembler],
    regressor_target_col,
    regressor_predictions_col,
    affinity_feature_columns,
    random_seed,
    split_df_summary,
    True,
)

affinity_model = mlflow.spark.load_model(
    f"runs:/{regressor_model_run_id}/model"
)

# COMMAND ----------
# Run test model scoring
test_df_popularity_scored = popularity_model.transform(test_df)
test_eval = classification_eval(test_df_popularity_scored)
for metric, value in test_eval.items():
    mlflow.log_metric(f"test_{metric}", value, run_id=classifier_model_run_id)

# Calculate the residuals for the Regressor model
test_df_popularity_scored = test_df_popularity_scored.withColumn(
    popularity_click_prob_col,
    vector_to_array(F.col(popularity_probability_col))[1],
).withColumn(
    regressor_target_col,
    F.col(popularity_target_col) - F.col(popularity_click_prob_col),
)

# Run affinity model scoring
test_df_affinity_scored = affinity_model.transform(test_df_popularity_scored)
test_eval_reg = regression_eval(test_df_affinity_scored)
for metric, value in test_eval_reg.items():
    mlflow.log_metric(f"test_{metric}", value, run_id=regressor_model_run_id)

# COMMAND ----------
# Run final validation model processing
## Validation section with ALL adverts for all customers
final_validation_df = spark.table(final_validation_data_table)
final_validation_df = final_validation_df.fillna(fill_zeros_columns)


final_validation_df_popularity_scored = popularity_model.transform(
    final_validation_df
)
final_val_eval = classification_eval(
    final_validation_df_popularity_scored.filter(
        F.col(popularity_target_col).isNotNull()
    ),
    False,
)
for metric, value in final_val_eval.items():
    mlflow.log_metric(
        f"final_validation_{metric}", value, run_id=classifier_model_run_id
    )

# Calculate the residuals for the Regressor model
final_validation_df_popularity_scored = (
    final_validation_df_popularity_scored.withColumn(
        popularity_click_prob_col,
        vector_to_array(F.col(popularity_probability_col))[1],
    ).withColumn(
        regressor_target_col,
        F.col(popularity_target_col) - F.col(popularity_click_prob_col),
    )
)

# Run affinity model scoring
final_validation_df_affinity_scored = affinity_model.transform(
    final_validation_df_popularity_scored
)
final_val_eval_reg = regression_eval(
    final_validation_df_affinity_scored.filter(
        F.col(popularity_target_col).isNotNull()
    )
)
for metric, value in final_val_eval_reg.items():
    mlflow.log_metric(
        f"final_validation__{metric}", value, run_id=regressor_model_run_id
    )

## cache the dataset
final_validation_df_affinity_scored.cache()

# COMMAND ----------
final_validation_results_table = (
    catalog_schema_prefix + ".pctr_final_validation_predictions"
)
final_validation_df_affinity_scored.write.mode("overwrite").format(
    "delta"
).saveAsTable(final_validation_results_table)

# COMMAND ---------- [markdown]
# ## Feature Importance

# COMMAND ----------
# Feature importance
def feature_imortances_append_names(importances, feature_names):
    cleaned_importance = {}
    for f_index, score in importances.items():
        # Extract the integer from 'f0', 'f1', etc.
        idx = int(f_index.replace("f", ""))
        # Look up the actual column name
        actual_name = feature_names[idx]
        cleaned_importance[actual_name] = score
    df_importance = pd.DataFrame(
        list(cleaned_importance.items()), columns=["Feature", "Importance"]
    ).sort_values(by="Importance", ascending=False)

    return df_importance


affinity_feature_importances = feature_imortances_append_names(
    affinity_model.stages[-1].get_booster().get_score(importance_type="gain"),
    affinity_model.stages[-2].getInputCols(),
)
popularity_feature_importances = feature_imortances_append_names(
    popularity_model.stages[-1]
    .get_booster()
    .get_score(importance_type="gain"),
    popularity_model.stages[-2].getInputCols(),
)

# COMMAND ----------
plt.figure(figsize=(10, 10))
sns.barplot(x="Importance", y="Feature", data=popularity_feature_importances)
plt.title("Popularity Feature Importance")
plt.xlabel

# COMMAND ----------
plt.figure(figsize=(10, 10))
sns.barplot(x="Importance", y="Feature", data=affinity_feature_importances)
plt.title("Affinity Feature Importance")
plt.xlabel

# COMMAND ---------- [markdown]
# ## Ranking Evaluation Functions 

# COMMAND ----------
def rank_n_and_above_lift_metrics(df, n=2):
    ranking_above_col = f"rank_above_{n}"
    top_n_ctr = (
        df.filter(F.col(popularity_target_col).isNotNull())
        .withColumn(ranking_above_col, F.col(weighted_ranking_col) <= n)
        .groupBy(F.col(ranking_above_col))
        .agg(
            F.sum(F.col(popularity_target_col)).alias("clicks"),
            F.count(F.col(popularity_target_col)).alias("all_impressions"),
        )
        .withColumn("ctr", F.col("clicks") / F.col("all_impressions"))
    )
    lift = (
        top_n_ctr.filter(F.col(ranking_above_col) == True)
        .select("ctr")
        .collect()[0][0]
        / top_n_ctr.filter(F.col(ranking_above_col) == False)
        .select("ctr")
        .collect()[0][0]
    )
    return lift, top_n_ctr

# COMMAND ----------
def ctr_at_all_ranks(df):
    filtered_df = df.filter(F.col(popularity_target_col).isNotNull())
    all_clicks = filtered_df.agg(
        F.sum(F.col(popularity_target_col))
    ).collect()[0][0]
    all_rankdf = (
        filtered_df.groupBy(F.col(weighted_ranking_col))
        .agg(
            F.sum(F.col(popularity_target_col)).alias("clicks"),
            F.count(F.col(popularity_target_col)).alias("all_impressions"),
        )
        .withColumn("ctr", F.col("clicks") / F.col("all_impressions"))
        .withColumn("perc_total_clicks", F.col("clicks") / all_clicks)
    )
    return all_rankdf

# COMMAND ----------
def rank1_advert_distribution(df):
    fitered_df = df.filter((F.col(weighted_ranking_col) == 1))
    total_number_adverts_rank1 = fitered_df.count()

    aggregated_advert_rank1_distribution = (
        fitered_df.groupBy("uniqueAdID")
        .agg(
            F.count("uniqueAdID").alias("number_rank1"),
            (F.count("uniqueAdID") / total_number_adverts_rank1).alias(
                "perc_total"
            ),
        )
        .orderBy("number_rank1")
    )
    return aggregated_advert_rank1_distribution

# COMMAND ---------- [markdown]
# ## Ranking Evaluation

# COMMAND ----------
final_validation_scoring = spark.table(final_validation_results_table)
clicks_history_table = spark.table(
    f"{catalog_schema_prefix}.{table_prefix}_training_clicks_lookback"
)
dates_table = final_validation_scoring.select("rundate").distinct()
control_sheet = spark.table(
    "marketingdata_prod.warehouse.next_uk_nextads_control_sheet"
)

join_condition = clicks_history_table.date.between(
    F.date_sub(dates_table.rundate, lookback_period + 1),
    F.date_sub(dates_table.rundate, 1),
)

overall_ad_impressions = (
    clicks_history_table.join(dates_table, on=join_condition, how="inner")
    .groupBy("rundate", "title", "campaign", "versionnumber")
    .agg(
        F.sum("number_impressions").alias("num_impressions"),
        F.sum("number_clicks").alias("num_clicks"),
    )
)

overall_impressions = overall_ad_impressions.groupBy("rundate").agg(
    F.sum("num_impressions").alias("total_num_impressions"),
    F.sum("num_clicks").alias("total_num_clicks"),
    (F.sum("num_clicks") / F.sum("num_impressions")).alias(
        "global_clickthrough_rate"
    ),
    F.median("num_impressions").alias("median_impressions"),
)


join_condition_control_sheet = (
    (F.upper(control_sheet.CampaignNumber) == overall_ad_impressions.campaign)
    & (control_sheet.Title == overall_ad_impressions.title)
    & (
        overall_ad_impressions.versionnumber
        == F.regexp_extract(control_sheet.UniqueAdID, r"^.*_(V[1-9])_.*$", 1)
    )
    & (overall_ad_impressions.rundate == control_sheet.rundate)
)

global_ads_table = (
    overall_ad_impressions.select(
        "rundate", "title", "campaign", "versionnumber", "num_impressions"
    )
    .join(overall_impressions, on=["rundate"], how="inner")
    .join(control_sheet, on=join_condition_control_sheet)
    .withColumnsRenamed({"num_impressions": "advert_impressions_30days"})
    .select(
        overall_ad_impressions["rundate"],
        "uniqueAdID",
        "advert_impressions_30days",
        "median_impressions",
        "global_clickthrough_rate",
    )
    .distinct()
)

# join_condition =clicks_history_table.date.between(F.date_sub(dates_table.rundate, lookback_period + 1),F.date_sub(dates_table.rundate, 1))

# overall_ad_impressions= clicks_history_table.join(dates_table, on=join_condition, how='inner').groupBy('rundate', 'control_sheet_Adid').agg(F.sum('number_impressions').alias('num_impressions'), F.sum('number_clicks').alias('num_clicks'))

# overall_impressions= overall_ad_impressions.groupBy("rundate").agg(F.sum('num_impressions').alias('total_num_impressions'), F.sum('num_clicks').alias('total_num_clicks'), (F.sum('num_clicks')/F.sum('num_impressions')).alias('global_clickthrough_rate'), F.median("num_impressions").alias("median_impressions"))

# global_ads_table=overall_ad_impressions.select("rundate", "control_sheet_Adid", "num_impressions").join(overall_impressions, on=["rundate"], how="inner").withColumnsRenamed({"control_sheet_Adid": "uniqueAdID", "num_impressions": "advert_impressions_30days"}
# ).select("rundate", "uniqueAdID", "advert_impressions_30days", "median_impressions", "global_clickthrough_rate")

# COMMAND ----------
## Determine the scoring multiplication factor:
affinity_multiplication_factor = (
    final_validation_scoring.groupBy()
    .agg(
        F.stddev(F.col(regressor_predictions_col)).alias("regressor_stddev"),
        F.stddev(F.col(popularity_click_prob_col)).alias("popularity_stddev"),
    )
    .withColumn(
        "multiplication_factor",
        F.round(F.col("popularity_stddev") / F.col("regressor_stddev"), 1),
    )
    .select("multiplication_factor")
    .collect()[0][0]
)

# COMMAND ----------
display(affinity_multiplication_factor)

# COMMAND ----------
## Based on the above as guidance set the affinity weighting
affinity_weighting_factor = 4

# COMMAND ----------
best_pred_val = (
    final_validation_scoring.join(
        global_ads_table, how="left", on=["rundate", "uniqueAdID"]
    )
    .withColumn(
        "popularity_scoring_multiplier",
        (
            (F.col("advert_impressions_30days") + 1)
            / (
                F.col("advert_impressions_30days")
                + 1
                + F.col("median_impressions")
            )
        ),
    )
    .withColumn(
        popularity_smoothed_score_col,
        F.col(popularity_click_prob_col)
        * F.col("popularity_scoring_multiplier"),
    )
    .withColumn(
        regression_weighted_score_col,
        F.col(regressor_predictions_col) * affinity_weighting_factor,
    )
    .withColumn(
        combined_score_col,
        F.col(regression_weighted_score_col)
        + F.col(popularity_smoothed_score_col),
    )
    .withColumn(
        weighted_ranking_col,
        F.dense_rank().over(
            Window.partitionBy("rundate", "account_number").orderBy(
                F.desc(combined_score_col)
            )
        ),
    )
)

# COMMAND ----------
lift_2above, rank_2above_ctr = rank_n_and_above_lift_metrics(best_pred_val, 2)
all_rank_ctrs = ctr_at_all_ranks(best_pred_val)
advert_rank1_distribution = rank1_advert_distribution(best_pred_val)

# COMMAND ----------
lift_2above

# COMMAND ----------
display(rank_2above_ctr)

# COMMAND ----------
display(all_rank_ctrs.orderBy(F.col(weighted_ranking_col)))

# COMMAND ----------
display(advert_rank1_distribution.orderBy(F.desc("perc_total")))

# COMMAND ---------- [markdown]
# ## Register Best Models
# 

# COMMAND ----------
classifier_model = f"runs:/{classifier_model_run_id}/model"
regressor_model = f"runs:/{regressor_model_run_id}/model"


classifier_registered_model = mlflow.register_model(
    model_uri=classifier_model,
    name=classifier_registered_model_name,
)

regressor_registered_model = mlflow.register_model(
    model_uri=regressor_model,
    name=regresssor_registered_model_name,
)



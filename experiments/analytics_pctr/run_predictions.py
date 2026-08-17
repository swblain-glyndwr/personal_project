# COMMAND ----------
#!pip install "/Workspace/Users/claire_wilsonbarnes@next.co.uk/next-ads/wheels/dsutils-0.1.13-py3-none-any.whl"

# COMMAND ----------
import argparse
import hashlib
import json
import sys

import mlflow
import mlflow.spark

from pyspark.sql import functions as F
from pyspark.sql import Window

# Pipelining
from pyspark.ml.functions import vector_to_array

# Models
from dsutils.etl import (
    truncate_and_load,
    delete_from_and_load,
)

# COMMAND ----------
# Spark Performance Settings
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


# COMMAND ----------
MODEL_ARGUMENTS = (
    "catalog_schema_prefix",
    "lookback_period",
    "table_prefix",
    "affinity_weighting_factor",
    "regressor_model_uri",
    "classifier_model_uri",
)


def parse_command_line_values(argv):
    """Read Python-task parameters without breaking notebook widgets."""
    parser = argparse.ArgumentParser(add_help=False)
    for name in MODEL_ARGUMENTS:
        parser.add_argument(f"--{name}")
    parsed, _unknown = parser.parse_known_args(argv)
    return {
        name: value
        for name, value in vars(parsed).items()
        if value not in (None, "")
    }


COMMAND_LINE_VALUES = parse_command_line_values(sys.argv[1:])


def get_widget_value(name, default):
    if name in COMMAND_LINE_VALUES:
        return COMMAND_LINE_VALUES[name]
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


def optional_first_value(rows):
    """Return the first scalar, or None when an aggregate has no rows."""
    return rows[0][0] if rows else None


def require_non_empty(frame, stage):
    """Stop before publication when a model stage produces no rows."""
    if not frame.limit(1).collect():
        raise ValueError(f"Analytics pCTR {stage} contains no rows")
    return frame


# COMMAND ----------
dbutils.widgets.text(
    name="catalog_schema_prefix",
    defaultValue="marketingdata_dev.claire_wilsonbarnes",
    label="catalog_schema_prefix",
)
dbutils.widgets.text(
    name="lookback_period", defaultValue="30", label="lookback_period"
)
dbutils.widgets.text(
    name="table_prefix",
    defaultValue="next_uk_nextAds_analytics_pctr",
    label="table_prefix",
)
dbutils.widgets.text(
    name="affinity_weighting_factor",
    defaultValue="4",
    label="affinity_weighting_factor",
)
dbutils.widgets.text(
    name="regressor_model_uri",
    defaultValue="models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_affinity_regression_model/2",
    label="regressor_model_uri",
)
dbutils.widgets.text(
    name="classifier_model_uri",
    defaultValue="models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_popularity_classification_model/2",
    label="classifier_model_uri",
)

# COMMAND ----------
catalog_schema_prefix = get_widget_value(
    "catalog_schema_prefix", "marketingdata_dev.claire_wilsonbarnes"
)
lookback_period = int(get_widget_value("lookback_period", "30"))
table_prefix = get_widget_value(
    "table_prefix", "next_uk_nextAds_analytics_pctr"
)
affinity_weighting_factor = int(
    get_widget_value("affinity_weighting_factor", "4")
)
regressor_model_uri = get_widget_value(
    "regressor_model_uri",
    "models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_affinity_regression_model/2",
)
classifier_model_uri = get_widget_value(
    "classifier_model_uri",
    "models:/marketingdata_dev.ds_sandbox.nextads_analytics_pctr_popularity_classification_model/2",
)

FEATURE_TABLE = catalog_schema_prefix + "." + table_prefix + "_features"
TARGET_TABLE = catalog_schema_prefix + "." + table_prefix + "_predictions"
TARGET_TABLE_LATEST = (
    catalog_schema_prefix + "." + table_prefix + "_predictions_latest"
)

fill_zeros_columns = {
    "day_impressions": 0,
    "prior_day_impressions": 0,
    "week_impressions": 0,
    "prior_week_impressions": 0,
    "customer_total_clicks": 0,
    "customer_total_unique_adverts_clicked": 0,
    "customer_advert_previous_click_number": 0,
    "number_clicks_same_algodivision": 0,
    "view_highest_catid_weight": 0,
    "view_lift_adjusted": 0,
    "view_cs": 0,
    "purchase_highest_catid_weight": 0,
    "purchase_lift_adjusted": 0,
    "purchase_cs": 0,
}

popularity_smoothed_score_col = "popularity_smoothed_score"
regression_weighted_score_col = "regression_weighted_score"
popularity_click_prob_col = "popularity_prob_click"
popularity_probability_col = "probability"
regressor_predictions_col = "residual_predictions"
combined_score_col = "combined_weighted_score"
weighted_ranking_col = "weighted_ranking"
pk_cols = ["account_number", "UniqueAdID"]

target_cols = pk_cols + [
    popularity_smoothed_score_col,
    regression_weighted_score_col,
    popularity_click_prob_col,
    regressor_predictions_col,
    combined_score_col,
    weighted_ranking_col,
    "advert_impressions_30days",
    "advert_item_revenue",
    #'rundate',
]

# QA Thresholds
distribution_number_ads_threshold = 15
cumulative_coverage_threshold = 0.8
rank1_advert_coverage_threshold = 0.25

# COMMAND ----------
clicks_history_table = spark.table(
    catalog_schema_prefix + "." + table_prefix + "_training_clicks_lookback"
)
current_control_sheet = spark.table(
    "marketingdata_prod.warehouse.next_uk_nextads_control_sheet_latest"
)

ad_items_table = spark.table(
    "marketingdata_prod.warehouse.next_ads_sort_order_latest"
).select("uniqueAdID", "items")
baskets_table = spark.table(
    "marketingdata_prod.warehouse.baskets_uk_3y"
).filter(F.col("order_date") >= F.date_sub(F.current_date(), lookback_period))

# COMMAND ----------
# pctr_prediction_features
predictions_input = spark.table(FEATURE_TABLE)
predictions_input = predictions_input.fillna(fill_zeros_columns)
require_non_empty(predictions_input, "feature input")

# COMMAND ----------
mlflow.set_registry_uri("databricks-uc")

try:
    popularity_model = mlflow.spark.load_model(classifier_model_uri)
    affinity_model = mlflow.spark.load_model(regressor_model_uri)

except Exception as e:
    print(f"Error in loading models :{e}")

# COMMAND ----------
popularity_scored_df = popularity_model.transform(predictions_input)
require_non_empty(popularity_scored_df, "popularity output")
affinity_scored_df = affinity_model.transform(popularity_scored_df)
require_non_empty(affinity_scored_df, "affinity output")

# COMMAND ----------
# Addition of advert click data over the last 30 days
dates_table = affinity_scored_df.select("rundate").distinct()

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

# COMMAND ----------
join_condition_control_sheet = (
    (
        F.upper(current_control_sheet.CampaignNumber)
        == clicks_history_table.campaign
    )
    & (current_control_sheet.Title == clicks_history_table.title)
    & (
        clicks_history_table.versionnumber
        == F.regexp_extract(
            current_control_sheet.UniqueAdID, r"^.*_(V[1-9])_.*$", 1
        )
    )
)

global_ads_table = (
    overall_ad_impressions.select(
        "rundate", "title", "campaign", "versionnumber", "num_impressions"
    )
    .join(overall_impressions, on=["rundate"], how="inner")
    .join(current_control_sheet, on=join_condition_control_sheet)
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

# COMMAND ----------
median_impression_rows = (
    global_ads_table.filter(F.col("median_impressions").isNotNull())
    .dropDuplicates(["median_impressions"])
    .select("median_impressions")
    .limit(1)
    .collect()
)
median_impressions = optional_first_value(median_impression_rows)
if median_impressions is None:
    print(
        "No advert impression history matched the current control sheet; "
        "the popularity contribution will be zero."
    )

# COMMAND ----------
## Addition of items from adverts revenue as a tiebreaker if necessary

ads_item_revenue_last_30days = (
    ad_items_table.join(
        baskets_table,
        how="left",
        on=[baskets_table["itemno"] == ad_items_table["items"]],
    )
    .groupBy("uniqueAdID")
    .agg(F.sum(F.col("s740orderstakenvalue")).alias("advert_item_revenue"))
)

# COMMAND ----------
combined_data = (
    affinity_scored_df.join(
        global_ads_table, how="left", on=["rundate", "uniqueAdID"]
    )
    .join(ads_item_revenue_last_30days, how="left", on=["uniqueAdID"])
    .withColumn(
        "advert_impressions_30days",
        F.coalesce(F.col("advert_impressions_30days"), F.lit(0)),
    )
    .withColumn(
        "advert_item_revenue",
        F.coalesce(F.col("advert_item_revenue"), F.lit(0)),
    )
    .withColumn("median_impressions", F.lit(median_impressions))
)

predictions = (
    combined_data.withColumn(
        "popularity_scoring_multiplier",
        F.coalesce(
            (
                (F.col("advert_impressions_30days") + 1)
                / (
                    F.col("advert_impressions_30days")
                    + 1
                    + F.col("median_impressions")
                )
            ),
            F.lit(0),
        ),
    )
    .withColumn(
        popularity_click_prob_col,
        vector_to_array(F.col(popularity_probability_col))[1],
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
                F.desc(combined_score_col),
                F.desc("advert_impressions_30days"),
                F.desc("advert_item_revenue"),
            )
        ),
    )
)
require_non_empty(predictions, "ranked output")

# COMMAND ----------
print("Loading output to table (latest)")
truncate_and_load(
    predictions.select(*target_cols),
    TARGET_TABLE_LATEST,
    pk_cols=pk_cols,
)
# predictions.select(*target_cols).write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(TARGET_TABLE_LATEST)

# COMMAND ----------
print("Loading output to table")
delete_from_and_load(
    predictions.select(*target_cols),
    TARGET_TABLE,
    pk_cols=pk_cols,
    del_where={"rundate": "current_date()"},
)

# COMMAND ----------
prediction_scores = spark.table(TARGET_TABLE_LATEST)

# COMMAND ----------
## Run QA
errors = []

## Only 1 run date and is current date
distinct_rundates = prediction_scores.select("rundate").distinct().count()
try:
    assert distinct_rundates == 1, (
        f"Multiple rundates in {TARGET_TABLE_LATEST}"
    )
except AssertionError as e:
    errors.append(str(e))

# rank 1 & 2 have as many predictions as input customers
rank_1_number_predictions = prediction_scores.filter(
    F.col(weighted_ranking_col) == 1
).count()
rank_2_number_predictions = prediction_scores.filter(
    F.col(weighted_ranking_col) == 2
).count()
number_customers = (
    predictions_input.select("account_number").distinct().count()
)

try:
    assert rank_1_number_predictions == number_customers, (
        f"Number of rank 1 predictions {rank_1_number_predictions} does not match number of customers {number_customers}"
    )
except AssertionError as e:
    errors.append(str(e))

try:
    assert rank_2_number_predictions == number_customers, (
        f"Number of rank 2 predictions {rank_2_number_predictions} does not match number of customers {number_customers}"
    )
except AssertionError as e:
    errors.append(str(e))

# number of adverts covered in rank 1 & 2
filtered_df = prediction_scores.filter(F.col(weighted_ranking_col).isin(1, 2))
total_prediction_number = filtered_df.count()
total_number_adverts_rank12 = (
    filtered_df.select("uniqueAdID").distinct().count()
)
# At least 50% of adverts available represented in rank 1 & 2 positions
min_threshold_number_of_ads = round(
    predictions_input.select("uniqueAdID").distinct().count() / 2, 0
)

try:
    assert total_number_adverts_rank12 >= min_threshold_number_of_ads, (
        "Less than 50% of adverts available represented in rank 1 & 2 positions"
    )
except AssertionError as e:
    errors.append(str(e))

## Advert distribution
cumulative_sum_advert_coverage_window = Window.orderBy(
    F.desc("perc_total")
).rowsBetween(Window.unboundedPreceding, Window.currentRow)
aggregated_advert_rank1_distribution = (
    filtered_df.groupBy("uniqueAdID")
    .agg(
        F.count("uniqueAdID").alias("rank1_2"),
        (F.count("uniqueAdID") / total_prediction_number).alias("perc_total"),
    )
    .orderBy(F.desc("perc_total"))
    .withColumn(
        "cumulative_coverage",
        F.sum("perc_total").over(cumulative_sum_advert_coverage_window),
    )
)

# top 80% distribution
number_ads_cumulative_coverage = aggregated_advert_rank1_distribution.filter(
    F.col("cumulative_coverage") <= cumulative_coverage_threshold
).count()
try:
    assert (
        number_ads_cumulative_coverage >= distribution_number_ads_threshold
    ), (
        f"Less than {distribution_number_ads_threshold} ads cover {cumulative_coverage_threshold * 100}% of all rank 1 & 2 positions"
    )
except AssertionError as e:
    errors.append(str(e))


# max coverage percentage - does this meet the threshold

rank1_coverage_perc = optional_first_value(
    aggregated_advert_rank1_distribution.select("perc_total")
    .limit(1)
    .collect()
)
if rank1_coverage_perc is None:
    errors.append("No rank 1 or 2 Analytics pCTR predictions were produced")
else:
    try:
        assert rank1_coverage_perc <= rank1_advert_coverage_threshold, (
            "Top ranked advert covers 25% or more of all rank 1 & 2 positions"
        )
    except AssertionError as e:
        errors.append(str(e))

# COMMAND ----------
if errors:
    final_errors = "\n".join(errors)
    print(final_errors)
    raise AssertionError(final_errors)

# COMMAND ----------
latest_history = spark.sql(
    f"DESCRIBE HISTORY {TARGET_TABLE_LATEST} LIMIT 1"
).first()
accepted_dates = [
    row["rundate"].isoformat()
    for row in prediction_scores.select("rundate").distinct().collect()
]
try:
    producing_run_id = spark.conf.get("spark.databricks.job.runId")
except Exception:
    producing_run_id = "unknown"
prediction_receipt = {
    "classifier_model_uri": classifier_model_uri,
    "output_delta_version": int(latest_history["version"]),
    "output_row_count": prediction_scores.count(),
    "output_schema_sha256": hashlib.sha256(
        prediction_scores.schema.json().encode("utf-8")
    ).hexdigest(),
    "output_table": TARGET_TABLE_LATEST,
    "producing_run_id": producing_run_id,
    "regressor_model_uri": regressor_model_uri,
    "run_dates": sorted(accepted_dates),
}
print(
    "ANALYTICS_PCTR_PREDICTION_RECEIPT="
    + json.dumps(prediction_receipt, sort_keys=True, separators=(",", ":"))
)

# Databricks notebook source
# MAGIC %md
# MAGIC # Score pCTR Ad Candidates
# MAGIC
# MAGIC Loads the registered pCTR Spark ML model and scores current account/ad
# MAGIC candidate rows. The output can be used directly for pCTR ranking or joined
# MAGIC into an LTR as a single click-propensity feature.

# COMMAND ----------

import json

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC `reference_date` is the scoring as-of date. Candidate rows are created as if
# MAGIC they are being scored for that date. With `feature_source_mode=latest`, the
# MAGIC notebook joins the current/latest feature tables. With
# MAGIC `feature_source_mode=snapshot`, it joins the suffixed snapshot tables and
# MAGIC filters them to the same `reference_date`.
# MAGIC
# MAGIC Use snapshot mode for backtests or smoke checks against snapshot features.
# MAGIC Use latest mode for current batch scoring after the latest feature tables have
# MAGIC been produced.
# MAGIC
# MAGIC The two modes exist because scoring has two use cases:
# MAGIC
# MAGIC - `feature_source_mode=latest` is the normal current scoring path. It reads
# MAGIC   the unsuffixed latest feature tables and writes current pCTR scores.
# MAGIC - `feature_source_mode=snapshot` is for backtesting or smoke checks. It reads
# MAGIC   suffixed snapshot tables such as `_snapshots` or `_smoke` and filters them
# MAGIC   to the scoring `reference_date`.

# COMMAND ----------

def get_widget_value(name, default):
    try:
        dbutils.widgets.text(name, str(default))
        value = dbutils.widgets.get(name)
        return value if value not in (None, "") else default
    except NameError:
        return default


def validate_widget_choice(name, value, valid_values):
    if value not in valid_values:
        raise ValueError(f"Invalid widget value for {name}: {value}. Valid values are: {valid_values}")
    return value


def snapshot_table_name(table_name):
    return f"{table_name}_{snapshot_table_suffix}"


def read_feature_table(table_name):
    if feature_source_mode == "snapshot":
        return spark.table(snapshot_table_name(table_name)).where(F.col("reference_date") == F.lit(reference_date).cast("date"))
    return spark.table(table_name)


@F.udf(T.DoubleType())
def cosine_similarity(left_values, right_values):
    if left_values is None or right_values is None:
        return None
    length = min(len(left_values), len(right_values))
    if length == 0:
        return None
    left_dense = [float(value) for value in left_values[:length]]
    right_dense = [float(value) for value in right_values[:length]]
    left_norm = sum(value * value for value in left_dense) ** 0.5
    right_norm = sum(value * value for value in right_dense) ** 0.5
    if left_norm == 0 or right_norm == 0:
        return None
    return float(sum(left_dense[index] * right_dense[index] for index in range(length)) / (left_norm * right_norm))


feature_source_mode_options = ["latest", "snapshot"]

reference_date = get_widget_value("reference_date", "2026-04-15")
snapshot_table_suffix = get_widget_value("snapshot_table_suffix", "snapshots")
feature_source_mode = validate_widget_choice("feature_source_mode", get_widget_value("feature_source_mode", "latest"), feature_source_mode_options)
model_alias = get_widget_value("model_alias", "dev_candidate")

print(
    "pCTR candidate scoring run config: "
    f"reference_date={reference_date}, "
    f"feature_source_mode={feature_source_mode}, "
    f"snapshot_table_suffix={snapshot_table_suffix}, "
    f"model_alias={model_alias}. "
    f"Widget options: reference_date='YYYY-MM-DD'; feature_source_mode in {feature_source_mode_options}; "
    "snapshot_table_suffix is free text, for example 'snapshots' or 'smoke'; "
    "model_alias is the registered MLflow model alias, for example 'dev_candidate'. "
    "Meaning: latest mode reads unsuffixed latest feature tables; snapshot mode "
    "reads suffixed tables filtered to reference_date."
)

dev_schema = spark.sql("SELECT current_user()").first()[0].split("@")[0].replace(".", "_")

registered_model_name = f"marketingdata_dev.{dev_schema}.nextads_pctr_spark_model"

customer_behaviour_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_customer_behaviour_features"
advert_attribute_profile_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_attribute_profile_90d"
advert_semantic_embeddings_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_semantic_embeddings_90d"
advert_product_features_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_product_features_90d"
customer_product_features_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_customer_product_features"
customer_seasonal_product_features_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_customer_seasonal_product_features"
advert_seasonal_product_features_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_seasonal_product_features"

assignments_latest_tbl = "marketingdata_prod.warehouse.next_uk_nextads_assignments_latest"

scores_latest_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_ad_candidate_scores_latest"
scores_history_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_ad_candidate_scores"

score_col = "predicted_pctr"
probability_col = "probability"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Model And Feature Contract

# COMMAND ----------

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

client = MlflowClient()
model_version = client.get_model_version_by_alias(registered_model_name, model_alias)
model_uri = f"models:/{registered_model_name}/{model_version.version}"
model = mlflow.spark.load_model(model_uri)

feature_columns_path = client.download_artifacts(model_version.run_id, "feature_columns.json")
numeric_columns_path = client.download_artifacts(model_version.run_id, "numeric_feature_columns.json")
categorical_columns_path = client.download_artifacts(model_version.run_id, "categorical_feature_columns.json")

with open(feature_columns_path, "r", encoding="utf-8") as file:
    feature_columns = json.load(file)
with open(numeric_columns_path, "r", encoding="utf-8") as file:
    numeric_feature_columns = json.load(file)
with open(categorical_columns_path, "r", encoding="utf-8") as file:
    categorical_feature_columns = json.load(file)

print(f"Loaded {registered_model_name} version {model_version.version} using alias {model_alias}")
print(f"Feature count: {len(feature_columns):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Candidate Rows

# COMMAND ----------

df_candidates = (
    spark.table(assignments_latest_tbl)
    .select(
        F.col("AccountNumber").cast("string").alias("account_number"),
        F.lit(reference_date).cast("date").alias("reference_date"),
        F.lit(reference_date).cast("date").alias("session_date"),
        F.col("Location").cast("string").alias("placement_id"),
        F.col("UniqueAdIDMeasurement").cast("string").alias("unique_ad_id"),
        F.col("UniqueAdIDAssigned").cast("string").alias("assigned_unique_ad_id"),
        F.col("Treatment").cast("string").alias("treatment"),
        F.col("MASID").cast("string").alias("masid"),
    )
    .where(F.col("unique_ad_id").isNotNull())
    .where(F.col("unique_ad_id") != "NoAdFound")
    .where(F.col("unique_ad_id").rlike("^P"))
    .withColumn("exposure_hour", F.lit(12))
    .withColumn("exposure_dayofweek", F.dayofweek("session_date"))
    .withColumn("exposure_month", F.month("session_date"))
    .withColumn("exposure_weekofyear", F.weekofyear("session_date"))
    .withColumn("exposure_quarter", F.quarter("session_date"))
    .withColumn("exposure_is_weekend", F.when(F.dayofweek("session_date").isin(1, 7), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("exposure_month_sin", F.sin((F.month("session_date").cast("double") - F.lit(1.0)) * F.lit(2.0 * 3.141592653589793 / 12.0)))
    .withColumn("exposure_month_cos", F.cos((F.month("session_date").cast("double") - F.lit(1.0)) * F.lit(2.0 * 3.141592653589793 / 12.0)))
    .withColumn("exposure_week_sin", F.sin((F.weekofyear("session_date").cast("double") - F.lit(1.0)) * F.lit(2.0 * 3.141592653589793 / 52.0)))
    .withColumn("exposure_week_cos", F.cos((F.weekofyear("session_date").cast("double") - F.lit(1.0)) * F.lit(2.0 * 3.141592653589793 / 52.0)))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Feature Layers

# COMMAND ----------

advert_attribute_feature_cols = [
    "campaign_id", "advert_url", "advert_brand_name", "control_sheet_brand", "advert_title", "headline", "subtext", "cta",
    "template_name", "page_path", "advert_active_placement_count", "has_item_attribute_profile",
    "attribute_profile_attribute_count", "attribute_profile_value_count", "advert_item_count", "advert_item_weight_sum",
    "top_brand", "top_use", "top_colour", "top_style", "top_category", "top_department", "top_gender",
]

advert_semantic_feature_cols = [
    "advert_semantic_token_count", "advert_semantic_unique_token_count", "advert_has_destination_image",
    "advert_embedding_neighbour_count", "advert_embedding_top_similarity", "advert_embedding_avg_similarity",
] + [f"advert_semantic_dim_{dim_index:03d}" for dim_index in range(32)]

advert_product_feature_cols = [
    "advert_product_item_count", "advert_product_embedded_item_count", "advert_product_embedding_coverage", "advert_product_embedding",
] + [f"advert_product_dim_{dim_index:03d}" for dim_index in range(32)]

advert_seasonal_product_feature_cols = [
    "advert_product_views_7d", "advert_product_views_30d", "advert_product_purchases_7d", "advert_product_purchases_30d",
    "advert_product_views_ly_same_month", "advert_product_purchases_ly_same_month", "advert_product_trending_7x30",
    "seasonal_advert_product_embedding_coverage", "seasonal_advert_product_embedding",
] + [f"seasonal_advert_product_dim_{dim_index:03d}" for dim_index in range(32)]

customer_product_feature_cols = [
    "customer_product_interaction_count", "customer_product_purchase_interaction_count", "customer_product_view_interaction_count",
    "customer_product_distinct_item_count", "customer_product_embedded_item_count", "customer_product_embedding_coverage",
    "customer_product_embedding",
] + [f"customer_product_dim_{dim_index:03d}" for dim_index in range(32)]

customer_seasonal_product_feature_cols = [
    "customer_same_month_ly_purchase_count", "customer_same_month_ly_distinct_item_count",
    "customer_seasonal_product_embedding_coverage", "customer_seasonal_product_embedding",
] + [f"customer_seasonal_product_dim_{dim_index:03d}" for dim_index in range(32)]

df_advert_attribute_source = read_feature_table(advert_attribute_profile_tbl)
df_advert_semantic_source = read_feature_table(advert_semantic_embeddings_tbl)
df_advert_product_source = read_feature_table(advert_product_features_tbl)
df_advert_seasonal_product_source = read_feature_table(advert_seasonal_product_features_tbl)
df_customer_behaviour_source = read_feature_table(customer_behaviour_tbl)
df_customer_product_source = read_feature_table(customer_product_features_tbl)
df_customer_seasonal_product_source = read_feature_table(customer_seasonal_product_features_tbl)

df_advert_attribute_features = (
    df_advert_attribute_source
    .select(
        F.col("feature_date").alias("advert_attribute_feature_date"),
        F.col("advert_id").alias("advert_attribute_feature_advert_id"),
        *[F.col(col_name) for col_name in advert_attribute_feature_cols if col_name in df_advert_attribute_source.columns],
    )
    .dropDuplicates(["advert_attribute_feature_date", "advert_attribute_feature_advert_id"])
)

df_advert_semantic_features = (
    df_advert_semantic_source
    .select(
        F.col("feature_date").alias("advert_semantic_feature_date"),
        F.col("advert_id").alias("advert_semantic_feature_advert_id"),
        *[F.col(col_name) for col_name in advert_semantic_feature_cols if col_name in df_advert_semantic_source.columns],
    )
    .dropDuplicates(["advert_semantic_feature_date", "advert_semantic_feature_advert_id"])
)

df_advert_product_features = (
    df_advert_product_source
    .select(
        F.col("feature_date").alias("advert_product_feature_date"),
        F.col("advert_id").alias("advert_product_feature_advert_id"),
        *[F.col(col_name) for col_name in advert_product_feature_cols if col_name in df_advert_product_source.columns],
    )
    .dropDuplicates(["advert_product_feature_date", "advert_product_feature_advert_id"])
)

df_advert_seasonal_product_features = (
    df_advert_seasonal_product_source
    .select(
        F.col("feature_date").alias("advert_seasonal_product_feature_date"),
        F.col("advert_id").alias("advert_seasonal_product_feature_advert_id"),
        *[F.col(col_name) for col_name in advert_seasonal_product_feature_cols if col_name in df_advert_seasonal_product_source.columns],
    )
    .dropDuplicates(["advert_seasonal_product_feature_date", "advert_seasonal_product_feature_advert_id"])
)

df_customer_product_features = (
    df_customer_product_source
    .select("account_number", *[F.col(col_name) for col_name in customer_product_feature_cols if col_name in df_customer_product_source.columns])
    .dropDuplicates(["account_number"])
)

df_customer_seasonal_product_features = (
    df_customer_seasonal_product_source
    .select("account_number", *[F.col(col_name) for col_name in customer_seasonal_product_feature_cols if col_name in df_customer_seasonal_product_source.columns])
    .dropDuplicates(["account_number"])
)

df_scoring_features = (
    df_candidates.alias("base")
    .join(
        df_advert_attribute_features.alias("attr"),
        (F.col("base.session_date") == F.col("attr.advert_attribute_feature_date"))
        & (F.col("base.unique_ad_id") == F.col("attr.advert_attribute_feature_advert_id")),
        "left",
    )
    .drop("advert_attribute_feature_date", "advert_attribute_feature_advert_id")
    .join(
        df_advert_semantic_features.alias("sem"),
        (F.col("base.session_date") == F.col("sem.advert_semantic_feature_date"))
        & (F.col("base.unique_ad_id") == F.col("sem.advert_semantic_feature_advert_id")),
        "left",
    )
    .drop("advert_semantic_feature_date", "advert_semantic_feature_advert_id")
    .join(
        df_advert_product_features.alias("prod"),
        (F.col("base.session_date") == F.col("prod.advert_product_feature_date"))
        & (F.col("base.unique_ad_id") == F.col("prod.advert_product_feature_advert_id")),
        "left",
    )
    .drop("advert_product_feature_date", "advert_product_feature_advert_id")
    .join(
        df_advert_seasonal_product_features.alias("seasonal_prod"),
        (F.col("base.session_date") == F.col("seasonal_prod.advert_seasonal_product_feature_date"))
        & (F.col("base.unique_ad_id") == F.col("seasonal_prod.advert_seasonal_product_feature_advert_id")),
        "left",
    )
    .drop("advert_seasonal_product_feature_date", "advert_seasonal_product_feature_advert_id")
    .join(df_customer_behaviour_source.drop("roamingprofileid", "reference_date"), on="account_number", how="left")
    .join(df_customer_product_features, on="account_number", how="left")
    .join(df_customer_seasonal_product_features, on="account_number", how="left")
    .withColumn("customer_ad_product_cosine_similarity", cosine_similarity(F.col("customer_product_embedding"), F.col("advert_product_embedding")))
    .withColumn("customer_ad_product_embedding_coverage", F.when(F.col("customer_product_embedding").isNotNull() & F.col("advert_product_embedding").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("customer_ad_seasonal_product_cosine_similarity", cosine_similarity(F.col("customer_seasonal_product_embedding"), F.col("seasonal_advert_product_embedding")))
    .withColumn("customer_ad_seasonal_product_embedding_coverage", F.when(F.col("customer_seasonal_product_embedding").isNotNull() & F.col("seasonal_advert_product_embedding").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
    .drop("advert_product_embedding", "customer_product_embedding", "seasonal_advert_product_embedding", "customer_seasonal_product_embedding")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Align To Model Feature Contract

# COMMAND ----------

for col_name in numeric_feature_columns:
    if col_name not in df_scoring_features.columns:
        df_scoring_features = df_scoring_features.withColumn(col_name, F.lit(0.0))
    df_scoring_features = df_scoring_features.withColumn(col_name, F.coalesce(F.col(col_name).cast("double"), F.lit(0.0)))

for col_name in categorical_feature_columns:
    if col_name not in df_scoring_features.columns:
        df_scoring_features = df_scoring_features.withColumn(col_name, F.lit("__missing__"))
    df_scoring_features = df_scoring_features.withColumn(col_name, F.coalesce(F.col(col_name).cast("string"), F.lit("__missing__")))

missing_feature_cols = [col_name for col_name in feature_columns if col_name not in df_scoring_features.columns]
if missing_feature_cols:
    raise ValueError(f"Missing model feature columns after alignment: {missing_feature_cols}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Score And Persist

# COMMAND ----------

df_predictions = model.transform(df_scoring_features)

df_scores = (
    df_predictions
    .withColumn(score_col, vector_to_array(F.col(probability_col)).getItem(1))
    .select(
        "account_number",
        "reference_date",
        "placement_id",
        "unique_ad_id",
        score_col,
        F.lit(registered_model_name).alias("model_name"),
        F.lit(model_version.version).alias("model_version"),
        F.current_date().alias("rundate"),
    )
)

df_scores.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(scores_latest_output_tbl)

if spark.catalog.tableExists(scores_history_output_tbl):
    spark.sql(f"DELETE FROM {scores_history_output_tbl} WHERE reference_date = DATE '{reference_date}'")
    df_scores.write.mode("append").option("mergeSchema", "true").saveAsTable(scores_history_output_tbl)
else:
    df_scores.write.mode("overwrite").option("overwriteSchema", "true").partitionBy("reference_date").saveAsTable(scores_history_output_tbl)

display(df_scores.limit(20))

print(f"Wrote latest scores to {scores_latest_output_tbl}")
print(f"Wrote score history for {reference_date} to {scores_history_output_tbl}")

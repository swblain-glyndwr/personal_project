# Databricks notebook source
# MAGIC %md
# MAGIC # pCTR Seasonal Product Features
# MAGIC
# MAGIC Builds seasonal product-match features for the pCTR training flow. The
# MAGIC workbook reuses product embeddings created by `pctr_product_embedding_features`
# MAGIC and adds point-in-time seasonal signals:
# MAGIC
# MAGIC - customer same-month-last-year purchase embeddings
# MAGIC - advert linked-product recent demand and same-month-last-year demand
# MAGIC - scalar customer/ad seasonal embedding dimensions for modelling

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC `reference_date` is the point-in-time anchor for seasonal product features.
# MAGIC Same-month-last-year, recent 7-day demand, recent 30-day demand, and trend
# MAGIC ratios are all calculated relative to this date.
# MAGIC
# MAGIC `advert_feature_horizon_days` extends advert seasonal feature dates forward
# MAGIC so the tagged-click training rows can join these features on their future
# MAGIC exposure `session_date`.
# MAGIC
# MAGIC `write_mode=overwrite_latest` writes the normal latest tables. Use
# MAGIC `write_mode=append_snapshot` to write a partition into suffixed snapshot
# MAGIC tables such as `_snapshots` or `_smoke`.
# MAGIC
# MAGIC The modes exist for different jobs:
# MAGIC
# MAGIC - `overwrite_latest` is for the current interactive/latest build. It refreshes
# MAGIC   the unsuffixed seasonal product tables used by current debugging or latest
# MAGIC   scoring flows.
# MAGIC - `append_snapshot` is for repeatable point-in-time training data. It writes
# MAGIC   seasonal features into a history-style table and replaces only the current
# MAGIC   `reference_date` partition when rerun.
# MAGIC - `snapshot_table_suffix` chooses which history-style table is used.
# MAGIC   `snapshots` is the proper backfill/training suffix. `smoke` is a safe test
# MAGIC   suffix so a one-month dry run does not overwrite the latest table or the
# MAGIC   real training snapshots.

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
    if write_mode == "append_snapshot":
        return spark.table(snapshot_table_name(table_name)).where(F.col("reference_date") == F.lit(reference_date).cast("date"))
    return spark.table(table_name)


def write_output_table(df, table_name, partition_col="reference_date"):
    if write_mode == "append_snapshot":
        target_table = snapshot_table_name(table_name)
        df_to_write = df.withColumn(partition_col, F.lit(reference_date).cast("date"))
        if spark.catalog.tableExists(target_table):
            spark.sql(f"DELETE FROM {target_table} WHERE {partition_col} = DATE '{reference_date}'")
            df_to_write.write.mode("append").option("mergeSchema", "true").saveAsTable(target_table)
        else:
            df_to_write.write.mode("overwrite").option("overwriteSchema", "true").partitionBy(partition_col).saveAsTable(target_table)
        print(f"Wrote snapshot for {reference_date} to {target_table}")
    else:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
        print(f"Wrote latest output to {table_name}")


write_mode_options = ["overwrite_latest", "append_snapshot"]

reference_date = get_widget_value("reference_date", "2026-04-15")
write_mode = validate_widget_choice("write_mode", get_widget_value("write_mode", "overwrite_latest"), write_mode_options)
snapshot_table_suffix = get_widget_value("snapshot_table_suffix", "snapshots")
advert_feature_horizon_days = int(get_widget_value("advert_feature_horizon_days", "14"))

embedding_feature_dims_for_model = 32
purchase_weight = 10.0
view_weight = 1.0

print(
    "pCTR seasonal product feature run config: "
    f"reference_date={reference_date}, "
    f"write_mode={write_mode}, "
    f"snapshot_table_suffix={snapshot_table_suffix}, "
    f"advert_feature_horizon_days={advert_feature_horizon_days}. "
    f"Widget options: reference_date='YYYY-MM-DD'; write_mode in {write_mode_options}; "
    "snapshot_table_suffix is free text, for example 'snapshots' or 'smoke'; "
    "advert_feature_horizon_days is an integer number of days. "
    "Meaning: 7-day, 30-day, and same-month-last-year windows are anchored to "
    "reference_date."
)

dev_schema = spark.sql("SELECT current_user()").first()[0].split("@")[0].replace(".", "_")

product_embeddings_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_product_embeddings_latest"
customer_seasonal_product_features_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_customer_seasonal_product_features"
advert_seasonal_product_features_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_seasonal_product_features"

control_sheet_tbl = "marketingdata_prod.warehouse.next_uk_nextads_control_sheet"
ad_items_tbl = "marketingdata_prod.warehouse.next_uk_nextads_ad_items"
baskets_tbl = "marketingdata_prod.warehouse.baskets_uk_3y"
bq_sessions_with_accounts_tbl = "marketingdata_prod.warehouse.bq_views_sessions_next_uk_with_accounts"

feature_end_date = F.lit(reference_date).cast("date")
advert_feature_end_date = F.date_add(feature_end_date, advert_feature_horizon_days)
same_month_start_ly = F.add_months(F.trunc(feature_end_date, "MM"), -12)
same_month_end_ly = F.add_months(F.last_day(feature_end_date), -12)
recent_7d_start = F.date_sub(feature_end_date, 7)
recent_30d_start = F.date_sub(feature_end_date, 30)
assignment_rundate_start = F.date_sub(F.date_sub(feature_end_date, 90), 1)
assignment_rundate_end = F.date_sub(advert_feature_end_date, 1)
run_date = F.lit(reference_date).cast("date")

# COMMAND ----------

def clean_text_col(col):
    return F.trim(F.regexp_replace(col.cast("string"), r"\s+", " "))


def normalise_item_col(col):
    return F.regexp_replace(F.lower(clean_text_col(col)), r"[^a-z0-9]", "")


def parse_items(col_name):
    return F.expr(f"filter(split(coalesce({col_name}, ''), '[^A-Za-z0-9]+'), x -> x <> '')")


@F.udf(T.ArrayType(T.DoubleType()))
def weighted_mean_embedding(rows):
    if not rows:
        return None

    accumulator = None
    total_weight = 0.0
    for row in rows:
        if row is None:
            continue
        weight = row["weight"] or 0.0
        embedding = row["embedding"]
        if embedding is None or weight <= 0:
            continue
        values = [float(value) for value in embedding]
        if accumulator is None:
            accumulator = [0.0 for _ in values]
        for index, value in enumerate(values):
            accumulator[index] += float(weight) * value
        total_weight += float(weight)

    if accumulator is None or total_weight == 0:
        return None

    averaged = [value / total_weight for value in accumulator]
    norm = sum(value * value for value in averaged) ** 0.5
    if norm == 0:
        return averaged
    return [value / norm for value in averaged]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Embeddings

# COMMAND ----------

df_product_embeddings = (
    read_feature_table(product_embeddings_tbl)
    .select("itemno", "product_embedding")
    .dropDuplicates(["itemno"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Same-Month-Last-Year Product Features

# COMMAND ----------

df_customer_same_month_purchases_ly = (
    spark.table(baskets_tbl)
    .where((F.col("order_date") >= same_month_start_ly) & (F.col("order_date") <= same_month_end_ly))
    .select(
        F.col("account_number").cast("string").alias("account_number"),
        normalise_item_col(F.col("itemno")).alias("itemno"),
        F.col("order_date").alias("order_date"),
    )
    .where(F.col("itemno") != "")
    .dropDuplicates(["account_number", "itemno", "order_date"])
    .join(df_product_embeddings, on="itemno", how="left")
)

df_customer_seasonal_product_features = (
    df_customer_same_month_purchases_ly
    .groupBy("account_number")
    .agg(
        F.count("*").alias("customer_same_month_ly_purchase_count"),
        F.countDistinct("itemno").alias("customer_same_month_ly_distinct_item_count"),
        F.countDistinct(F.when(F.col("product_embedding").isNotNull(), F.col("itemno"))).alias("customer_same_month_ly_embedded_item_count"),
        weighted_mean_embedding(
            F.collect_list(
                F.struct(
                    F.lit(purchase_weight).alias("weight"),
                    F.col("product_embedding").alias("embedding"),
                )
            )
        ).alias("customer_seasonal_product_embedding"),
    )
    .withColumn(
        "customer_seasonal_product_embedding_coverage",
        F.when(
            F.col("customer_same_month_ly_distinct_item_count") > 0,
            F.col("customer_same_month_ly_embedded_item_count") / F.col("customer_same_month_ly_distinct_item_count"),
        ).otherwise(F.lit(0.0)),
    )
    .withColumn("rundate", run_date)
)

for dim_index in range(embedding_feature_dims_for_model):
    df_customer_seasonal_product_features = df_customer_seasonal_product_features.withColumn(
        f"customer_seasonal_product_dim_{dim_index:03d}",
        F.col("customer_seasonal_product_embedding").getItem(dim_index).cast("double"),
    )

display(df_customer_seasonal_product_features.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advert Linked Product Seasonal Demand

# COMMAND ----------

df_advert_daily_core = (
    spark.table(control_sheet_tbl)
    .where((F.col("rundate") >= assignment_rundate_start) & (F.col("rundate") <= assignment_rundate_end))
    .withColumn("feature_date", F.date_add(F.to_date(F.col("rundate")), 1))
    .select(
        "feature_date",
        F.col("UniqueAdID").cast("string").alias("advert_id"),
        F.col("Location").cast("string").alias("placement_id"),
        F.col("Items").cast("string").alias("control_sheet_items"),
    )
    .where(F.col("advert_id").rlike("^P"))
    .dropDuplicates(["feature_date", "placement_id", "advert_id"])
)

df_control_sheet_items = (
    df_advert_daily_core
    .select(
        "feature_date",
        "advert_id",
        F.posexplode_outer(parse_items("control_sheet_items")).alias("item_position", "itemno_raw"),
    )
    .withColumn("itemno", normalise_item_col(F.col("itemno_raw")))
    .where(F.col("itemno").isNotNull())
    .where(F.col("itemno") != "")
    .withColumn("item_source", F.lit("control_sheet_items"))
    .select("feature_date", "advert_id", "itemno", "item_position", "item_source")
)

df_ad_items_raw = spark.table(ad_items_tbl)
if "rundate" in df_ad_items_raw.columns:
    latest_ad_items_rundate = (
        df_ad_items_raw
        .where(F.col("rundate") <= feature_end_date)
        .agg(F.max("rundate").alias("latest_rundate"))
        .first()["latest_rundate"]
    )
    df_ad_items_raw = df_ad_items_raw.where(F.col("rundate") == F.lit(latest_ad_items_rundate))

df_representative_items = (
    df_advert_daily_core
    .select("feature_date", "advert_id")
    .dropDuplicates()
    .join(
        df_ad_items_raw.select(
            F.col("UniqueAdID").cast("string").alias("advert_id"),
            "RepresentativeItems",
        ),
        on="advert_id",
        how="inner",
    )
    .select(
        "feature_date",
        "advert_id",
        F.posexplode_outer("RepresentativeItems").alias("item_position", "itemno_raw"),
    )
    .withColumn("itemno", normalise_item_col(F.col("itemno_raw")))
    .where(F.col("itemno").isNotNull())
    .where(F.col("itemno") != "")
    .withColumn("item_source", F.lit("representative_items"))
    .select("feature_date", "advert_id", "itemno", "item_position", "item_source")
)

source_priority = (
    F.when(F.col("item_source") == "control_sheet_items", F.lit(1))
    .when(F.col("item_source") == "representative_items", F.lit(2))
    .otherwise(F.lit(9))
)

dedupe_advert_item_window = Window.partitionBy("feature_date", "advert_id", "itemno").orderBy(
    source_priority.asc(),
    F.col("item_position").asc_nulls_last(),
)

df_advert_items = (
    df_control_sheet_items
    .unionByName(df_representative_items)
    .withColumn("item_row_num", F.row_number().over(dedupe_advert_item_window))
    .where(F.col("item_row_num") == 1)
    .select("feature_date", "advert_id", "itemno")
)

df_item_views = (
    spark.table(bq_sessions_with_accounts_tbl)
    .select(
        F.col("date").alias("event_date"),
        normalise_item_col(F.col("productSku")).alias("itemno"),
    )
    .where(F.col("itemno") != "")
    .where((F.col("event_date") >= same_month_start_ly) & (F.col("event_date") <= feature_end_date))
)

df_item_purchases = (
    spark.table(baskets_tbl)
    .select(
        F.col("order_date").alias("event_date"),
        normalise_item_col(F.col("itemno")).alias("itemno"),
    )
    .where(F.col("itemno") != "")
    .where((F.col("event_date") >= same_month_start_ly) & (F.col("event_date") <= feature_end_date))
)

df_item_demand = (
    df_product_embeddings.select("itemno")
    .join(
        df_item_views
        .groupBy("itemno")
        .agg(
            F.sum(F.when(F.col("event_date") >= recent_7d_start, 1).otherwise(0)).alias("item_views_7d"),
            F.sum(F.when(F.col("event_date") >= recent_30d_start, 1).otherwise(0)).alias("item_views_30d"),
            F.sum(F.when((F.col("event_date") >= same_month_start_ly) & (F.col("event_date") <= same_month_end_ly), 1).otherwise(0)).alias("item_views_ly_same_month"),
        ),
        on="itemno",
        how="left",
    )
    .join(
        df_item_purchases
        .groupBy("itemno")
        .agg(
            F.sum(F.when(F.col("event_date") >= recent_7d_start, 1).otherwise(0)).alias("item_purchases_7d"),
            F.sum(F.when(F.col("event_date") >= recent_30d_start, 1).otherwise(0)).alias("item_purchases_30d"),
            F.sum(F.when((F.col("event_date") >= same_month_start_ly) & (F.col("event_date") <= same_month_end_ly), 1).otherwise(0)).alias("item_purchases_ly_same_month"),
        ),
        on="itemno",
        how="left",
    )
    .fillna(
        {
            "item_views_7d": 0,
            "item_views_30d": 0,
            "item_views_ly_same_month": 0,
            "item_purchases_7d": 0,
            "item_purchases_30d": 0,
            "item_purchases_ly_same_month": 0,
        }
    )
    .withColumn("item_demand_weight", F.lit(1.0) + F.col("item_views_30d") + (F.col("item_purchases_30d") * F.lit(purchase_weight)))
)

df_advert_items_with_demand = (
    df_advert_items
    .join(df_item_demand, on="itemno", how="left")
    .join(df_product_embeddings, on="itemno", how="left")
)

df_advert_seasonal_product_features = (
    df_advert_items_with_demand
    .groupBy("feature_date", "advert_id")
    .agg(
        F.sum("item_views_7d").alias("advert_product_views_7d"),
        F.sum("item_views_30d").alias("advert_product_views_30d"),
        F.sum("item_purchases_7d").alias("advert_product_purchases_7d"),
        F.sum("item_purchases_30d").alias("advert_product_purchases_30d"),
        F.sum("item_views_ly_same_month").alias("advert_product_views_ly_same_month"),
        F.sum("item_purchases_ly_same_month").alias("advert_product_purchases_ly_same_month"),
        F.countDistinct("itemno").alias("advert_seasonal_product_item_count"),
        F.countDistinct(F.when(F.col("product_embedding").isNotNull(), F.col("itemno"))).alias("advert_seasonal_product_embedded_item_count"),
        weighted_mean_embedding(
            F.collect_list(
                F.struct(
                    F.coalesce(F.col("item_demand_weight"), F.lit(1.0)).alias("weight"),
                    F.col("product_embedding").alias("embedding"),
                )
            )
        ).alias("seasonal_advert_product_embedding"),
    )
    .withColumn(
        "advert_product_trending_7x30",
        F.when(F.col("advert_product_views_30d") > 0, (F.col("advert_product_views_7d") / F.lit(7.0)) / (F.col("advert_product_views_30d") / F.lit(30.0))).otherwise(F.lit(0.0)),
    )
    .withColumn(
        "seasonal_advert_product_embedding_coverage",
        F.when(
            F.col("advert_seasonal_product_item_count") > 0,
            F.col("advert_seasonal_product_embedded_item_count") / F.col("advert_seasonal_product_item_count"),
        ).otherwise(F.lit(0.0)),
    )
    .withColumn("rundate", run_date)
)

for dim_index in range(embedding_feature_dims_for_model):
    df_advert_seasonal_product_features = df_advert_seasonal_product_features.withColumn(
        f"seasonal_advert_product_dim_{dim_index:03d}",
        F.col("seasonal_advert_product_embedding").getItem(dim_index).cast("double"),
    )

display(df_advert_seasonal_product_features.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Outputs

# COMMAND ----------

write_output_table(df_customer_seasonal_product_features, customer_seasonal_product_features_output_tbl)
write_output_table(df_advert_seasonal_product_features, advert_seasonal_product_features_output_tbl)

# COMMAND ----------

df_output_summary = spark.createDataFrame(
    [
        ("customer_seasonal_product_feature_rows", df_customer_seasonal_product_features.count()),
        ("customer_seasonal_product_embedding_rows", df_customer_seasonal_product_features.where(F.col("customer_seasonal_product_embedding").isNotNull()).count()),
        ("advert_seasonal_product_feature_rows", df_advert_seasonal_product_features.count()),
        ("advert_seasonal_product_embedding_rows", df_advert_seasonal_product_features.where(F.col("seasonal_advert_product_embedding").isNotNull()).count()),
    ],
    ["check_name", "row_count"],
)

display(df_output_summary)

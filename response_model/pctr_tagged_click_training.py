# Databricks notebook source
# MAGIC %md
# MAGIC # SB Tagged-Click pCTR Training Table
# MAGIC
# MAGIC This notebook builds a simple pCTR training table from Shopping Bag (SB)
# MAGIC tagged advert clicks.
# MAGIC
# MAGIC Grain: one row per observed account-ad exposure.
# MAGIC
# MAGIC Labels: same-session, 24h, and 7d tagged click windows.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC `reference_date` is the feature cut-off date for the labelled pCTR training
# MAGIC build. The notebook builds customer/ad/product features as of this date, then
# MAGIC creates exposure rows from `reference_date + 1` to `reference_date + 14`.
# MAGIC Click labels are then observed for the configured attribution window after
# MAGIC each exposure.
# MAGIC
# MAGIC This means a one-month snapshot represents "what would the pCTR training rows
# MAGIC look like if the model was being built from this point in time?" Multi-month
# MAGIC training repeats that same logic for a sequence of reference dates.
# MAGIC
# MAGIC `write_mode=overwrite_latest` writes the normal latest training table. Use
# MAGIC `write_mode=append_snapshot` to write a partition into a suffixed snapshot
# MAGIC table such as `_snapshots` or `_smoke`.
# MAGIC
# MAGIC The modes exist for different jobs:
# MAGIC
# MAGIC - `overwrite_latest` is for the current interactive/latest build. It refreshes
# MAGIC   the unsuffixed training table for debugging a single current cut of data.
# MAGIC - `append_snapshot` is for multi-month model training. It writes labelled
# MAGIC   rows into a history-style table and replaces only the current
# MAGIC   `reference_date` partition when rerun, so one bad month can be rebuilt
# MAGIC   without touching the others.
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
lookback_days = 90
attribution_window_days = 7
negative_sample_ratio = 20

print(
    "pCTR tagged-click training run config: "
    f"reference_date={reference_date}, "
    f"write_mode={write_mode}, "
    f"snapshot_table_suffix={snapshot_table_suffix}, "
    f"attribution_window_days={attribution_window_days}. "
    f"Widget options: reference_date='YYYY-MM-DD'; write_mode in {write_mode_options}; "
    "snapshot_table_suffix is free text, for example 'snapshots' or 'smoke'; "
    "attribution_window_days is fixed in this notebook unless edited in code. "
    "Meaning: features are cut off at reference_date; exposures run from "
    "reference_date + 1 through reference_date + 14; click labels look forward "
    "from those exposures."
)

dev_schema = spark.sql("SELECT current_user()").first()[0].split("@")[0].replace(".", "_")
customer_behaviour_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_customer_behaviour_features"
advert_attribute_profile_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_attribute_profile_90d"
advert_semantic_embeddings_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_semantic_embeddings_90d"
advert_product_features_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_product_features_90d"
customer_product_features_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_customer_product_features"
customer_seasonal_product_features_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_customer_seasonal_product_features"
advert_seasonal_product_features_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_seasonal_product_features"

rpid_tbl = "marketingdata_prod.warehouse.rpid_with_accounts"
bq_sessions_tbl = "marketingdata_prod.warehouse.bq_sessions_next_uk"
bq_pages_tbl = "marketingdata_prod.warehouse.bq_pages_next_uk"
bq_actions_tbl = "marketingdata_prod.warehouse.bq_actions_next_uk"
assignments_tbl = "marketingdata_prod.warehouse.next_uk_nextads_assignments"
control_sheet_tbl = "marketingdata_prod.warehouse.next_uk_nextads_control_sheet"
multipage_locations_tbl = "marketingdata_prod.warehouse.next_uk_nextads_multipage_locations"

sb_exposures_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_sb_observed_exposures"
training_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_sb_tagged_click_training"
training_sample_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_sb_tagged_click_training_sampled"

sb_page_path = "/shoppingbag"
target_action = "Banner Click - Next Ads"

feature_start_date = F.date_sub(F.lit(reference_date).cast("date"), lookback_days)
exposure_start_date = F.date_add(F.lit(reference_date).cast("date"), 1)
exposure_end_date = F.date_add(F.lit(reference_date).cast("date"), 14)
click_end_date = F.date_add(exposure_end_date, attribution_window_days)
assignment_rundate_start = F.date_sub(exposure_start_date, 1)
assignment_rundate_end = F.date_sub(exposure_end_date, 1)

# COMMAND ----------

def normalise_path(col_name):
    """Lowercase a URL path and strip query strings so path joins stay simple."""
    cleaned = F.trim(
        F.lower(
            F.regexp_replace(
                F.regexp_replace(F.col(col_name).cast("string"), r"[?#].*$", ""),
                r"\s+",
                "",
            )
        )
    )
    return F.when(cleaned == "", None).otherwise(cleaned)


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

    dot_product = sum(left_dense[index] * right_dense[index] for index in range(length))
    return float(dot_product / (left_norm * right_norm))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Features
# MAGIC
# MAGIC The customer behaviour table is already account-grain, so this notebook
# MAGIC reuses it directly and keeps one row per account.

# COMMAND ----------

df_customer_behaviour = (
    read_feature_table(customer_behaviour_tbl)
)

# display(df_customer_behaviour.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Account Mapping
# MAGIC
# MAGIC Click events are keyed by visit/session identifiers. This lookup gives us
# MAGIC the account number for each clean web session.

# COMMAND ----------

df_customer_accounts = (
    df_customer_behaviour
    .select("account_number")
    .dropDuplicates()
)


# COMMAND ----------

df_rpid_lookup = (
    df_customer_behaviour
    .select(
        "account_number",
        F.col("roamingprofileid").cast("string").alias("rpid"),
    )
    .where(F.col("rpid").isNotNull())
    .unionByName(
        spark.table(rpid_tbl)
        .select(
            F.col("account_number").cast("string").alias("account_number"),
            F.col("roamingprofileid").cast("string").alias("rpid"),
        )
        .where(F.col("rpid").isNotNull()),
        allowMissingColumns=True,
    )
    .join(F.broadcast(df_customer_accounts), on="account_number", how="inner")
    .dropDuplicates(["account_number", "rpid"])
)


# COMMAND ----------

df_session_accounts_raw = (
    spark.table(bq_sessions_tbl)
    .where((F.col("date") >= exposure_start_date) & (F.col("date") <= click_end_date))
    .select(
        F.col("date").alias("session_date"),
        F.col("UniqueVisitID").cast("string").alias("unique_visit_id"),
        F.col("RPID").cast("string").alias("rpid"),
        F.col("Device").cast("string").alias("device"),
    )
    .join(F.broadcast(df_rpid_lookup), on="rpid", how="inner")
    .select("account_number", "session_date", "unique_visit_id", "device")
    .dropDuplicates()
)

# display(df_session_accounts_raw.limit(10))

# COMMAND ----------

# Drop visit IDs that map to multiple accounts. These are not safe for labels.
df_multi_account_sessions = (
    df_session_accounts_raw
    .groupBy("session_date", "unique_visit_id")
    .agg(F.countDistinct("account_number").alias("account_count"))
    .where(F.col("account_count") > 1)
    .select("session_date", "unique_visit_id")
)

# display(df_multi_account_sessions.limit(10))

# COMMAND ----------

df_session_accounts = (
    df_session_accounts_raw
    .join(df_multi_account_sessions, on=["session_date", "unique_visit_id"], how="leftanti")
    .dropDuplicates(["account_number", "session_date", "unique_visit_id"])
)

# display(df_session_accounts.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Tagged Clicks
# MAGIC
# MAGIC First inspect where the tagged click action appears. SB is expected to be
# MAGIC `/shoppingbag`, but the check stays visible for review.

# COMMAND ----------

df_click_pagepath_check = (
    spark.table(bq_actions_tbl)
    .where((F.col("date") >= exposure_start_date) & (F.col("date") <= click_end_date))
    .where(F.col("Action") == target_action)
    .where(F.col("Level2").cast("string").rlike("^P"))
    .groupBy(F.col("PagePath").cast("string").alias("page_path"))
    .agg(F.count("*").alias("click_rows"))
    .orderBy(F.col("click_rows").desc())
)

display(df_click_pagepath_check)

# COMMAND ----------

df_click_actions_raw = (
    spark.table(bq_actions_tbl)
    .where((F.col("date") >= exposure_start_date) & (F.col("date") <= click_end_date))
    .where(F.col("Action") == target_action)
    .where(F.col("Level2").cast("string").rlike("^P"))
    .where(F.col("PagePath") == sb_page_path)
    .select(
        F.col("date").alias("click_date"),
        F.col("UniqueVisitID").cast("string").alias("unique_visit_id"),
        F.split(F.col("UniqueVisitID").cast("string"), "-").getItem(0).alias("unique_visitor_id"),
        F.col("Timestamp").cast("timestamp").alias("click_ts"),
        F.col("Level2").cast("string").alias("unique_ad_id"),
        F.col("PagePath").cast("string").alias("page_path"),
    )
    .where(F.col("unique_visit_id").isNotNull())
    .where(F.col("click_ts").isNotNull())
    .dropDuplicates(["click_date", "unique_visit_id", "click_ts", "unique_ad_id"])
)

# display(df_click_actions_raw.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clicks Mapped To Accounts
# MAGIC
# MAGIC Join clicks to sessions on exact date and visit ID. Anything that can't be mapped to one account is kept out of the training label.

# COMMAND ----------

df_click_actions_mapped = (
    df_click_actions_raw
    .join(
        df_session_accounts,
        (df_click_actions_raw.click_date == df_session_accounts.session_date)
        & (df_click_actions_raw.unique_visit_id == df_session_accounts.unique_visit_id),
        how="inner",
    )
    .select(
        df_session_accounts.account_number,
        df_click_actions_raw.click_date,
        df_click_actions_raw.unique_visit_id,
        df_click_actions_raw.unique_visitor_id,
        df_click_actions_raw.click_ts,
        df_click_actions_raw.unique_ad_id,
        df_click_actions_raw.page_path,
        df_session_accounts.device.alias("click_device"),
    )
    .dropDuplicates(["account_number", "click_ts", "unique_ad_id"])
)

display(df_click_actions_mapped.limit(10))

# COMMAND ----------

df_click_mapping_check = spark.createDataFrame(
    [
        ("raw_tagged_sb_clicks", df_click_actions_raw.count()),
        ("mapped_tagged_sb_clicks", df_click_actions_mapped.count()),
        ("unmapped_or_ambiguous_clicks", df_click_actions_raw.count() - df_click_actions_mapped.count()),
    ],
    ["check_name", "row_count"],
)

display(df_click_mapping_check)

# COMMAND ----------

df_click_counts_by_day_ad = (
    df_click_actions_mapped
    .groupBy("click_date", "unique_ad_id")
    .agg(F.count("*").alias("mapped_click_rows"))
    .orderBy("click_date", F.col("mapped_click_rows").desc())
)

display(df_click_counts_by_day_ad)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Observed SB Exposures
# MAGIC
# MAGIC Build the denominator directly for SB so we do not need the broad exposure
# MAGIC table from the advert URL visit EDA.

# COMMAND ----------

df_clicked_ad_population = (
    df_click_actions_raw
    .select("unique_ad_id")
    .dropDuplicates()
)

display(df_clicked_ad_population.limit(10))

# COMMAND ----------

df_control_sheet_tagged = (
    spark.table(control_sheet_tbl)
    .where((F.col("rundate") >= assignment_rundate_start) & (F.col("rundate") <= assignment_rundate_end))
    .withColumn("session_date", F.date_add(F.to_date(F.col("rundate")), 1))
    .select(
        F.col("session_date"),
        F.col("Location").cast("string").alias("placement_id"),
        F.col("UniqueAdID").cast("string").alias("unique_ad_id"),
        F.col("URL").cast("string").alias("advert_url"),
        F.col("CampaignNumber").cast("string").alias("campaign_id"),
        F.col("AdTrend").cast("string").alias("advert_theme"),
        F.coalesce(F.col("AdCategory").cast("string"), F.col("AdSubcategory").cast("string")).alias("advert_category"),
        F.col("Page").cast("string").alias("page_path"),
    )
    .where(F.col("unique_ad_id").rlike("^P"))
)

display(df_control_sheet_tagged.limit(10))

# COMMAND ----------

df_multipage_sb = (
    spark.table(multipage_locations_tbl)
    .where((F.col("rundate") >= assignment_rundate_start) & (F.col("rundate") <= assignment_rundate_end))
    .withColumn("session_date", F.date_add(F.to_date(F.col("rundate")), 1))
    .select(
        F.col("session_date"),
        F.col("Location").cast("string").alias("placement_id"),
        F.col("Page").cast("string").alias("multipage_page_path"),
    )
    .where(normalise_path("multipage_page_path") == sb_page_path)
    .dropDuplicates(["session_date", "placement_id"])
)

display(df_multipage_sb.limit(10))

# COMMAND ----------

df_ad_metadata_sb = (
    df_control_sheet_tagged
    .join(df_multipage_sb, on=["session_date", "placement_id"], how="left")
    .withColumn("page_path", F.coalesce(F.col("multipage_page_path"), F.col("page_path")))
    .where(normalise_path("page_path") == sb_page_path)
    .select(
        "session_date",
        "placement_id",
        "unique_ad_id",
        "advert_url",
        "campaign_id",
        "advert_theme",
        "advert_category",
        F.col("page_path").alias("configured_page_path"),
    )
    .dropDuplicates(["session_date", "placement_id", "unique_ad_id"])
)

display(df_ad_metadata_sb.limit(10))

# COMMAND ----------

# These are actual web visits to the SB page, mapped to account numbers.
df_sb_page_visits = (
    spark.table(bq_pages_tbl)
    .where((F.col("date") >= exposure_start_date) & (F.col("date") <= exposure_end_date))
    .select(
        F.col("date").alias("session_date"),
        F.col("UniqueVisitID").cast("string").alias("unique_visit_id"),
        F.col("PagePath").cast("string").alias("page_path"),
        F.coalesce(F.col("FirstTimestamp"), F.col("LastTimestamp")).cast("timestamp").alias("event_ts"),
    )
    .where(normalise_path("page_path") == sb_page_path)
    .join(df_session_accounts, on=["session_date", "unique_visit_id"], how="inner")
    .select("account_number", "session_date", "unique_visit_id", "device", "page_path", "event_ts")
    .where(F.col("event_ts").isNotNull())
    .dropDuplicates(["account_number", "session_date", "unique_visit_id", "event_ts"])
)

display(df_sb_page_visits.limit(10))

# COMMAND ----------

df_sb_account_days = (
    df_sb_page_visits
    .select("account_number", "session_date")
    .dropDuplicates()
)

display(df_sb_account_days.limit(10))

# COMMAND ----------

# Pull only assignment rows for accounts that actually visited SB on the day.
df_assignment_candidates_sb = (
    spark.table(assignments_tbl)
    .where((F.col("rundate") >= assignment_rundate_start) & (F.col("rundate") <= assignment_rundate_end))
    .withColumn("session_date", F.date_add(F.to_date(F.col("rundate")), 1))
    .select(
        F.col("AccountNumber").cast("string").alias("account_number"),
        F.col("session_date"),
        F.col("Location").cast("string").alias("placement_id"),
        F.col("Treatment").cast("string").alias("treatment"),
        F.col("UniqueAdIDMeasurement").cast("string").alias("unique_ad_id"),
        F.col("UniqueAdIDAssigned").cast("string").alias("assigned_unique_ad_id"),
        F.col("MASID").cast("string").alias("masid"),
    )
    .where(F.col("treatment") != "AdSuppressed")
    .where(F.col("unique_ad_id").isNotNull())
    .where(F.col("unique_ad_id") != "NoAdFound")
    .where(F.col("unique_ad_id").rlike("^P"))
    .join(df_sb_account_days, on=["account_number", "session_date"], how="inner")
    .join(df_ad_metadata_sb, on=["session_date", "placement_id", "unique_ad_id"], how="inner")
)

display(df_assignment_candidates_sb.limit(10))

# COMMAND ----------

df_exposures_sb = (
    df_sb_page_visits
    .join(df_assignment_candidates_sb, on=["account_number", "session_date"], how="inner")
    .groupBy(
        "account_number",
        "session_date",
        "unique_visit_id",
        "placement_id",
        "unique_ad_id",
        "assigned_unique_ad_id",
        "advert_url",
        "campaign_id",
        "advert_theme",
        "advert_category",
        "device",
        "page_path",
        "treatment",
    )
    .agg(F.min("event_ts").alias("exposure_ts"))
    .withColumn("exposure_source", F.lit("sb_page_visit_assignment"))
    .withColumn("exposure_confidence", F.lit("inferred_sb_surface_visit"))
    .withColumn("fallow_control", F.lit(None).cast("string"))
    .dropDuplicates(["account_number", "unique_ad_id", "placement_id", "unique_visit_id", "exposure_ts"])
)

display(df_exposures_sb.limit(10))

# COMMAND ----------

df_exposure_build_check = spark.createDataFrame(
    [
        ("sb_page_visits", df_sb_page_visits.count()),
        ("sb_account_days", df_sb_account_days.count()),
        ("sb_assignment_candidates", df_assignment_candidates_sb.count()),
        ("sb_observed_exposures", df_exposures_sb.count()),
    ],
    ["check_name", "row_count"],
)

display(df_exposure_build_check)

# COMMAND ----------

write_output_table(df_exposures_sb, sb_exposures_output_tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Training Base
# MAGIC
# MAGIC This is the model row grain before labels or account features are added.

# COMMAND ----------

df_training_base = (
    df_exposures_sb
    .select(
        F.lit(reference_date).cast("date").alias("reference_date"),
        "account_number",
        "unique_ad_id",
        "assigned_unique_ad_id",
        "placement_id",
        "unique_visit_id",
        "session_date",
        "exposure_ts",
        "advert_url",
        "campaign_id",
        "advert_theme",
        "advert_category",
        "device",
        "page_path",
        "treatment",
        "fallow_control",
        "exposure_source",
        "exposure_confidence",
        F.hour("exposure_ts").alias("exposure_hour"),
        F.dayofweek("session_date").alias("exposure_dayofweek"),
        F.month("session_date").alias("exposure_month"),
        F.weekofyear("session_date").alias("exposure_weekofyear"),
        F.quarter("session_date").alias("exposure_quarter"),
        F.when(F.dayofweek("session_date").isin(1, 7), F.lit(1)).otherwise(F.lit(0)).alias("exposure_is_weekend"),
        F.sin((F.month("session_date").cast("double") - F.lit(1.0)) * F.lit(2.0 * 3.141592653589793 / 12.0)).alias("exposure_month_sin"),
        F.cos((F.month("session_date").cast("double") - F.lit(1.0)) * F.lit(2.0 * 3.141592653589793 / 12.0)).alias("exposure_month_cos"),
        F.sin((F.weekofyear("session_date").cast("double") - F.lit(1.0)) * F.lit(2.0 * 3.141592653589793 / 52.0)).alias("exposure_week_sin"),
        F.cos((F.weekofyear("session_date").cast("double") - F.lit(1.0)) * F.lit(2.0 * 3.141592653589793 / 52.0)).alias("exposure_week_cos"),
        # Normalised campaign key for label join (strips creative/audience suffix)
        F.regexp_extract("unique_ad_id", r"^(P\d+_C\d+)", 1).alias("campaign_key"),
        F.regexp_extract("assigned_unique_ad_id", r"^(P\d+_C\d+)", 1).alias("assigned_campaign_key"),
    )
)

# display(df_training_base.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attribution Labels
# MAGIC
# MAGIC Join each exposure to later tagged clicks for the same account and advert.
# MAGIC The same join supports same-session, 24h, and 7d labels.

# COMMAND ----------

df_clicks_for_join = (
    df_click_actions_mapped
    .select(
        "account_number",
        F.col("unique_ad_id").alias("click_unique_ad_id"),
        F.col("unique_visit_id").alias("click_unique_visit_id"),
        "click_ts",
        # Normalised campaign key to match exposure side
        F.regexp_extract("unique_ad_id", r"^(P\d+_C\d+)", 1).alias("click_campaign_key"),
    )
)

# display(df_clicks_for_join.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Label Join Diagnostics
# MAGIC
# MAGIC If labels are all zero, these checks show where the click/exposure overlap
# MAGIC disappears: ad IDs, accounts, same visit, or timestamp ordering.

# COMMAND ----------

df_click_ad_ids = (
    df_clicks_for_join
    .select(F.col("click_unique_ad_id").alias("ad_id"))
    .where(F.col("ad_id").isNotNull())
    .dropDuplicates()
)

# display(df_click_ad_ids.limit(10))

# COMMAND ----------

df_exposure_measurement_ad_ids = (
    df_training_base
    .select(F.col("unique_ad_id").alias("ad_id"))
    .where(F.col("ad_id").isNotNull())
    .dropDuplicates()
)

# display(df_exposure_measurement_ad_ids.limit(10))

# COMMAND ----------

df_exposure_assigned_ad_ids = (
    df_training_base
    .select(F.col("assigned_unique_ad_id").alias("ad_id"))
    .where(F.col("ad_id").isNotNull())
    .dropDuplicates()
)

# display(df_exposure_assigned_ad_ids.limit(10))

# COMMAND ----------

df_click_campaign_keys = (
    df_clicks_for_join
    .select(F.col("click_campaign_key").alias("campaign_key"))
    .where(F.col("campaign_key") != "")
    .dropDuplicates()
)

df_exposure_campaign_keys = (
    df_training_base
    .select("campaign_key")
    .where(F.col("campaign_key") != "")
    .dropDuplicates()
)

df_label_join_overlap_check = spark.createDataFrame(
    [
        ("exposure_rows", df_training_base.count()),
        ("click_rows", df_clicks_for_join.count()),
        ("measurement_ad_id_overlap (exact)", df_exposure_measurement_ad_ids.join(df_click_ad_ids, on="ad_id", how="inner").count()),
        ("campaign_key_overlap", df_exposure_campaign_keys.join(df_click_campaign_keys, on="campaign_key", how="inner").count()),
        (
            "account_overlap",
            df_training_base.select("account_number").dropDuplicates()
            .join(df_clicks_for_join.select("account_number").dropDuplicates(), on="account_number", how="inner")
            .count(),
        ),
        (
            "account_campaign_key_overlap",
            df_training_base.select("account_number", "campaign_key").where(F.col("campaign_key") != "").dropDuplicates()
            .join(
                df_clicks_for_join.select("account_number", F.col("click_campaign_key").alias("campaign_key")).where(F.col("campaign_key") != "").dropDuplicates(),
                on=["account_number", "campaign_key"],
                how="inner",
            )
            .count(),
        ),
        (
            "same_visit_overlap",
            df_training_base.select("account_number", "unique_visit_id").dropDuplicates()
            .join(
                df_clicks_for_join.select("account_number", F.col("click_unique_visit_id").alias("unique_visit_id")).dropDuplicates(),
                on=["account_number", "unique_visit_id"],
                how="inner",
            )
            .count(),
        ),
    ],
    ["check_name", "row_count"],
)

display(df_label_join_overlap_check)

# COMMAND ----------

df_label_join_example_matches = (
    df_training_base.alias("exp")
    .join(
        df_clicks_for_join.alias("clk"),
        (
            (F.col("exp.account_number") == F.col("clk.account_number"))
            & (
                (F.col("exp.campaign_key") == F.col("clk.click_campaign_key"))
                | (F.col("exp.assigned_campaign_key") == F.col("clk.click_campaign_key"))
            )
        ),
        how="inner",
    )
    .select(
        F.col("exp.account_number"),
        F.col("exp.unique_visit_id").alias("exposure_unique_visit_id"),
        F.col("clk.click_unique_visit_id"),
        F.col("exp.campaign_key").alias("matched_campaign_key"),
        F.col("exp.unique_ad_id").alias("measurement_unique_ad_id"),
        F.col("exp.assigned_unique_ad_id"),
        F.col("clk.click_unique_ad_id"),
        F.col("exp.exposure_ts"),
        F.col("clk.click_ts"),
        F.when(F.col("exp.campaign_key") == F.col("clk.click_campaign_key"), F.lit("measurement"))
        .when(F.col("exp.assigned_campaign_key") == F.col("clk.click_campaign_key"), F.lit("assigned"))
        .otherwise(F.lit("unknown"))
        .alias("ad_id_match_type"),
        ((F.unix_timestamp("clk.click_ts") - F.unix_timestamp("exp.exposure_ts")) / F.lit(60.0)).alias("minutes_after_exposure"),
    )
    .orderBy(F.abs(F.col("minutes_after_exposure")).asc())
)

display(df_label_join_example_matches.limit(50))

# COMMAND ----------

df_training_with_clicks = (
    df_training_base.alias("exp")
    .join(
        df_clicks_for_join.alias("clk"),
        (
            (F.col("exp.account_number") == F.col("clk.account_number"))
            & (
                (F.col("exp.campaign_key") == F.col("clk.click_campaign_key"))
                | (F.col("exp.assigned_campaign_key") == F.col("clk.click_campaign_key"))
            )
            & (F.col("clk.click_ts") >= F.col("exp.exposure_ts"))
            & (F.col("clk.click_ts") <= F.col("exp.exposure_ts") + F.expr("INTERVAL 7 DAYS"))
        ),
        how="left",
    )
)

display(df_training_with_clicks.limit(10))

# COMMAND ----------

df_training_labelled = (
    df_training_with_clicks
    .groupBy(
        F.col("exp.reference_date").alias("reference_date"),
        F.col("exp.account_number").alias("account_number"),
        F.col("exp.unique_ad_id").alias("unique_ad_id"),
        F.col("exp.assigned_unique_ad_id").alias("assigned_unique_ad_id"),
        F.col("exp.campaign_key").alias("campaign_key"),
        F.col("exp.assigned_campaign_key").alias("assigned_campaign_key"),
        F.col("exp.placement_id").alias("placement_id"),
        F.col("exp.unique_visit_id").alias("unique_visit_id"),
        F.col("exp.session_date").alias("session_date"),
        F.col("exp.exposure_ts").alias("exposure_ts"),
        F.col("exp.advert_url").alias("advert_url"),
        F.col("exp.campaign_id").alias("campaign_id"),
        F.col("exp.advert_theme").alias("advert_theme"),
        F.col("exp.advert_category").alias("advert_category"),
        F.col("exp.device").alias("device"),
        F.col("exp.page_path").alias("page_path"),
        F.col("exp.treatment").alias("treatment"),
        F.col("exp.fallow_control").alias("fallow_control"),
        F.col("exp.exposure_source").alias("exposure_source"),
        F.col("exp.exposure_confidence").alias("exposure_confidence"),
        F.col("exp.exposure_hour").alias("exposure_hour"),
        F.col("exp.exposure_dayofweek").alias("exposure_dayofweek"),
        F.col("exp.exposure_month").alias("exposure_month"),
        F.col("exp.exposure_weekofyear").alias("exposure_weekofyear"),
        F.col("exp.exposure_quarter").alias("exposure_quarter"),
        F.col("exp.exposure_is_weekend").alias("exposure_is_weekend"),
        F.col("exp.exposure_month_sin").alias("exposure_month_sin"),
        F.col("exp.exposure_month_cos").alias("exposure_month_cos"),
        F.col("exp.exposure_week_sin").alias("exposure_week_sin"),
        F.col("exp.exposure_week_cos").alias("exposure_week_cos"),
    )
    .agg(
        F.min("clk.click_ts").alias("first_click_ts_7d"),
        F.max(
            F.when(F.col("clk.click_unique_visit_id") == F.col("exp.unique_visit_id"), 1).otherwise(0)
        ).alias("label_same_session"),
        F.max(
            F.when(F.col("clk.click_ts") <= F.col("exp.exposure_ts") + F.expr("INTERVAL 24 HOURS"), 1).otherwise(0)
        ).alias("label_24h"),
        F.max(
            F.when(F.col("clk.click_ts") <= F.col("exp.exposure_ts") + F.expr("INTERVAL 7 DAYS"), 1).otherwise(0)
        ).alias("label_7d"),
    )
    .withColumn(
        "hours_to_first_click",
        F.when(
            F.col("first_click_ts_7d").isNotNull(),
            (F.unix_timestamp("first_click_ts_7d") - F.unix_timestamp("exposure_ts")) / F.lit(3600.0),
        ),
    )
)

display(df_training_labelled.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Label Checks
# MAGIC
# MAGIC These checks let us choose the final training label after seeing how sparse
# MAGIC each attribution window is.

# COMMAND ----------

df_label_rate_check = (
    df_training_labelled
    .agg(
        F.count("*").alias("exposure_rows"),
        F.sum("label_same_session").alias("positive_same_session"),
        F.sum("label_24h").alias("positive_24h"),
        F.sum("label_7d").alias("positive_7d"),
    )
    .withColumn("rate_same_session", F.col("positive_same_session") / F.col("exposure_rows"))
    .withColumn("rate_24h", F.col("positive_24h") / F.col("exposure_rows"))
    .withColumn("rate_7d", F.col("positive_7d") / F.col("exposure_rows"))
)

display(df_label_rate_check)

# COMMAND ----------

df_label_rate_by_ad = (
    df_training_labelled
    .groupBy("unique_ad_id")
    .agg(
        F.count("*").alias("exposure_rows"),
        F.sum("label_same_session").alias("positive_same_session"),
        F.sum("label_24h").alias("positive_24h"),
        F.sum("label_7d").alias("positive_7d"),
    )
    .orderBy(F.col("positive_7d").desc(), F.col("exposure_rows").desc())
)

display(df_label_rate_by_ad)

# COMMAND ----------

# This check should return zero rows: a 7d negative should not have a 7d click.
df_negative_leakage_check = (
    df_training_labelled.alias("lab")
    .where(F.col("label_7d") == 0)
    .join(
        df_clicks_for_join.alias("clk"),
        (
            (F.col("lab.account_number") == F.col("clk.account_number"))
            & (
                (F.col("lab.campaign_key") == F.col("clk.click_campaign_key"))
                | (F.col("lab.assigned_campaign_key") == F.col("clk.click_campaign_key"))
            )
            & (F.col("clk.click_ts") >= F.col("lab.exposure_ts"))
            & (F.col("clk.click_ts") <= F.col("lab.exposure_ts") + F.expr("INTERVAL 7 DAYS"))
        ),
        how="inner",
    )
)

display(df_negative_leakage_check.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Advert Features
# MAGIC
# MAGIC Join the reusable advert-side feature layers built by
# MAGIC `pctr_advert_metadata_attribute_profile` and
# MAGIC `pctr_advert_semantic_embeddings`. The original control-sheet theme and
# MAGIC category fields remain on the table for audit, but the useful modelling
# MAGIC signal now comes from linked items, catalogue attributes, creative text, and
# MAGIC semantic dimensions.

# COMMAND ----------

advert_attribute_feature_cols = [
    "top_brand",
    "top_use",
    "top_colour",
    "top_style",
    "top_category",
    "top_department",
    "top_gender",
    "top_brand_weight",
    "top_use_weight",
    "top_colour_weight",
    "top_style_weight",
    "top_category_weight",
    "top_department_weight",
    "top_gender_weight",
    "advert_active_placement_count",
    "attribute_profile_attribute_count",
    "attribute_profile_value_count",
    "advert_item_count",
    "advert_item_weight_sum",
    "brand_profile_distinct_values",
    "use_profile_distinct_values",
    "colour_profile_distinct_values",
    "style_profile_distinct_values",
    "category_profile_distinct_values",
    "department_profile_distinct_values",
    "gender_profile_distinct_values",
    "has_item_attribute_profile",
]

advert_semantic_feature_cols = [
    "advert_semantic_char_count",
    "advert_semantic_token_count",
    "advert_semantic_unique_token_count",
    "advert_has_destination_image",
    "advert_embedding_neighbour_count",
    "advert_embedding_top_similarity",
    "advert_embedding_avg_similarity",
] + [f"advert_semantic_dim_{dim_index:03d}" for dim_index in range(32)]

advert_product_feature_cols = [
    "advert_product_item_count",
    "advert_product_embedded_item_count",
    "advert_product_embedding_coverage",
    "advert_product_embedding",
] + [f"advert_product_dim_{dim_index:03d}" for dim_index in range(32)]

df_advert_attribute_features = (
    read_feature_table(advert_attribute_profile_tbl)
    .select(
        F.col("feature_date").alias("advert_attribute_feature_date"),
        F.col("advert_id").alias("advert_attribute_feature_advert_id"),
        *[F.col(col_name) for col_name in advert_attribute_feature_cols],
    )
    .dropDuplicates(["advert_attribute_feature_date", "advert_attribute_feature_advert_id"])
)

df_advert_semantic_features = (
    read_feature_table(advert_semantic_embeddings_tbl)
    .select(
        F.col("feature_date").alias("advert_semantic_feature_date"),
        F.col("advert_id").alias("advert_semantic_feature_advert_id"),
        *[F.col(col_name) for col_name in advert_semantic_feature_cols],
    )
    .dropDuplicates(["advert_semantic_feature_date", "advert_semantic_feature_advert_id"])
)

df_advert_product_features = (
    read_feature_table(advert_product_features_tbl)
    .select(
        F.col("feature_date").alias("advert_product_feature_date"),
        F.col("advert_id").alias("advert_product_feature_advert_id"),
        *[F.col(col_name) for col_name in advert_product_feature_cols],
    )
    .dropDuplicates(["advert_product_feature_date", "advert_product_feature_advert_id"])
)

df_training_with_attribute_features = (
    df_training_labelled.alias("train")
    .join(
        df_advert_attribute_features.alias("attr"),
        (F.col("train.session_date") == F.col("attr.advert_attribute_feature_date"))
        & (F.col("train.unique_ad_id") == F.col("attr.advert_attribute_feature_advert_id")),
        how="left",
    )
    .drop("advert_attribute_feature_date", "advert_attribute_feature_advert_id")
)

df_training_with_semantic_features = (
    df_training_with_attribute_features.alias("train")
    .join(
        df_advert_semantic_features.alias("sem"),
        (F.col("train.session_date") == F.col("sem.advert_semantic_feature_date"))
        & (F.col("train.unique_ad_id") == F.col("sem.advert_semantic_feature_advert_id")),
        how="left",
    )
    .drop("advert_semantic_feature_date", "advert_semantic_feature_advert_id")
)

df_training_with_advert_features = (
    df_training_with_semantic_features.alias("train")
    .join(
        df_advert_product_features.alias("prod"),
        (F.col("train.session_date") == F.col("prod.advert_product_feature_date"))
        & (F.col("train.unique_ad_id") == F.col("prod.advert_product_feature_advert_id")),
        how="left",
    )
    .drop("advert_product_feature_date", "advert_product_feature_advert_id")
)

advert_seasonal_product_feature_cols = [
    "advert_product_views_7d",
    "advert_product_views_30d",
    "advert_product_purchases_7d",
    "advert_product_purchases_30d",
    "advert_product_views_ly_same_month",
    "advert_product_purchases_ly_same_month",
    "advert_product_trending_7x30",
    "seasonal_advert_product_embedding_coverage",
    "seasonal_advert_product_embedding",
] + [f"seasonal_advert_product_dim_{dim_index:03d}" for dim_index in range(32)]

df_advert_seasonal_product_features = (
    read_feature_table(advert_seasonal_product_features_tbl)
    .select(
        F.col("feature_date").alias("advert_seasonal_product_feature_date"),
        F.col("advert_id").alias("advert_seasonal_product_feature_advert_id"),
        *[F.col(col_name) for col_name in advert_seasonal_product_feature_cols],
    )
    .dropDuplicates(["advert_seasonal_product_feature_date", "advert_seasonal_product_feature_advert_id"])
)

df_training_with_advert_features = (
    df_training_with_advert_features.alias("train")
    .join(
        df_advert_seasonal_product_features.alias("seasonal_prod"),
        (F.col("train.session_date") == F.col("seasonal_prod.advert_seasonal_product_feature_date"))
        & (F.col("train.unique_ad_id") == F.col("seasonal_prod.advert_seasonal_product_feature_advert_id")),
        how="left",
    )
    .drop("advert_seasonal_product_feature_date", "advert_seasonal_product_feature_advert_id")
)

display(df_training_with_advert_features.limit(10))

# COMMAND ----------

df_advert_feature_join_check = spark.createDataFrame(
    [
        ("training_labelled_rows", df_training_labelled.count()),
        ("training_rows_with_attribute_profile", df_training_with_advert_features.where(F.col("has_item_attribute_profile")).count()),
        ("training_rows_with_semantic_features", df_training_with_advert_features.where(F.col("advert_semantic_token_count").isNotNull()).count()),
        ("training_rows_with_advert_product_features", df_training_with_advert_features.where(F.col("advert_product_embedding").isNotNull()).count()),
        ("training_rows_with_advert_seasonal_product_features", df_training_with_advert_features.where(F.col("seasonal_advert_product_embedding").isNotNull()).count()),
    ],
    ["check_name", "row_count"],
)

display(df_advert_feature_join_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Customer Features
# MAGIC
# MAGIC Keep all labels. A downstream modelling notebook can choose which label
# MAGIC column becomes the final target.

# COMMAND ----------

customer_product_feature_cols = [
    "customer_product_interaction_count",
    "customer_product_purchase_interaction_count",
    "customer_product_view_interaction_count",
    "customer_product_distinct_item_count",
    "customer_product_embedded_item_count",
    "customer_product_embedding_coverage",
    "customer_product_embedding",
] + [f"customer_product_dim_{dim_index:03d}" for dim_index in range(32)]

df_customer_product_features = (
    read_feature_table(customer_product_features_tbl)
    .select(
        "account_number",
        *[F.col(col_name) for col_name in customer_product_feature_cols],
    )
    .dropDuplicates(["account_number"])
)

customer_seasonal_product_feature_cols = [
    "customer_same_month_ly_purchase_count",
    "customer_same_month_ly_distinct_item_count",
    "customer_seasonal_product_embedding_coverage",
    "customer_seasonal_product_embedding",
] + [f"customer_seasonal_product_dim_{dim_index:03d}" for dim_index in range(32)]

df_customer_seasonal_product_features = (
    read_feature_table(customer_seasonal_product_features_tbl)
    .select(
        "account_number",
        *[F.col(col_name) for col_name in customer_seasonal_product_feature_cols],
    )
    .dropDuplicates(["account_number"])
)

df_training_with_customer_behaviour_features = (
    df_training_with_advert_features
    .join(
        df_customer_behaviour.drop("roamingprofileid", "reference_date"),
        on="account_number",
        how="left",
    )
)

df_training_with_customer_product_features = (
    df_training_with_customer_behaviour_features
    .join(
        df_customer_product_features,
        on="account_number",
        how="left",
    )
    .join(
        df_customer_seasonal_product_features,
        on="account_number",
        how="left",
    )
)

raw_product_match_cols = [
    "advert_product_embedding",
    "customer_product_embedding",
    "seasonal_advert_product_embedding",
    "customer_seasonal_product_embedding",
]

df_training_features = (
    df_training_with_customer_product_features
    .withColumn(
        "customer_ad_product_cosine_similarity",
        cosine_similarity(F.col("customer_product_embedding"), F.col("advert_product_embedding")),
    )
    .withColumn(
        "customer_ad_product_embedding_coverage",
        F.when(
            F.col("customer_product_embedding").isNotNull()
            & F.col("advert_product_embedding").isNotNull(),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    .withColumn(
        "customer_ad_seasonal_product_cosine_similarity",
        cosine_similarity(F.col("customer_seasonal_product_embedding"), F.col("seasonal_advert_product_embedding")),
    )
    .withColumn(
        "customer_ad_seasonal_product_embedding_coverage",
        F.when(
            F.col("customer_seasonal_product_embedding").isNotNull()
            & F.col("seasonal_advert_product_embedding").isNotNull(),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    .drop(*raw_product_match_cols)
)

display(df_training_features.limit(10))

# COMMAND ----------

df_product_feature_join_check = spark.createDataFrame(
    [
        ("training_rows_with_customer_product_features", df_training_features.where(F.col("customer_product_interaction_count").isNotNull()).count()),
        ("training_rows_with_customer_seasonal_product_features", df_training_features.where(F.col("customer_same_month_ly_purchase_count").isNotNull()).count()),
        ("training_rows_with_customer_ad_product_match", df_training_features.where(F.col("customer_ad_product_embedding_coverage") == 1).count()),
        ("training_rows_with_customer_ad_seasonal_product_match", df_training_features.where(F.col("customer_ad_seasonal_product_embedding_coverage") == 1).count()),
        (
            "customer_ad_product_cosine_out_of_bounds",
            df_training_features
            .where(
                F.col("customer_ad_product_cosine_similarity").isNotNull()
                & (
                    (F.col("customer_ad_product_cosine_similarity") < -1.000001)
                    | (F.col("customer_ad_product_cosine_similarity") > 1.000001)
                )
            )
            .count(),
        ),
        (
            "customer_ad_seasonal_product_cosine_out_of_bounds",
            df_training_features
            .where(
                F.col("customer_ad_seasonal_product_cosine_similarity").isNotNull()
                & (
                    (F.col("customer_ad_seasonal_product_cosine_similarity") < -1.000001)
                    | (F.col("customer_ad_seasonal_product_cosine_similarity") > 1.000001)
                )
            )
            .count(),
        ),
    ],
    ["check_name", "row_count"],
)

display(df_product_feature_join_check)

# COMMAND ----------

df_training_row_count_check = spark.createDataFrame(
    [
        ("training_base_rows", df_training_base.count()),
        ("training_labelled_rows", df_training_labelled.count()),
        ("training_with_advert_feature_rows", df_training_with_advert_features.count()),
        ("training_with_customer_product_feature_rows", df_training_with_customer_product_features.count()),
        ("training_feature_rows", df_training_features.count()),
    ],
    ["check_name", "row_count"],
)

display(df_training_row_count_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Full Training Table
# MAGIC
# MAGIC The full table keeps every observed exposure and every label window.

# COMMAND ----------

write_output_table(df_training_features, training_output_tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional Negative Downsample
# MAGIC
# MAGIC Keep every positive. Sample 7d negatives at a fixed ratio for modelling.
# MAGIC The full unsampled table above remains the source of truth.

# COMMAND ----------

positive_rows_7d = df_training_features.where(F.col("label_7d") == 1).count()
negative_rows_7d = df_training_features.where(F.col("label_7d") == 0).count()

negative_fraction = min(
    1.0,
    (positive_rows_7d * negative_sample_ratio) / negative_rows_7d if negative_rows_7d else 0.0,
)

print(f"7d positives: {positive_rows_7d:,}")
print(f"7d negatives: {negative_rows_7d:,}")
print(f"Negative sample fraction: {negative_fraction:.6f}")

# COMMAND ----------

df_training_positive = (
    df_training_features
    .where(F.col("label_7d") == 1)
    .withColumn("sample_weight", F.lit(1.0))
)

display(df_training_positive.limit(10))

# COMMAND ----------

df_training_negative_sample = (
    df_training_features
    .where(F.col("label_7d") == 0)
    .sample(withReplacement=False, fraction=negative_fraction, seed=42)
    .withColumn(
        "sample_weight",
        F.when(F.lit(negative_fraction) > 0, F.lit(1.0) / F.lit(negative_fraction)).otherwise(F.lit(None).cast("double")),
    )
)

display(df_training_negative_sample.limit(10))

# COMMAND ----------

df_training_sampled = (
    df_training_positive
    .unionByName(df_training_negative_sample, allowMissingColumns=True)
)

display(df_training_sampled.limit(10))

# COMMAND ----------

df_sample_count_check = spark.createDataFrame(
    [
        ("full_training_rows", df_training_features.count()),
        ("sampled_training_rows", df_training_sampled.count()),
        ("sampled_positive_rows_7d", df_training_sampled.where(F.col("label_7d") == 1).count()),
        ("sampled_negative_rows_7d", df_training_sampled.where(F.col("label_7d") == 0).count()),
    ],
    ["check_name", "row_count"],
)

display(df_sample_count_check)

# COMMAND ----------

write_output_table(df_training_sampled, training_sample_output_tbl)

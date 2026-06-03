# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Behaviour Feature Table
# MAGIC
# MAGIC This notebook builds a lightweight account-grain customer behaviour feature
# MAGIC table for response models.
# MAGIC
# MAGIC It is intentionally smaller than the EDA notebook: it keeps stable customer
# MAGIC descriptors, recent web browsing activity, and simple action behaviour.

# COMMAND ----------

from pyspark.sql import functions as F

spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Keep inputs, outputs, and dates visible so the feature table is easy to
# MAGIC rerun for a new response-model build.
# MAGIC
# MAGIC `reference_date` is the point-in-time anchor for the feature build. Treat it
# MAGIC as "the day we are pretending to run the pCTR pipeline". Customer recency,
# MAGIC browsing activity, and action lookbacks are calculated backwards from this
# MAGIC date.
# MAGIC
# MAGIC `write_mode=overwrite_latest` writes the normal latest table. Use
# MAGIC `write_mode=append_snapshot` to write a partition into a suffixed snapshot
# MAGIC table such as `_snapshots` or `_smoke`.
# MAGIC
# MAGIC The modes exist for different jobs:
# MAGIC
# MAGIC - `overwrite_latest` is for the current interactive/latest build. It refreshes
# MAGIC   the unsuffixed table that downstream notebooks can treat as "the latest
# MAGIC   available features".
# MAGIC - `append_snapshot` is for repeatable point-in-time training data. It writes
# MAGIC   the same feature grain into a history-style table and replaces only the
# MAGIC   current `reference_date` partition when rerun.
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

print(
    "pCTR customer behaviour run config: "
    f"reference_date={reference_date}, "
    f"write_mode={write_mode}, "
    f"snapshot_table_suffix={snapshot_table_suffix}. "
    f"Widget options: reference_date='YYYY-MM-DD'; write_mode in {write_mode_options}; "
    "snapshot_table_suffix is free text, for example 'snapshots' or 'smoke'. "
    "Meaning: reference_date is the as-of date; lookback features are calculated "
    "backwards from that point in time."
)

customer_source_tbl = "marketingdata_prod.warehouse.customers_data_mart"
svoc_customer_history_tbl = "marketingdata_prod.warehouse.svoccust_hist"
rpid_tbl = "marketingdata_prod.warehouse.rpid_with_accounts"
bq_sessions_tbl = "marketingdata_prod.warehouse.bq_sessions_next_uk"
bq_pages_tbl = "marketingdata_prod.warehouse.bq_pages_next_uk"
bq_actions_tbl = "marketingdata_prod.warehouse.bq_actions_next_uk"

dev_schema = spark.sql("SELECT current_user()").first()[0].split("@")[0].replace(".", "_")
customer_behaviour_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_customer_behaviour_features"

country_code = "GB"
client_name = "NEXT"
true_like_values = ["Y", "YES", "TRUE", "1", "CURRENT", "ACTIVE"]

feature_end_date = F.lit(reference_date).cast("date")
feature_start_date = F.date_sub(feature_end_date, lookback_days)

# COMMAND ----------

def normalise_path(col_name):
    """Lowercase a URL path and strip query strings for simple path features."""
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


# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Base
# MAGIC
# MAGIC Start with current GB NEXT full accounts. These are the accounts that can
# MAGIC receive response-model features and later join to click/exposure labels.

# COMMAND ----------

df_svoc_latest = (
    spark.table(svoc_customer_history_tbl)
    .where(F.upper(F.trim(F.col("CountryCode"))) == country_code)
    .where(F.upper(F.trim(F.col("client"))) == client_name)
    .where(F.col("LatestAccountKeyIndicator") == F.lit(True))
    .where(F.upper(F.trim(F.col("AccountIsCurrent"))).isin(true_like_values))
    .select(
        F.col("account_number").cast("string").alias("account_number"),
        F.col("PostcodeArea").cast("string").alias("postcode_area"),
        F.when(F.upper(F.trim(F.col("ThreeStepFlag"))) == "Y", F.lit("ThreeStep"))
        .when(F.upper(F.trim(F.col("CashIndicatorDescription"))) == "CREDIT", F.lit("Pay"))
        .otherwise(F.lit("Cash"))
        .alias("svoc_credit_type"),
        F.coalesce(
            F.to_date(F.col("InternetCreditAgreementAcceptDate").cast("string")),
            F.to_date(F.col("CreditAgreementSignedDate").cast("string")),
            F.to_date(F.col("CreditAgreementLastSentDate").cast("string")),
            F.when(
                F.upper(F.trim(F.col("CashIndicatorDescription"))) == "CREDIT",
                F.to_date(F.col("AccountStartDate").cast("string")),
            ),
        ).alias("svoc_credit_date"),
    )
    .dropDuplicates(["account_number"])
)

# display(df_svoc_latest.limit(10))

# COMMAND ----------

df_customer_base = (
    spark.table(customer_source_tbl)
    .select(
        F.col("accountnumberkey").cast("string").alias("accountnumberkey"),
        F.col("account_number").cast("string").alias("account_number"),
        F.col("roamingprofileid").cast("string").alias("roamingprofileid"),
        F.col("countrycode").cast("string").alias("countrycode"),
        F.col("next_group_client").cast("string").alias("next_group_client"),
        F.col("currentcustomerflag").cast("string").alias("currentcustomerflag"),
        F.col("account_type").cast("string").alias("account_type"),
        F.to_date(F.col("accountstartdate").cast("string")).alias("accountstartdate_dt"),
        F.to_date(F.col("lastsitevisit").cast("string")).alias("lastsitevisit_dt"),
        F.to_date(F.col("lastonlineorderdate").cast("string")).alias("lastonlineorderdate_dt"),
        F.to_date(F.col("laststorepurchase").cast("string")).alias("laststorepurchase_dt"),
        F.col("online_orders").cast("double").alias("online_orders_n"),
        F.col("online_spend").cast("double").alias("online_spend_n"),
        F.col("online_returns").cast("double").alias("online_returns_n"),
        F.col("retail_orders").cast("double").alias("retail_orders_n"),
        F.col("retail_spend").cast("double").alias("retail_spend_n"),
        F.col("retail_returns").cast("double").alias("retail_returns_n"),
        F.col("creditcustomer").cast("string").alias("creditcustomer"),
        F.col("CreditActive").cast("string").alias("creditactive"),
        F.col("CashActive").cast("string").alias("cashactive"),
        F.col("emailoptin").cast("string").alias("emailoptin"),
        F.col("smsoptin").cast("string").alias("smsoptin"),
        F.col("gender").cast("string").alias("gender"),
        F.col("uk_region").cast("string").alias("uk_region"),
        F.col("lapsingstatus").cast("string").alias("lapsingstatus"),
        F.col("SpecialAccount").cast("string").alias("specialaccount"),
        F.col("CustomerProfile").cast("string").alias("customerprofile"),
    )
    .join(F.broadcast(df_svoc_latest), on="account_number", how="left")
    .withColumn("countrycode_norm", F.upper(F.trim(F.col("countrycode"))))
    .withColumn("next_group_client_norm", F.upper(F.trim(F.col("next_group_client"))))
    .withColumn("currentcustomerflag_norm", F.upper(F.trim(F.col("currentcustomerflag"))))
    .where(F.col("countrycode_norm") == country_code)
    .where(F.col("next_group_client_norm") == client_name)
    .where(F.col("currentcustomerflag_norm").isin(true_like_values))
    .where(F.upper(F.trim(F.col("account_type"))) == "FULL")
    .withColumn("online_orders_n", F.coalesce(F.col("online_orders_n"), F.lit(0.0)))
    .withColumn("online_spend_n", F.coalesce(F.col("online_spend_n"), F.lit(0.0)))
    .withColumn("online_returns_n", F.coalesce(F.col("online_returns_n"), F.lit(0.0)))
    .withColumn("retail_orders_n", F.coalesce(F.col("retail_orders_n"), F.lit(0.0)))
    .withColumn("retail_spend_n", F.coalesce(F.col("retail_spend_n"), F.lit(0.0)))
    .withColumn("retail_returns_n", F.coalesce(F.col("retail_returns_n"), F.lit(0.0)))
    .withColumn("online_aov", F.when(F.col("online_orders_n") > 0, F.col("online_spend_n") / F.col("online_orders_n")))
    .withColumn("retail_aov", F.when(F.col("retail_orders_n") > 0, F.col("retail_spend_n") / F.col("retail_orders_n")))
    .withColumn("online_return_rate", F.when(F.col("online_spend_n") > 0, F.col("online_returns_n") / F.col("online_spend_n")))
    .withColumn("retail_return_rate", F.when(F.col("retail_spend_n") > 0, F.col("retail_returns_n") / F.col("retail_spend_n")))
    .withColumn(
        "latest_known_activity_date",
        F.greatest(
            F.col("lastsitevisit_dt"),
            F.col("lastonlineorderdate_dt"),
            F.col("laststorepurchase_dt"),
            F.col("svoc_credit_date"),
        ),
    )
    .withColumn("latest_known_activity_recency_days", F.datediff(feature_end_date, F.col("latest_known_activity_date")))
    .withColumn("account_age_days", F.datediff(feature_end_date, F.col("accountstartdate_dt")))
    .drop("countrycode_norm", "next_group_client_norm", "currentcustomerflag_norm")
    .dropDuplicates(["account_number"])
)

# display(df_customer_base.limit(10))

# COMMAND ----------

df_customer_accounts = (
    df_customer_base
    .select("account_number")
    .dropDuplicates()
)

# display(df_customer_accounts.limit(10))

# COMMAND ----------

df_rpid_lookup = (
    df_customer_base
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

# display(df_rpid_lookup.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Web Session Linkage
# MAGIC
# MAGIC Build a clean account-to-session bridge for the feature window. Ambiguous
# MAGIC visit IDs are removed so behaviour is not assigned to the wrong account.

# COMMAND ----------

df_feature_sessions_raw = (
    spark.table(bq_sessions_tbl)
    .where((F.col("date") >= feature_start_date) & (F.col("date") <= feature_end_date))
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

# display(df_feature_sessions_raw.limit(10))

# COMMAND ----------

df_multi_account_feature_sessions = (
    df_feature_sessions_raw
    .groupBy("session_date", "unique_visit_id")
    .agg(F.countDistinct("account_number").alias("account_count"))
    .where(F.col("account_count") > 1)
    .select("session_date", "unique_visit_id")
)

# display(df_multi_account_feature_sessions.limit(10))

# COMMAND ----------

df_feature_sessions = (
    df_feature_sessions_raw
    .join(df_multi_account_feature_sessions, on=["session_date", "unique_visit_id"], how="leftanti")
    .dropDuplicates(["account_number", "session_date", "unique_visit_id"])
)

# display(df_feature_sessions.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Browse Features
# MAGIC
# MAGIC Keep browsing features compact: session counts, active days, recency, and
# MAGIC page volume. These are cheap to explain and useful as activity signals.

# COMMAND ----------

df_page_events = (
    spark.table(bq_pages_tbl)
    .where((F.col("date") >= feature_start_date) & (F.col("date") <= feature_end_date))
    .select(
        F.col("date").alias("session_date"),
        F.col("UniqueVisitID").cast("string").alias("unique_visit_id"),
        F.col("PagePath").cast("string").alias("page_path"),
    )
    .join(df_feature_sessions, on=["session_date", "unique_visit_id"], how="inner")
)

# display(df_page_events.limit(10))

# COMMAND ----------

df_pages_per_session = (
    df_page_events
    .groupBy("account_number", "session_date", "unique_visit_id")
    .agg(
        F.count("*").alias("pages_in_session"),
        F.sum(F.when(normalise_path("page_path") == "/shoppingbag", 1).otherwise(0)).alias("shopping_bag_pages_in_session"),
    )
)

# display(df_pages_per_session.limit(10))

# COMMAND ----------

df_browse_features = (
    df_feature_sessions
    .groupBy("account_number")
    .agg(
        F.countDistinct("unique_visit_id").alias("browse_sessions_90d"),
        F.countDistinct("session_date").alias("browse_active_days_90d"),
        F.max("session_date").alias("last_browse_session_date"),
    )
    .join(
        df_pages_per_session
        .groupBy("account_number")
        .agg(
            F.sum("pages_in_session").alias("page_events_90d"),
            F.avg("pages_in_session").alias("avg_pages_per_session_90d"),
            F.sum("shopping_bag_pages_in_session").alias("shopping_bag_page_events_90d"),
        ),
        on="account_number",
        how="left",
    )
    .withColumn("browse_session_recency_days", F.datediff(feature_end_date, F.col("last_browse_session_date")))
)

# display(df_browse_features.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Action Features
# MAGIC
# MAGIC Add a small set of web action features, mainly to capture recent add-to-bag
# MAGIC and product-detail engagement.

# COMMAND ----------

df_action_events = (
    spark.table(bq_actions_tbl)
    .where((F.col("date") >= feature_start_date) & (F.col("date") <= feature_end_date))
    .select(
        F.col("date").alias("session_date"),
        F.col("UniqueVisitID").cast("string").alias("unique_visit_id"),
        F.col("Timestamp").cast("timestamp").alias("event_timestamp"),
        F.col("Action").cast("string").alias("action"),
        F.col("Level1").cast("string").alias("level1"),
        F.col("Level2").cast("string").alias("level2"),
        F.col("Level3").cast("string").alias("level3"),
        F.col("PagePath").cast("string").alias("page_path"),
    )
    .join(df_feature_sessions, on=["session_date", "unique_visit_id"], how="inner")
    .withColumn(
        "action_text",
        F.lower(
            F.concat_ws(
                " | ",
                F.coalesce(F.col("action"), F.lit("")),
                F.coalesce(F.col("level1"), F.lit("")),
                F.coalesce(F.col("level2"), F.lit("")),
                F.coalesce(F.col("level3"), F.lit("")),
                F.coalesce(F.col("page_path"), F.lit("")),
            )
        ),
    )
    .withColumn("is_add_to_bag", F.when(F.col("action_text").rlike("add.?to.?bag|atb"), 1).otherwise(0))
    .withColumn("is_pdp_action", F.when(F.col("action_text").rlike("pdp|product"), 1).otherwise(0))
    .select(
        "account_number",
        F.col("session_date").alias("event_date"),
        "unique_visit_id",
        "event_timestamp",
        "is_add_to_bag",
        "is_pdp_action",
    )
)

# display(df_action_events.limit(10))

# COMMAND ----------

df_action_features = (
    df_action_events
    .groupBy("account_number")
    .agg(
        F.count("*").alias("action_events_90d"),
        F.countDistinct("event_date").alias("action_active_days_90d"),
        F.sum("is_add_to_bag").alias("add_to_bag_actions_90d"),
        F.sum("is_pdp_action").alias("pdp_action_rows_90d"),
        F.max("event_date").alias("last_action_date"),
    )
    .withColumn("action_recency_days", F.datediff(feature_end_date, F.col("last_action_date")))
)

# display(df_action_features.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Feature Table
# MAGIC
# MAGIC Join the compact behaviour features back to the account base and persist the
# MAGIC account-grain feature table for the pCTR training notebook.

# COMMAND ----------

df_customer_behaviour = (
    df_customer_base
    .join(df_browse_features, on="account_number", how="left")
    .join(df_action_features, on="account_number", how="left")
    .fillna(
        {
            "browse_sessions_90d": 0,
            "browse_active_days_90d": 0,
            "page_events_90d": 0,
            "shopping_bag_page_events_90d": 0,
            "action_events_90d": 0,
            "action_active_days_90d": 0,
            "add_to_bag_actions_90d": 0,
            "pdp_action_rows_90d": 0,
        }
    )
)

display(df_customer_behaviour.limit(10))

# COMMAND ----------

write_output_table(df_customer_behaviour, customer_behaviour_output_tbl)

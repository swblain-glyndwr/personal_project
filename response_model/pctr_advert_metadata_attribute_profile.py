# Databricks notebook source
# MAGIC %md
# MAGIC # pCTR Advert Metadata And Attribute Profile
# MAGIC
# MAGIC Builds the structured advert-side feature layer used by the Shopping Bag
# MAGIC pCTR training table.
# MAGIC
# MAGIC Outputs:
# MAGIC
# MAGIC - `next_uk_pctr_advert_daily_core_90d`: advert metadata by feature date
# MAGIC - `next_uk_pctr_item_attribute_lookup_latest`: latest item attribute lookup
# MAGIC - `next_uk_pctr_advert_attribute_profile_90d`: weighted advert attribute profile

# COMMAND ----------

from functools import reduce

from pyspark.sql import Window
from pyspark.sql import functions as F

spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC `reference_date` is the point-in-time anchor for this advert feature build.
# MAGIC The notebook looks backwards for historical advert/product context and also
# MAGIC writes advert feature dates forward by `advert_feature_horizon_days` so the
# MAGIC tagged-click training rows can join advert features on their future
# MAGIC exposure `session_date`.
# MAGIC
# MAGIC `write_mode=overwrite_latest` writes the normal latest tables. Use
# MAGIC `write_mode=append_snapshot` to write a partition into suffixed snapshot
# MAGIC tables such as `_snapshots` or `_smoke`.
# MAGIC
# MAGIC The modes exist for different jobs:
# MAGIC
# MAGIC - `overwrite_latest` is for the current interactive/latest build. It refreshes
# MAGIC   the unsuffixed advert feature tables used by current debugging or latest
# MAGIC   scoring flows.
# MAGIC - `append_snapshot` is for repeatable point-in-time training data. It writes
# MAGIC   the advert features into a history-style table and replaces only the
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
advert_feature_horizon_days = int(get_widget_value("advert_feature_horizon_days", "14"))

print(
    "pCTR advert metadata/product descriptor run config: "
    f"reference_date={reference_date}, "
    f"write_mode={write_mode}, "
    f"snapshot_table_suffix={snapshot_table_suffix}, "
    f"advert_feature_horizon_days={advert_feature_horizon_days}. "
    f"Widget options: reference_date='YYYY-MM-DD'; write_mode in {write_mode_options}; "
    "snapshot_table_suffix is free text, for example 'snapshots' or 'smoke'; "
    "advert_feature_horizon_days is an integer number of days. "
    "Meaning: reference_date is the as-of date; advert feature dates are extended "
    "forward so training exposures can join to the correct advert state."
)

dev_schema = spark.sql("SELECT current_user()").first()[0].split("@")[0].replace(".", "_")

control_sheet_tbl = "marketingdata_prod.warehouse.next_uk_nextads_control_sheet"
ad_items_tbl = "marketingdata_prod.warehouse.next_uk_nextads_ad_items"
item_attributes_tbl = "marketingdata_prod.warehouse.next_uk_nextads_item_attributes_latest"
product_catalog_history_tbl = "marketingdata_prod.warehouse.product_catalog_history"

advert_daily_core_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_daily_core_90d"
item_attribute_lookup_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_item_attribute_lookup_latest"
advert_attribute_profile_output_tbl = f"marketingdata_dev.{dev_schema}.next_uk_pctr_advert_attribute_profile_90d"

attribute_names = [
    "gender",
    "department",
    "category",
    "brand",
    "use",
    "style",
    "pattern",
    "fit",
    "room",
    "activity",
    "material",
    "collaboration",
    "colour",
]

top_profile_attributes = ["brand", "use", "colour", "style", "category", "department", "gender"]

feature_end_date = F.lit(reference_date).cast("date")
feature_start_date = F.date_sub(feature_end_date, lookback_days)
advert_feature_end_date = F.date_add(feature_end_date, advert_feature_horizon_days)

# COMMAND ----------


def clean_string(col):
    return F.when(F.trim(col.cast("string")) == "", None).otherwise(F.trim(col.cast("string")))


def clean_lower_string(col):
    return F.lower(clean_string(col))


def existing_col(df, candidates, alias):
    for candidate in candidates:
        if candidate in df.columns:
            return clean_string(F.col(candidate)).alias(alias)
    return F.lit(None).cast("string").alias(alias)


def normalise_item_col(col):
    return F.regexp_replace(clean_lower_string(col), r"[^a-z0-9]", "")


def parse_items(col):
    return F.expr(f"filter(split(coalesce({col}, ''), '[^A-Za-z0-9]+'), x -> x <> '')")


def left_join_all(base_df, dfs, keys):
    return reduce(lambda left, right: left.join(right, on=keys, how="left"), dfs, base_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Advert Daily Core
# MAGIC
# MAGIC The control sheet is the advert metadata source of truth. The run date is
# MAGIC shifted by one day to match the assignment-to-session convention already
# MAGIC used in `pctr_tagged_click_training`.

# COMMAND ----------

df_advert_daily_core = (
    spark.table(control_sheet_tbl)
    .where((F.col("rundate") >= F.date_sub(feature_start_date, 1)) & (F.col("rundate") <= F.date_sub(advert_feature_end_date, 1)))
    .withColumn("feature_date", F.date_add(F.to_date(F.col("rundate")), 1))
    .select(
        "feature_date",
        F.col("UniqueAdID").cast("string").alias("advert_id"),
        F.col("Location").cast("string").alias("placement_id"),
        F.col("CampaignNumber").cast("string").alias("campaign_id"),
        F.col("URL").cast("string").alias("advert_url"),
        F.col("Items").cast("string").alias("control_sheet_items"),
        F.col("ProductURLs").cast("string").alias("product_urls"),
        F.col("Title").cast("string").alias("advert_title"),
        F.col("Headline").cast("string").alias("headline"),
        F.col("Subtext").cast("string").alias("subtext"),
        F.col("CTA").cast("string").alias("cta"),
        F.col("AdTrend").cast("string").alias("advert_theme"),
        F.coalesce(F.col("AdCategory").cast("string"), F.col("AdSubcategory").cast("string")).alias("advert_category"),
        F.col("AdBrandName").cast("string").alias("advert_brand_name"),
        F.col("AdCampaign").cast("string").alias("advert_campaign"),
        F.col("AdMission").cast("string").alias("advert_mission"),
        F.col("AlgoDivision").cast("string").alias("algo_division"),
        F.col("TradeDivision").cast("string").alias("trade_division"),
        F.col("Brand").cast("string").alias("control_sheet_brand"),
        F.col("TemplateName").cast("string").alias("template_name"),
        F.col("HeaderColour").cast("string").alias("header_colour"),
        F.col("TextColour").cast("string").alias("text_colour"),
        F.col("BackGroundColour").cast("string").alias("background_colour"),
        F.col("ButtonTextColour").cast("string").alias("button_text_colour"),
        F.col("ButtonColour").cast("string").alias("button_colour"),
        F.col("BackgroundImage").cast("string").alias("background_image"),
        F.col("MobileImage").cast("string").alias("mobile_image"),
        F.col("FlatJPG").cast("string").alias("flat_jpg"),
        F.col("Tags").cast("string").alias("tags"),
        F.col("TargetingAttributes").cast("string").alias("targeting_attributes"),
        F.col("Themes").cast("string").alias("themes"),
        F.col("Page").cast("string").alias("page_path"),
        F.col("Screen").cast("string").alias("screen"),
        F.col("PageGroup").cast("string").alias("page_group"),
    )
    .where(F.col("advert_id").rlike("^P"))
    .dropDuplicates(["feature_date", "placement_id", "advert_id"])
)

display(df_advert_daily_core.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latest Item Attribute Lookup
# MAGIC
# MAGIC Item attributes are kept long in the source table. This lookup gives the
# MAGIC advert profile builder both a long attribute map and convenient wide columns
# MAGIC for the most useful profile fields. Product catalogue metadata is joined in
# MAGIC so the profile can fall back to catalogue text and images when explicit
# MAGIC attributes are sparse.

# COMMAND ----------

df_item_attributes_latest_long = (
    spark.table(item_attributes_tbl)
    .select(
        normalise_item_col(F.col("pid")).alias("itemno"),
        clean_lower_string(F.col("attribute")).alias("attribute"),
        clean_lower_string(F.col("value")).alias("value"),
    )
    .where(F.col("itemno").isNotNull())
    .where(F.col("attribute").isin(attribute_names))
    .where(F.col("value").isNotNull())
    .dropDuplicates(["itemno", "attribute", "value"])
)

df_item_attribute_single_value = (
    df_item_attributes_latest_long
    .groupBy("itemno", "attribute")
    .agg(F.first("value", ignorenulls=True).alias("value"))
)

df_item_attribute_wide = (
    df_item_attribute_single_value
    .groupBy("itemno")
    .pivot("attribute", attribute_names)
    .agg(F.first("value", ignorenulls=True))
)

df_item_attribute_map = (
    df_item_attribute_single_value
    .groupBy("itemno")
    .agg(
        F.map_from_entries(
            F.collect_list(F.struct(F.col("attribute"), F.col("value")))
        ).alias("attribute_value_map")
    )
)

# COMMAND ----------

df_catalog_history_raw = spark.table(product_catalog_history_tbl)
catalog_pid_col = next(
    (col_name for col_name in ["pid", "itemno", "item_number", "itemNumber"] if col_name in df_catalog_history_raw.columns),
    None,
)

if catalog_pid_col is None:
    raise ValueError(
        f"Could not find an item identifier column in {product_catalog_history_tbl}. "
        f"Available columns: {df_catalog_history_raw.columns}"
    )

df_catalog_history = df_catalog_history_raw.withColumn("itemno", normalise_item_col(F.col(catalog_pid_col)))

if "start_date" in df_catalog_history.columns:
    df_catalog_history = df_catalog_history.withColumn("_catalog_start_date", F.to_date(F.col("start_date")))
else:
    df_catalog_history = df_catalog_history.withColumn("_catalog_start_date", F.lit(None).cast("date"))

if "end_date" in df_catalog_history.columns:
    df_catalog_history = df_catalog_history.withColumn("_catalog_end_date", F.to_date(F.col("end_date")))
else:
    df_catalog_history = df_catalog_history.withColumn("_catalog_end_date", F.lit(None).cast("date"))

df_catalog_history = (
    df_catalog_history
    .where(F.col("itemno").isNotNull())
    .where(F.col("_catalog_start_date").isNull() | (F.col("_catalog_start_date") <= feature_end_date))
    .where(F.col("_catalog_end_date").isNull() | (F.col("_catalog_end_date") >= feature_start_date))
)

catalog_latest_window = Window.partitionBy("itemno").orderBy(
    F.col("_catalog_end_date").desc_nulls_last(),
    F.col("_catalog_start_date").desc_nulls_last(),
)

df_product_catalog_latest = (
    df_catalog_history
    .withColumn("catalog_row_num", F.row_number().over(catalog_latest_window))
    .where(F.col("catalog_row_num") == 1)
    .select(
        "itemno",
        existing_col(df_catalog_history, ["title", "product_title", "item_title"], "item_title"),
        existing_col(df_catalog_history, ["URL", "url", "product_url"], "item_url"),
        existing_col(df_catalog_history, ["large_image", "largeImage", "image_url", "image"], "item_image_url"),
        existing_col(df_catalog_history, ["brand"], "catalog_brand"),
        existing_col(df_catalog_history, ["next_colour", "colour", "color"], "catalog_colour"),
        existing_col(df_catalog_history, ["next_category", "category"], "catalog_category"),
        existing_col(df_catalog_history, ["department", "next_department"], "catalog_department"),
        existing_col(df_catalog_history, ["next_gender", "gender"], "catalog_gender"),
        existing_col(df_catalog_history, ["range"], "catalog_range"),
    )
    .dropDuplicates(["itemno"])
)

df_item_attribute_lookup = (
    df_item_attribute_wide
    .join(df_item_attribute_map, on="itemno", how="left")
    .join(df_product_catalog_latest, on="itemno", how="full")
    .withColumn("brand", F.coalesce(F.col("brand"), clean_lower_string(F.col("catalog_brand"))))
    .withColumn("colour", F.coalesce(F.col("colour"), clean_lower_string(F.col("catalog_colour"))))
    .withColumn("category", F.coalesce(F.col("category"), clean_lower_string(F.col("catalog_category"))))
    .withColumn("department", F.coalesce(F.col("department"), clean_lower_string(F.col("catalog_department"))))
    .withColumn("gender", F.coalesce(F.col("gender"), clean_lower_string(F.col("catalog_gender"))))
    .withColumn(
        "item_text_corpus",
        F.lower(
            F.regexp_replace(
                F.concat_ws(
                    " ",
                    "item_title",
                    "brand",
                    "use",
                    "colour",
                    "style",
                    "category",
                    "department",
                    "gender",
                    "room",
                    "activity",
                    "material",
                    "catalog_range",
                ),
                r"\s+",
                " ",
            )
        ),
    )
    .select(
        "itemno",
        "brand",
        "use",
        "colour",
        "style",
        "category",
        "department",
        "gender",
        "pattern",
        "fit",
        "room",
        "activity",
        "material",
        "collaboration",
        "attribute_value_map",
        "item_title",
        "item_url",
        "item_image_url",
        "item_text_corpus",
    )
    .dropDuplicates(["itemno"])
)

display(df_item_attribute_lookup.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advert Items
# MAGIC
# MAGIC Prefer items explicitly listed in the control sheet, then supplement with the
# MAGIC reusable `RepresentativeItems` table. We keep item order and convert it into
# MAGIC a normalised weight, so the first item in an advert influences the profile
# MAGIC more than later supporting items.

# COMMAND ----------

df_control_sheet_items = (
    df_advert_daily_core
    .select(
        "feature_date",
        "advert_id",
        F.posexplode_outer(parse_items("control_sheet_items")).alias("item_position", "itemno_raw"),
    )
    .withColumn("itemno", normalise_item_col(F.col("itemno_raw")))
    .where(F.col("itemno").isNotNull())
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
    .withColumn("item_source", F.lit("representative_items"))
    .select("feature_date", "advert_id", "itemno", "item_position", "item_source")
)

source_priority = (
    F.when(F.col("item_source") == "control_sheet_items", F.lit(1))
    .when(F.col("item_source") == "representative_items", F.lit(2))
    .otherwise(F.lit(9))
)

dedupe_item_window = Window.partitionBy("feature_date", "advert_id", "itemno").orderBy(
    source_priority.asc(),
    F.col("item_position").asc_nulls_last(),
)

weight_window = Window.partitionBy("feature_date", "advert_id")

df_advert_items_weighted = (
    df_control_sheet_items
    .unionByName(df_representative_items)
    .withColumn("item_row_num", F.row_number().over(dedupe_item_window))
    .where(F.col("item_row_num") == 1)
    .withColumn("raw_item_weight", F.lit(1.0) / (F.col("item_position").cast("double") + F.lit(1.0)))
    .withColumn("item_weight", F.col("raw_item_weight") / F.sum("raw_item_weight").over(weight_window))
    .select("feature_date", "advert_id", "itemno", "item_position", "item_source", "item_weight")
)

display(df_advert_items_weighted.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weighted Advert Attribute Profile

# COMMAND ----------

df_profile_long = (
    df_advert_items_weighted
    .join(
        df_item_attributes_latest_long,
        on="itemno",
        how="inner",
    )
    .groupBy("feature_date", "advert_id", "attribute", "value")
    .agg(
        F.sum("item_weight").alias("profile_weight"),
        F.countDistinct("itemno").alias("matched_item_count"),
    )
)

top_attribute_window = Window.partitionBy("feature_date", "advert_id", "attribute").orderBy(
    F.col("profile_weight").desc(),
    F.col("matched_item_count").desc(),
    F.col("value").asc(),
)

df_top_attribute_values = (
    df_profile_long
    .withColumn("attribute_rank", F.row_number().over(top_attribute_window))
    .where(F.col("attribute_rank") == 1)
    .groupBy("feature_date", "advert_id")
    .agg(
        *[
            F.first(
                F.when(F.col("attribute") == attribute, F.col("value")),
                ignorenulls=True,
            ).alias(f"top_{attribute}")
            for attribute in top_profile_attributes
        ],
        *[
            F.first(
                F.when(F.col("attribute") == attribute, F.col("profile_weight")),
                ignorenulls=True,
            ).alias(f"top_{attribute}_weight")
            for attribute in top_profile_attributes
        ],
    )
)

df_profile_stats = (
    df_profile_long
    .groupBy("feature_date", "advert_id")
    .agg(
        F.countDistinct("attribute").alias("attribute_profile_attribute_count"),
        F.countDistinct("value").alias("attribute_profile_value_count"),
        *[
            F.countDistinct(
                F.when(F.col("attribute") == attribute, F.col("value"))
            ).alias(f"{attribute}_profile_distinct_values")
            for attribute in top_profile_attributes
        ],
    )
)

df_item_profile_stats = (
    df_advert_items_weighted
    .join(df_item_attribute_lookup.select("itemno", "item_text_corpus"), on="itemno", how="left")
    .groupBy("feature_date", "advert_id")
    .agg(
        F.countDistinct("itemno").alias("advert_item_count"),
        F.sum("item_weight").alias("advert_item_weight_sum"),
        F.collect_set("item_source").alias("advert_item_sources"),
        F.concat_ws(" ", F.collect_set("item_text_corpus")).alias("advert_item_text_corpus"),
    )
)

profile_maps = []
for attribute in top_profile_attributes:
    profile_maps.append(
        df_profile_long
        .where(F.col("attribute") == attribute)
        .groupBy("feature_date", "advert_id")
        .agg(
            F.map_from_entries(
                F.collect_list(
                    F.struct(
                        F.col("value"),
                        F.round(F.col("profile_weight"), 6),
                    )
                )
            ).alias(f"{attribute}_profile_map")
        )
    )

df_advert_attribute_profile = (
    df_advert_daily_core.select(
        "feature_date",
        "advert_id",
        "placement_id",
        "campaign_id",
        "advert_url",
        "advert_theme",
        "advert_category",
        "advert_brand_name",
        "control_sheet_brand",
        "advert_title",
        "headline",
        "subtext",
        "cta",
        "template_name",
        "page_path",
    )
    .dropDuplicates(["feature_date", "advert_id", "placement_id"])
    .groupBy("feature_date", "advert_id")
    .agg(
        F.first("campaign_id", ignorenulls=True).alias("campaign_id"),
        F.first("advert_url", ignorenulls=True).alias("advert_url"),
        F.first("advert_theme", ignorenulls=True).alias("advert_theme"),
        F.first("advert_category", ignorenulls=True).alias("advert_category"),
        F.first("advert_brand_name", ignorenulls=True).alias("advert_brand_name"),
        F.first("control_sheet_brand", ignorenulls=True).alias("control_sheet_brand"),
        F.first("advert_title", ignorenulls=True).alias("advert_title"),
        F.first("headline", ignorenulls=True).alias("headline"),
        F.first("subtext", ignorenulls=True).alias("subtext"),
        F.first("cta", ignorenulls=True).alias("cta"),
        F.first("template_name", ignorenulls=True).alias("template_name"),
        F.first("page_path", ignorenulls=True).alias("page_path"),
        F.countDistinct("placement_id").alias("advert_active_placement_count"),
    )
)

df_advert_attribute_profile = left_join_all(
    df_advert_attribute_profile,
    [df_top_attribute_values, df_profile_stats, df_item_profile_stats, *profile_maps],
    ["feature_date", "advert_id"],
)

df_advert_attribute_profile = (
    df_advert_attribute_profile
    .withColumn("has_item_attribute_profile", F.col("attribute_profile_attribute_count").isNotNull())
    .withColumn("attribute_profile_attribute_count", F.coalesce(F.col("attribute_profile_attribute_count"), F.lit(0)))
    .withColumn("attribute_profile_value_count", F.coalesce(F.col("attribute_profile_value_count"), F.lit(0)))
    .withColumn("advert_item_count", F.coalesce(F.col("advert_item_count"), F.lit(0)))
    .withColumn("advert_item_weight_sum", F.coalesce(F.col("advert_item_weight_sum"), F.lit(0.0)))
)

display(df_advert_attribute_profile.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persist Outputs

# COMMAND ----------

write_output_table(df_advert_daily_core, advert_daily_core_output_tbl)
write_output_table(df_item_attribute_lookup, item_attribute_lookup_output_tbl)
write_output_table(df_advert_attribute_profile, advert_attribute_profile_output_tbl)

# COMMAND ----------

df_output_summary = spark.createDataFrame(
    [
        ("advert_daily_core_rows", df_advert_daily_core.count()),
        ("item_attribute_lookup_rows", df_item_attribute_lookup.count()),
        ("advert_attribute_profile_rows", df_advert_attribute_profile.count()),
        (
            "advert_profiles_with_items",
            df_advert_attribute_profile.where(F.col("advert_item_count") > 0).count(),
        ),
        (
            "advert_profiles_with_attributes",
            df_advert_attribute_profile.where(F.col("has_item_attribute_profile")).count(),
        ),
    ],
    ["check_name", "row_count"],
)

display(df_output_summary)

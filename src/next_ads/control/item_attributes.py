"""Reusable item-attribute parsing helpers for NextAds control metadata."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dsutils.etl import build_spark_schema


def build_item_attribute_catalog(
    df_catalog_full: DataFrame,
    attributes: Sequence[str],
) -> DataFrame:
    """Normalise product catalogue columns used by the legacy parser."""
    return (
        df_catalog_full.drop("gender", "category")
        .withColumnRenamed("department", "next_department")
        .withColumn(
            "gender",
            F.when(F.lower(F.col("next_gender")).contains("women"), "women")
            .when(F.lower(F.col("next_gender")).contains("men"), "men")
            .when(F.lower(F.col("next_gender")).contains("girls"), "girls")
            .when(F.lower(F.col("next_gender")).contains("boys"), "boys")
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "lifestage",
            F.when(
                F.lower(F.col("next_gender")).contains("newborn"), "newborn"
            )
            .when(F.lower(F.col("gender")).isin("women", "men", "unisex"), "adult")
            .when(
                F.lower(F.col("next_gender")).contains("older"), "kids_older"
            )
            .when(
                F.lower(F.col("next_gender")).contains("younger"), "kids_younger"
            )
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "department",
            F.when(
                F.lower(F.col("next_department")).contains("wear"), "fashion"
            )
            .when(F.lower(F.col("next_department")).contains("home"), "home")
            .when(F.lower(F.col("next_department")).contains("beauty"), "beauty")
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "brand",
            F.when(
                (
                    F.array_contains(
                        F.split(F.lower(F.col("range")), "\\|"), "npremium"
                    )
                )
                | (
                    F.array_contains(
                        F.split(F.lower(F.col("range")), "\\|"),
                        "n premium the snuggle grand",
                    )
                ),
                "npremium",
            )
            .when(
                (F.lower(F.col("title")).rlike("signature"))
                & (
                    F.lower(F.col("range")).rlike("signature")
                    | F.lower(F.col("range")).rlike("next signature")
                )
                & (F.lower(F.col("brand")) == "next")
                & (F.col("next_department") == "menswear"),
                "nextsignature",
            )
            .otherwise(F.col("brand")),
        )
        .withColumnsRenamed(
            {
                "next_category": "category",
                "next_colour": "colour",
            }
        )
        .select("pid", *attributes)
    )


def build_recent_catalog(df_product_catalog: DataFrame, lookback_days: int) -> DataFrame:
    """Filter product catalogue rows to the configured attribute lookback."""
    return df_product_catalog.where(
        F.col("end_date") > F.date_sub(F.current_date(), lookback_days)
    )


def build_recent_basket_items(df_baskets: DataFrame, lookback_days: int) -> DataFrame:
    """Return distinct recent basket item/order pairs."""
    return (
        df_baskets.where(
            F.col("orderdate") > F.date_sub(F.current_date(), lookback_days)
        )
        .withColumnRenamed("itemno", "pid")
        .select("pid", "orderid")
        .distinct()
    )


def count_recent_baskets(df_baskets: DataFrame, lookback_days: int) -> int:
    """Count distinct recent baskets for legacy prevalence metrics."""
    return (
        df_baskets.where(
            F.col("orderdate") > F.date_sub(F.current_date(), lookback_days)
        )
        .select("orderid")
        .distinct()
        .count()
    )


def extract_attribute_values(df_catalog: DataFrame, attribute: str) -> DataFrame:
    """Explode and normalise one configured product attribute."""
    return (
        df_catalog.select("pid", attribute)
        .withColumn("value_raw", F.explode(F.split(F.col(attribute), r"\|")))
        .withColumn("value", F.lower(F.trim(F.col("value_raw"))))
        .filter(F.col("value") != "")
        .select("pid", "value")
        .distinct()
    )


def build_attribute_prevalence(
    df_attribute_values: DataFrame,
    df_baskets: DataFrame,
    *,
    n_items: int,
    n_items_total: int,
    n_baskets_total: int,
) -> DataFrame:
    """Build item and basket prevalence metrics for one attribute."""
    df_count_items = (
        df_attribute_values.groupBy("value")
        .agg(F.countDistinct("pid").alias("n_products"))
        .withColumn("pc_products", (F.col("n_products") / n_items) * 100)
        .withColumn(
            "pc_products_total",
            (F.col("n_products") / n_items_total) * 100,
        )
    )

    df_count_baskets = (
        df_baskets.join(df_attribute_values, on="pid", how="inner")
        .groupBy("value")
        .agg(F.countDistinct("orderid").alias("n_orders"))
        .withColumn(
            "pc_orders_total",
            (F.col("n_orders") / n_baskets_total) * 100,
        )
    )

    return df_count_items.join(df_count_baskets, on="value", how="inner")


def build_attribute_mapping_for_attribute(
    *,
    spark: SparkSession,
    df_catalog: DataFrame,
    df_baskets: DataFrame,
    attribute: str,
    n_items_total: int,
    n_baskets_total: int,
    set_attributes: bool,
    attribute_set_latest_table: str,
    pc_cutoff_col: str,
    frequency_cutoff_pc: float,
) -> DataFrame | None:
    """Return pid/value rows accepted for one configured attribute."""
    df_values = extract_attribute_values(df_catalog, attribute)
    n_items = df_values.select("pid").distinct().count()
    df_count = build_attribute_prevalence(
        df_values,
        df_baskets,
        n_items=n_items,
        n_items_total=n_items_total,
        n_baskets_total=n_baskets_total,
    )

    if set_attributes:
        df_count = df_count.filter(F.col(pc_cutoff_col) >= frequency_cutoff_pc)
    else:
        df_set_values = (
            spark.table(attribute_set_latest_table)
            .filter(F.col("attribute") == attribute)
            .select("value")
            .distinct()
        )
        if df_set_values.isEmpty():
            return None
        df_count = df_count.join(df_set_values, on="value", how="inner")

    return df_values.join(df_count, on="value", how="inner")


def build_attribute_mappings(
    *,
    spark: SparkSession,
    df_catalog: DataFrame,
    df_baskets: DataFrame,
    attributes: Sequence[str],
    set_attributes: bool,
    attribute_set_latest_table: str,
    pc_cutoff_col: str,
    frequency_cutoff_pc: float,
) -> dict[str, DataFrame]:
    """Build accepted item/value mappings for each configured attribute."""
    n_items_total = df_catalog.select("pid").distinct().count()
    n_baskets_total = df_baskets.select("orderid").distinct().count()
    attribute_dfs = {}

    for attribute in attributes:
        df_attribute = build_attribute_mapping_for_attribute(
            spark=spark,
            df_catalog=df_catalog,
            df_baskets=df_baskets,
            attribute=attribute,
            n_items_total=n_items_total,
            n_baskets_total=n_baskets_total,
            set_attributes=set_attributes,
            attribute_set_latest_table=attribute_set_latest_table,
            pc_cutoff_col=pc_cutoff_col,
            frequency_cutoff_pc=frequency_cutoff_pc,
        )
        if df_attribute is not None:
            attribute_dfs[attribute] = df_attribute

    return attribute_dfs


def build_attributes_master(
    spark: SparkSession,
    attribute_dfs: Mapping[str, DataFrame],
) -> DataFrame:
    """Concatenate item-attribute rows into the output table shape."""
    attr_schema = build_spark_schema(
        [
            ["pid", "string", "not null"],
            ["attribute", "string", "not null"],
            ["value", "string", "not null"],
        ]
    )
    df_attributes_master = spark.createDataFrame([], attr_schema)

    for attribute, df_attribute in attribute_dfs.items():
        df_attr = (
            df_attribute.select("pid", F.lit(attribute).alias("attribute"), "value")
            .distinct()
        )
        df_attributes_master = df_attributes_master.unionByName(df_attr)

    return df_attributes_master


def build_attribute_set(df_attributes_master: DataFrame) -> DataFrame:
    """Build the distinct attribute set output from item attributes."""
    return (
        df_attributes_master.select("attribute", "value")
        .distinct()
        .orderBy("attribute", "value")
    )


def build_bigquery_item_attributes(
    *,
    spark: SparkSession,
    df_attributes_master: DataFrame,
    attributes: Sequence[str],
    nov_scores_csv: str,
    product_catalog_latest_table: str,
) -> DataFrame:
    """Shape item attributes for the legacy BigQuery dashboard export."""
    nov_scores = (
        spark.read.csv(nov_scores_csv, header=True)
        .select("item_number", "next_order_value")
        .withColumnRenamed("item_number", "pid")
    )

    product_catalog_latest = (
        spark.table(product_catalog_latest_table)
        .select("pid", "title", "URL", "large_image")
        .withColumn("URL", F.regexp_replace("URL", "#", "/"))
    )

    product_catalog_with_nov = (
        product_catalog_latest.join(nov_scores, on="pid", how="left").distinct()
    )

    attributes_pivot = (
        df_attributes_master.groupBy("pid")
        .pivot("attribute")
        .agg(F.collect_list("value"))
    )

    for attribute in attributes:
        attributes_pivot = attributes_pivot.withColumn(
            attribute,
            F.explode_outer(attribute),
        )

    attributes_pivot = attributes_pivot.select("pid", *attributes).distinct()

    return (
        product_catalog_with_nov.join(attributes_pivot, on="pid", how="inner")
        .distinct()
        .fillna("Unknown")
    )

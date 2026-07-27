from pyspark.sql import Window
from pyspark.sql import functions as F


def load_control_ads(spark, control_sheet_latest: str):
    return spark.table(control_sheet_latest)


def build_theme_to_ad_mapping(df_ads):
    return (
        df_ads.where(F.col("AudienceOnly") != 1)
        .select("Themes", "UniqueAdID", "AdVariant")
        .where(F.col("Themes").isNotNull())
        .where(F.col("Themes") != "")
        .distinct()
    )


def load_customer_base(spark, customer_cells_latest: str):
    return spark.table(customer_cells_latest).select("AccountNumber")


def load_theme_scores(spark, theme_scores_latest: str, customer_base_df):
    return spark.table(theme_scores_latest).join(
        customer_base_df,
        on="AccountNumber",
        how="inner",
    )


def build_ad_group_mappings(
    spark,
    control_sheet_latest: str,
    logger,
    group_col: str = "Location",
):
    df_ad2group = (
        spark.table(control_sheet_latest)
        .where(F.col("AudienceOnly") != 1)
        .select("UniqueAdID", group_col)
        .distinct()
    )

    df_adsets = (
        df_ad2group.groupBy(group_col)
        .agg(F.array_sort(F.collect_list("UniqueAdID")).alias("AdSetSorted"))
        .withColumn("AdSet", F.concat_ws("|", F.col("AdSetSorted")))
        .groupBy("AdSet")
        .agg(F.collect_set(group_col).alias("GroupSet"))
        .withColumn(
            "AdSetID",
            F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.col("AdSet"))),
        )
        .select("AdSetID", "GroupSet")
    )

    df_adset2group = df_adsets.select(
        "AdSetID",
        F.explode("GroupSet").alias(group_col),
    )

    n_groups = df_adset2group.select(group_col).distinct().count()
    n_ad_sets = df_adsets.count()
    logger.info(
        f"{n_ad_sets:,} distinct ad sets found across "
        f"{n_groups:,} {group_col} values"
    )

    for row in df_adsets.collect():
        groups = ", ".join(sorted(row["GroupSet"]))
        logger.info(f"AdSetID {row['AdSetID']}: {group_col} [{groups}]")

    df_ad2adset = (
        df_ad2group.join(df_adset2group, on=group_col, how="inner")
        .select("UniqueAdID", "AdSetID")
        .distinct()
    )

    return df_ad2group, df_adset2group, df_ad2adset


def build_ad_location_mappings(spark, control_sheet_latest: str, logger):
    return build_ad_group_mappings(
        spark,
        control_sheet_latest,
        logger,
        group_col="Location",
    )

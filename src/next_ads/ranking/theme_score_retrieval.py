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


def build_ad_location_mappings(spark, control_sheet_latest: str, logger):
    df_ad2loc = (
        spark.table(control_sheet_latest)
        .where(F.col("AudienceOnly") != 1)
        .select("UniqueAdID", "Location")
        .distinct()
    )

    df_adsets = (
        df_ad2loc.groupBy("Location")
        .agg(F.array_sort(F.collect_list("UniqueAdID")).alias("AdSetSorted"))
        .withColumn("AdSet", F.concat_ws("|", F.col("AdSetSorted")))
        .groupBy("AdSet")
        .agg(F.collect_set("Location").alias("LocationSet"))
        .withColumn(
            "AdSetID",
            F.row_number().over(Window.partitionBy(F.lit(1)).orderBy(F.col("AdSet"))),
        )
        .select("AdSetID", "LocationSet")
    )

    df_adset2loc = df_adsets.select(
        "AdSetID",
        F.explode("LocationSet").alias("Location"),
    )

    n_locs = df_adset2loc.select("Location").distinct().count()
    n_ad_sets = df_adsets.count()
    logger.info(f"{n_ad_sets:,} distinct ad sets found across {n_locs:,} locations")

    for row in df_adsets.collect():
        locations = ", ".join(sorted(row["LocationSet"]))
        logger.info(f"AdSetID {row['AdSetID']}: Locations [{locations}]")

    df_ad2adset = (
        df_ad2loc.join(df_adset2loc, on="Location", how="inner")
        .select("UniqueAdID", "AdSetID")
        .distinct()
    )

    return df_ad2loc, df_adset2loc, df_ad2adset

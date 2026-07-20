from dsutils.logtools import get_logger

logger = get_logger(__name__)


def create_realtime_known_reranking_weighting_rules(
    spark, reference_date: str, output_table: str
):

    import pyspark.sql.functions as F
    import json
    from pathlib import Path

    # TODO migrate to central config functionality ?
    file = "src/next_ads/realtime/decisioning/realtime_known.json"
    rtcfg = json.loads(Path(file).read_text())

    flattened_data = [
        {**value, "ruleID": key} for key, value in rtcfg["rules"].items()
    ]

    rt_rules_df = spark.createDataFrame(flattened_data).select(
        F.col("ruleID"),
        F.col("action"),
        F.col("feature"),
        F.col("weight"),
        F.lit(reference_date).cast("date").alias("rundate"),
    )

    logger.info("Running validation checks on realtime reranking rules data")
    rules_len = rt_rules_df.select("ruleID").count()
    if rules_len == 0:
        raise ValueError("Realtime known rules config contains no data")

    if spark.catalog.tableExists(output_table):
        current_rules_len = spark.table(output_table).select("ruleID").count()
        if not (
            (current_rules_len * 0.9) <= rules_len <= (current_rules_len * 1.1)
        ):
            logger.warning(
                f"Greater than 10% change in number of rules: prior records-{current_rules_len}, new records-{rules_len}"
            )
    num_dups = (
        rt_rules_df.groupBy("action", "feature")
        .agg(F.count("*").alias("number_duplicates"))
        .filter(F.col("number_duplicates") > 1)
        .count()
    )

    if num_dups > 0:
        raise ValueError("Duplicate action/feature records for rules")

    rt_rules_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(output_table)

    logger.info(f"Data in {output_table} updated")

    return


def create_central_product_details_by_pid(
    spark, config, cfg, reference_date: str, output_table: str
):

    from pyspark.sql import functions as F
    from pyspark.sql import Window

    premium_brands = config.premium_brands
    super_premium_brands = config.super_premium_brands

    read_tables = cfg["tables"]["read"]
    PRODUCT_CATALOG = (
        spark.table(read_tables["product_catalog_latest"])
        .withColumn("premium_brand", F.col("brand").isin(premium_brands))
        .withColumn(
            "super_premium_brand", F.col("brand").isin(super_premium_brands)
        )
        .withColumn(
            "prem_level_brand",
            F.col("premium_brand") | F.col("super_premium_brand"),
        )
    )

    pids_spine = PRODUCT_CATALOG.select("pid").distinct()
    cols = ["brand", "department", "next_category", "prem_level_brand"]
    for col in cols:
        prod_data = (
            PRODUCT_CATALOG.groupBy(F.col("pid"), F.col(col))
            .agg(F.count("*").alias("number_items"))
            .withColumn(
                "pid_max",
                F.row_number().over(
                    Window.partitionBy("pid").orderBy(
                        F.desc(F.col("number_items")), F.desc(F.col(col))
                    )
                ),
            )
            .filter(F.col("pid_max") == 1)
            .select(
                F.col("pid"),
                F.col(col),
            )
        )
        pids_spine = pids_spine.join(prod_data, on="pid", how="left")
    pids_spine = pids_spine.withColumn(
        "rundate", F.lit(reference_date).cast("date")
    )

    logger.info("Running validation on product dataset")

    pids_len = pids_spine.select("pid").count()

    if pids_len == 0:
        raise ValueError("No records in unique productid table")

    duplicate_pids = (
        pids_spine.groupBy("pid")
        .agg(F.count("*").alias("number_duplicates"))
        .filter(F.col("number_duplicates") > 1)
        .count()
    )
    if duplicate_pids > 0:
        raise ValueError("Duplicate records in productid table")

    duplicate_pids = (
        pids_spine.groupBy("pid")
        .agg(F.count("*").alias("number_duplicates"))
        .filter(F.col("number_duplicates") > 1)
        .count()
    )
    pids_spine.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(output_table)

    logger.info(f"Data in {output_table} updated")

    return


def advert_details_build(
    spark,
    cfg,
    tbl_configs: dict,
    reference_date: str,
    output_table: str,
    coverage_min_threshold: float = 0.1,
):

    from pyspark.sql import functions as F
    from pyspark.sql import Window
    from next_ads.utils import etl

    read_tables = cfg["tables"]["read"]
    write_tables = cfg["tables"]["write"]

    write_tables = cfg["tables"]["write"]
    SORT_ORDER_LATEST = spark.table(read_tables["sort_order_latest"])
    CONTROL_SHEET = spark.table(
        etl.map_tbl(write_tables["control_sheet_latest"], **tbl_configs)
    )
    PRODUCT_FEATURES = spark.table(
        etl.map_tbl(
            write_tables["nextads_realtime_reranking_product_features"],
            **tbl_configs,
        )
    )

    unique_ads = CONTROL_SHEET.select("UniqueAdID").distinct()
    aditems = unique_ads.join(
        SORT_ORDER_LATEST, on="UniqueAdId", how="left"
    ).select(
        F.col("UniqueAdID"), F.col("items").alias("pid"), F.col("item_pos")
    )
    ad_items_features = aditems.join(PRODUCT_FEATURES, on="pid", how="inner")
    overall_number_items = aditems.groupBy("UniqueAdID").agg(
        F.count("*").alias("totalitems")
    )
    cols = ["brand", "department", "next_category", "prem_level_brand"]
    for col in cols:
        feat_df = (
            ad_items_features.groupBy(F.col("UniqueAdID"), F.col(col))
            .agg(F.count("*").alias("number_records"))
            .join(overall_number_items, on="UniqueAdID", how="left")
            .withColumn(
                f"{col}_perc_coverage",
                F.col("number_records") / F.col("totalitems"),
            )
            .withColumn(
                f"{col}_ranking",
                F.row_number().over(
                    Window.partitionBy("UniqueAdID").orderBy(
                        F.desc(F.col(f"{col}_perc_coverage")),
                        F.desc(F.col(col)),
                    )
                ),
            )
            .filter(
                (F.col(f"{col}_ranking") == 1)
                & (F.col(f"{col}_ranking") > coverage_min_threshold)
            )
            .select(
                F.col("UniqueAdID"), F.col(col), F.col(f"{col}_perc_coverage")
            )
        )

        unique_ads = unique_ads.join(
            feat_df, on="UniqueAdID", how="left"
        ).withColumn("rundate", F.lit(reference_date).cast("date"))

    logger.info("Running validation on Advert features dataset")

    features_len = unique_ads.select("UniqueAdID").count()

    if features_len == 0:
        raise ValueError("No records in advert features table")

    if unique_ads.select("UniqueAdID").distinct().count() != features_len:
        raise ValueError("Duplicate Adverts in advert features table")

    unique_ads.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(output_table)

    logger.info(f"Data in {output_table} updated")

    return


def realtime_reranking_preranked_ads_build(): 

    from pyspark.sql import functions as F

    tbls={"rpid_account": "marketingdata_prod.warehouse.rpid_with_accounts",
        "preranked_ads": "marketingdata_prod.warehouse.next_uk_nextads_preranked_ads_from_themes_v2_latest",
        "customer_cells": "marketingdata_prod.warehouse.next_uk_nextads_customer_cells_latest",
        "ad_features": "marketingdata_dev.claire_wilsonbarnes.next_uk_nextads_realtime_reranking_advert_features",}

    CUSTOMER_ADS=spark.table(tbls["preranked_ads"])
    RPIDS=spark.table(tbls["rpid_account"])
    AD_FEATURES=spark.table(tbls['ad_features'])
    CUSTOMER_CELLS=spark.table(tbls['customer_cells'])
    rpid_accounts=(CUSTOMER_ADS.join(CUSTOMER_CELLS, on="AccountNumber", how="inner")
                    #Filter out automatically any control accounts 
                    .filter(F.col("FallowControl")== "Ads")
                   .join(AD_FEATURES, on="UniqueAdID", how="inner")
                   .join(RPIDS, on=((CUSTOMER_ADS["AccountNumber"]==RPIDS["account_number"]) 
                                    &(RPIDS["latestflag"]==F.lit(1))), how="inner")
                    ).select()
                
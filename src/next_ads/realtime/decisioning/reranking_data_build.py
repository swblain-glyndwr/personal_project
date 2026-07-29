from dsutils.logtools import get_logger

logger = get_logger(__name__)


def create_realtime_known_reranking_weighting_rules(
    spark, rules: dict, reference_date: str, output_table: str
):
    import pyspark.sql.functions as F

    flattened_data = [{**value, "ruleID": key} for key, value in rules.items()]

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
    from next_ads.common import etl

    read_tables = cfg["tables"]["read"]
    write_tables = cfg["tables"]["write"]

    write_tables = cfg["tables"]["write"]
    SORT_ORDER_LATEST = spark.table(read_tables["sort_order_latest_v2"])
    CONTROL_SHEET = spark.table(
        etl.map_tbl(write_tables["control_sheet_latest_v2"], **tbl_configs)
    )
    PRODUCT_FEATURES = spark.table(
        etl.map_tbl(
            write_tables["nextads_realtime_reranking_product_features"],
            **tbl_configs,
        )
    )

    unique_ads = CONTROL_SHEET.select("UniqueAdID", "CMSPageID").distinct()
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


def realtime_reranking_preranked_ads_build(
    spark, cfg, tbl_configs, reference_date, output_table
):
    from pyspark.sql import functions as F
    from next_ads.common import etl
    from pyspark.sql.types import DoubleType

    read_tables = cfg["tables"]["read"]
    write_tables = cfg["tables"]["write"]

    RPIDS = spark.table(read_tables["rpid_with_accounts"])
    CUSTOMER_ADS = spark.table(
        etl.map_tbl(
            write_tables["preranked_ads_from_themes_v2_latest"], **tbl_configs
        )
    )
    CUSTOMER_CELLS = spark.table(
        etl.map_tbl(write_tables["customer_cells_latest"], **tbl_configs)
    )
    AD_FEATURES = spark.table(
        etl.map_tbl(
            write_tables["nextads_realtime_reranking_advert_features"],
            **tbl_configs,
        )
    )

    dup_cols = ["UniqueAdID", "AccountNumber", "rundate"]
    cells_cols = CUSTOMER_CELLS.columns
    [cells_cols.remove(i) for i in dup_cols if i in cells_cols]
    features_cols = AD_FEATURES.columns
    [features_cols.remove(i) for i in dup_cols if i in features_cols]

    rpid_accounts = (
        CUSTOMER_ADS.join(CUSTOMER_CELLS, on="AccountNumber", how="inner")
        # Filter out automatically any control accounts
        .filter(F.col("FallowControl") == "Ads")
        .join(AD_FEATURES, on="UniqueAdID", how="inner")
        .join(
            RPIDS,
            on=(
                (CUSTOMER_ADS["AccountNumber"] == RPIDS["account_number"])
                & (RPIDS["latestflag"] == F.lit(1))
            ),
            how="inner",
        )
        .select(
            F.col("UniqueAdID"),
            F.col("AccountNumber"),
            F.col("roamingprofileid"),
            F.col("PageType"),
            F.col("Score").cast(DoubleType()),
            F.col("TriggerScore").cast(DoubleType()),
            F.col("Rank"),
            *cells_cols,
            *features_cols,
            F.lit(reference_date).cast("date").alias("rundate"),
        )
    )

    logger.info("Running validation on pre-ranked advert features dataset")

    tbl_len = rpid_accounts.select("AccountNumber").count()
    if tbl_len == 0:
        raise ValueError("No records in pre-ranked advert features table")

    num_dups = (
        rpid_accounts.groupBy("roamingprofileid", "UniqueAdID", "PageType")
        .agg(F.count("*").alias("num_dups"))
        .filter(F.col("num_dups") > 1)
        .count()
    )

    if num_dups > 0:
        raise ValueError(
            "Duplicate Adverts for customer, locaiton in preranked advert features table"
        )

    rpid_accounts.write.format("delta").mode("overwrite").saveAsTable(
        output_table
    )
    logger.info(f"Data in {output_table} updated")

    return


def realtime_reranking_item_weights_build(
    spark, cfg, tbl_configs, reference_date, output_table
):
    from pyspark.sql import functions as F
    from next_ads.common import etl

    write_tables = cfg["tables"]["write"]
    ITEMS_DATA = spark.table(
        etl.map_tbl(
            write_tables["nextads_realtime_reranking_product_features"],
            **tbl_configs,
        )
    )
    ITEM_WEIGHTS = spark.table(
        etl.map_tbl(
            write_tables["nextads_realtime_reranking_rules_weighting"],
            **tbl_configs,
        )
    )

    product_columns = [
        "pid",
        "brand",
        "next_category",
        "department",
        "prem_level_brand",
    ]
    cols_to_drop = ["weighting_prem_level_brand", "rundate"]

    items_data = ITEMS_DATA.select(*product_columns)

    weights = (
        ITEM_WEIGHTS.groupBy("action", "rundate")
        .pivot("feature")
        .agg(F.first("weight"))
        .na.fill(0)
    )
    weights = weights.select(
        [
            F.col(c).alias(f"weighting_{c}")
            if c not in ("action", "rundate")
            else F.col(c)
            for c in weights.columns
        ]
    )

    # Columns to select
    weights_cols = weights.columns
    [weights_cols.remove(i) for i in cols_to_drop if i in weights_cols]

    # Combined data view
    combined = items_data.crossJoin(weights).select(
        *product_columns,
        *weights_cols,
        (
            F.when(
                F.col("prem_level_brand"), F.col("weighting_prem_level_brand")
            ).otherwise(F.lit(0))
        ).alias("weighting_prem_level_brand"),
        F.lit(reference_date).cast("date").alias("rundate"),
    )

    logger.info("Running validation on item weighting rules dataset")

    tbl_len = combined.select("pid").count()
    if tbl_len == 0:
        raise ValueError("No records in item weighting rules table")

    combined.write.format("delta").mode("overwrite").option(
        "mergeSchema", "true"
    ).saveAsTable(output_table)

    logger.info(f"Data in {output_table} updated")

    return

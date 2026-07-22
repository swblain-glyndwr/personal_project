from dsutils.logtools import get_logger

logger = get_logger(__name__)


## TODO: migrate these to be pulled from next_ads_core functionality
def _date_window_offset(
    reference_date: str, lookback_days: int, offset_days: int = 0
):
    """Determine a start & end date given the reference date,
    lookback_days and offset number of days
    """
    from pyspark.sql import functions as F

    end_date = F.date_sub(F.lit(reference_date).cast("date"), offset_days)
    start_date = F.date_sub(end_date, lookback_days)
    return start_date, end_date


def build_product_catid_df(spark, cfg, reference_date: str, output_table: str):
    """Build a table of the catid by product for all products in last 365 days

    cat_id is build from a combination of department*, brand & next_category
     *when department is childrenswear next_gender is used instead of department

    Due to some products ranges having multiple cat_ids we take the most common one
    (from different info for different items in the range )
    """
    from pyspark.sql import functions as F
    from pyspark.sql import Window

    read_tables = cfg["tables"]["read"]
    PRODUCT_CATALOG = spark.table(read_tables["product_catalog_latest"])
    PRODUCT_CATALOG_HISTORY = spark.table(read_tables["product_catalog"])

    start_date, end_date = _date_window_offset(reference_date, 365)

    # All current item categories
    product_cat_ids_current = (
        PRODUCT_CATALOG.withColumn(
            "catid",
            F.concat_ws(
                "_",
                F.when(
                    F.col("department") == "childrenswear",
                    F.col("next_gender"),
                ).otherwise(F.col("department")),
                F.col("brand"),
                F.col("next_category"),
            ),
        )
        .groupBy(F.col("pid"), F.col("catid"))
        .agg(F.count("*").alias("number_items"))
        .withColumn(
            "pid_max",
            F.row_number().over(
                Window.partitionBy("pid").orderBy(
                    F.desc(F.col("number_items")), F.desc(F.col("catid"))
                )
            ),
        )
        .filter(F.col("pid_max") == 1)
        .select(
            F.col("pid").alias("itemno"),
            F.col("catid"),
            F.lit(reference_date).cast("date").alias("rundate"),
        )
    )
    # All historical item categories (that are not in current)
    prod_cat_history = (
        PRODUCT_CATALOG_HISTORY.filter(
            (F.col("rundate") >= start_date) & (F.col("rundate") <= end_date)
        )
        .withColumn(
            "catid",
            F.concat_ws(
                "_",
                F.when(
                    F.col("department") == "childrenswear",
                    F.col("next_gender"),
                ).otherwise(F.col("department")),
                F.col("brand"),
                F.col("next_category"),
            ),
        )
        .groupBy(F.col("pid"), F.col("catid"))
        .agg(F.count("*").alias("number_items"))
        .withColumn(
            "pid_max",
            F.row_number().over(
                Window.partitionBy("pid").orderBy(
                    F.desc(F.col("number_items")), F.desc(F.col("catid"))
                )
            ),
        )
        .filter(F.col("pid_max") == 1)
        .select(
            F.col("pid").alias("itemno"),
            F.col("catid"),
            F.lit(reference_date).cast("date").alias("rundate"),
        )
        .join(product_cat_ids_current, how="left_anti", on="itemno")
    )

    product_cat_ids = product_cat_ids_current.union(prod_cat_history)

    logger.info("Running validation checks on item catid data")

    #  length of prod_catids >0  & similar to prior records in table: if true overwrite
    product_cat_ids_len = product_cat_ids.select("itemno").count()

    if product_cat_ids_len == 0:
        raise ValueError("Item catid table contains no data")

    if (
        product_cat_ids.groupBy(F.col("itemno"))
        .agg(F.count("*").alias("number_records"))
        .filter(F.col("number_records") > 1)
        .count()
    ) > 0:
        raise ValueError("Duplicate pids generated in item catid table")

    if spark.catalog.tableExists(output_table):
        current_prod_cat_len = (
            spark.table(output_table).select("itemno").count()
        )
        if not (
            (current_prod_cat_len * 0.9)
            <= product_cat_ids_len
            <= (current_prod_cat_len * 1.1)
        ):
            logger.warning(
                f"Greater than 10% change in number of records for item catid: prior records-{current_prod_cat_len}, new records-{product_cat_ids_len}"
            )

    product_cat_ids.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(output_table)

    logger.info(f"Data in {output_table} updated")

    return


def build_advert_items_df(
    spark, cfg, tbl_configs: dict, reference_date: str, output_table: str
):
    """Build the advert items & catid list by advert for all currently live adverts"""
    from pyspark.sql import functions as F
    from next_ads.utils import etl

    read_tables = cfg["tables"]["read"]
    write_tables = cfg["tables"]["write"]

    PRODUCT_CATALOG = spark.table(read_tables["product_catalog_latest"])
    CONTROL_SHEET = spark.table(
        etl.map_tbl(write_tables["control_sheet_latest"], **tbl_configs)
    )
    SORT_ORDER_LATEST = spark.table(read_tables["sort_order_latest"])
    PRODUCT_CATIDS_LATEST = spark.table(
        etl.map_tbl(write_tables["nextads_items_catid"], **tbl_configs)
    )

    # Currently sort order table seems to contain both sku_id & pid aliases
    # Joining on both to account for issue
    # Should ideally resolve at source for future

    ads = (
        CONTROL_SHEET.filter(F.col("AudienceOnly") == F.lit(0))
        .select("UniqueAdID")
        .distinct()
    )

    advert_items = (
        ads.alias("ad")
        .join(
            SORT_ORDER_LATEST.alias("s_o"),
            on="UniqueAdID",
            how="inner",
        )
        .select(F.col("ad.UniqueAdID"), F.col("s_o.items"))
        .join(
            PRODUCT_CATALOG.alias("cat"),
            on=(F.col("s_o.items") == F.col("cat.pid")),
            how="left",
        )
        .withColumnRenamed("pid", "pid_")
        .join(
            PRODUCT_CATALOG.alias("sku"),
            on=(F.col("s_o.items") == F.col("sku.sku_id")),
            how="left",
        )
        .withColumn(
            "itemno", F.coalesce(F.col("pid_"), F.col("pid"), F.col("items"))
        )
        .select(F.col("ad.UniqueAdID"), F.col("itemno"))
        .join(PRODUCT_CATIDS_LATEST, on="itemno", how="left")
        .select(
            F.col("UniqueAdID"),
            F.col("itemno"),
            F.col("catid"),
            F.lit(reference_date).cast("date").alias("rundate"),
        )
        .distinct()
    )

    logger.info(
        "Running validation checks on advert linked items & catids dataset"
    )
    ad_item_catid_len = advert_items.select("UniqueAdID").count()
    duplicates = (
        advert_items.groupBy("UniqueAdID", "itemno")
        .agg(F.count("*").alias("number_records"))
        .filter(F.col("number_records") > 1)
    )
    if ad_item_catid_len == 0:
        raise ValueError("Advert Item table build contains no data")
    if duplicates.count() > 0:
        duplicate_ads = ", ".join(
            [
                row[0]
                for row in duplicates.distinct().select("UniqueAdID").collect()
            ]
        )
        raise KeyError(
            f"Advert Items has duplicate items for adverts {duplicate_ads}"
        )
    if (
        ads.select("UniqueAdID").count()
        != advert_items.select("UniqueAdID").distinct().count()
    ):
        missing_ads = ", ".join(
            [
                row[0]
                for row in ads.select("UniqueAdID")
                .join(
                    advert_items.select("UniqueAdID").distinct(),
                    on="UniqueAdID",
                    how="leftanti",
                )
                .collect()
            ]
        )
        logger.warning(
            f"Mismatch in the number of adverts with catids. Adverts not represented: {missing_ads}"
        )
    advert_items.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(output_table)
    logger.info(f"Data in {output_table} updated ")

    return


def determine_ad_profile_similiarity(
    spark, cfg, tbl_configs: dict, reference_date: str, output_table: str
):

    from pyspark.sql import functions as F
    from next_ads.utils import etl

    write_tables = cfg["tables"]["write"]

    ADVERT_ITEMS = spark.table(
        etl.map_tbl(write_tables["nextads_advert_items_catid"], **tbl_configs)
    )
    # Determine Ad profile similarity
    ad_items_array = ADVERT_ITEMS.groupBy(
        F.col("UniqueAdID"), F.col("rundate")
    ).agg(
        F.array_distinct(F.collect_list("itemno")).alias("items_list"),
        F.countDistinct("itemno").alias("itemcount"),
    )

    ad_items_overlap = (
        ad_items_array.alias("a")
        .crossJoin(ad_items_array.alias("b"))
        .withColumn(
            "intersect_array",
            F.array_intersect(F.col("a.items_list"), F.col("b.items_list")),
        )
        .withColumn("intersection_count", F.size(F.col("intersect_array")))
        .withColumn(
            "overlap_proportion",
            F.col("intersection_count") / F.col("a.itemcount"),
        )
        .select(
            F.col("a.UniqueAdID"),
            F.col("b.UniqueAdID").alias("TargetUniqueAdID"),
            F.col("a.itemcount"),
            F.col("b.itemcount").alias("target_itemcount"),
            "intersection_count",
            "overlap_proportion",
            F.lit(reference_date).cast("date").alias("rundate"),
        )
    )

    ad_item_profile_similarity_len = ad_items_overlap.select(
        "UniqueAdID"
    ).count()

    # How many ads have similarity of 1 (need to account for duplicate locations)
    if ad_item_profile_similarity_len == 0:
        raise ValueError("Advert Item similarity table buildcontains no data")

    #  How many ads have similarity of 1 (need to account for duplicate locations)
    similarity = (
        ad_items_overlap.withColumns(
            {
                "Adcampaign_components": F.split(F.col("UniqueAdID"), "_"),
                "TargetAdcampaign_components": F.split(
                    F.col("TargetUniqueAdID"), "_"
                ),
            }
        )
        .withColumns(
            {
                "Adcampaign": F.concat_ws(
                    "_",
                    F.col("Adcampaign_components")[0],
                    F.col("Adcampaign_components")[1],
                ),
                "Target_Adcampaign": F.concat_ws(
                    "_",
                    F.col("TargetAdcampaign_components")[0],
                    F.col("TargetAdcampaign_components")[1],
                ),
            }
        )
        .withColumn("check", F.col("Target_Adcampaign") != F.col("Adcampaign"))
        .filter(
            (F.col("overlap_proportion") == F.lit(1))
            & (F.col("UniqueAdID") != F.col("TargetUniqueAdID"))
            & (F.col("Target_Adcampaign") != F.col("Adcampaign"))
        )
    )

    if similarity.count() > 0:
        similarity_records = ", ".join(
            [
                f"{row[0]} : {row[1]}"
                for row in similarity.select(
                    "UniqueAdID", "TargetUniqueAdID"
                ).collect()
            ]
        )
        logger.warning(
            f"Multiple Adverts found with identical item profiles: {similarity_records}"
        )

    ad_items_overlap.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(output_table)

    logger.info(f"Data in {output_table} updated ")

    return


def build_item_action_data(
    spark,
    cfg,
    tbl_configs,
    start_date,
    end_date,
    data_source: str,
    aggregation_level: str,
):
    from pyspark.sql import functions as F
    from next_ads.utils import etl

    read_tables = cfg["tables"]["read"]
    write_tables = cfg["tables"]["write"]

    if data_source == "views":
        WEB_TABLE = spark.table(read_tables["bq_views"])
        APP_TABLE = spark.table(read_tables["bq_views_app"])
    elif data_source == "atbs":
        WEB_TABLE = spark.table(read_tables["bq_atbs"])
        APP_TABLE = spark.table(read_tables["bq_atbs_app"])
    else:
        logger.warning(
            "Incorrect data source passed in for build item dataset"
        )

    web_data = WEB_TABLE.filter(
        F.col("date").between(start_date, end_date)
    ).withColumnRenamed("ProductSKU", "itemno")

    app_data = APP_TABLE.filter(
        (F.col("date").between(start_date, end_date))
    ).withColumnRenamed("ProductSKU", "itemno")

    if data_source == "views":
        web_data = web_data.filter(
            (F.col("ViewTimeSpentSecs") > 0)
            & (F.col("EventType").ilike("pdp_view"))
        )
        app_data = app_data.filter(
            (F.col("ViewTimeSpentSecs") > 0) & (F.col("ScreenName") == "PDP")
        )

    if aggregation_level == "catid":
        PRODUCT_CAT_IDS = spark.table(
            etl.map_tbl(write_tables["nextads_items_catid"], **tbl_configs)
        )
        web_data = web_data.alias("v").join(
            PRODUCT_CAT_IDS, how="inner", on="itemno"
        )
        app_data = app_data.join(PRODUCT_CAT_IDS, how="inner", on="itemno")

    group_cols = (
        ["catid", "date", "UniqueVisitID"]
        if aggregation_level == "catid"
        else ["itemno", "date", "UniqueVisitID"]
    )

    web_data = web_data.groupBy(*group_cols).agg(
        F.min(F.col("Timestamp")).alias("Timestamp")
    )

    app_data = app_data.groupBy(*group_cols).agg(
        F.min(F.col("Timestamp")).alias("Timestamp")
    )

    all_ad_item_views = web_data.union(app_data)

    return all_ad_item_views


def build_advert_affinity(
    spark,
    cfg,
    tbl_configs,
    reference_date,
    output_table,
    history_data_weighting: float = 1,
    ad_percentage_coverage_threshold: float = 0.95,
    lift_threshold: float = 1.1,
):

    from pyspark.sql import functions as F
    from pyspark.sql import Window
    from next_ads.utils import etl

    write_tables = cfg["tables"]["write"]

    ADVERT_ITEMS = spark.table(
        etl.map_tbl(write_tables["nextads_advert_items_catid"], **tbl_configs)
    )

    advert_catids = ADVERT_ITEMS.drop("itemno").distinct()

    CONTROL_SHEET = spark.table(
        etl.map_tbl(write_tables["control_sheet_latest"], **tbl_configs)
    )
    ad_page_types = (
        CONTROL_SHEET.filter(F.col("AudienceOnly") == F.lit(0))
        .groupBy("rundate", "UniqueAdID")
        .pivot("PageGroup")
        .agg(F.max(F.lit(True)))
        .na.fill(False)
    )

    AD_ITEM_SIMILIARITY_LATEST = spark.table(
        etl.map_tbl(
            write_tables["nextads_advert_items_profile_similarity"],
            **tbl_configs,
        )
    )

    #  Validate there is no duplication
    total_ad_number = ad_page_types.select("UniqueAdID").distinct().count()
    if total_ad_number != ad_page_types.select("UniqueAdID").count():
        raise ValueError(
            "Duplicate advert IDs identified in Advert page type view"
        )

    # Build out the afinity levels
    start_date, end_date = _date_window_offset(reference_date, 30)
    prior_year_start_date = F.date_sub(start_date, 365)
    prior_year_end_date = F.date_sub(end_date, 365)

    logger.info("Gathering item activity from prior 30 days at item level")
    ## Prior 30 days at item level:
    recent_item_views = build_item_action_data(
        spark,
        cfg,
        tbl_configs,
        start_date,
        end_date,
        data_source="views",
        aggregation_level="itemno",
    )
    recent_item_atbs = build_item_action_data(
        spark,
        cfg,
        tbl_configs,
        start_date,
        end_date,
        data_source="atbs",
        aggregation_level="itemno",
    )

    logger.info("Building prior 30 days association at item level")

    recent_associated_view_basket_items = (
        recent_item_views.alias("v")
        .join(
            recent_item_atbs.alias("b"),
            how="inner",
            on=(
                (F.col("v.UniqueVisitID") == F.col("b.UniqueVisitID"))
                & (F.col("b.Timestamp") > F.col("v.Timestamp"))
            ),
        )
        .select(
            F.col("v.itemno").alias("viewitem"),
            F.col("b.itemno").alias("atbitem"),
            F.col("v.date"),
            F.col("v.UniqueVisitID"),
        )
    )

    advert_item_associations = (
        recent_associated_view_basket_items.alias("vb")
        .join(
            F.broadcast(ADVERT_ITEMS.alias("av")),
            on=(F.col("vb.viewitem") == F.col("av.itemno")),
            how="inner",
        )
        .select("date", "UniqueVisitID", "UniqueAdID", "atbitem")
        .withColumnRenamed("UniqueAdID", "ViewUniqueAdID")
        .join(
            F.broadcast(ADVERT_ITEMS.alias("ab")),
            on=(F.col("vb.atbitem") == F.col("ab.itemno")),
            how="inner",
        )
        .withColumnRenamed("UniqueAdID", "AtbUniqueAdID")
        .select("date", "UniqueVisitID", "ViewUniqueAdID", "AtbUniqueAdID")
        .distinct()
    )

    item_number_views = advert_item_associations.groupBy("ViewUniqueAdID").agg(
        F.countDistinct("UniqueVisitID").alias("number_views")
    )
    item_number_atbs = advert_item_associations.groupBy("AtbUniqueAdID").agg(
        F.countDistinct("UniqueVisitID").alias("number_atbs")
    )
    item_number_views_atbs = advert_item_associations.groupBy(
        "ViewUniqueAdID", "AtbUniqueAdID"
    ).agg(F.countDistinct("UniqueVisitID").alias("number_views_atbs"))

    logger.info("Running validation checks on prior 30 days association data")
    total_item_associations = (
        advert_item_associations.select("UniqueVisitID").distinct().count()
    )
    perc_recent_ads_covered = (
        advert_item_associations.select("ViewUniqueAdID").distinct().count()
        / total_ad_number
    )
    if not (total_item_associations > 0):
        logger.warning("No records identified for recent item associations")

    if perc_recent_ads_covered < ad_percentage_coverage_threshold:
        logger.warning(
            f"Less than {round(ad_percentage_coverage_threshold * 100, 0)} of adverts covered associations data"
        )

    logger.info(
        "Gathering item activity from prior years next 30 days at catid level"
    )

    prior_year_item_views = build_item_action_data(
        spark,
        cfg,
        tbl_configs,
        prior_year_start_date,
        prior_year_end_date,
        data_source="views",
        aggregation_level="catid",
    )
    logger.info("completed prior year views data")

    prior_year_item_atbs = build_item_action_data(
        spark,
        cfg,
        tbl_configs,
        prior_year_start_date,
        prior_year_end_date,
        data_source="atbs",
        aggregation_level="catid",
    )
    logger.info("completed prior year actions data")

    prior_year_associated_view_basket_catid = (
        prior_year_item_views.alias("v")
        .join(
            prior_year_item_atbs.alias("b"),
            how="inner",
            on=(
                (F.col("v.UniqueVisitID") == F.col("b.UniqueVisitID"))
                & (F.col("b.Timestamp") > F.col("v.Timestamp"))
            ),
        )
        .select(
            F.col("v.catid").alias("viewcatid"),
            F.col("b.catid").alias("atbcatid"),
            F.col("v.date"),
            F.col("v.UniqueVisitID"),
        )
    )

    prior_year_advert_item_associations = (
        prior_year_associated_view_basket_catid.alias("vb")
        .join(
            F.broadcast(advert_catids.alias("cv")),
            on=(F.col("vb.viewcatid") == F.col("cv.catid")),
            how="inner",
        )
        .select("date", "UniqueVisitID", "UniqueAdID", "atbcatid")
        .withColumnRenamed("UniqueAdID", "ViewUniqueAdID")
        .join(
            F.broadcast(advert_catids.alias("cb")),
            on=(F.col("vb.atbcatid") == F.col("cb.catid")),
            how="inner",
        )
        .withColumnRenamed("UniqueAdID", "AtbUniqueAdID")
        .select("date", "UniqueVisitID", "ViewUniqueAdID", "AtbUniqueAdID")
        .distinct()
    )
    number_views_prior_year = prior_year_advert_item_associations.groupBy(
        "ViewUniqueAdID"
    ).agg(F.countDistinct("UniqueVisitID").alias("number_views"))
    number_atbs_prior_year = prior_year_advert_item_associations.groupBy(
        "AtbUniqueAdID"
    ).agg(F.countDistinct("UniqueVisitID").alias("number_atbs"))
    number_views_atbs_prior_year = prior_year_advert_item_associations.groupBy(
        "ViewUniqueAdID", "AtbUniqueAdID"
    ).agg(F.countDistinct("UniqueVisitID").alias("number_views_atbs"))

    logger.info(
        "Running validation checks on prior year 30 days association data"
    )
    prior_year_total_item_associations = (
        prior_year_advert_item_associations.select("UniqueVisitID")
        .distinct()
        .count()
    )

    if not (prior_year_total_item_associations > 0):
        logger.warning(
            "No records identified for prior year catid associations"
        )

    if (
        prior_year_advert_item_associations.select("ViewUniqueAdID")
        .distinct()
        .count()
        / total_ad_number
    ) < ad_percentage_coverage_threshold:
        logger.warning(
            f"Less than {round(ad_percentage_coverage_threshold * 100, 0)} of adverts covered in prior year associations data"
        )

    logger.info("Combining recent & prior year affinity datasets")
    logger.info(
        f"Prior year dataset weighting factor used: {history_data_weighting}"
    )

    total_sessions = (
        prior_year_associated_view_basket_catid.select("UniqueVisitID")
        .distinct()
        .count()
        * history_data_weighting
    ) + total_item_associations

    logger.info(
        f"Total weighted number of sessions used for analysis :{total_sessions}"
    )

    number_views_combined = (
        item_number_views.alias("v")
        .join(
            number_views_prior_year.alias("pv"),
            on="ViewUniqueAdID",
            how="left",
        )
        .withColumn(
            "number_views_",
            F.col("v.number_views")
            + (F.col("pv.number_views") * F.lit(history_data_weighting)),
        )
    ).select(F.col("v.ViewUniqueAdID"), F.col("number_views_"))

    number_atbs_combined = (
        item_number_atbs.alias("b")
        .join(
            number_atbs_prior_year.alias("pb"), on="AtbUniqueAdID", how="left"
        )
        .withColumn(
            "number_atbs_",
            F.col("b.number_atbs")
            + (F.col("pb.number_atbs") * F.lit(history_data_weighting)),
        )
    ).select(F.col("b.AtbUniqueAdID"), F.col("number_atbs_"))

    number_views_atbs_combined = (
        item_number_views_atbs.alias("vb")
        .join(
            number_views_atbs_prior_year.alias("pvb"),
            on=(
                (F.col("vb.ViewUniqueAdID") == F.col("pvb.ViewUniqueAdID"))
                & (F.col("vb.AtbUniqueAdID") == F.col("pvb.AtbUniqueAdID"))
            ),
            how="left",
        )
        .withColumn(
            "number_views_atbs_",
            F.col("vb.number_views_atbs")
            + (F.col("pvb.number_views_atbs") * F.lit(history_data_weighting)),
        )
    ).select(
        F.col("vb.AtbUniqueAdID"),
        F.col("pvb.ViewUniqueAdID"),
        F.col("number_views_atbs_"),
    )

    association = (
        number_views_atbs_combined.alias("base")
        .join(
            number_views_combined.alias("views"),
            how="left",
            on="ViewUniqueAdID",
        )
        .join(
            number_atbs_combined.alias("atb"),
            how="left",
            on="AtbUniqueAdID",
        )
        .withColumn("support_views", (F.col("number_views_") / total_sessions))
        .withColumn("support_atbs", (F.col("number_atbs_") / total_sessions))
        .withColumn(
            "support_views_atbs",
            (F.col("number_views_atbs_") / total_sessions),
        )
        .withColumn(
            "cosine_similarity",
            (
                F.col("number_views_atbs_")
                / (
                    F.sqrt(F.col("number_views_"))
                    * F.sqrt(F.col("number_atbs_"))
                )
            ),
        )
        .withColumn(
            "lift",
            (
                F.col("support_views_atbs")
                / (F.col("support_views") * F.col("support_atbs"))
            ),
        )
        .select(
            F.col("base.ViewUniqueAdID"),
            F.col("base.AtbUniqueAdID"),
            F.col("base.number_views_atbs_").alias("number_views_atbs"),
            F.col("views.number_views_").alias("number_views"),
            F.col("atb.number_atbs_").alias("number_atbs"),
            F.col("support_views"),
            F.col("support_atbs"),
            F.col("support_views_atbs"),
            F.col("cosine_similarity"),
            F.col("lift"),
        )
    )

    logger.info(
        f"Adjusting lift based on advert item similarity  & filtering to a lift threshold of {lift_threshold}"
    )

    final_associations = (
        association.alias("base")
        .join(
            F.broadcast(AD_ITEM_SIMILIARITY_LATEST).alias("overlap"),
            on=(
                (F.col("base.ViewUniqueAdID") == F.col("overlap.UniqueAdID"))
                & (
                    F.col("base.AtbUniqueAdID")
                    == F.col("overlap.TargetUniqueAdID")
                )
            ),
            how="left",
        )
        .withColumn(
            "lift_adjusted",
            (
                (
                    F.col("support_views_atbs")
                    / (F.col("support_views") * F.col("support_atbs"))
                )
                * F.power(F.col("support_atbs"), F.lit(0.25))
            )
            / F.power(
                F.lit(1) + F.coalesce(F.col("overlap_proportion"), F.lit(0)),
                F.lit(2),
            ),
        )
        .filter(F.col("lift_adjusted") > F.lit(lift_threshold))
        .withColumn(
            "lift_adjusted_ranking",
            F.row_number().over(
                Window.partitionBy("ViewUniqueAdID").orderBy(
                    F.desc(F.col("lift_adjusted"))
                )
            ),
        )
        .join(
            ad_page_types.alias("ad"),
            on=F.col("base.AtbUniqueAdID") == F.col("ad.UniqueAdID"),
            how="left",
        )
        .select(
            F.col("base.ViewUniqueAdID"),
            F.col("base.AtbUniqueAdID"),
            F.col("number_views_atbs"),
            F.col("number_views"),
            F.col("number_atbs"),
            F.col("support_views"),
            F.col("support_atbs"),
            F.col("support_views_atbs"),
            F.col("cosine_similarity"),
            F.col("lift"),
            F.col("lift_adjusted"),
            F.col("lift_adjusted_ranking"),
            F.col("overlap_proportion"),
            F.col("intersection_count"),
            *(
                [
                    F.col(i)
                    for i in ad_page_types.columns
                    if i not in ["UniqueAdID", "rundate"]
                ]
            ),
            F.lit(reference_date).cast("date").alias("rundate"),
        )
    )

    logger.info("Running validation checks on advert: advert affinity dataset")

    rank1 = final_associations.filter(
        F.col("lift_adjusted_ranking") == F.lit(1)
    )
    number_rank1_ads = rank1.select("ViewUniqueAdID").distinct().count()
    number_self_ranked1 = (
        rank1.select("ViewUniqueAdID")
        .filter(F.col("ViewUniqueAdID") == F.col("AtbUniqueAdID"))
        .select("ViewUniqueAdID")
        .distinct()
        .count()
    )

    if (
        rank1.groupBy("ViewUniqueAdID")
        .agg(F.count("*").alias("number_records"))
        .filter(F.col("number_records") > 1)
        .count()
    ) > 0:
        raise ValueError("Multiple rank 1 records for Adverts")

    if (number_self_ranked1 / number_rank1_ads) > 0.5:
        logger.warning("Over 50% of adverts are self reccomending")

    if (number_rank1_ads / total_ad_number) < ad_percentage_coverage_threshold:
        logger.warning(
            f"Less than {round((ad_percentage_coverage_threshold * 100), 0)} of adverts covered in associations data"
        )

    final_associations.write.format("delta").mode("overwrite").option(
        "mergeSchema", "true"
    ).saveAsTable(output_table)

    logger.info(f"Data in {output_table} updated")

    return

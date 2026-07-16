
from dsutils.logtools import get_logger

logger = get_logger(__name__)


def create_realtime_known_reranking_weighting_rules(spark , reference_date:str , output_table:str  ):

    import pyspark.sql.functions as F
    import json
    from pathlib import Path
    #TODO migrate to central config functionality ?
    file="src/next_ads/realtime/decisioning/realtime_known.json"
    rtcfg=json.loads(Path(file).read_text())

    flattened_data = [{**value, "ruleID": key} for key, value in rtcfg["rules"].items()]

    # Create Spark DataFrame directly
    rt_rules_df = spark.createDataFrame(flattened_data).select(
        F.col("ruleID"),
        F.col("action"),
        F.col("feature"),
        F.col("weight"), 
        F.lit(reference_date).cast("date").alias("rundate"),
    )

    logger.info("Running validation checks on realtime reranking rules data") 

    rules_len= rt_rules_df.select("ruleID").count()
    if rules_len == 0:
        raise ValueError("Realtime known rules config contains no data")
    
    if spark.catalog.tableExists(output_table):
        current_rules_len= (
            spark.table(output_table).select("ruleID").count()
        )
        if not (
            (current_rules_len * 0.9)
            <= rules_len
            <= (current_rules_len * 1.1)
        ):
            logger.warning(
                f"Greater than 10% change in number of rules: prior records-{current_rules_len}, new records-{rules_len}"
            )
    num_dups=rt_rules_df.groupBy("action", "feature").agg(F.count("*").alias("number_duplicates")).filter(F.col("number_duplicates") > 1).count()
    if num_dups >0: 
        raise ValueError("Duplicate action/feature records for rules")

    rt_rules_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(output_table)

    logger.info(f"Data in {output_table} updated")

    return 



def central_product_details_by_pid(spark, cfg): 


    from pyspark.sql import functions as F
    from pyspark.sql import Window

    read_tables = cfg["tables"]["read"]
    PRODUCT_CATALOG = spark.table(read_tables["product_catalog_latest"])

    pids_spine=PRODUCT_CATALOG.select("pid").distinct()
    cols=["brand", "department", "next_category"]
    for col in cols:
         PRODUCT_CATALOG.select(F.col("pid"), F.col(col)).a



    
def advert_details_build():

    #TODO: Build out adv features 
    
    # Start with this as the primary cat & a weighting 

    # Brand 
    # premium brand 
    # department 
    # category
    pass
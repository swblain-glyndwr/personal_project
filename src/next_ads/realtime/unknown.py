import json
import re
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.etl import map_tbl
from dsutils.streaming import (
    EventHubConnectionHelper,
    configure_streaming_spark,
    get_kafka_streaming_source,
    decode_kafka_message,
)
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StringType,
    ArrayType,
)


logger = get_logger(__name__)


def create_backfill_udf(top_ads_map, all_locations):
    """Creates a UDF that backfills missing locations with top performing ads.

    Args:
        top_ads_map: Dict mapping location to default MASID value
        all_locations: List of all expected locations

    Returns:
        PySpark UDF for backfilling
    """

    def backfill_masids(personalized_map):
        if personalized_map is None:
            personalized_map = {}

        result = []

        # Add personalized MASIDs first
        for location in personalized_map.keys():
            result.append(personalized_map[location])

        # Add default MASIDs for missing locations
        for location in all_locations:
            if location not in personalized_map:
                result.append(top_ads_map.get(location, f"{location}_default"))

        return result

    return F.udf(backfill_masids, ArrayType(StringType()))


def set_ads(
    stream: DataFrame,
    df_vb: DataFrame,
    df_sort_order_latest: DataFrame,
    df_ads: DataFrame,
    df_top_performing_ads: DataFrame,
    spark: SparkSession,
    test_split: float,
    deterministic: bool,
    control_MASID: str,
    exclude_locations_like: list[str] | None = None,
    streaming: bool = False,
) -> DataFrame:
    """Assigns personalized MASID tokens to users based on product viewing
    history and ad performance.

    This function processes user event streams to determine the most
    relevant ads for each user at different locations, using product
    co-viewing data and ad performance metrics. It generates a personalized
    mapping of MASIDs for each user, backfilling with top-performing ads
    where personalized data is missing.

    Args:
        stream: Input event stream containing user product views
        vb_df: DataFrame with product co-viewing (viewed-bought)
            relationships and lift scores
        sort_order_df: DataFrame mapping ads to products for sorting
        ad_locs_df: DataFrame mapping ads to locations and MASID tokens
        top_performing_ads: DataFrame with top-performing ads by location
        spark: Spark session object
        test_split: Fraction (0.0-1.0) for control group assignment
        deterministic: If False, applies test/control split; if True,
            assigns test MASID to all
        control_MASID: MASID value for control group
        streaming: Whether input is a streaming DataFrame

    Returns:
        DataFrame with columns 'RPID' and 'MASID', where 'MASID'
        is a pipe-delimited string of personalized or default MASID tokens
        for each location
    """
    if streaming:
        # Limit state for deduplication
        stream = stream.withWatermark("event_timestamp", "1 minutes")

    # Filter for valid events
    stream = (
        stream.filter(F.col("RPID").isNotNull())
        .filter(F.length(F.col("MASID")) < 2)
        .filter(F.col("RPID") != "-1")
        .select(
            F.col("RPID"), F.upper(F.col("ProductSKU")[0]).alias("product")
        )
    )

    # Prepare input data
    df_vb = df_vb.select("itemno1", "itemno2", "lift")

    df_sort_order_latest = df_sort_order_latest.select(
        "UniqueAdID", F.col("items").alias("itemno2")
    )

    exclude_locations_like = exclude_locations_like or []
    exclude_pattern = r"^(%s)" % "|".join(
        map(re.escape, exclude_locations_like)
    )

    df_ads = df_ads.filter(~F.col("Location").rlike(exclude_pattern)).select(
        "UniqueAdID", "Location", "MASIDToken"
    )

    # Pre-join viewed-bought table on items behind live ads
    df_vb = df_vb.join(df_sort_order_latest, on=["itemno2"])

    # Prepare top performing ads for backfill
    all_locations = [
        row["Location"]
        for row in df_top_performing_ads.select("Location")
        .distinct()
        .collect()
    ]
    top_ads_data = df_top_performing_ads.select(
        "Location", "MASIDToken"
    ).collect()
    top_ads_map = {
        row["Location"]: f"{row['Location']}_{row['MASIDToken']}"
        for row in top_ads_data
    }

    backfill_udf = create_backfill_udf(top_ads_map, all_locations)

    # Join with customer viewed products
    stream = df_vb.join(
        stream,
        (stream["product"] == df_vb["itemno1"])
        & (stream["product"] != df_vb["itemno2"]),
        how="inner",
    )

    # Aggregate by rpid and ad to get average lift
    df_ad_relevance_scores = (
        stream.groupBy("RPID", "UniqueAdID")
        .agg(F.mean("lift").alias("adRelevanceScore"))
        .join(df_ads, on="UniqueAdID")
    )

    # Handle duplicate keys in maps
    spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")

    # Create personalized mappings
    personalised_maisd = (
        df_ad_relevance_scores.withColumn(
            "MASID", F.concat_ws("_", "Location", "MASIDToken")
        )
        # First, get the best ad per RPID/Location combination
        .groupBy("RPID", "Location")
        .agg(F.max_by("MASID", "adRelevanceScore").alias("best_masid"))
        # Then aggregate by RPID to create the final map
        .groupBy("RPID")
        .agg(
            # Now we're guaranteed no duplicate locations per RPID
            F.map_from_arrays(
                F.collect_list("Location"), F.collect_list("best_masid")
            ).alias("personalized_map")
        )
        .withColumn("final_masids", backfill_udf(F.col("personalized_map")))
        .withColumn("MASID", F.concat_ws("|", F.col("final_masids")))
        .select("RPID", "MASID")
    )

    # Perform test/control split
    if not deterministic:
        return personalised_maisd.withColumn(
            "MASID",
            F.when(
                F.abs((F.hash(F.col("RPID")) % 100) / 100.0)
                > F.lit(test_split),
                F.col("MASID"),
            ).otherwise(F.lit(control_MASID)),
        )
    else:
        # Apply test MASID to all
        return personalised_maisd


def format_stream_archive(stream: DataFrame) -> DataFrame:
    """Formats the raw EventHub stream for processing.

    Decodes Kafka messages and extracts relevant fields from the
    GA event payload.

    Args:
        stream: Raw Kafka/EventHub streaming DataFrame

    Returns:
        Formatted DataFrame with event fields
    """
    decoded = decode_kafka_message(stream)
    return decoded.selectExpr(
        "payload_json.event_type",
        "payload_json.event_timestamp",
        "payload_json.RPID",
        "payload_json.visitid",
        "payload_json.ProductSKU",
        "payload_json.MASID",
        "payload_json.event_timestamp::date as event_date",
    )


def run_realtime_unknown(
    jobname: str | None,
    job_env: str,
    client: str | None,
    territory: str | None,
    log_level: str | None,
) -> None:
    configure_logging(log_level=log_level) if log_level else configure_logging()
    logger.info(f"Running in job environment: {job_env}")

    if not client:
        assert not jobname, "Client must be specified when running as a job"
        client = "next_uk"  # Client can be specified for interactive debugging
        logger.warning(f"Client not specified (defaulting to {client})")

    logger.info(f"Configuring real-time run for client: {client}")
    with open(f"real_time/config/{client}.json") as f:
        cfg = json.load(f)
    rtu_cfg = cfg["real_time_unknown"]

    schema = rtu_cfg["schema"][job_env]
    logger.info(f"Read schema set to {schema}")

    target = rtu_cfg["target_by_env"][job_env]
    SPN_secretscope = rtu_cfg["SPN_secretscope"]
    db_secretscope = rtu_cfg["db_secretscope"]
    checkpoint_base_path = rtu_cfg["checkpoint_base_path"]
    rpid_filter = rtu_cfg.get("rpid_filter", "")
    test_split = float(rtu_cfg["test_split"])
    control_masid = rtu_cfg["control_masid"]

    tbls = rtu_cfg["tables"]["read"]
    tbl_args = {"schema": schema, "client": client}
    viewed_bought_latest = tbls["vb"]
    sort_order_latest = tbls["sort_order_latest"]
    control_sheet_latest = tbls["control_sheet_latest"]
    top_performing_ads_latest = map_tbl(
        tbls["top_performing_ads_by_location"],
        **tbl_args,
    )
    spark = configure_spark()
    configure_streaming_spark(spark)
    spark.conf.set(
        "spark.sql.streaming.statefulOperator.checkCorrectness.enabled", False
    )

    cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    logger.info(f"Connected to cluster {cluster_id}")
    logger.info("Starting Real-Time Ads Personalization Streaming Job")

    streams = []
    for conn in rtu_cfg["eventhub_connections"]:
        streams.append(
            EventHubConnectionHelper(
                target=target,
                db_secretscope=db_secretscope,
                SPN_secretscope=SPN_secretscope,
                eventhub_namespace=conn["eventhub_namespace"],
                inbound_topic=conn["inbound_topic"],
                outbound_topic="",
                checkpoint_path=(
                    f"{checkpoint_base_path}"
                    f"{conn.get('checkpoint_suffix', '')}"
                ),
            )
        )

    df_vb = spark.read.table(viewed_bought_latest)
    df_sort_order_latest = spark.table(sort_order_latest)
    df_ads = spark.table(control_sheet_latest)
    df_top_performing_ads = spark.table(top_performing_ads_latest)
    logger.info(f"Loaded {df_vb.count():,} viewed-bought relationships")
    logger.info(f"Loaded {df_sort_order_latest.count():,} ad sort orders")
    logger.info(f"Loaded {df_ads.count():,} ad locations")
    logger.info(f"Loaded {df_top_performing_ads.count():,} top performing ads")

    raw = None
    for stream in streams:
        raw_stream = get_kafka_streaming_source(spark, stream)
        raw = raw_stream if raw is None else raw.union(raw_stream)

    formatted = format_stream_archive(raw)

    if rpid_filter:
        formatted = formatted.filter(f"RPID in ({rpid_filter})")
        logger.info(f"Applied RPID filter: {rpid_filter}")

    ads = set_ads(
        stream=formatted,
        df_vb=df_vb,
        df_sort_order_latest=df_sort_order_latest,
        df_ads=df_ads,
        df_top_performing_ads=df_top_performing_ads,
        spark=spark,
        test_split=test_split,
        deterministic=False,
        control_MASID=control_masid,
        exclude_locations_like=rtu_cfg["exclude_locations_like"],
        streaming=True,
    )
    logger.info(f"Personalization configured (test_split={test_split})")

    global query
    query = ads.writeStream.outputMode("update").format("console").option(
        "truncate", False
    ).start()

    logger.info("Streaming job started successfully")


def main() -> None:
    jobparser = get_job_parser()
    jobparser._parse_args()
    run_realtime_unknown(
        jobname=jobparser.get_arg("--jobname"),
        job_env=jobparser.get_arg("--job_env"),
        client=jobparser.get_arg("--client"),
        territory=jobparser.get_arg("--territory"),
        log_level=jobparser.get_arg("--log_level"),
    )


if __name__ == "__main__":
    main()

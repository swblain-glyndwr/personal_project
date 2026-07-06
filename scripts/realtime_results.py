import sys
from pathlib import Path
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() # type: ignore # noqa
    if not notebook_path.startswith('/Workspace'):
        notebook_path = '/Workspace' + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
    MapType
)

from datetime import date, timedelta

from dsutils.dbc import configure_spark
from dsutils.argparser import get_job_parser
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import delete_from_and_load, truncate_and_load
from next_ads.utils import config_manager

jobparser = get_job_parser()
LOG_LEVEL = jobparser.get_arg('--log_level')
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()

jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client")
LOG_LEVEL = jobparser.get_arg("--log_level")
if LOG_LEVEL:
    configure_logging(log_level=LOG_LEVEL)
else:
    configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert JOB_ENV.lower() == "dev", (
        f"Client must be specified when running in {JOB_ENV}"
    )
    CLIENT = "next_uk"  # Client can be specified for interactive debugging
    logger.warning(f"Client not specified (defaulting to {CLIENT})")

# load configuration
config = config_manager.load_config(JOB_ENV)

# read
BQ_ACTIONS_NEXT_UK = config.tables_read.bq_actions
BQ_ACTIONS_NEXT_UK_APP = config.tables_read.bq_actions_app
BQ_TRANSACTIONS_NEXT_UK = config.tables_read.bq_transactions
BQ_TRANSACTIONS_NEXT_UK_APP = config.tables_read.bq_transactions_app
# write
REALTIME_RESULTS = config.tables_write.realtime_results
REALTIME_RESULTS_LATEST = config.tables_write.realtime_results_latest

session_date_str = (
    jobparser.get_arg("--session_date")
    or (date.today() - timedelta(days=1)).isoformat()
)
SESSION_DATE = F.lit(session_date_str).cast("date")
logger.info(f"Using SESSION_DATE: {session_date_str}")


def safe_bigint(column_name: str):
    return F.expr(
        """
        try_cast(
            case
                when lower(trim(cast({column_name} as string)))
                    in ('', 'null', 'none') then null
                else cast({column_name} as string)
            end as BIGINT
        )
        """.strip().format(column_name=column_name)
    )


# Explanation of results script:
# Collect all transaction and action data for both web and app
# along side some additional nice to have metadata cols which
# could be used to breakdown results further.

# We then collect real time masid updates from rtp_exponea_tracking
# table and assign a TreatmentGroup flag. Using the AnonRPID value
# we then join this onto the actions table and take the latest row
# value (using ActionTimestamp), this allows us to use the UniqueVisitID
# session identifier present in the actions data to then join onto transactions
# data (using UniqueVisitID). The results is df_purchases which we then can
# then do some aggredations on to get high level metrics like RPV, CV, AOV.

# collect transaction data with session identifiers
df_transactions_web = (
    spark.table(BQ_TRANSACTIONS_NEXT_UK_APP)
    .select("TransactionID",
            "UniqueVisitID",
            F.col("AccountNumber_Transaction").alias("AccountNumber"),
            F.col("date").alias("TransactionDate"),
            F.col("Timestamp").alias("TransactionTimestamp"),
            F.col("ProductSKU").alias("PID"),
            "ProductRevenue")
    .filter(F.col('Date') == SESSION_DATE)
)

df_transactions_app = (
    spark.table(BQ_TRANSACTIONS_NEXT_UK_APP)
    .select("TransactionID",
            "UniqueVisitID",
            F.col("AccountNumber_Transaction").alias("AccountNumber"),
            F.col("date").alias("TransactionDate"),
            F.col("Timestamp").alias("TransactionTimestamp"),
            F.col("ProductSKU").alias("PID"),
            "ProductRevenue")
    .filter(F.col('Date') == SESSION_DATE)
)

df_transactions = df_transactions_web.union(df_transactions_app)
logger.info(f"Collected {df_transactions.count()} transactions.")


# web actions metadata with session identifiers, a web action is identifieable
#  by having a PagePath which is NOT null
df_actions_web = (
    spark.table(BQ_ACTIONS_NEXT_UK)
    .select(
        "UniqueVisitID",
        safe_bigint("RPID_Hit").alias("RPID"),
        F.col("Date").alias("ActionDate"),
        F.col("Timestamp").alias("ActionTimestamp"),
        "PagePath",
        F.lit(None).cast(StringType()).alias("ScreenName"),  # Missing in Web
        "Action"
    )
    .filter(F.col("ActionDate") == SESSION_DATE)
)

# app actions metadata with session identifiers, an app action is
# identifieable by having a ScreenName which is NOT null
df_actions_app = (
    spark.table(BQ_ACTIONS_NEXT_UK_APP)
    .select(
        "UniqueVisitID",
        safe_bigint("RPID_Hit").alias("RPID"),
        F.col("Date").alias("ActionDate"),
        F.col("Timestamp").alias("ActionTimestamp"),
        F.lit(None).cast(StringType()).alias("PagePath"),  # Missing in App
        "ScreenName",
        "Action"
    )
    .filter(F.col("ActionDate") == SESSION_DATE)
)

df_actions = df_actions_web.union(df_actions_app)
logger.info(f"Collected {df_actions.count()} actions.")


request_body_schema = StructType([
    StructField("customer_ids", MapType(StringType(), StringType()), True),
    StructField("properties", MapType(StringType(), StringType()), True),
    StructField("update_timestamp", LongType(), True)
])

df_rt = (
    spark
    .table("marketingdata_prod.warehouse.rtp_exponea_tracking")
    .withColumn("parsed_body", F.from_json(F.col("request_body"),
                                           request_body_schema))
    .select(F.col("ID"),
            F.col("parsed_body.customer_ids").getItem("anon_rpid"
                                                      ).alias("AnonRPID"),
            F.col("parsed_body.properties").getItem("rt_masid").alias("MASID"),
            F.col("parsed_body.update_timestamp").alias("UpdateTimestampUnix"),
            F.from_unixtime(F.col("parsed_body.update_timestamp"),
                            "yyyy-MM-dd HH:mm:ss"
                            ).cast(TimestampType()
                                   ).alias("UpdateTimestampDatetime"),
            F.col("response_timestamp").alias("ResponseTimestamp"),
            F.col("api_response_status").alias("ApiResponseStatus"),
            F.col("api_response_body").alias("ApiResponseBody"))
    .withColumn("TreatmentGroup", F.col("MASID") != 'PS1_Z')
    .withColumn("AnonRPID", safe_bigint("AnonRPID"))
    .withColumn("UpdateDate", F.to_date(F.col("UpdateTimestampDatetime")))
    .filter(F.col('UpdateDate') == SESSION_DATE)
)

logger.info(f"Collected {df_rt.count()} RT masid updates.")
logger.info(f"Num distinct RPIDs with RT updates: {df_rt.select('AnonRPID').distinct().count()}") # noqa

df_rt_distinct = (
    df_rt
    .select("AnonRPID", "TreatmentGroup")
    .distinct()
)

window_spec = Window.partitionBy("RPID").orderBy(
    F.col("ActionTimestamp").desc())

df_latest_actions = (
    df_actions
    .join(df_rt_distinct, df_actions.RPID == df_rt_distinct.AnonRPID, "inner")
    .withColumn("rank", F.row_number().over(window_spec))
    .filter(F.col("rank") == 1)
    .drop("rank")
)

df_purchases = (
    df_latest_actions
    .join(df_transactions, "UniqueVisitID", "inner")
    .select(
        "UniqueVisitID",
        "AnonRPID",
        "TreatmentGroup",
        "RPID",
        "AccountNumber",
        "TransactionID",
        "TransactionDate",
        "TransactionTimestamp",
        "PID",
        "ProductRevenue"
    )
)
logger.info(f"Num distinct RPIDs with purchases: {df_purchases.select('RPID').distinct().count()}") # noqa

df_metrics_agg = (
    df_latest_actions
    .join(
        df_transactions.select("UniqueVisitID",
                               "TransactionID",
                               "ProductRevenue"),
        on="UniqueVisitID",
        how="left"
    )
    .groupBy("TreatmentGroup")
    .agg(
        F.countDistinct("UniqueVisitID").alias("total_sessions"),
        F.countDistinct("TransactionID").alias("total_orders"),
        F.sum(F.coalesce(F.col("ProductRevenue"), F.lit(0))
              ).alias("total_revenue")
    )
)

df_treatment_groups = spark.range(2).select(
    (F.col("id") == 1).alias("TreatmentGroup")
)

df_metrics = (
    df_treatment_groups
    .join(df_metrics_agg, on="TreatmentGroup", how="left")
    .withColumn(
        "total_sessions", F.coalesce(F.col("total_sessions"), F.lit(0))
    )
    .withColumn(
        "total_orders", F.coalesce(F.col("total_orders"), F.lit(0))
    )
    .withColumn(
        "total_revenue", F.coalesce(F.col("total_revenue"), F.lit(0))
    )
    .withColumn("RPV", F.round(F.col("total_revenue") / F.col("total_sessions"
                                                              ), 4))
    .withColumn("CVR", F.round(F.col("total_orders") / F.col("total_sessions"
                                                             ), 4))
    .withColumn("AOV",
                F.round(
                    F.when(
                        F.col("total_orders") > 0,
                        F.col("total_revenue") / F.col("total_orders"))
                    .otherwise(0), 4))
    .withColumn(
        "RPV",
        F.when(F.col("total_sessions") > 0, F.col("RPV")).otherwise(F.lit(0))
    )
    .withColumn(
        "CVR",
        F.when(F.col("total_sessions") > 0, F.col("CVR")).otherwise(F.lit(0))
    )
    .select(
            F.lit(session_date_str).cast("date").alias("SessionDate"),
            "*"
        )
)
df_metrics.show()

logger.info('Writing item-theme mapping to output tables')
truncate_and_load(
    df_metrics,
    REALTIME_RESULTS_LATEST,
    pk_cols=['SessionDate', 'TreatmentGroup']
)

delete_from_and_load(
    df_metrics,
    REALTIME_RESULTS,
    pk_cols=['SessionDate', 'TreatmentGroup'],
    del_where={'SessionDate': f"'{session_date_str}'"}
)

logger.info('Run complete')

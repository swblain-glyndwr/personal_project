import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

import json
from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql import Window
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from next_ads.utils import config_manager
from next_ads.utils import etl
from pyspark.sql.functions import broadcast


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client")
LOG_LEVEL = jobparser.get_arg("--log_level")
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

reporting_metadata_cols = [
    "PotNumber",
    "CampaignNumber",
    "Title",
    "AlgoDivision",
    "TradeDivision",
    "Brand",
    "MASIDToken",
    "Segment",
    "AdDriver",
    "TemplateName",
    "TargetingCriteria",
    "AdCategory",
    "AdMission",
    "AdTrend",
    "AdSubcategory",
    "AdBrandName",
    "AdCampaign",
    "Tags",
]

if not CLIENT:
    assert JOB_ENV.lower() == "dev", (
        f"Client must be specified when running in {JOB_ENV}"
    )
    CLIENT = "next_uk"  # Client can be specified for interactive debugging
    logger.warning(f"Client not specified (defaulting to {CLIENT})")

# load configuration
config = config_manager.load_config(JOB_ENV)
logger.info(f"Configuring run for client: {CLIENT}")
with open(PROJECT_ROOT / f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

DATESTART = jobparser.get_typed_arg("--datestart", str)
DATEEND = jobparser.get_typed_arg("--dateend", str)
HISTORY_CELLS_DATE = jobparser.get_arg("--history_cells_from_date")
if HISTORY_CELLS_DATE:
    logger.info(
        f"Use of fixed customer cells from {HISTORY_CELLS_DATE} requested"
    )

tbls = cfg["tables"]["write"]
SCHEMA = config.schema_read
logger.info(f"Read schema set to {SCHEMA} (always read from prod for results)")

# Map write schema to parameterised read table names
tbl_args = {"catalog": config.catalog_read, "schema": SCHEMA, "client": CLIENT}
FIXED_CELLS_LATEST_TABLE = etl.map_tbl(
    tbls["customer_cells_fixed_latest"], **tbl_args
)
FIXED_CELLS_HISTORY_TABLE = etl.map_tbl(
    tbls["customer_cells_fixed_history"], **tbl_args
)
ASSIGNMENTS_TABLE = etl.map_tbl(tbls["assignments"], **tbl_args)
TRANSIENT_CELLS_TABLE = etl.map_tbl(
    tbls["customer_cells_transient"], **tbl_args
)
CONTROL_SHEET_TABLE = etl.map_tbl(tbls["control_sheet"], **tbl_args)
MULTIPAGE_LOCATIONS_TABLE = etl.map_tbl(
    tbls["multipage_locations"], **tbl_args
)

# Get read tables
RPID_WITH_ACCOUNTS = cfg["tables"]["read"]["rpid_with_accounts"]
PREFERENCE_FRAMEWORK = cfg["tables"]["read"]["preference_framework"]
BQ_SESSIONS = cfg["tables"]["read"]["bq_sessions"]
BQ_SESSIONS_APP = cfg["tables"]["read"]["bq_sessions_app"]
BQ_PAGES = cfg["tables"]["read"]["bq_pages"]
BQ_SCREENS = cfg["tables"]["read"]["bq_screens"]

CREDIT_AD_ACCOUNTS_TABLE = cfg["tables"]["read"]["credit_ad_accounts"]

LOCATIONS = cfg["locations"]
FIXED_CELLS = cfg["fixed_cells"]
FALLOW_TRUE = cfg["fallow_control"]["true_label"]
FALLOW_FALSE = cfg["fallow_control"]["false_label"]

VALID_ASSIGNMENT_THRESHOLD = cfg["results_prm"]["valid_assignment_threshold"]
MASID_REFRESH_HOUR = cfg["results_prm"]["masid_refresh_hour"]

WEBHOOK_URL = cfg["webhooks"]["DS Warnings"]

TMP_RESULTS_LOCATION = f"{cfg['dbfs_base_path']}/{JOB_ENV}/tmp"

# LOADING data from results_2
df_sessions_master = spark.read.parquet(
    f"{TMP_RESULTS_LOCATION}/df_sessions_master"
)
df_ad_metadata_non_loc = spark.read.parquet(
    f"{TMP_RESULTS_LOCATION}/df_ad_metadata_non_loc"
)


# Prior to introduction of UniqueAdIDMeasurement (2nd Dec 2025),
# impute UniqueAdIDAssigned as UniqueAdIDMeasurement (Ads customers only)
df_sessions_master = df_sessions_master.withColumn(
    "UniqueAdIDMeasurement",
    F.when(
        (F.col("UniqueAdIDMeasurement").isNull())
        & (F.col("FallowControl") == FALLOW_FALSE)
        & (F.col("SessionDate") < "2025-01-02")
        & (F.col("UniqueAdIDAssigned") != "NoAd"),
        F.col("UniqueAdIDAssigned"),
    ).otherwise(F.col("UniqueAdIDMeasurement")),
)

df_sessions_master_meta = (
    df_sessions_master.join(
        (
            broadcast(df_ad_metadata_non_loc)
            .select(
                "SessionDate",
                "UniqueAdID",
                "AudienceOnly",
                *reporting_metadata_cols,
            )
            # .distinct()
            .withColumnRenamed("UniqueAdID", "UniqueAdIDMeasurement")
        ),
        on=["SessionDate", "UniqueAdIDMeasurement"],
        how="left",
    )
    .fillna({"AudienceOnly": 0})
    .where(
        ~(
            (F.col("UniqueAdIDMeasurement") == F.col("Treatment"))
            & (F.col("AudienceOnly") != 1)
        )
    )
    .withColumn(
        "AlgoDivision_Brand",
        F.concat(F.col("AlgoDivision"), F.lit("_"), F.col("Brand")),
    )
)

# Dropping AudienceOnly column after use avoids downstream schema changes
df_sessions_master_meta = df_sessions_master_meta.drop("AudienceOnly")

# Remove Seasons Ads from App sessions
# This is a live exclusion
# live_exclusions variable used to modify QA checks that these exclusions
# would cause to fail
live_exclusions = True
excl_seasons_ads_app = [
    "P128_C1676_Seasons_Category_Womens_Footwear_Womens",
    "P128_C1625_Seasons_Category_Womens_Bags_Womens",
    "P128_C1626_Seasons_Solus_Womens_Womens",
    "P128_C1627_Seasons_Solus_Mens_Mens",
    "P128_C1662_Seasons_SolusBrand_Veja_Mens",
    "P128_C1662_Seasons_SolusBrand_Veja_Womens",
    "P131_C1626_Seasons_Womens_Womens_Womens",
    "P131_C1625_Seasons_Womens_Bags",
    "P132_C1735_Seasons_Womens_Athleisure_Womens",
    "P132_C1737_Seasons_Womens_NewIn_Womens",
    "P133_C1782_Seasons_Womens_Coach_Womens",
    "P133_C1777_Seasons_Womens_Rixo_Womens",
    "P133_C1781_Seasons_Womens_Ganni_Womens",
    "P133_C1780_Seasons_Womens_MarcJacobs_Womens",
    "P133_C1778_Seasons_Mixed_PoloRalphLauren_Womens",
    "P133_C1779_Seasons_Womens_Missoma_Womens",
    "P133_C1776_Seasons_Womens_Varley_Womens",
]

# Remove Homepage 'switched off' dates
list_hp_remove_dates = [
    "2024-12-12",
    "2024-12-13",
    "2024-12-14",
    "2024-12-15",
    "2024-12-16",
    "2024-12-17",
    "2024-12-18",
    "2024-12-29",
    "2024-12-30",
    "2024-12-31",
    "2025-01-01",
    "2025-01-02",
    "2025-01-30",
    "2025-01-31",
    "2025-02-01",
    "2025-02-02",
    "2025-02-03",
    "2025-11-05",
]
df_sessions_master_meta = df_sessions_master_meta.where(
    ~(
        (F.col("Device") == "App")
        & (F.col("UniqueAdIDMeasurement").isin(excl_seasons_ads_app))
        & (F.col("SessionDate") <= "2025-03-04")
    )
    & ~(
        (F.col("PageGroup") == "HomePage")
        & (F.col("SessionDate").isin(list_hp_remove_dates))
    )
    # Remove Homepage (Desktop only) for affected dates
    & ~(
        (F.col("PageGroup") == "HomePage")
        & (F.col("Device") == "Desktop")
        & (F.col("SessionDate") >= date(2025, 3, 11))
        & (F.col("SessionDate") <= date(2025, 3, 19))
    )
    # Remove Homepage (App only) for affected dates
    & ~(
        (F.col("PageGroup") == "HomePage")
        & (F.col("Device") == "App")
        & (F.col("SessionDate") >= date(2025, 6, 21))
        & (F.col("SessionDate") <= date(2025, 6, 22))
    )
    # Remove Shopping Bag (App only) for affected dates
    & ~(
        (F.col("PageGroup") == "ShoppingBag")
        & (F.col("Device") == "App")
        & (F.col("SessionDate") >= date(2025, 7, 8))
        & (F.col("SessionDate") <= date(2025, 7, 17))
    )
    # Remove Homepage (Desktop only) for affected dates
    & ~(
        (F.col("PageGroup") == "HomePage")
        & (F.col("Device") == "Desktop")
        & (F.col("SessionDate") >= date(2025, 5, 1))
        & (F.col("SessionDate") <= date(2025, 5, 5))
    )
    # Remove OrderComplete (Desktop only) for affected dates
    & ~(
        (F.col("PageGroup") == "OrderComplete")
        & (F.col("Device") == "Desktop")
        & (F.col("SessionDate") >= date(2025, 8, 29))
        & (F.col("SessionDate") <= date(2025, 9, 25))
    )
    # Remove Shopping bag (desktop/ mobile) for affected dates
    & ~(
        (F.col("PageGroup") == "ShoppingBag")
        & (
            F.col("Device").isin(["Desktop", "Mobile"])
            & F.col("PageGroup").isNotNull()
        )
        & (F.col("SessionDate") >= date(2025, 4, 2))
        & (F.col("SessionDate") <= date(2025, 4, 22))
    )
    # Remove Order Complete (desktop, mobile, app) for affected dates
    & ~(
        (F.col("PageGroup") == "OrderComplete")
        & (
            F.col("Device").isin(["Desktop", "Mobile", "App"])
            & F.col("PageGroup").isNotNull()
        )
        & (F.col("SessionDate") >= date(2025, 4, 2))
        & (F.col("SessionDate") <= date(2025, 4, 22))
    )
    # Remove SB2 assignments from results prior to 2025-03-07 as
    # content wasn't live in CMS
    & ~((F.col("Location") == "SB2") & (F.col("SessionDate") < "2025-03-07"))
    # Remove TheSet ads from results as audiance overrides
    # over-ran the lifecycle of these ads in the control sheet.
    & ~(
        (
            F.col("UniqueAdIDMeasurement").isin(
                [
                    "P136_C788_Next_Womens_Multipacks_Womens",
                    "P136_C873_Next_Womens_TheSet_Womens",
                ]
            )
        )
        & (F.col("SessionDate") >= "2025-05-30")
    )
    # Shopping Bag switch off (App only) - Aug 2025
    & ~(
        (F.col("PageGroup") == "ShoppingBag")
        & (F.col("Device") == "App")
        & (F.col("FirstTimestamp") > "2025-08-01 16:00:00")
        & (F.col("FirstTimestamp") < "2025-08-06 10:00:00")
    )
    # Homepage switch off across all devices - Aug 2025
    & ~(
        (F.col("PageGroup") == "HomePage")
        & (F.col("FirstTimestamp") > "2025-08-07 15:00:00")
        & (F.col("FirstTimestamp") < "2025-08-08 14:00:00")
    )
    # Homepage not live yet - Jan/Feb 2026
    & ~(
        (F.col("PageGroup") == "HomePage")
        & (F.col("SessionDate") >= date(2026, 1, 20))
        # Update when we have the end date for this HomePage exclusion
        # & (F.col('FirstTimestamp') <= date(2026, 2, XX))
    )
    # MASID switch off - Aug 2025
    & ~(
        (F.col("SessionDate") >= "2025-08-18")
        & (F.col("SessionDate") <= "2025-08-22")
    )
)

df_sessions_master_meta = df_sessions_master_meta.repartition(
    32, "AccountNumber"
).cache()
logger.info(
    f"df_sessions_master_meta.count(): {df_sessions_master_meta.count()}"
)
df_sessions_master_meta.show(5, truncate=False)
df_sessions_master.unpersist()

# Credit Ad test - account exclusions
df_exclude_credit_accounts = (
    spark.table(CREDIT_AD_ACCOUNTS_TABLE)
    .select("account_number")
    .withColumnRenamed("account_number", "AccountNumber")
)
df_sessions_master_meta = (
    df_sessions_master_meta.join(
        broadcast(df_exclude_credit_accounts.select("AccountNumber")),
        on="AccountNumber",
        how="left",
    )
    .withColumn("_is_credit", F.col("AccountNumber").isNotNull())
    # ← Spark uses the right-side null to flag matched rows
    .where(
        ~F.col("_is_credit")  # not a credit account
        | ~F.col("SessionDate").between(
            "2025-09-16", "2025-11-10"
        )  # or outside range
    )
    .drop("_is_credit")
)
df_sessions_master_meta.show(5, truncate=False)

session_level_cols = ["SessionDate", "Device", "OS"]  # Move to config?
w_apportion = Window.partitionBy(*session_level_cols, "UniqueVisitID")

df_apportionment = (
    df_sessions_master_meta.select(
        "AccountNumber",
        "SessionDate",
        "Device",
        "OS",
        "UniqueVisitID",
        "PagePath",
        "Location",
        "FirstTimestamp",
        "Revenue",
    )
    .withColumn("SessionPortions", F.count("*").over(w_apportion))
    .withColumn(
        "ApportionedRevenue", F.col("Revenue") / F.col("SessionPortions")
    )
    .drop(
        "SessionPortions", "Revenue"
    )  # ← Revenue already on wide df, drop here
    .select(
        "AccountNumber",
        "SessionDate",
        "Device",
        "OS",
        "UniqueVisitID",
        "PagePath",
        "Location",
        "FirstTimestamp",
        "ApportionedRevenue",
    )
)
df_sessions_master_meta = df_sessions_master_meta.join(
    df_apportionment,
    on=[
        "AccountNumber",
        "SessionDate",
        "Device",
        "OS",
        "UniqueVisitID",
        "PagePath",
        "Location",
        "FirstTimestamp",
    ],
    how="left",
)

# # Drop Page and Screen columns as only needed for temporal join
df_sessions_master_meta = df_sessions_master_meta.drop("Page", "Screen")

df_sessions_master_meta = df_sessions_master_meta.cache()
logger.info(
    f"df_sessions_master_meta.count(): {df_sessions_master_meta.count()}"
)
df_sessions_master_meta.show(5, truncate=False)

# Writing out key dataframes to modularise results scripting into:
# - results
# - results_agg

logger.info(
    f"Writing: df_sessions_master_meta to -> "
    f"{TMP_RESULTS_LOCATION}/df_sessions_master_meta"
)
(
    df_sessions_master_meta.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .parquet(f"{TMP_RESULTS_LOCATION}/df_sessions_master_meta")
)

logger.info("Run Complete")

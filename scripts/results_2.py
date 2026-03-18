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

# LOADING data from results_1
df_ad_metadata = spark.read.parquet(f"{TMP_RESULTS_LOCATION}/df_ad_metadata")
df_ad_metadata_non_loc = spark.read.parquet(
    f"{TMP_RESULTS_LOCATION}/df_ad_metadata_non_loc"
)

VALID_ASSIGNMENTS_TMP = f"{TMP_RESULTS_LOCATION}/df_valid_assignments"
df_valid_assignments = spark.read.parquet(VALID_ASSIGNMENTS_TMP)

PAGE_MAPPING_TMP = f"{TMP_RESULTS_LOCATION}/df_page_mapping"
df_page_mapping = spark.read.parquet(PAGE_MAPPING_TMP)

SCREEN_MAPPING_TMP = f"{TMP_RESULTS_LOCATION}/df_screen_mapping"
df_screen_mapping = spark.read.parquet(SCREEN_MAPPING_TMP)

df_fixed_cells = spark.read.parquet(f"{TMP_RESULTS_LOCATION}/df_fixed_cells")

df_sessions_pages_trimmed = spark.read.parquet(
    f"{TMP_RESULTS_LOCATION}/df_sessions_pages_trimmed"
)

logger.info("Data Loading completed")

# before big join, split to PLX vs non-PLX and broadcast the small mapping tables to avoid shuffle explosion — only join on what was actually seen in sessions

df_page_mapping_plx = broadcast(
    df_page_mapping.filter(F.col("Location") == "PLX")
)
df_page_mapping_non_plx = broadcast(
    df_page_mapping.filter(F.col("Location") != "PLX")
)
df_screen_mapping_plx = broadcast(
    df_screen_mapping.filter(F.col("Location") == "PLX")
)
df_screen_mapping_non_plx = broadcast(
    df_screen_mapping.filter(F.col("Location") != "PLX")
)

df_assignments_plx = df_valid_assignments.filter(
    F.col("Location") == "PLX"
).cache()
logger.info(f"df_assignments_plx.count(): {df_assignments_plx.count()}")

df_assignments_non_plx = df_valid_assignments.filter(
    F.col("Location") != "PLX"
).cache()
logger.info(
    f"df_assignments_non_plx.count(): {df_assignments_non_plx.count()}"
)

df_pages_visited = (
    df_sessions_pages_trimmed.select(
        "AccountNumber", "SessionDate", "PagePath"
    )
    .distinct()
    .cache()
)
logger.info(f"df_pages_visited.count(): {df_pages_visited.count()}")

# Visited PLX pages/screens — filter explosion to only what was actually seen
df_va_pages_non_plx = df_assignments_non_plx.join(
    df_page_mapping_non_plx,
    on=["SessionDate", "Location"],
    how="inner",
).join(
    broadcast(df_pages_visited),
    on=["AccountNumber", "SessionDate", "PagePath"],
    how="inner",
)

df_va_pages_plx = df_assignments_plx.join(
    broadcast(df_pages_visited),
    on=["AccountNumber", "SessionDate"],
    how="inner",
).join(
    df_page_mapping_plx,
    on=["SessionDate", "Location", "PagePath"],
    how="inner",
)

VA_PAGES_TMP = f"{TMP_RESULTS_LOCATION}/df_va_pages"
logger.info(f"Writing: df_va_pages to -> {VA_PAGES_TMP}")
df_va_pages_non_plx.unionByName(df_va_pages_plx).write.mode(
    "overwrite"
).parquet(VA_PAGES_TMP)
df_va_pages = spark.read.parquet(VA_PAGES_TMP)
logger.info(f"df_va_pages.count(): {df_va_pages.count()}")

# Same for screens
df_va_screens_non_plx = df_assignments_non_plx.join(
    df_screen_mapping_non_plx,
    on=["SessionDate", "Location"],
    how="inner",
).join(
    broadcast(
        df_pages_visited
    ),
    on=["AccountNumber", "SessionDate", "PagePath"],
    how="inner",
)
df_va_screens_plx = df_assignments_plx.join(
    broadcast(df_pages_visited),
    on=["AccountNumber", "SessionDate"],
    how="inner",
).join(
    df_screen_mapping_plx,
    on=["SessionDate", "Location", "PagePath"],
    how="inner",
)
VA_SCREENS_TMP = f"{TMP_RESULTS_LOCATION}/df_va_screens"
logger.info(f"Writing: df_va_screens to -> {VA_SCREENS_TMP}")
df_va_screens_non_plx.unionByName(df_va_screens_plx).write.mode(
    "overwrite"
).parquet(VA_SCREENS_TMP)
df_va_screens = spark.read.parquet(VA_SCREENS_TMP)
logger.info(f"df_va_screens.count(): {df_va_screens.count()}")

df_assignments_plx.unpersist()
df_assignments_non_plx.unpersist()
df_pages_visited.unpersist()
logger.info(
    "Unpersisted: df_assignments_plx, df_assignments_non_plx, df_pages_visited"
)

df_valid_assignments_mapped = (
    df_va_pages.withColumn("Device", F.lit("Desktop"))
    .unionByName(df_va_pages.withColumn("Device", F.lit("Mobile")))
    .unionByName(df_va_screens.withColumn("Device", F.lit("App")))
)

VALID_ASSIGNMENTS_MAPPED_TMP = (
    f"{TMP_RESULTS_LOCATION}/df_valid_assignments_mapped"
)
logger.info(
    f"Writing: df_valid_assignments_mapped to -> {VALID_ASSIGNMENTS_MAPPED_TMP}"
)
df_valid_assignments_mapped.write.mode("overwrite").parquet(
    VALID_ASSIGNMENTS_MAPPED_TMP
)
df_valid_assignments_mapped = spark.read.parquet(VALID_ASSIGNMENTS_MAPPED_TMP)
logger.info(f"Writing and read complete: df_valid_assignments_mapped")

# No .distinct() needed — deduped at source above
df_valid_assignments_pages = df_va_pages.select(
    "AccountNumber", "SessionDate", "PagePath"
).unionByName(df_va_screens.select("AccountNumber", "SessionDate", "PagePath"))
logger.info(
    f"df_valid_assignments_pages.count(): {df_valid_assignments_pages.count()}"
)
df_valid_assignments_pages.show(5, truncate=False)

df_sessions_ads_valid = df_sessions_pages_trimmed.join(
    df_fixed_cells.select("AccountNumber", "FallowControl"),
    on="AccountNumber",
    how="inner",
).join(
    df_valid_assignments_pages,
    on=["AccountNumber", "SessionDate", "PagePath"],
    how="inner",
)
df_sessions_ads_valid = df_sessions_ads_valid.repartition(
    32, "AccountNumber"
).cache()
logger.info(f"df_sessions_ads_valid.count(): {df_sessions_ads_valid.count()}")
df_sessions_ads_valid.show(5, truncate=False)

# Broadcast the small ad URL lookup (SessionDate × UniqueAdID) to avoid a shuffle
df_ad_url_lookup = broadcast(
    df_ad_metadata.select(
        "SessionDate", "UniqueAdID", "Location", "URL"
    ).withColumnRenamed("UniqueAdID", "UniqueAdIDMeasurement")
)

# Broadcast the small pagegroup lookup (SessionDate × Location) similarly
df_pagegroup_mapping = broadcast(
    df_ad_metadata.select("SessionDate", "Location", "PageGroup")
    .filter(F.col("PageGroup").isNotNull())
    .distinct()
)

df_sessions_master = (
    df_sessions_ads_valid.join(
        df_valid_assignments_mapped,
        on=["AccountNumber", "SessionDate", "Device", "PagePath"],
        how="left",
    )
    .join(
        df_ad_url_lookup,
        on=["SessionDate", "Location", "UniqueAdIDMeasurement"],
        how="left",
    )
    .withColumn(
        "Clicked",
        F.when(
            (F.col("NextPagePath") == F.col("URL"))
            & (F.col("URL").isNotNull()),
            1,
        ).otherwise(0),
    )
    .join(df_pagegroup_mapping, on=["SessionDate", "Location"], how="left")
    .groupBy(
        "AccountNumber",
        "FallowControl",
        "SessionDate",
        "Device",
        "OS",
        "UniqueVisitID",
        "PagePath",
        "PageGroup",
        "Location",
        "UniqueAdIDBasic",
        "UniqueAdIDBest",
        "UniqueAdIDBestChallenger",
        "Treatment",
        "UniqueAdIDAssigned",
        "UniqueAdIDMeasurement",
        "Revenue",
    )
    .agg(
        F.countDistinct("FirstTimestamp").alias("SoftImpressions"),
        F.max("Clicked").alias("SoftClicks"),
        F.min("FirstTimestamp").alias("FirstTimestamp"),
    )
)

logger.info(
    f"Writing: df_sessions_master to -> "
    f"{TMP_RESULTS_LOCATION}/df_sessions_master"
)
(
    df_sessions_master.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .parquet(f"{TMP_RESULTS_LOCATION}/df_sessions_master")
)
logger.info(f"Writing and read complete: df_sessions_master")
# df_sessions_master.show(5, truncate=False)

logger.info("Run Complete")

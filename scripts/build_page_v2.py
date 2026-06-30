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

from pyspark.sql import functions as F
from next_ads.Assignment import assign_random_ads_v2, assign_preranked_ads_v2
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import chain_when_thens, delete_from_and_load, post_to_webhook
from dsutils.argparser import get_job_parser
from next_ads.utils import config_manager
from next_ads.common.paths import load_client_config
from next_ads.utils import etl


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client")
LOG_LEVEL = jobparser.get_arg("--log_level")
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
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
logger.info(f"Configuring run for client: {CLIENT}")
cfg = load_client_config(CLIENT)

PAGE_TYPE = jobparser.get_arg("--page_type")
if not PAGE_TYPE:
    assert JOB_ENV.lower() == "dev", (
        f"page_type must be specified when running in {JOB_ENV}"
    )
    PAGE_TYPE = "sb"  # Page type can be specified for interactive debugging
    logger.warning(f"page_type not specified (defaulting to {PAGE_TYPE})")

PAGE_TYPES = cfg["page_types"]


tbls = cfg["tables"]["write"]
SCHEMA = config.schema_write
logger.info(f"Write schema set to {SCHEMA}")

# Map write schema to parameterised write table names
tbl_args = {
    "catalog": config.catalog_write,
    "schema": SCHEMA,
    "client": CLIENT,
}
CONTROL_SHEET_LATEST = config.tables_write.control_sheet_latest_v2
ASSIGNMENTS_TABLE_V2 = etl.map_tbl(tbls["assignments_v2"], **tbl_args)
ASSIGNMENTS_TABLE_V2_LATEST = etl.map_tbl(
    tbls["assignments_v2_latest"], **tbl_args
)
CELLS_TABLE_LATEST = etl.map_tbl(tbls["customer_cells_latest"], **tbl_args)
PRERANKED_THEMES_TABLE = etl.map_tbl(
    tbls["preranked_ads_from_themes_v2_latest"], **tbl_args
)

# Read results data from prod schema dataset
tbl_args_results = {
    "catalog": config.catalog_read,
    "schema": config.schema_read,
    "client": CLIENT,
}
AD_RESULTS_TABLE = etl.map_tbl(tbls["results_ads"], **tbl_args_results)

FALLOW_TRUE_LABEL = cfg["fallow_control"]["true_label"]

WEBHOOK_URL = cfg["webhooks"]["DS Warnings"]

try:
    CELL_MAP = PAGE_TYPES[PAGE_TYPE]
except KeyError as ke:
    loc_key_msg = f"{PAGE_TYPE} build requested but not in config"
    logger.warning(loc_key_msg)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, loc_key_msg)
    raise ke

logger.info(f"Assigning Ads for Page Type: {PAGE_TYPE}")

logger.info("Getting Ads")
df_ads = (
    spark.table(CONTROL_SHEET_LATEST)
    .where(F.col("PageType") == PAGE_TYPE)
    .select(
        "UniqueAdID",
        "UniqueAdIDPremium",
        "AlgoDivision",
        "TargetingCriteria",
        "AudienceOnly",
        "Tags",
        "Themes",
    )
)

df_ads_tgt = df_ads.fillna(0, subset=["AudienceOnly"]).where(
    (F.col("AudienceOnly") != 1)
)

# Create subset of ads for Best
df_ads_tgt_best = df_ads_tgt.where(F.col("Themes").isNotNull()).where(
    F.col("Themes") != ""
)

# Drop unneeded columns following processing dataframe
ads_required_cols = [
    "UniqueAdID",
    "UniqueAdIDPremium",
    "AlgoDivision",
    "TargetingCriteria",
]
df_ads = df_ads.select(ads_required_cols)
df_ads_tgt = df_ads_tgt.select(ads_required_cols)
df_ads_tgt_best = df_ads_tgt_best.select(ads_required_cols)

if df_ads_tgt.count() == 0:
    no_ads_msg = f"No ads found for Page Type: {PAGE_TYPE}"
    logger.warning(no_ads_msg)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, no_ads_msg)
    logger.info("Skipping assignment")
    raise SystemExit(0)

logger.info("Getting customer cell assignments")
df_cells = spark.table(CELLS_TABLE_LATEST).drop("rundate")
df_cells.cache()

logger.info("Assigning Ads with Basic Targeting")

basic_within = PAGE_TYPES[PAGE_TYPE]["basic_within"]


df_assigned_basic = assign_random_ads_v2(
    df_ads_tgt.select("UniqueAdID", basic_within),
    df_cells.select("AccountNumber", basic_within),
    grp_col=basic_within,
)

# Basic/random selection does not use the model score, but V2 JSON can carry
# the score the assigned customer x ad pair has in the model-ranked table.
df_trigger_scores = (
    spark.table(PRERANKED_THEMES_TABLE)
    .where(F.col("PageType") == PAGE_TYPE)
    .select(
        "AccountNumber",
        "UniqueAdID",
        F.col("TriggerScore").alias("TriggerScoreLookup"),
    )
)

df_assigned_basic = df_assigned_basic.join(
    df_trigger_scores, on=["AccountNumber", "UniqueAdID"], how="left"
).withColumnRenamed("TriggerScoreLookup", "TriggerScore")

df_assigned_basic.cache()

logger.info("Assigning Ads with Best Targeting")

df_assigned_best = assign_preranked_ads_v2(
    df_ads=df_ads_tgt_best,
    preranked_ads_table=PRERANKED_THEMES_TABLE,
    page_type=PAGE_TYPE,
    df_cust=df_cells,
)
df_assigned_best.cache()

df_assigned_best_challenger = df_assigned_best

logger.info("Determining Ad to show based on assignments and fixed cells")
# Build a rank spine from the union of all [AccountNumber, Rank] pairs
# across basic and best. This ensures we keep the maximum rank coverage
# per customer — e.g. if basic has 2 rows but best has 20, we retain all
# 20 rows (with nulls for UniqueAdIDBasic at ranks 3-20).
df_rank_spine = (
    df_assigned_basic.select("AccountNumber", "Rank")
    .unionByName(df_assigned_best.select("AccountNumber", "Rank"))
    .unionByName(df_assigned_best_challenger.select("AccountNumber", "Rank"))
    .distinct()
)

df_assignments = (
    df_rank_spine.join(
        df_assigned_basic.select(
            "AccountNumber",
            F.col("UniqueAdID").alias("UniqueAdIDBasic"),
            F.col("TriggerScore").alias("TriggerScoreBasic"),
            "Rank",
        ),
        on=["AccountNumber", "Rank"],
        how="left",
    )
    .join(
        df_assigned_best.select(
            "AccountNumber",
            F.col("UniqueAdID").alias("UniqueAdIDBest"),
            F.col("TriggerScore").alias("TriggerScoreBest"),
            "Rank",
        ),
        on=["AccountNumber", "Rank"],
        how="left",
    )
    .join(
        df_assigned_best_challenger.select(
            "AccountNumber",
            F.col("UniqueAdID").alias("UniqueAdIDBestChallenger"),
            F.col("TriggerScore").alias("TriggerScoreBestChallenger"),
            "Rank",
        ),
        on=["AccountNumber", "Rank"],
        how="left",
    )
    .join(
        df_cells.withColumn("AdSuppressed", F.lit("AdSuppressed")),
        on="AccountNumber",
        how="left",
    )
)
df_assignments.cache()

# chain_when_thens selects UniqueAdIDBasic or UniqueAdIDBest per row
# based on the customer's cell — no change to the config map or function
# needed since the column names and cell columns are unchanged.
df_ad_assigned = (
    df_assignments.withColumn(
        "UniqueAdIDMeasurement", chain_when_thens(CELL_MAP["map"])
    )
    .join(
        (
            df_ads.select("UniqueAdID", "UniqueAdIDPremium").withColumnRenamed(
                "UniqueAdID", "UniqueAdIDMeasurement"
            )
        ),
        on="UniqueAdIDMeasurement",
        how="left",
    )
    .withColumn(
        "UniqueAdIDMeasurement",
        F.when(
            (
                (F.col("IsPremium") == 1)
                & (F.col("UniqueAdIDPremium").isNotNull())
            ),
            F.col("UniqueAdIDPremium"),
        ).otherwise(F.col("UniqueAdIDMeasurement")),
    )
    .fillna("NoAdFound", subset=["UniqueAdIDMeasurement"])
    .withColumn(
        "UniqueAdIDAssigned",
        F.when(
            F.col("FallowControl") == FALLOW_TRUE_LABEL, F.lit("NoAd")
        ).otherwise(F.col("UniqueAdIDMeasurement")),
    )
)

# Treatment is a function of the customer's cell only (not rank), so it is
# constant across all rank rows for a customer. Include Rank in the select
# so the join back onto df_ad_assigned is on [AccountNumber, Rank] and
# avoids any fan-out.
df_ad_treatments = (
    df_assignments.drop(
        "AdSuppressed",
        "UniqueAdIDBasic",
        "UniqueAdIDBest",
        "UniqueAdIDBestChallenger",
    )
    .withColumns(
        {
            "AdSuppressed": F.lit("AdSuppressed"),
            "UniqueAdIDBasic": F.lit("Basic"),
            "UniqueAdIDBest": F.lit("Best"),
            "UniqueAdIDBestChallenger": F.lit("BestChallenger"),
        }
    )
    .withColumn("Treatment", chain_when_thens(CELL_MAP["map"]))
    .select("AccountNumber", "Rank", "Treatment")
)

df_ad_assigned = df_ad_assigned.join(
    df_ad_treatments, on=["AccountNumber", "Rank"], how="left"
).withColumn(
    "Treatment",
    F.when(
        ((F.col("IsPremium") == 1) & (F.col("UniqueAdIDPremium").isNotNull())),
        F.concat(F.col("Treatment"), F.lit("Prem")),
    ).otherwise(F.col("Treatment")),
)

df_ad_assigned = df_ad_assigned.withColumn(
    "TriggerScore",
    F.when(
        F.col("Treatment").isin("Best", "BestPrem"), F.col("TriggerScoreBest")
    )
    .when(
        F.col("Treatment").isin("BestChallenger", "BestChallengerPrem"),
        F.col("TriggerScoreBestChallenger"),
    )
    .when(
        F.col("Treatment").isin("Basic", "BasicPrem"),
        F.col("TriggerScoreBasic"),
    )
    .when(
        # Suppressed rows have no served ad, so use the model-selected
        # Best score as the counterfactual score that would have applied.
        F.col("Treatment") == "AdSuppressed",
        F.col("TriggerScoreBest"),
    )
    .otherwise(F.lit(None).cast("float")),
).withColumn(
    "TriggerScore",
    F.when(
        # Fallow-control rows keep the score of the ad that would have
        # been selected; NoAdFound has no customer x ad score to carry.
        F.col("UniqueAdIDAssigned") == "NoAdFound",
        F.lit(None).cast("float"),
    ).otherwise(F.col("TriggerScore")),
)


# Check and warn if null Treatments exist
n_null_treatment = df_ad_assigned.where(F.col("Treatment").isNull()).count()
if n_null_treatment > 0:
    null_treatment_msg = (
        f"{n_null_treatment:,} accounts removed during "
        + f"assignment of {PAGE_TYPE} due to null Treatment"
    )
    logger.warning(null_treatment_msg)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, null_treatment_msg)
    df_ad_assigned = df_ad_assigned.where(F.col("Treatment").isNotNull())

# Check and warn if UniqueAdIDMeasurement is null
n_null_measure = (
    df_ad_assigned.where(F.col("UniqueAdIDMeasurement").isNull())
).count()
if n_null_measure > 0:
    null_measure_msg = (
        f"{n_null_measure:,} assignments removed during "
        + f"assignment of {PAGE_TYPE} due to null "
        + "UniqueAdIDMeasurement"
    )
    logger.warning(null_measure_msg)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, null_measure_msg)
    df_ad_assigned = df_ad_assigned.where(
        F.col("UniqueAdIDMeasurement").isNotNull()
    )


df_ad_assigned = df_ad_assigned.withColumn(
    "PageType", F.lit(PAGE_TYPE)
).select(
    "AccountNumber",
    "PageType",
    "Rank",
    "UniqueAdIDBasic",
    "UniqueAdIDBest",
    "UniqueAdIDBestChallenger",
    "Treatment",
    "UniqueAdIDMeasurement",
    "UniqueAdIDAssigned",
    "TriggerScore",
)

logger.info(f"Loading assignments to {ASSIGNMENTS_TABLE_V2}")
delete_from_and_load(
    df_ad_assigned,
    ASSIGNMENTS_TABLE_V2,
    pk_cols=["AccountNumber", "PageType", "Rank"],
    del_where={"rundate": "current_date()", "PageType": f"'{PAGE_TYPE}'"},
)

logger.info(f"Loading assignments to {ASSIGNMENTS_TABLE_V2_LATEST}")
delete_from_and_load(
    df_ad_assigned,
    ASSIGNMENTS_TABLE_V2_LATEST,
    pk_cols=["AccountNumber", "PageType", "Rank"],
    del_where={"PageType": f"'{PAGE_TYPE}'"},
)

df_cells.unpersist()
df_assigned_basic.unpersist()
df_assigned_best.unpersist()
df_assigned_best_challenger.unpersist()
df_assignments.unpersist()

logger.info("Run complete")

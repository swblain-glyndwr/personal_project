import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    from dsutils.dbc import get_dbutils

    dbutils = get_dbutils()
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
import pyspark.sql.functions as F
from datetime import date, timedelta
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (
    assert_pk,
    truncate_and_load,
    delete_from_and_load,
    post_to_webhook,
)
from dsutils.argparser import get_job_parser
import dsutils.gcp as gcp
from next_ads.Scoring import append_targeting_criteria
from next_ads.utils import config_manager
from next_ads.data_validation import schemas


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

logger.info(f"Configuring run for client: {CLIENT}")
with open(PROJECT_ROOT / f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

# load configuration
config = config_manager.load_config(JOB_ENV)

LOCATIONS = cfg["locations"]
VALID_LOCATIONS = list(LOCATIONS.keys())
READ_LOCATIONS = list()
INHERITED_LOCATIONS = dict()
for k in VALID_LOCATIONS:
    if "inherit_ads_from" in LOCATIONS[k]:
        INHERITED_LOCATIONS[k] = LOCATIONS[k]["inherit_ads_from"]
    else:
        READ_LOCATIONS.append(k)

CONTROL_SHEET = cfg["control_sheet"]
PLACEMENTS_SHEET = cfg["placements_sheet"]
PLX_URLS_SHEET = cfg["plx_urls_sheet"]

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f"Write schema set to {SCHEMA}")

# Map write schema to parameterised write table names
TARGET_TABLE = config.tables_write.control_sheet
TARGET_TABLE_LATEST = config.tables_write.control_sheet_latest
TARGET_MPL_TABLE = config.tables_write.multipage_locations
TARGET_MPL_TABLE_LATEST = config.tables_write.multipage_locations_latest

WEBHOOK_URL = cfg["webhooks"]["Input Warnings"]

logger.info(f"Valid locations: {' '.join(VALID_LOCATIONS)}")
logger.info(f"Locations to read: {' '.join(READ_LOCATIONS)}")
logger.info(f"Locations with inherited ads: {INHERITED_LOCATIONS}")

for v in READ_LOCATIONS:
    CONTROL_SHEET["read_schema"].append([v, "string", "null"])

# log all params
logger.info(
    f"Configuration - "
    f"ENV: {JOB_ENV}, "
    f"SCHEMA: {SCHEMA}, "
    f"CLIENT: {CLIENT}, "
    f"TARGET_TABLE: {TARGET_TABLE}, "
    f"TARGET_TABLE_LATEST: {TARGET_TABLE_LATEST}, "
    f"TARGET_MPL_TABLE: {TARGET_MPL_TABLE}, "
    f"TARGET_MPL_TABLE_LATEST: {TARGET_MPL_TABLE_LATEST}, "
)

logger.info("Reading Control Sheet from Google Sheets")

df_ctrl_raw = gcp.spark_df_from_sheets(
    url=CONTROL_SHEET["url"],
    worksheet_name=CONTROL_SHEET["sheet"],
    gcp_scope=cfg["gcp"]["scope"],
    gcp_key=cfg["gcp"]["key"],
    schema=CONTROL_SHEET["read_schema"],
)

logger.info("Reading Placements Sheet from Google Sheets")

df_placements = gcp.spark_df_from_sheets(
    url=PLACEMENTS_SHEET["url"],
    worksheet_name=PLACEMENTS_SHEET["sheet"],
    gcp_scope=cfg["gcp"]["scope"],
    gcp_key=cfg["gcp"]["key"],
    schema=PLACEMENTS_SHEET["read_schema"],
)

logger.info("Reading PLX URLs Sheet from Google Sheets")

try:
    df_plx_urls = gcp.spark_df_from_sheets(
        url=PLX_URLS_SHEET["url"],
        worksheet_name=PLX_URLS_SHEET["sheet"],
        gcp_scope=cfg["gcp"]["scope"],
        gcp_key=cfg["gcp"]["key"],
        schema=PLX_URLS_SHEET["read_schema"],
    )
except Exception as e:
    df_plx_urls = None
    plx_load_msg = "Error loading PLX URLs sheet - URLs not refreshed"
    logger.warning(plx_load_msg)
    logger.error(e)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, plx_load_msg)

df_ctrl_raw_filtered = df_ctrl_raw.filter(df_ctrl_raw.UniqueAdID != "")

delete_from_and_load(
    df=df_ctrl_raw_filtered,
    table=config.tables_write.control_sheet_raw,
    pk_cols=["Realm", "Territory", "UniqueAdID"],
    del_where={"rundate": "current_date()"},
)

logger.info(f"Writing Control Sheet to {config.tables_write.control_sheet_raw_latest}")
truncate_and_load(
    df=df_ctrl_raw_filtered,
    table=config.tables_write.control_sheet_raw_latest,
    pk_cols=["Realm", "Territory", "UniqueAdID"],
)

logger.info(f"Writing Placements Sheet to {config.tables_write.control_sheet_plp_raw}")
delete_from_and_load(
    df=df_placements,
    table=config.tables_write.control_sheet_plp_raw,
    pk_cols=["Location"],
    del_where={"rundate": "current_date()"},
)

logger.info(
    f"Writing Placements Sheet to {config.tables_write.control_sheet_plp_raw_latest}"
)
truncate_and_load(
    df=df_placements,
    table=config.tables_write.control_sheet_plp_raw_latest,
    pk_cols=["Location"],
)

if df_plx_urls:
    try:
        logger.info("Updating PLX URLs in multipage locations table")
        df_multipage_locs = (
            df_plx_urls.withColumnRenamed("URL", "Page")
            .withColumn("Screen", F.lit("PLP"))
            .withColumn("Location", F.lit("PLX"))
            .select("Location", "Page", "Screen")
            .drop_duplicates()
        )
        logger.info("Loading multipage locations to table")
        delete_from_and_load(
            df_multipage_locs,
            TARGET_MPL_TABLE,
            pk_cols=["Location", "Page", "Screen"],
            del_where={"rundate": "current_date()"},
        )

        logger.info("Loading multipage locations to table (latest)")
        truncate_and_load(
            df_multipage_locs,
            TARGET_MPL_TABLE_LATEST,
            pk_cols=["Location", "Page", "Screen"],
        )

    except Exception as e:
        plx_write_msg = "Error writing to multipage locations table; URLs not refreshed"  # noqa
        logger.warning(plx_write_msg)
        logger.error(e)
        if JOB_ENV == "prod":
            post_to_webhook(WEBHOOK_URL, plx_write_msg)

### Data Validation
# NOTE: soft validation (no assert)
logger.info("Validating Control Sheet data schema")
df_ctrl_raw_filtered = df_ctrl_raw.filter(df_ctrl_raw.UniqueAdID != "").filter(
    df_ctrl_raw.CMSPageID != ""
)

df_ctrl_raw_filtered = schemas.ControlSheetInputModel.validate(
    df_ctrl_raw_filtered, lazy=True
)
errors_json = json.dumps(
    dict(df_ctrl_raw_filtered.pandera.errors),
    indent=2,
)
logger.info(f"Data validation errors: {errors_json}")

logger.info("Validating Control Sheet PLP data schema")
df_placements = schemas.ControlSheetPlacementsInputModel.validate(
    df_placements, lazy=True
)
errors_json = json.dumps(
    dict(df_placements.pandera.errors),
    indent=2,
)
logger.info(f"Data validation errors: {errors_json}")

logger.info("Validating Control Sheet PLX data schema")
df_plx_urls = schemas.ControlSheetPLXInputModel.validate(df_plx_urls, lazy=True)
errors_json = json.dumps(
    dict(df_plx_urls.pandera.errors),
    indent=2,
)
logger.info(f"Data validation errors: {errors_json}")

date_fmt = CONTROL_SHEET["date_format"]
date_regex = CONTROL_SHEET["date_regex"]

logger.info("Stripping empty UniqueAdID entries")
df_ctrl_not_empty = df_ctrl_raw.where(F.col("UniqueAdID") != "")
df_ctrl_valid_date_fmt = df_ctrl_not_empty.where(
    (F.col("StartDate").rlike(date_regex)) & (F.col("EndDate").rlike(date_regex))
)
df_ctrl_valid_date_fmt.count()

df_invalid_date_ads = df_ctrl_not_empty.join(
    df_ctrl_valid_date_fmt, on="UniqueAdID", how="leftanti"
).select("UniqueAdID")
invalid_date_ads = [x[0] for x in df_invalid_date_ads.collect()]

if len(invalid_date_ads) > 0:
    date_fmt_msg = (
        "Start or End date of the following ads was invlaid\n"
        + "\n".join(invalid_date_ads)
        + f"\nDate must be entered in the format: {date_fmt}"
    )
    logger.warning(date_fmt_msg)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, date_fmt_msg)

logger.info("Getting active status of ads based on StartDate and EndDate")
date_tomorrow = date.today() + timedelta(days=1)
df_ctrl_active = (
    df_ctrl_valid_date_fmt.drop("Status")
    .withColumn("StartDate", F.to_date(F.col("StartDate"), date_fmt))
    .withColumn("EndDate", F.to_date(F.col("EndDate"), date_fmt))
    .where(F.col("StartDate") <= date_tomorrow)
    .where(F.col("EndDate") >= date_tomorrow)
)
logger.info(f"Active Ads: {df_ctrl_active.count():,}")

# Legacy coercion of item codes to upper case and replace("-","")
df_ctrl_active = df_ctrl_active.withColumn(
    "Items", F.regexp_replace(F.upper(F.col("Items")), "-", "")
)

df_ctrl_active = (
    df_ctrl_active.withColumn(
        "AudienceOnlyInt", F.when(F.col("AudienceOnly") == "TRUE", 1).otherwise(0)
    )
    .drop("AudienceOnly")
    .withColumnRenamed("AudienceOnlyInt", "AudienceOnly")
)

if INHERITED_LOCATIONS:
    for k in INHERITED_LOCATIONS:
        df_ctrl_active = df_ctrl_active.withColumn(
            k, F.col(LOCATIONS[k]["inherit_ads_from"])
        )

df_id_loc = (
    df_ctrl_active.unpivot(
        ids="UniqueAdID",
        values=VALID_LOCATIONS,
        variableColumnName="Location",
        valueColumnName="Requested",
    )
    .where(F.col("Requested") == "TRUE")
    .drop_duplicates()
    .drop("Requested")
)

active_locs = set([row[0] for row in df_id_loc.select("Location").collect()])
logger.info(f"Active Locations: {len(active_locs):,} {sorted(active_locs)}")
logger.info(f"Active Ad-Locations: {df_id_loc.count():,}")


df_ad_attributes = (
    df_ctrl_active.drop(*VALID_LOCATIONS).drop_duplicates().replace("", None)
)

df_processed = df_id_loc.join(df_ad_attributes, on="UniqueAdID", how="left")


# join on placements sheet to collect Page and Screen
df_processed = df_processed.join(df_placements, on="Location", how="left")
df_processed = df_processed.fillna({"ModelCombination": "and"}).withColumn(
    "ModelCombination",
    F.when(F.col("Models").isNull(), F.lit(None)).otherwise(F.col("ModelCombination")),
)
df_processed = append_targeting_criteria(df_processed)


# Ensure UniqueAdIDPremium is only present on locations in sibling ad
logger.info("Constraining Premium Ads to Only Show on Sibling Locations")
location_lookup_df = (
    df_processed.groupBy("UniqueAdID")
    .agg(F.collect_set("Location").alias("ValidLocations"))
    .withColumnRenamed("UniqueAdID", "LookupAdID")
)
df_processed = df_processed.join(
    location_lookup_df,
    df_processed["UniqueAdIDPremium"] == location_lookup_df["LookupAdID"],
    "left",
)

df_processed = df_processed.withColumn(
    "UniqueAdIDPremium",
    F.when(
        (F.col("UniqueAdIDPremium").isNotNull())
        & (
            F.col("ValidLocations").isNull()
            | ~F.array_contains(F.col("ValidLocations"), F.col("Location"))
        ),
        F.lit(None),
    ).otherwise(F.col("UniqueAdIDPremium")),
).drop("LookupAdID", "ValidLocations")

logger.info("Checking input Primary Key")
assert_pk(df_processed, ["UniqueAdID", "Location"])


df_dup_masids = (
    df_processed.groupBy("AlgoDivision", "Location", "MASIDToken")
    .agg(F.countDistinct("UniqueAdID").alias("AdsPerMASID"))
    .where(F.col("AdsPerMASID") > 1)
)
if df_dup_masids.count() > 1:
    dup_masid_list = list(
        set([row[0] for row in (df_dup_masids.select("MASIDToken").collect())])
    )

    warn_dup_masid = (
        "Duplicate MASID suffixes assigned to Ads"
        + f" in same AlgoDivision: {dup_masid_list}"
    )
    logger.warning(warn_dup_masid)

    for m in dup_masid_list:
        res_conflict = f"Resolving conflict for MASID suffix: {m}"
        logger.info(res_conflict)
        warn_dup_masid += "\n\n" + res_conflict

        df_dups_m = (
            df_processed.where(F.col("MASIDToken") == m).select("UniqueAdID")
        ).collect()

        clashing_ids = list(set([row[0] for row in df_dups_m]))
        clashing_ids.sort()  # Sort alphabetically as proxy for latest
        try:
            keep_ad = f"Keeping ad: {clashing_ids[-1]}"
            logger.info(keep_ad)
            warn_dup_masid += "\n" + keep_ad

            ids_to_del = clashing_ids[:-1]

            for id_del in ids_to_del:
                drop_ad = f"Dropping conflicting ad: {id_del}"
                logger.warning(drop_ad)
                warn_dup_masid += "\n" + drop_ad

                df_processed = df_processed.where(F.col("UniqueAdID") != id_del)
        except IndexError as e:
            logger.error(f"Error resolving MASID conflict: {e}")
            logger.warning(f"Unable to resolve conflict for suffix: {m}")
            logger.warning(f"Removing all ads associated with suffix: {m}")
            issue_ad = (
                "Issue resolving conflict for ads with MASID suffix:"
                + f" {m} - all {m} ads removed"
            )
            warn_dup_masid += "\n" + issue_ad
            df_processed = df_processed.where(F.col("MASIDToken") != m)

    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, warn_dup_masid)

logger.info("Cleaning theme strings (lowercase, strip whitespace)")
df_processed = df_processed.withColumn(
    "Themes",
    F.when(F.col("Themes").isNotNull(), F.trim(F.lower(F.col("Themes")))).otherwise(
        F.col("Themes")
    ),
)

df_valid_ad_ids = df_processed.select(F.col("UniqueAdID").alias("valid_id")).distinct()

df_processed = df_processed.join(
    df_valid_ad_ids, F.col("UniqueAdIDPremium") == F.col("valid_id"), "left_outer"
)

df_processed = df_processed.withColumn(
    "UniqueAdIDPremium",
    F.when(F.col("valid_id").isNull(), F.lit(None)).otherwise(
        F.col("UniqueAdIDPremium")
    ),
)

target_cols = (spark.table(TARGET_TABLE).drop("rundate")).columns


if set(target_cols) == set(df_processed.columns):
    logger.info("Control Sheet columns match Target table columns")
elif set(target_cols).issubset(set(df_processed.columns)):
    logger.warning("Target table cols are subset of Control Sheet cols")
    extra_cols = set(df_processed.columns).difference(set(target_cols))
    logger.warning("Dropping superfluous columns: %s", ", ".join(extra_cols))
    df_processed = df_processed.drop(*list(extra_cols))
else:
    raise Exception("Target table cols not a subset of Control Sheet cols")


logger.info("Loading output to table")
delete_from_and_load(
    df_processed.select(*target_cols),
    TARGET_TABLE,
    pk_cols=["UniqueAdID", "Location"],
    del_where={"rundate": "current_date()"},
)

logger.info("Loading output to table (latest)")
truncate_and_load(
    df_processed.select(*target_cols),
    TARGET_TABLE_LATEST,
    pk_cols=["UniqueAdID", "Location"],
)

logger.info("Run complete")

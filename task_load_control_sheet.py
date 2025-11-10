import json
import pyspark.sql.functions as F
from datetime import date, timedelta
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (assert_pk,
                         truncate_and_load,
                         delete_from_and_load,
                         map_tbl,
                         post_to_webhook)
from dsutils.argparser import get_job_parser
import dsutils.gcp as gcp
from next_ads.Scoring import append_targeting_criteria


jobparser = get_job_parser()
jobparser._parse_args()
JOBNAME = jobparser.get_arg('--jobname')
JOB_ENV = jobparser.get_arg('--job_env')
CLIENT = jobparser.get_arg('--client')
LOG_LEVEL = jobparser.get_arg('--log_level')
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert not JOBNAME, 'Client must be specified when running as a job'
    CLIENT = 'next_uk'  # Client can be specified for interactive debugging
    logger.warning(f'Client not specified (defaulting to {CLIENT})')

logger.info(f"Configuring run for client: {CLIENT}")
with open(f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

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

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

# Map write schema to parameterised write table names
tbl_args = {'schema': SCHEMA, 'client': CLIENT}
TARGET_TABLE = map_tbl(tbls["control_sheet"], **tbl_args)
TARGET_TABLE_LATEST = map_tbl(tbls["control_sheet_latest"], **tbl_args)

WEBHOOK_URL = cfg["webhooks"]["Input Warnings"]

logger.info(f"Valid locations: {' '.join(VALID_LOCATIONS)}")
logger.info(f"Locations to read: {' '.join(READ_LOCATIONS)}")
logger.info(f"Locations with inherited ads: {INHERITED_LOCATIONS}")

for v in READ_LOCATIONS:
    CONTROL_SHEET["read_schema"].append([v, "string", "null"])


logger.info("Reading Control Sheet from Google Sheets")

df_ctrl_raw = gcp.spark_df_from_sheets(
    url=CONTROL_SHEET["url"],
    worksheet_name=CONTROL_SHEET["sheet"],
    gcp_scope=cfg["gcp"]["scope"],
    gcp_key=cfg["gcp"]["key"],
    schema=CONTROL_SHEET["read_schema"]
    )

logger.info("Reading Placements Sheet from Google Sheets")

df_placements = gcp.spark_df_from_sheets(
    url=PLACEMENTS_SHEET["url"],
    worksheet_name=PLACEMENTS_SHEET["sheet"],
    gcp_scope=cfg["gcp"]["scope"],
    gcp_key=cfg["gcp"]["key"],
    schema=PLACEMENTS_SHEET["read_schema"]
    )

date_fmt = CONTROL_SHEET["date_format"]
date_regex = CONTROL_SHEET["date_regex"]

logger.info("Stripping empty UniqueAdID entries")
df_ctrl_not_empty = df_ctrl_raw.where(F.col("UniqueAdID") != "")
df_ctrl_valid_date_fmt = (
    df_ctrl_not_empty
    .where(
        (F.col("StartDate").rlike(date_regex))
        & (F.col("EndDate").rlike(date_regex))
        )
)
df_ctrl_valid_date_fmt.count()

df_invalid_date_ads = (
    df_ctrl_not_empty
    .join(df_ctrl_valid_date_fmt,
          on="UniqueAdID",
          how="leftanti")
    .select("UniqueAdID")
)
invalid_date_ads = [x[0] for x in df_invalid_date_ads.collect()]

if len(invalid_date_ads) > 0:
    date_fmt_msg = (
        "Start or End date of the following ads was invlaid\n" +
        "\n".join(invalid_date_ads) +
        f"\nDate must be entered in the format: {date_fmt}"
    )
    logger.warning(date_fmt_msg)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, date_fmt_msg)

logger.info("Getting active status of ads based on StartDate and EndDate")
date_tomorrow = date.today() + timedelta(days=1)
df_ctrl_active = (
    df_ctrl_valid_date_fmt
    .drop("Status")
    .withColumn("StartDate", F.to_date(F.col("StartDate"), date_fmt))
    .withColumn("EndDate", F.to_date(F.col("EndDate"), date_fmt))
    .where(F.col("StartDate") <= date_tomorrow)
    .where(F.col("EndDate") >= date_tomorrow)
)
logger.info(f"Active Ads: {df_ctrl_active.count():,}")

# Legacy coercion of item codes to upper case and replace("-","")
df_ctrl_active = df_ctrl_active.withColumn(
    "Items",
    F.regexp_replace(F.upper(F.col("Items")), "-", "")
)

df_ctrl_active = (
    df_ctrl_active
    .withColumn('AudienceOnlyInt',
                F.when(F.col('AudienceOnly') == 'TRUE', 1).otherwise(0))
    .drop('AudienceOnly')
    .withColumnRenamed('AudienceOnlyInt', 'AudienceOnly')
)

if INHERITED_LOCATIONS:
    for k in INHERITED_LOCATIONS:
        df_ctrl_active = (
            df_ctrl_active
            .withColumn(k, F.col(LOCATIONS[k]["inherit_ads_from"]))
        )

df_id_loc = (
    df_ctrl_active
    .unpivot(ids="UniqueAdID",
             values=VALID_LOCATIONS,
             variableColumnName="Location",
             valueColumnName="Requested")
    .where(F.col("Requested") == "TRUE")
    .drop_duplicates()
    .drop("Requested")
)

active_locs = set([row[0] for row in df_id_loc.select('Location').collect()])
logger.info(f"Active Locations: {len(active_locs):,} {sorted(active_locs)}")
logger.info(f"Active Ad-Locations: {df_id_loc.count():,}")


df_ad_attributes = (
    df_ctrl_active
    .drop(*VALID_LOCATIONS)
    .drop_duplicates()
    .replace("", None)
)

df_processed = (
    df_id_loc
    .join(df_ad_attributes, on="UniqueAdID", how="left")
)


# join on placements sheet to collect Page and Screen
df_processed = (
    df_processed
    .join(df_placements, on="Location", how="left")
)
df_processed = (
    df_processed
    .fillna({"ModelCombination": "and"})
    .withColumn("ModelCombination",
                F.when(F.col("Models").isNull(),
                       F.lit(None)).otherwise(F.col("ModelCombination")))
)
df_processed = append_targeting_criteria(df_processed)


# Ensure UniqueAdIDPremium is only present on locations in sibling ad
logger.info("Constraining Premium Ads to Only Show on Sibling Locations")
location_lookup_df = df_processed.groupBy("UniqueAdID") \
                    .agg(F.collect_set("Location").alias("ValidLocations")) \
                    .withColumnRenamed("UniqueAdID", "LookupAdID")
df_processed = df_processed.join(
    location_lookup_df,
    df_processed["UniqueAdIDPremium"] == location_lookup_df["LookupAdID"],
    "left"
)

df_processed = df_processed.withColumn(
    "UniqueAdIDPremium",
    F.when(
        (F.col("UniqueAdIDPremium").isNotNull()) &
        (
            F.col("ValidLocations").isNull() |
            ~F.array_contains(F.col("ValidLocations"), F.col("Location"))
        ),
        F.lit(None)
    ).otherwise(F.col("UniqueAdIDPremium"))
).drop("LookupAdID", "ValidLocations")

logger.info("Checking input Primary Key")
assert_pk(df_processed, ["UniqueAdID", "Location"])


df_dup_masids = (
    df_processed
    .groupBy("AlgoDivision", "Location", "MASIDToken")
    .agg(F.countDistinct("UniqueAdID").alias("AdsPerMASID"))
    .where(F.col("AdsPerMASID") > 1)
)
if df_dup_masids.count() > 1:

    dup_masid_list = list(set([row[0] for row in (df_dup_masids
                                                  .select("MASIDToken")
                                                  .collect())]))

    warn_dup_masid = ("Duplicate MASID suffixes assigned to Ads" +
                      f" in same AlgoDivision: {dup_masid_list}")
    logger.warning(warn_dup_masid)

    for m in dup_masid_list:

        res_conflict = f"Resolving conflict for MASID suffix: {m}"
        logger.info(res_conflict)
        warn_dup_masid += "\n\n" + res_conflict

        df_dups_m = (
            df_processed
            .where(F.col("MASIDToken") == m)
            .select("UniqueAdID")
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

                df_processed = (
                    df_processed
                    .where(F.col("UniqueAdID") != id_del)
                )
        except IndexError as e:
            logger.error(f"Error resolving MASID conflict: {e}")
            logger.warning(f"Unable to resolve conflict for suffix: {m}")
            logger.warning(f"Removing all ads associated with suffix: {m}")
            issue_ad = ("Issue resolving conflict for ads with MASID suffix:"
                        + f" {m} - all {m} ads removed")
            warn_dup_masid += "\n" + issue_ad
            df_processed = df_processed.where(F.col("MASIDToken") != m)

    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, warn_dup_masid)

logger.info('Cleaning theme strings (lowercase, strip whitespace)')
df_processed = (
    df_processed
    .withColumn(
        'Themes',
        F.when(
            F.col('Themes').isNotNull(),
            F.trim(F.lower(F.col('Themes')))
        ).otherwise(F.col('Themes'))
    )
)

logger.info('Theme:Ad mapping should be one-to-one - checking for violations')
multi_ad_themes = (
    df_processed
    .where(F.col('Themes').isNotNull())
    .groupBy('Themes')
    .agg(F.countDistinct('UniqueAdID').alias('nAds'))
    .where(F.col('nAds') > 1)
    .select('Themes')
).collect()

if len(multi_ad_themes) > 0:
    mat_found_msg = 'Themes mappped to multiple ads found'
    logger.warning(mat_found_msg)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, mat_found_msg)
    for mat in [x[0] for x in multi_ad_themes]:
        mat_remove_msg = f'Removing theme "{mat}" and associated ads'
        logger.warning(mat_remove_msg)
        if JOB_ENV == "prod":
            post_to_webhook(WEBHOOK_URL, mat_remove_msg)
        df_processed = df_processed.where(F.col('Themes') != mat)


df_valid_ad_ids = df_processed.select(
    F.col('UniqueAdID').alias('valid_id')
).distinct()

df_processed = df_processed.join(
    df_valid_ad_ids,
    F.col("UniqueAdIDPremium") == F.col("valid_id"),
    "left_outer"
)

df_processed = df_processed.withColumn(
    "UniqueAdIDPremium",
    F.when(F.col("valid_id").isNull(), F.lit(None))
    .otherwise(F.col("UniqueAdIDPremium"))
)

target_cols = (
    spark
    .table(TARGET_TABLE)
    .drop("rundate")
    ).columns


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
delete_from_and_load(df_processed.select(*target_cols),
                     TARGET_TABLE,
                     pk_cols=["UniqueAdID", "Location"],
                     del_where={"rundate": "current_date()"})

logger.info("Loading output to table (latest)")
truncate_and_load(df_processed.select(*target_cols),
                  TARGET_TABLE_LATEST,
                  pk_cols=["UniqueAdID", "Location"])

logger.info("Run complete")

import logging
import logging.config
import json
from next_ads.Assignment import (
    assign_best_ads_with_constraints,
    assign_random_ads,
    assign_best_ads
    )
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import (JobParser,
                                build_spark_schema,
                                chain_when_thens,
                                map_schema,
                                delete_from_and_load,
                                post_to_webhook)
from pyspark.sql import functions as F
from next_ads.utils.columnscalers import subtract_mean


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)
with open("config/parameters.json") as f:
    prm = json.load(f)

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname", "--location"])
LOCATION = pargs["location"] if pargs["location"] else "PH4"
log.info(f"Running in job environment: {job_env}")

LOCATIONS = prm["locations"]
SCHEMA = rsc["schema"][job_env]

tbls = rsc["tables"]["write"]
CONTROL_SHEET_LATEST = map_schema(tbls["control_sheet_latest"], SCHEMA)
TARGETING_SCORES_TABLE = map_schema(tbls["targeting_scores_latest"], SCHEMA)
ASSIGNMENTS_TABLE = map_schema(tbls["assignments"], SCHEMA)
ASSIGNMENTS_TABLE_LATEST = map_schema(tbls["assignments_latest"], SCHEMA)
CELLS_TABLE_LATEST = map_schema(tbls["customer_cells_latest"], SCHEMA)

FALLOW_TRUE_LABEL = prm["fallow_control"]["true_label"]

WEBHOOK_URL = rsc["webhooks"]["DS Warnings"]

try:
    CELL_MAP = LOCATIONS[LOCATION]
except KeyError as ke:
    loc_key_msg = f"{LOCATION} build requested but not in config"
    log.warning(loc_key_msg)
    if job_env == "prod":
        post_to_webhook(WEBHOOK_URL, loc_key_msg)
    raise ke

log.info(f"Assigning Ads for Location: {LOCATION}")

log.info("Getting Ads")
df_ads = (
    get_spark()
    .table(CONTROL_SHEET_LATEST)
    .where(F.col("Location") == LOCATION)
    .select(
        "UniqueAdID",
        "AlgoDivision",
        "MASIDToken",
        "TargetingCriteria")
)
# TODO: Remove underperforming Ads

if "exclude_ads_from_targeting" in prm.keys():
    df_ads_tgt = (
        df_ads
        .where(~F.col("UniqueAdID").isin(prm["exclude_ads_from_targeting"]))
    )

if df_ads_tgt.count() == 0:

    no_ads_msg = f"No ads found for Location: {LOCATION}"
    log.warning(no_ads_msg)

    if job_env == "prod":
        post_to_webhook(WEBHOOK_URL, no_ads_msg)
    log.info("Skipping assignment")

else:

    log.info("Getting customer cell assignments")
    df_cells = (
        get_spark()
        .table(CELLS_TABLE_LATEST)
        .drop("rundate")
    )
    df_cells.cache()

    log.info("Assigning Ads with Basic Targeting")
    df_assigned_basic = assign_random_ads(
        df_ads_tgt.select("UniqueAdID", "AlgoDivision"),
        df_cells.select("AccountNumber", "AlgoDivision"),
        grp_col="AlgoDivision"
        )
    df_assigned_basic.cache()

    log.info("Assigning Ads with Best Targeting")

    best_kwargs = {
        "targeting_scores_table": TARGETING_SCORES_TABLE,
        "score_scale_fn": subtract_mean
    }

    if "best_kwargs" in LOCATIONS[LOCATION]:
        best_kwargs = best_kwargs | LOCATIONS[LOCATION]["best_kwargs"]

    if "constraints" in LOCATIONS[LOCATION]:
        df_assigned_best = assign_best_ads_with_constraints(
            df_ads=df_ads_tgt,
            df_cust=df_cells.select("AccountNumber", "AlgoDivision"),
            constraints=LOCATIONS[LOCATION]["constraints"],
            best_kwargs=best_kwargs
        )
    else:
        df_assigned_best = assign_best_ads(
            df_ads=df_ads_tgt,
            df_cust=df_cells.select("AccountNumber", "AlgoDivision"),
            **best_kwargs
        )

    df_assigned_best.cache()

    log.info("Assigning Best Ads (Challenger)")
    # Assigning best to best_challenger effectively switches challenger off
    df_assigned_best_challenger = df_assigned_best

    log.info("Determining Ad to be shown based on assignments and fixed cells")
    df_assignments = (
        df_cells
        .withColumn("AdSuppressed", F.lit("AdSuppressed"))
        .join(
            (
                df_assigned_basic
                .select("AccountNumber", "UniqueAdID")
                .withColumnRenamed("UniqueAdID", "UniqueAdIDBasic")
            ),
            on="AccountNumber", how="left")
        .join(
            (
                df_assigned_best
                .select("AccountNumber", "UniqueAdID")
                .withColumnRenamed("UniqueAdID", "UniqueAdIDBest")
            ),
            on="AccountNumber", how="left")
        .join(
            (
                df_assigned_best_challenger
                .select("AccountNumber", "UniqueAdID")
                .withColumnRenamed("UniqueAdID", "UniqueAdIDBestChallenger")
            ),
            on="AccountNumber", how="left")
        )
    df_assignments.cache()

    df_ad_assigned = (
            df_assignments
            .withColumn(
                "UniqueAdIDMeasurement",
                chain_when_thens(CELL_MAP["map"])
                )
            .withColumn(
                "UniqueAdIDAssigned",
                F.when(
                    F.col('FallowControl') == FALLOW_TRUE_LABEL,
                    F.lit('NoAd')
                    ).otherwise(F.col('UniqueAdIDMeasurement'))
                )
        )

    df_ad_treatments = (
        df_assignments
        .drop('AdSuppressed',
              'UniqueAdIDBasic'
              'UniqueAdIDBest'
              'UniqueAdIDBestChallenger')
        .withColumns(
            {
                'AdSuppressed': F.lit('AdSuppressed'),
                'UniqueAdIDBasic': F.lit('Basic'),
                'UniqueAdIDBest': F.lit('Best'),
                'UniqueAdIDBestChallenger': F.lit('BestChallenger')
            }
        )
        .withColumn('Treatment', chain_when_thens(CELL_MAP['map']))
        .select('AccountNumber', 'Treatment')
    )

    df_ad_assigned = (
        df_ad_assigned.join(df_ad_treatments,
                            on='AccountNumber', how='left')
    )

    df_ad_masid = (
        df_ads
        .select("UniqueAdID", "MASIDToken")
        .withColumn("Location", F.lit(LOCATION))
        .withColumn("MASID",
                    F.concat(F.col("Location"),
                             F.lit("_"),
                             F.col("MASIDToken")))
        .drop("Location", "MASIDToken")
        .distinct()
        )

    ctrl_masid_cols = ["UniqueAdID", "MASID"]
    ctrl_masid_vals = [("NoAd", f"{LOCATION}_Z")]

    df_control_masid = (
        get_spark().createDataFrame(
            data=ctrl_masid_vals,
            schema=build_spark_schema(
                [["UniqueAdID", "string", "not null"],
                 ["MASID", "string", "not null"]]
                )
            )
        )
    df_ad_masid = df_ad_masid.unionByName(df_control_masid)

    df_ad_assigned_masid = (
        df_ad_assigned
        .join(df_ad_masid,
              on=df_ad_assigned.UniqueAdIDAssigned == df_ad_masid.UniqueAdID,
              how="left")
        .drop("UniqueAdID")
    )
    df_ad_assigned_masid.cache()

    # Check and warn if null MASID assignments exist
    n_null_masid = df_ad_assigned_masid.where(F.col("MASID").isNull()).count()
    if n_null_masid > 0:
        null_masid_msg = (f"{n_null_masid:,} accounts removed during " +
                          f"assignment of {LOCATION} due to null MASID")
        log.warning(null_masid_msg)
        if job_env == "prod":
            post_to_webhook(WEBHOOK_URL, null_masid_msg)
        df_ad_assigned_masid = (
            df_ad_assigned_masid
            .where(F.col("MASID").isNotNull())
        )

    df_ad_assigned_masid_output = (
        df_ad_assigned_masid
        .withColumn("Location", F.lit(LOCATION))
        .select(
            "AccountNumber",
            "Location",
            "UniqueAdIDBasic",
            "UniqueAdIDBest",
            "UniqueAdIDBestChallenger",
            "Treatment",
            "UniqueAdIDMeasurement",
            "UniqueAdIDAssigned",
            "MASID")
    )

    log.info(f"Loading assignments to {ASSIGNMENTS_TABLE}")
    delete_from_and_load(df_ad_assigned_masid_output,
                         ASSIGNMENTS_TABLE,
                         pk_cols=["AccountNumber", "Location"],
                         del_where={"rundate": "current_date()",
                                    "Location": f"'{LOCATION}'"})

    log.info(f"Loading assignments to {ASSIGNMENTS_TABLE_LATEST}")
    delete_from_and_load(df_ad_assigned_masid_output,
                         ASSIGNMENTS_TABLE_LATEST,
                         pk_cols=["AccountNumber", "Location"],
                         del_where={"Location": f"'{LOCATION}'"})

    df_cells.unpersist()
    df_assigned_best.unpersist()
    df_assignments.unpersist()
    df_ad_assigned_masid.unpersist()

log.info("Run complete")

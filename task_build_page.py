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
                                map_tbl,
                                delete_from_and_load,
                                post_to_webhook)
from pyspark.sql import functions as F
from next_ads.utils.columnscalers import subtract_mean


logging.config.fileConfig("logging.conf")
log = logging.getLogger("mylog")

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname", "--location"])
LOCATION = pargs["location"] if pargs["location"] else "SB2"
log.info(f"Running in job environment: {job_env}")

DOMAIN = pargs["domain"] if pargs["domain"] else "next_uk"

log.info(f"Configuring run for domain: {DOMAIN}")
with open(f"config/{DOMAIN}.json") as f:
    cfg = json.load(f)

LOCATIONS = cfg["locations"]
tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][job_env]
tbl_args = {'schema': SCHEMA, 'domain': DOMAIN}
CONTROL_SHEET_LATEST = map_tbl(tbls["control_sheet_latest"], **tbl_args)
TARGETING_SCORES_TABLE = map_tbl(tbls["targeting_scores_latest"], **tbl_args)
ASSIGNMENTS_TABLE = map_tbl(tbls["assignments"], **tbl_args)
ASSIGNMENTS_TABLE_LATEST = map_tbl(tbls["assignments_latest"], **tbl_args)
CELLS_TABLE_LATEST = map_tbl(tbls["customer_cells_latest"], **tbl_args)

tbl_args_results = tbl_args | {'schema': cfg['schema']['prod']}
AD_RESULTS_TABLE = map_tbl(tbls['results_ads'], **tbl_args_results)

FALLOW_TRUE_LABEL = cfg["fallow_control"]["true_label"]

WEBHOOK_URL = cfg["webhooks"]["DS Warnings"]

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
        "TargetingCriteria",
        "AudienceOnly")
)
# TODO: Remove underperforming Ads

df_ads_tgt = (
    df_ads
    .fillna(0, subset=['AudienceOnly'])
    .where((F.col("AudienceOnly") != 1))
    .drop('AudienceOnly')
)

df_ads = df_ads.drop('AudienceOnly')


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

    log.info("Assigning Ads with Best Targeting (Challenger)")
    best_kwargs |= {
        'apply_ad_feedback': True,
        'ad_results_table': AD_RESULTS_TABLE,
        'control_sheet_latest_table': CONTROL_SHEET_LATEST
        }

    if "constraints" in LOCATIONS[LOCATION]:
        df_assigned_best_challenger = assign_best_ads_with_constraints(
            df_ads=df_ads_tgt,
            df_cust=df_cells.select("AccountNumber", "AlgoDivision"),
            constraints=LOCATIONS[LOCATION]["constraints"],
            best_kwargs=best_kwargs
        )
    else:
        df_assigned_best_challenger = assign_best_ads(
            df_ads=df_ads_tgt,
            df_cust=df_cells.select("AccountNumber", "AlgoDivision"),
            **best_kwargs
        )

    df_assigned_best_challenger.cache()

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
            .fillna('NoAdFound', subset=['UniqueAdIDMeasurement'])
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
    ctrl_masid_vals = [("NoAd", f"{LOCATION}_Z"),
                       ('AdSuppressed', f'{LOCATION}_Z'),
                       ('NoAdFound', f'{LOCATION}_Z')]

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

    # Check and warn if null Treatments exist
    n_null_treatment = (
        df_ad_assigned_masid.where(F.col("Treatment").isNull()).count()
        )
    if n_null_treatment > 0:
        null_treatment_msg = (
            f"{n_null_treatment:,} accounts removed during " +
            f"assignment of {LOCATION} due to null Treatment")
        log.warning(null_treatment_msg)
        if job_env == "prod":
            post_to_webhook(WEBHOOK_URL, null_treatment_msg)
        df_ad_assigned_masid = (
            df_ad_assigned_masid
            .where(F.col("Treatment").isNotNull())
        )

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

    # Check and warn if UniqueAdIDMeasurement is null
    n_null_measure = (
        df_ad_assigned_masid
        .where(F.col("UniqueAdIDMeasurement").isNull())
        ).count()
    if n_null_measure > 0:
        null_measure_msg = (f"{n_null_measure:,} accounts removed during " +
                            f"assignment of {LOCATION} due to null " +
                            "UniqueAdIDMeasurement")
        log.warning(null_measure_msg)
        if job_env == "prod":
            post_to_webhook(WEBHOOK_URL, null_measure_msg)
        df_ad_assigned_masid = (
            df_ad_assigned_masid
            .where(F.col("UniqueAdIDMeasurement").isNotNull())
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

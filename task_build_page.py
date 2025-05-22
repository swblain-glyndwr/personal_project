import json
from pyspark.sql import functions as F
from next_ads.Assignment import (
    assign_random_ads,
    assign_best_ads,
    assign_best_ads_with_constraints,
    assign_best_ads_rec,
    assign_best_ads_with_constraints_rec
    )
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (build_spark_schema,
                         chain_when_thens,
                         map_tbl,
                         delete_from_and_load,
                         post_to_webhook)
from dsutils.argparser import get_job_parser
from dsutils.columnscalers import subtract_mean


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

LOCATION = jobparser.get_arg('--location')
if not LOCATION:
    assert not JOBNAME, 'Location must be specified when running as a job'
    LOCATION = 'PH3'  # Location can be specified for interactive debugging
    logger.warning(f'Location not specified (defaulting to {LOCATION})')

LOCATIONS = cfg["locations"]

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

# Map write schema to parameterised write table names
tbl_args = {'schema': SCHEMA, 'client': CLIENT}
CONTROL_SHEET_LATEST = map_tbl(tbls["control_sheet_latest"], **tbl_args)
TARGETING_SCORES_TABLE = map_tbl(tbls["targeting_scores_latest"], **tbl_args)
ASSIGNMENTS_TABLE = map_tbl(tbls["assignments"], **tbl_args)
ASSIGNMENTS_TABLE_LATEST = map_tbl(tbls["assignments_latest"], **tbl_args)
CELLS_TABLE_LATEST = map_tbl(tbls["customer_cells_latest"], **tbl_args)

REC_SCORES_TABLE = cfg["tables"]["read"]["recommender_scores_gru_latest"]

# Read results data from prod schema dataset
tbl_args_results = tbl_args | {'schema': cfg['schema']['prod']}
AD_RESULTS_TABLE = map_tbl(tbls['results_ads'], **tbl_args_results)

FALLOW_TRUE_LABEL = cfg["fallow_control"]["true_label"]

WEBHOOK_URL = cfg["webhooks"]["DS Warnings"]

try:
    CELL_MAP = LOCATIONS[LOCATION]
except KeyError as ke:
    loc_key_msg = f"{LOCATION} build requested but not in config"
    logger.warning(loc_key_msg)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, loc_key_msg)
    raise ke

logger.info(f"Assigning Ads for Location: {LOCATION}")

logger.info("Getting Ads")
df_ads = (
    spark
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
    logger.warning(no_ads_msg)

    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, no_ads_msg)
    logger.info("Skipping assignment")

else:

    logger.info("Getting customer cell assignments")
    df_cells = (
        spark
        .table(CELLS_TABLE_LATEST)
        .drop("rundate")
    )
    df_cells.cache()

    logger.info("Assigning Ads with Basic Targeting")
    df_assigned_basic = assign_random_ads(
        df_ads_tgt.select("UniqueAdID", "AlgoDivision"),
        df_cells.select("AccountNumber", "AlgoDivision"),
        grp_col="AlgoDivision"
        )
    df_assigned_basic.cache()

    logger.info("Assigning Ads with Best Targeting")

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

    logger.info("Assigning Ads with Best Targeting (Challenger)")
    best_kwargs |= {
        "recommender_scores_table": REC_SCORES_TABLE
    }
    del best_kwargs['targeting_scores_table']

    if "constraints" in LOCATIONS[LOCATION]:
        df_assigned_best_challenger = assign_best_ads_with_constraints_rec(
            df_ads=df_ads_tgt,
            df_cust=df_cells.select("AccountNumber", "AlgoDivision"),
            constraints=LOCATIONS[LOCATION]["constraints"],
            best_kwargs=best_kwargs
        )
    else:
        df_assigned_best_challenger = assign_best_ads_rec(
            df_ads=df_ads_tgt,
            df_cust=df_cells.select("AccountNumber", "AlgoDivision"),
            **best_kwargs
        )

    df_assigned_best_challenger.cache()

    logger.info("Determining Ad to show based on assignments and fixed cells")
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
        spark.createDataFrame(
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
        logger.warning(null_treatment_msg)
        if JOB_ENV == "prod":
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
        logger.warning(null_masid_msg)
        if JOB_ENV == "prod":
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
        logger.warning(null_measure_msg)
        if JOB_ENV == "prod":
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

    logger.info(f"Loading assignments to {ASSIGNMENTS_TABLE}")
    delete_from_and_load(df_ad_assigned_masid_output,
                         ASSIGNMENTS_TABLE,
                         pk_cols=["AccountNumber", "Location"],
                         del_where={"rundate": "current_date()",
                                    "Location": f"'{LOCATION}'"})

    logger.info(f"Loading assignments to {ASSIGNMENTS_TABLE_LATEST}")
    delete_from_and_load(df_ad_assigned_masid_output,
                         ASSIGNMENTS_TABLE_LATEST,
                         pk_cols=["AccountNumber", "Location"],
                         del_where={"Location": f"'{LOCATION}'"})

    df_cells.unpersist()
    df_assigned_basic.unpersist()
    df_assigned_best.unpersist()
    df_assigned_best_challenger.unpersist()
    df_assignments.unpersist()
    df_ad_assigned_masid.unpersist()

logger.info("Run complete")

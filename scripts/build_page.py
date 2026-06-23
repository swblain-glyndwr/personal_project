import sys
from pathlib import Path
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() # type: ignore # noqa
    if not notebook_path.startswith('/Workspace'):
        notebook_path = '/Workspace' + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

import json
from pyspark.sql import functions as F
from next_ads.Assignment import (assign_preranked_ads, assign_random_ads,
                                 assign_random_ads_with_exclusions,
                                 assign_nextgenads)
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (build_spark_schema,
                         chain_when_thens,
                         delete_from_and_load,
                         post_to_webhook)
from dsutils.argparser import get_job_parser
from next_ads.utils import config_manager
from next_ads.utils import etl
import datetime


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg('--job_env')
CLIENT = jobparser.get_arg('--client')
LOG_LEVEL = jobparser.get_arg('--log_level')
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert JOB_ENV.lower() == 'dev', \
        f'Client must be specified when running in {JOB_ENV}'
    CLIENT = 'next_uk'  # Client can be specified for interactive debugging
    logger.warning(f'Client not specified (defaulting to {CLIENT})')

# load configuration
config = config_manager.load_config(JOB_ENV)
logger.info(f"Configuring run for client: {CLIENT}")
with open(PROJECT_ROOT / f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

LOCATION = jobparser.get_arg('--location')
INHERIT_BASIC_FROM = jobparser.get_arg('--inherit_basic_from')
if not LOCATION:
    assert JOB_ENV.lower() == 'dev', \
        f'Location must be specified when running in {JOB_ENV}'
    LOCATION = 'SB1'  # Location can be specified for interactive debugging
    logger.warning(f'Location not specified (defaulting to {LOCATION})')

LOCATIONS = cfg["locations"]
MIN_C_SESSIONS = cfg['results_prm']['min_c_sessions']
INCREMENTAL_LOOKBACK = cfg['incrementality']['incremental_lookback']
CHECK_SESSIONS_FROM = (datetime.date.today() -
                       datetime.timedelta(days=INCREMENTAL_LOOKBACK+1))

# Switch to turn incrementality on or off
INCREMENTALITY_ADS_SUPPRESSION_SWITCH = (cfg['incrementality']
                                         ['incrementality_ads_suppression_switch']
                                         )
ADS_SWITCH_LABEL = cfg['incrementality']['ads_switch_label']
INCREMENTALITY_LOCATIONS = cfg['incrementality']['locations']
INCREMENTALITY_TREATMENTS = cfg['incrementality']['treatments']
AD_SUPPRESSION_MASID_TOKEN = cfg['incrementality']['masid_test_token']
INC_AD_SUPPRESSION_THRESHOLD = cfg['incrementality']['incremental_value_threshold']
INC_ADS_SUFFIX = cfg['incrementality']['incremental_ads_suffix']


tbls = cfg["tables"]["write"]
SCHEMA = config.schema_write
logger.info(f'Write schema set to {SCHEMA}')

# Map write schema to parameterised write table names
tbl_args = {'catalog': config.catalog_write, 'schema': SCHEMA, 'client': CLIENT}
CONTROL_SHEET_LATEST = etl.map_tbl(tbls["control_sheet_latest"], **tbl_args)
TARGETING_SCORES_TABLE = etl.map_tbl(tbls["targeting_scores_latest"], **tbl_args)
ASSIGNMENTS_TABLE = etl.map_tbl(tbls["assignments"], **tbl_args)
ASSIGNMENTS_TABLE_LATEST = etl.map_tbl(tbls["assignments_latest"], **tbl_args)
CELLS_TABLE_LATEST = etl.map_tbl(tbls["customer_cells_latest"], **tbl_args)
PRERANKED_THEMES_TABLE = etl.map_tbl(tbls["preranked_ads_from_themes_latest"],
                                 **tbl_args)
NEXTGENADS_ASSIGNMENTS_TABLE = cfg["tables"]["read"]["nextgenads_assignments_latest"]

# Read results data from prod schema dataset
tbl_args_results = {'catalog': config.catalog_read, 'schema': config.schema_read, 'client': CLIENT}
AD_RESULTS_TABLE = etl.map_tbl(tbls['results_ads'], **tbl_args_results)

FALLOW_TRUE_LABEL = cfg["fallow_control"]["true_label"]

_pt_isolation_cfg = cfg["page_type_isolation"]
PAGE_TYPE_ISOLATION_ENABLED = _pt_isolation_cfg["enabled"]
PAGE_TYPE_ALLOWED_GROUPS = (
    [grp for grp, locs in _pt_isolation_cfg["page_type_map"].items()
     if LOCATION in locs]
    + ["AllPages"]
    if PAGE_TYPE_ISOLATION_ENABLED else []
)

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
        "UniqueAdIDPremium",
        "AlgoDivision",
        "MASIDToken",
        "TargetingCriteria",
        "AudienceOnly",
        "Tags",
        "Themes",
        "ClusterID")
)

if INCREMENTALITY_ADS_SUPPRESSION_SWITCH:
    # Aggregate the results table to an ad level view
    df_incremental = (
        spark.table(AD_RESULTS_TABLE)
        .where((F.col('SessionDate') >= CHECK_SESSIONS_FROM)
               & (F.col('UniqueAdID').rlike(INC_ADS_SUFFIX+"$"))
               )
        .groupBy('UniqueAdID')
        .agg(
            F.sum('ApportionedRevenue').alias('ApportionedRevenue'),
            F.sum('Sessions').alias('Sessions'),
            F.sum('C_ApportionedRevenue').alias('C_ApportionedRevenue'),
            F.sum('C_Sessions').alias('C_Sessions'),
            F.when(
                F.sum('Sessions') > 0,
                F.sum(F.col('SessionOverlapRatio') *
                      F.col('Sessions')
                      ) / F.sum('Sessions')
            ).otherwise(F.lit(None)).alias('SessionOverlapRatio'),
        )
        .withColumn('ARPS',
            F.when(F.col('Sessions') > 0,
                F.col('ApportionedRevenue') / F.col('Sessions'))
            .otherwise(F.lit(None))
        )
        .withColumn('C_ARPS',
            F.when(F.col('C_Sessions') > 0,
                F.col('C_ApportionedRevenue') / F.col('C_Sessions'))
            .otherwise(F.lit(None))
        )
        .withColumn('IncARPS', F.col('ARPS') - F.col('C_ARPS'))
        .withColumn('IncARPSAdj',
            F.when(F.col('SessionOverlapRatio').isNotNull()
                & (F.col('SessionOverlapRatio') > 0),
                F.col('IncARPS') / F.col('SessionOverlapRatio'))
            .otherwise(F.lit(None))
        )
        .withColumn('EstContribution', F.col('IncARPSAdj') * F.col('Sessions'))
        .withColumn('IncPct',
            F.when(F.col('C_ARPS').isNotNull() & (F.col('C_ARPS') != 0),
                F.col('IncARPS') / F.col('C_ARPS'))
            .otherwise(F.lit(None))
        )
    )

    df_incremental = (df_incremental
                      .select(F.col('UniqueAdID').alias('UniqueAdIDAssigned'),
                              F.col('C_Sessions'),
                              'EstContribution'
                              )
                      )

df_ads_tgt = (
    df_ads
    .fillna(0, subset=['AudienceOnly'])
    .where((F.col("AudienceOnly") != 1))
)

# Create subset of ads for Best
df_ads_tgt_best = (
    df_ads_tgt
    .where(F.col('Themes').isNotNull())
    .where(F.col('Themes') != '')
)

df_ads_tgt_nextgenads = (
    df_ads
    .filter(F.col("ClusterID").isNotNull()
            & F.col("Themes").isNull())
)

# Drop unneeded columns following processing dataframe
ads_required_cols = ['UniqueAdID',
                     'UniqueAdIDPremium',
                     'AlgoDivision',
                     'MASIDToken',
                     'TargetingCriteria']
df_ads = df_ads.select(ads_required_cols)
df_ads_tgt = df_ads_tgt.select(ads_required_cols)
df_ads_tgt_best = df_ads_tgt_best.select(ads_required_cols)

if df_ads_tgt.count() == 0 and df_ads_tgt_nextgenads.count() == 0:

    no_ads_msg = f"No ads found for Location: {LOCATION}"
    logger.warning(no_ads_msg)

    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, no_ads_msg)

    logger.info(
        f"Clearing stale assignments for {LOCATION} from "
        f"{ASSIGNMENTS_TABLE_LATEST}"
    )
    spark.sql(f"""
        delete from {ASSIGNMENTS_TABLE_LATEST}
        where Location = '{LOCATION}'
    """)
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

    if df_ads_tgt.count() == 0:
        logger.info("No non-AudienceOnly ads - skipping basic/best")
        df_assigned_basic = spark.createDataFrame(
            [], schema="AccountNumber STRING, UniqueAdID STRING")
        df_assigned_best = spark.createDataFrame(
            [], schema="AccountNumber STRING, UniqueAdID STRING")
        df_assigned_best_challenger = df_assigned_best
        basic_within = LOCATIONS[LOCATION]["basic_within"]
        best_kwargs = {'return_ranks': [1]}
    else:
        if "basic_within" in LOCATIONS[LOCATION]:
            basic_within = LOCATIONS[LOCATION]["basic_within"]
        else:
            basic_default_warn_msg = (
                f'`basic_within` not specified in config for {LOCATION}' +
                ' - defaulting to "global"'
                )
            logger.warning(basic_default_warn_msg)
            basic_within = "global"

        # 'global' is a dummy column name used within the assign_random_ads
        # function when there are no grouping columns. This was done to minimise
        # refactoring the required, but a more generalisable assign_random_ads
        # function would be beneficial to remove this restriction
        if 'global' in df_ads_tgt.columns:
            protected_colname_msg = (
                'Protected column name "global" was found in df_ads_tgt'
            )
            raise Exception(protected_colname_msg)
        if 'global' in df_cells.columns:
            protected_colname_msg = (
                'Protected column name "global" was found in df_cells'
            )
            raise Exception(protected_colname_msg)

        # Check if we need to exclude ads from a previous location assignment
        inherit_location_key = "inherit_basic_from"
        if INHERIT_BASIC_FROM or inherit_location_key in LOCATIONS[LOCATION]:
            inherit_location = (
                INHERIT_BASIC_FROM or
                LOCATIONS[LOCATION].get(inherit_location_key)
            )
            logger.info(
                f"Inheriting basic assignments from {inherit_location} - "
                "excluding already-assigned ads"
            )

            # Get assignments from the inherited location
            df_inherited_assignments = (
                spark
                .table(ASSIGNMENTS_TABLE_LATEST)
                .where(F.col("Location") == inherit_location)
                .where(F.col("UniqueAdIDBasic").isNotNull())
                .select(
                    "AccountNumber",
                    F.col("UniqueAdIDBasic").alias("ExcludedAdID")
                )
            )

            # Join to cells to get excluded ads per customer
            df_cells_with_exclusions = (
                df_cells
                .join(df_inherited_assignments, on="AccountNumber", how="left")
            )

            # Assign random ads excluding the already-assigned ones
            if basic_within == 'global':
                df_assigned_basic = assign_random_ads_with_exclusions(
                    df_ads_tgt.select("UniqueAdID"),
                    df_cells_with_exclusions.select(
                        "AccountNumber", "ExcludedAdID"
                    )
                )
            else:
                df_assigned_basic = assign_random_ads_with_exclusions(
                    df_ads_tgt.select("UniqueAdID", basic_within),
                    df_cells_with_exclusions.select(
                        "AccountNumber", basic_within, "ExcludedAdID"
                    ),
                    grp_col=basic_within
                )
        else:
            # Original logic for locations without basic inheritance
            if basic_within == 'global':
                df_assigned_basic = assign_random_ads(
                    df_ads_tgt.select("UniqueAdID"),
                    df_cells.select("AccountNumber"),
                    )
            else:
                df_assigned_basic = assign_random_ads(
                    df_ads_tgt.select("UniqueAdID", basic_within),
                    df_cells.select("AccountNumber", basic_within),
                    grp_col=basic_within
                    )

        df_assigned_basic.cache()

        logger.info("Assigning Ads with Best Targeting")

        if "best_kwargs" in LOCATIONS[LOCATION]:
            best_kwargs = LOCATIONS[LOCATION]["best_kwargs"]
        else:
            best_kwargs = {'return_ranks': [1]}

        df_assigned_best = assign_preranked_ads(
            df_ads=df_ads_tgt_best,
            preranked_ads_table=PRERANKED_THEMES_TABLE,
            location=LOCATION,
            df_cust=df_cells.select("AccountNumber"),
            **best_kwargs
        )
        df_assigned_best.cache()

        df_assigned_best_challenger = df_assigned_best

    USE_NEXTGENADS = any(
        step.get("then", {}).get("col") == "UniqueAdIDNextGenAds"
        for step in CELL_MAP.get("map", [])
    )
    if USE_NEXTGENADS:
        logger.info(f"NextGenAds enabled for {LOCATION} - assigning cluster ads")
        df_assigned_nextgenads = assign_nextgenads(
            df_ads=df_ads_tgt_nextgenads,
            customer_to_cluster_table=NEXTGENADS_ASSIGNMENTS_TABLE,
            df_cust=df_cells.select("AccountNumber"),
            return_ranks=best_kwargs["return_ranks"]
        )
    else:
        logger.info(f"NextGenAds not referenced in {LOCATION} map - skipping")
        df_assigned_nextgenads = spark.createDataFrame(
            [], schema="AccountNumber STRING, UniqueAdID STRING")
    df_assigned_nextgenads.cache()

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
        .join(
            (
                df_assigned_nextgenads
                .select("AccountNumber", "UniqueAdID")
                .withColumnRenamed("UniqueAdID", "UniqueAdIDNextGenAds")
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
            .join(
                (
                    df_ads
                    .select('UniqueAdID', 'UniqueAdIDPremium')
                    .withColumnRenamed('UniqueAdID', 'UniqueAdIDMeasurement')
                ), on='UniqueAdIDMeasurement', how='left'
                )
            .withColumn(
                'UniqueAdIDMeasurement',
                F.when(
                    ((F.col('IsPremium') == 1)
                     & (F.col('UniqueAdIDPremium').isNotNull())),
                    F.col('UniqueAdIDPremium')
                    ).otherwise(F.col('UniqueAdIDMeasurement'))
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
                'UniqueAdIDBestChallenger',
                'UniqueAdIDNextGenAds')
        .withColumns(
            {
                'AdSuppressed': F.lit('AdSuppressed'),
                'UniqueAdIDBasic': F.lit('Basic'),
                'UniqueAdIDBest': F.lit('Best'),
                'UniqueAdIDBestChallenger': F.lit('BestChallenger'),
                'UniqueAdIDNextGenAds': F.lit('NextGenAds')
            }
        )
        .withColumn('Treatment', chain_when_thens(CELL_MAP['map']))
        .select('AccountNumber', 'Treatment')
    )

    df_ad_assigned = (
        df_ad_assigned
        .join(df_ad_treatments, on='AccountNumber', how='left')
        .withColumn(
            'Treatment',
            F.when(
                ((F.col('IsPremium') == 1)
                 & (F.col('UniqueAdIDPremium').isNotNull())),
                F.concat(F.col('Treatment'), F.lit('Prem'))
                ).otherwise(F.col('Treatment'))
            )
    )

    # --- Page-type isolation suppression ---
    # Customers in a page-type isolation bucket (e.g. HP_Only) should only
    # receive ads on locations that belong to their assigned page type.
    # For any other location we overwrite UniqueAdIDAssigned with 'NoAd'.
    if PAGE_TYPE_ISOLATION_ENABLED:
        logger.info(
            f"Page-type isolation enabled. "
            f"Allowed groups for {LOCATION}: {PAGE_TYPE_ALLOWED_GROUPS}"
        )
        df_ad_assigned = (
            df_ad_assigned
            .withColumn(
                "UniqueAdIDAssigned",
                F.when(
                    # Suppress if this customer's isolation cell is set AND
                    # their group is not in the permitted list for this location
                    F.col("PageTypeIsolation").isNotNull()
                    & ~F.col("PageTypeIsolation").isin(PAGE_TYPE_ALLOWED_GROUPS),
                    F.lit("NoAd")
                ).otherwise(F.col("UniqueAdIDAssigned"))
            )
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

    if INCREMENTALITY_ADS_SUPPRESSION_SWITCH:
        suppression_cond = (
            (F.col('Location').isin(INCREMENTALITY_LOCATIONS))
            & (F.col('Treatment').isin(INCREMENTALITY_TREATMENTS))
            & (F.col('EstContribution') < INC_AD_SUPPRESSION_THRESHOLD)
            & (F.col('EstContribution') < 0)
            & (F.col('C_Sessions') >= MIN_C_SESSIONS)
        )

        df_ad_assigned_masid = (
            df_ad_assigned_masid
            .join(df_incremental, on=['UniqueAdIDAssigned'], how='left')
            .withColumn('UniqueAdIDAssigned',
                        F.when(
                            suppression_cond,
                            F.lit(ADS_SWITCH_LABEL)
                        ).otherwise(F.col('UniqueAdIDAssigned')))
            .withColumn('MASID',
                        F.when(
                            suppression_cond,
                            F.concat(F.col('Location'),
                                     F.lit('_'),
                                     F.lit(AD_SUPPRESSION_MASID_TOKEN))
                        ).otherwise(F.col('MASID')))
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
                "UniqueAdIDNextGenAds",
                "Treatment",
                "UniqueAdIDMeasurement",
                "UniqueAdIDAssigned",
                "MASID")
        )

    else:
        df_ad_assigned_masid_output = (
            df_ad_assigned_masid
            .withColumn("Location", F.lit(LOCATION))
            .select(
                "AccountNumber",
                "Location",
                "UniqueAdIDBasic",
                "UniqueAdIDBest",
                "UniqueAdIDBestChallenger",
                "UniqueAdIDNextGenAds",
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

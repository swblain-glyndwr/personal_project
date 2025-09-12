import json
from datetime import date, timedelta
from pyspark.sql import functions as F
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import map_tbl, post_to_webhook
from dsutils.argparser import get_job_parser


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

LOOKBACK_DAYS = cfg['results_prm']['lookback_days']
CHECK_SESSIONS_FROM = date.today() - timedelta(days=LOOKBACK_DAYS+1)

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

# Map write schema to parameterised write table names
tbl_args = {'schema': SCHEMA, 'client': CLIENT}
CONTROL_SHEET_LATEST = map_tbl(tbls['control_sheet_latest'], **tbl_args)
AD_RESULTS = map_tbl(tbls['results_ads'], **tbl_args)

MIN_C_SESSIONS = cfg['results_prm']['min_c_sessions']

WEBHOOK_URL = cfg['webhooks']['Results Warnings']

df_ads_active = (
    spark
    .table(CONTROL_SHEET_LATEST)
    .select('UniqueAdID', 'Location')
    .distinct()
)

df_ad_results = (
    spark
    .table(AD_RESULTS)
    .where(F.col('SessionDate') >= CHECK_SESSIONS_FROM)
    .groupBy('UniqueAdID')
    .agg(
        F.sum('ApportionedRevenue').alias('ApportionedRevenue'),
        F.sum('Sessions').alias('Sessions'),
        F.sum('C_ApportionedRevenue').alias('C_ApportionedRevenue'),
        F.sum('C_Sessions').alias('C_Sessions'),
        F.mean('SessionOverlapRatio').alias('AvgSessionOverlapRatio')
    )
)

df_ad_results_underperf = (
    df_ad_results
    .withColumn('ARPS', F.col('ApportionedRevenue') / F.col('Sessions'))
    .withColumn('C_ARPS', F.col('C_ApportionedRevenue') / F.col('C_Sessions'))
    .withColumn('IncARPS', F.col('ARPS') - F.col('C_ARPS'))
    .withColumn('IncARPSAdj',
                F.col('IncARPS') / F.col('AvgSessionOverlapRatio'))
    .withColumn('EstContribution', F.col('IncARPSAdj') * F.col('Sessions'))
    .where(F.col('C_Sessions') >= MIN_C_SESSIONS)
    .where(F.col('IncARPSAdj') < 0)
)

underperf_ads_col = (
    df_ad_results_underperf
    .join(df_ads_active.select('UniqueAdID'),
          on='UniqueAdID', how='inner')
    .orderBy('IncARPSAdj')
    .select('UniqueAdID')
    .distinct()
    .collect()
)

underperf_ads = [x[0] for x in underperf_ads_col]

df_underperf_ads_loc = (
    df_ads_active.orderBy('UniqueAdID', 'Location')
    .filter(F.col('UniqueAdID')
            .isin(underperf_ads))
    .groupBy("UniqueAdID")
    .agg(F.collect_list("Location").alias("Location_List"))
)

df_underperf_ads_loc_metric = (
    df_ad_results_underperf
    .select("UniqueAdID", "IncARPSAdj", "Sessions", "ARPS", "EstContribution")
    .filter(F.col('UniqueAdID').isin(underperf_ads))
    .join(
        df_underperf_ads_loc, on='UniqueAdID', how='inner'
    )
    .withColumn("IncARPSAdj", F.round(F.col("IncARPSAdj"), 2))
    .withColumn("ARPS", F.round(F.col("ARPS"), 2))
    .withColumn("EstContribution", F.round(F.col("EstContribution"), 2))
)

output = [row.asDict() for row in df_underperf_ads_loc_metric.collect()]

output_str = '\n'.join(
    f"Ad: {row['UniqueAdID']}\n  Locations: {row['Location_List']}\n"
    f"  IncARPSAdj: {row['IncARPSAdj']}\n  Sessions: {row['Sessions']}\n"
    f"  ARPS: {row['ARPS']}\n"
    f"  EstContribution: {row['EstContribution']}\n"
    for row in output
)

if len(underperf_ads) > 0:
    msg = (
        'Underperforming Ads\n'
        f'(look-back to {CHECK_SESSIONS_FROM}; '
        f'min {MIN_C_SESSIONS:,} control sessions)\n\n'
        f'Ads with Active Location & Metrics:\n\n'
        f'{output_str}\n\n'
        'Check full results in dashboard'
    )
    logger.warning(msg)

    if JOB_ENV == 'prod':
        post_to_webhook(WEBHOOK_URL, msg)
else:
    msg = (
            'No underperforming ads found\n' +
            f'(look-back to {CHECK_SESSIONS_FROM}; ' +
            f'min {MIN_C_SESSIONS:,} control sessions)'
        )
    logger.warning(msg)

    if JOB_ENV == 'prod':
        post_to_webhook(WEBHOOK_URL, msg)

logger.info("Run Complete")

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
import re

from pyspark.sql import functions as F
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (truncate_and_load,
                         map_tbl,
                         post_to_webhook)
from dsutils.argparser import get_job_parser
from pyspark.sql.window import Window

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
with open(PROJECT_ROOT / f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

tbl_args = {'schema': SCHEMA, 'client': CLIENT}
LOOKBACK_DAYS = cfg['results_realtime_ads']['lookback_days']
CONTROL_SHEET_LATEST = map_tbl(tbls['control_sheet_latest'], **tbl_args)
AD_RESULTS = map_tbl(tbls['results_ads'], **tbl_args)
TOP_ADS_BY_LOC = map_tbl(tbls['results_ads_top_by_location'], **tbl_args)

WEBHOOK_URL = cfg['webhooks']['Results Warnings']

exclude_locations_like = (
    cfg['real_time_unknown']['backfill']['exclude_locations_like']
)
exclude_pattern = r"^(%s)" % "|".join(
    map(re.escape, exclude_locations_like)
)
location_filter = ~F.col("Location").rlike(exclude_pattern)

df_valid_realtime_ads = (
    spark.table(CONTROL_SHEET_LATEST)
    .filter(location_filter)
    .select("UniqueAdID", "Location", "MASIDToken")
)

df_ad_performance_by_loc = (
    spark
    .table(AD_RESULTS)
    .where(F.col('SessionDate') >= F.date_sub(F.current_date(), LOOKBACK_DAYS))
    .groupBy('UniqueAdID')
    .agg(
        F.sum('ApportionedRevenue').alias('ApportionedRevenue'),
        F.sum('Sessions').alias('Sessions'),
        F.sum('C_ApportionedRevenue').alias('C_ApportionedRevenue'),
        F.sum('C_Sessions').alias('C_Sessions'),
        F.mean('SessionOverlapRatio').alias('AvgSessionOverlapRatio')
    )
    .withColumn('ARPS', F.col('ApportionedRevenue') / F.col('Sessions'))
    .withColumn('C_ARPS', F.col('C_ApportionedRevenue') / F.col('C_Sessions'))
    .withColumn('IncARPS', F.col('ARPS') - F.col('C_ARPS'))
    .withColumn('IncARPSAdj',
                F.col('IncARPS') / F.col('AvgSessionOverlapRatio'))
    .withColumn('EstContribution', F.col('IncARPSAdj') * F.col('Sessions'))
    .join(df_valid_realtime_ads, on='UniqueAdID', how='inner')
)

window_spec = (
    Window
    .partitionBy("Location")
    .orderBy(F.col("EstContribution").desc(), F.col("UniqueAdID").asc())
)

df_with_rank = (
    df_ad_performance_by_loc
    .withColumn("rank", F.row_number().over(window_spec))
)

df_top_ad_by_loc = (
    df_with_rank
    .filter(F.col("rank") == 1)
    .select("Location", "UniqueAdID", "MASIDToken")
)

if df_top_ad_by_loc.isEmpty():
    msg = "No top ads returned - top ads table will not be updated."
    logger.warning(msg)
    if JOB_ENV == 'prod':
        post_to_webhook(WEBHOOK_URL, msg)
else:
    logger.info("Loading output to table")
    truncate_and_load(df_top_ad_by_loc,
                      TOP_ADS_BY_LOC,
                      pk_cols=["Location"])

logger.info("Run complete")

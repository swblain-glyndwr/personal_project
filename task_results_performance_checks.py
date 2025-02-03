from datetime import date, timedelta
import logging
import logging.config
import json
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import (JobParser,
                                map_tbl,
                                post_to_webhook)
from pyspark.sql import functions as F


logging.config.fileConfig("logging.conf")
log = logging.getLogger("mylog")

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")

DOMAIN = pargs["domain"] if pargs["domain"] else "next_uk"

log.info(f"Configuring run for domain: {DOMAIN}")
with open(f"config/{DOMAIN}.json") as f:
    cfg = json.load(f)

CHECK_SESSIONS_FROM = date.today() - timedelta(days=7)

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][job_env]
tbl_args = {'schema': SCHEMA, 'domain': DOMAIN}

CONTROL_SHEET_LATEST = map_tbl(tbls['control_sheet_latest'], **tbl_args)
AD_RESULTS = map_tbl(tbls['results_ads'], **tbl_args)

MIN_C_SESSIONS = cfg['results_prm']['min_c_sessions']

WEBHOOK_URL = cfg['webhooks']['Results Warnings']

df_ads_active = (
    get_spark()
    .table(CONTROL_SHEET_LATEST)
    .select('UniqueAdID')
    .distinct()
)

df_ad_results = (
    get_spark()
    .table(AD_RESULTS)
    .where(F.col('SessionDate') >= CHECK_SESSIONS_FROM)
    .groupBy('UniqueAdID')
    .agg(
        F.sum('Revenue').alias('Revenue'),
        F.sum('Sessions').alias('Sessions'),
        F.sum('C_Revenue').alias('C_Revenue'),
        F.sum('C_Sessions').alias('C_Sessions'),
    )
)

df_ad_results_underperf = (
    df_ad_results
    .withColumn('RPS', F.col('Revenue') / F.col('Sessions'))
    .withColumn('C_RPS', F.col('C_Revenue') / F.col('C_Sessions'))
    .withColumn('IncRPS', F.col('RPS') - F.col('C_RPS'))
    .withColumn('EstIncRev', F.col('IncRPS') * F.col('Sessions'))
    .where(F.col('C_Sessions') >= MIN_C_SESSIONS)
    .where(F.col('IncRPS') < 0)
)

underperf_ads_col = (
    df_ad_results_underperf
    .join(df_ads_active, on='UniqueAdID', how='inner')
    .orderBy('EstIncRev')
    .select('UniqueAdID')
    .distinct()
    .collect()
)

underperf_ads = [x[0] for x in underperf_ads_col]

if len(underperf_ads) > 0:
    msg = (
        'Underperforming ads:\n\n' +
        '\n'.join(underperf_ads) +
        '\n\nCheck full results in dashboard'
    )
    log.warning(msg)

if job_env == 'prod':
    post_to_webhook(WEBHOOK_URL, msg)

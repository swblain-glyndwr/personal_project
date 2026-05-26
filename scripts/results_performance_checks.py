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
import datetime
from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql import Window
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (post_to_webhook,
                         delete_from_and_load,
                         truncate_and_load)
from dsutils.argparser import get_job_parser
from next_ads.Results import check_control_ratio
from next_ads.utils import config_manager
from next_ads.utils import etl


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

LOOKBACK_DAYS = cfg['results_prm']['lookback_days']
CHECK_SESSIONS_FROM = date.today() - timedelta(days=LOOKBACK_DAYS+1)

TOLERANCE_CTRL = cfg['ctrl_ratio_checks']['tolerance']
MIN_CSESSIONS_CTRL = cfg['ctrl_ratio_checks']['min_c_sessions']
CONTROL_CHECK_TABLES = cfg['ctrl_ratio_checks']['table_refs']
CONTROL_CHECK_START = (
    date.today() - timedelta(days=cfg["results_prm"]["lookback_days"]))
ctrl_pc = cfg['fallow_control']['proportion']
CONTROL_RATIO = (ctrl_pc/(1-ctrl_pc))*100
INCREMENTAL_VALUE_THRESHOLD = cfg['incrementality'
                                  ]['incremental_value_threshold']

tbls = cfg["tables"]["write"]
SCHEMA = config.schema_write
logger.info(f'Write schema set to {SCHEMA}')

# Map write schema to parameterised write table names
tbl_args = {'catalog': config.catalog_write, 'schema': SCHEMA, 'client': CLIENT}
CONTROL_SHEET_LATEST = etl.map_tbl(tbls['control_sheet_latest'], **tbl_args)
AD_RESULTS = etl.map_tbl(tbls['results_ads'], **tbl_args)
UNDERPERFORMING_ADS = etl.map_tbl(tbls['results_underperforming_ads'],
                                  **tbl_args)

WEBHOOK_URL = cfg['webhooks']['Results Warnings']
WEBHOOK_URL_DS = cfg['webhooks']['DS Warnings']

df_ads_active = (
    spark
    .table(CONTROL_SHEET_LATEST)
    .select('UniqueAdID', 'Location', 'rundate')
    .distinct()
)

# Check 1: Identify underperforming ads and write out to table.
AUTO_TRADING_SWITCH = cfg['incrementality']['auto_trading_switch']
INCREMENTAL_LOOKBACK = cfg["incrementality"]["incremental_lookback"]
CHECK_SESSIONS_FROM = (datetime.date.today() -
                       datetime.timedelta(days=INCREMENTAL_LOOKBACK+1))
C_SESSIONS = cfg["incrementality"]["min_c_session"]

df_incremental = (
        spark.table(AD_RESULTS)
        .where((F.col('SessionDate') >= CHECK_SESSIONS_FROM))
        .groupBy('UniqueAdID')
        .agg(
            F.min('SessionDate').alias('min_session_date'),
            F.max('SessionDate').alias('max_session_date'),
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
df_ads_removed = (
    df_incremental
    .where((F.col('C_Sessions') >= C_SESSIONS) &
           (F.col('EstContribution') <= INCREMENTAL_VALUE_THRESHOLD))
)

# Inferences
total_ads = df_incremental.select('UniqueAdID').distinct().count()
removed_ads = df_ads_removed.select('UniqueAdID').distinct().count()

logger.info(f'Total Ads: {total_ads:,}')
logger.info(f'Removed Ads: {removed_ads:,} ({removed_ads/total_ads*100:.2f}%)')

total_sessions = df_incremental.agg(F.sum('Sessions')).collect()[0][0] or 0
ads_removed_sessions = df_ads_removed.agg(F.sum('Sessions')).collect()[0][0] or 0
impact_pct = round((ads_removed_sessions / total_sessions) * 100, 2) if total_sessions else None

logger.info(f'Total Ads Sessions: {total_sessions:,}')
logger.info(f'Potential impact on Sessions: {ads_removed_sessions:,} ({impact_pct:.2f}%)')

output = [row.asDict() for row in df_ads_removed.collect()]

output_str = '\n'.join(
    f"Ad: {row['UniqueAdID']}\n"
    f"  IncARPSAdj: {row['IncARPSAdj']}\n  Sessions: {row['Sessions']}\n"
    f"  ARPS: {row['ARPS']}\n"
    f"  EstContribution: {row['EstContribution']}\n"
    for row in output
)

auto_trading_status = 'AutoTrading ON' if AUTO_TRADING_SWITCH else 'AutoTrading OFF'

if removed_ads > 0:
    suppression_note = (
        'Ads removed from best targeting.'
        if AUTO_TRADING_SWITCH
        else 'Ads identified but NOT removed from best targeting (AutoTrading is OFF).'
    )
    msg = (
        f'{auto_trading_status}: {suppression_note}\n'
        f'- Num ads flagged: {removed_ads:,} ({removed_ads/total_ads*100:.2f}%)\n'
        f'- Min {C_SESSIONS:,} control sessions\n\n'
        f'{output_str}\n\n'
        'Check full results in dashboard.'
    )
    logger.warning(msg)

    # if JOB_ENV == 'prod':
    #     post_to_webhook(WEBHOOK_URL, msg)
else:
    msg = (
        f'{auto_trading_status}: No underperforming ads found\n'
        f'(look-back to {CHECK_SESSIONS_FROM}; '
        f'min {C_SESSIONS:,} control sessions)'
    )
    logger.warning(msg)

    # if JOB_ENV == 'prod':
    #     post_to_webhook(WEBHOOK_URL, msg)

logger.info(f"Loading assignments to {UNDERPERFORMING_ADS}")
delete_from_and_load(df_ads_removed,
                     UNDERPERFORMING_ADS,
                     pk_cols=["UniqueAdID"],
                     del_where={"rundate": "current_date()"})


# Check 2: check that control ratio is within tolerance for various splits
for ref in CONTROL_CHECK_TABLES:
    tbl = etl.map_tbl(cfg["tables"]["write"][ref], **tbl_args)
    df_ctrl_check = (
        spark
        .table(tbl)
        .where(F.col('SessionDate') >= CONTROL_CHECK_START)
    )

    df_ctrl_out_of_tolerance = (
        check_control_ratio(
            df_ctrl_check,
            control_ratio=CONTROL_RATIO,
            tolerance=TOLERANCE_CTRL,
            min_c_sessions=MIN_CSESSIONS_CTRL
            )
    )

    if not df_ctrl_out_of_tolerance.isEmpty():
        ctrl_warnings = [
            f'Control ratio out of tolerance for {tbl}'
            + f' since {CONTROL_CHECK_START} (target: {CONTROL_RATIO:.2f}%)']
        df_ctrl_out_of_tolerance = (
            df_ctrl_out_of_tolerance.drop('Sessions', 'C_Sessions'))
        for row in df_ctrl_out_of_tolerance.collect():
            ctrl_warnings += [' | '.join([str(c) for c in row])]
        for ctrl_warning in ctrl_warnings:
            logger.warning(ctrl_warning)
        if JOB_ENV == 'prod':
            post_to_webhook(WEBHOOK_URL_DS, '\n'.join(ctrl_warnings))

logger.info("Run Complete")

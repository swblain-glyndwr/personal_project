import logging
import logging.config
import json
from next_ads.Results import validate_assignments_match_pf
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import (JobParser,
                                assert_pk,
                                map_schema,
                                post_to_webhook)
from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import date, timedelta


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)
with open("config/parameters.json") as f:
    prm = json.load(f)

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")
RPID_WITH_ACCOUNTS = rsc["tables"]["read"]["rpid_with_accounts"]
PREFERENCE_FRAMEWORK = rsc["tables"]["read"]["preference_framework"]
BQ_SESSIONS = rsc["tables"]["read"]['bq_sessions']
BQ_SESSIONS_APP = rsc["tables"]["read"]['bq_sessions_app']
BQ_PAGES = rsc["tables"]["read"]['bq_pages']
BQ_SCREENS = rsc["tables"]["read"]['bq_screens']

# SCHEMA = rsc["schema"][job_env]
SCHEMA = 'warehouse'
tbls = rsc["tables"]["write"]
FIXED_CELLS_LATEST_TABLE = map_schema(tbls["customer_cells_fixed_latest"],
                                      SCHEMA)
ASSIGNMENTS_TABLE = map_schema(tbls["assignments"], SCHEMA)
CONTROL_SHEET_TABLE = map_schema(tbls["control_sheet"], SCHEMA)

LOCATIONS = prm['locations']
FIXED_CELLS = prm['fixed_cells']

WEBHOOK_URL = rsc["webhooks"]["DS Warnings"]

SESSION_DATE_START = date.today() - timedelta(days=13)
SESSION_DATE_END = date.today() - timedelta(days=13)

loc2page = dict()
loc2screen = dict()
loc2pf = dict()
for k in LOCATIONS:
    if 'page' in LOCATIONS[k]:
        loc2page[k] = LOCATIONS[k]['page']
    if 'screen' in LOCATIONS[k]:
        loc2screen[k] = LOCATIONS[k]['screen']
    if 'pf_col' in LOCATIONS[k]:
        loc2pf[k] = LOCATIONS[k]['pf_col']
pf2loc = {v: k for k, v in loc2pf.items()}
pf_cols = list(pf2loc.keys())


# Assignments run the evening before, therefore SessionDate == rundate + 1 day
df_assignments = (
    get_spark()
    .table(ASSIGNMENTS_TABLE)
    .where(F.col('rundate') >= (SESSION_DATE_START - timedelta(days=1)))
    .where(F.col('rundate') <= (SESSION_DATE_END - timedelta(days=1)))
    .withColumn('SessionDate', F.col('rundate') + timedelta(days=1))
    .select('AccountNumber',
            'SessionDate',
            'Location',
            'UniqueAdIDBasic',
            'UniqueAdIDBest',
            'UniqueAdIDBestChallenger',
            'Treatment',
            'UniqueAdIDAssigned',
            'MASID')
)

# MASID runs after midnight, therefore SessionDate == rundate
df_pf = (
    get_spark()
    .table(PREFERENCE_FRAMEWORK)
    .where(F.col('rundate') >= SESSION_DATE_START)
    .where(F.col('rundate') <= SESSION_DATE_END)
    .select('account_number', 'rundate', *pf_cols)
)

df_pf = df_pf.withColumnRenamed('account_number', 'AccountNumber')
df_pf = df_pf.withColumnRenamed('rundate', 'SessionDate')
df_pf = df_pf.withColumnsRenamed(pf2loc)

id_cols = ['AccountNumber', 'SessionDate']
df_pf_long = (
    df_pf
    .unpivot(ids=id_cols,
             values=[c for c in df_pf.columns if c not in id_cols],
             variableColumnName='Location',
             valueColumnName='MASIDPF')
)

assert_pk(df_pf_long, pk_cols=['AccountNumber', 'SessionDate', 'Location'])

# Validating assignments vs PF addresses any discrepancies or overrides
# that might have occurred during MASID creation
df_asgn_pf = df_assignments.join(
    df_pf_long,
    on=['AccountNumber', 'SessionDate', 'Location'],
    how='left')
df_asgn_pf.cache()

mismatch_msg_days = validate_assignments_match_pf(df_asgn_pf)

if mismatch_msg_days:
    for d in mismatch_msg_days:
        mismatch_msgs = mismatch_msg_days[d]
        log.warning(f'Mismatches in MASID found for SessionDate: {d}')
        for msg in mismatch_msgs:
            log.warning(msg)
        if job_env == 'prod':
            post_to_webhook(WEBHOOK_URL, '\n'.join(mismatch_msgs))

df_asgn_pf_nulls = (
    df_asgn_pf
    .where(F.col('MASIDPF').isNull())
    .groupBy('SessionDate')
    .agg(F.countDistinct('AccountNumber').alias('Accounts'))
)

if df_asgn_pf_nulls.count() > 0:
    df_nulls_dict = {x[0].strftime('%Y-%m-%d'): x[1]
                     for x in df_asgn_pf_nulls.collect()}
    for k, v in df_nulls_dict.items():
        missing_msg = (
            f'{v:,} customers assigned at least one Ad on {k} ' +
            f'but not found in PF on {k}'
        )
        log.warning(missing_msg)
        if job_env == 'prod':
            post_to_webhook(WEBHOOK_URL, '\n'.join(missing_msg))

df_valid_assignments = (
    df_asgn_pf
    .where(F.col('MASID') == F.col('MASIDPF'))
    .drop('MASIDPF')
)


# Get pages visited, limiting to pages showing Ads on given SessionDate
df_days_locations = (
    df_valid_assignments
    .select('SessionDate', 'Location')
    .distinct()
    .withColumn('PagePath', F.col('Location'))
)

df_days_pages = (
    df_days_locations
    .replace(loc2page, subset=['PagePath'])
    .where(F.col('PagePath').isin(list(loc2page.values())))
    .select('SessionDate', 'PagePath')
    .distinct()
)
df_days_screens = (
    df_days_locations
    .replace(loc2screen, subset=['PagePath'])
    .where(F.col('PagePath').isin(list(loc2screen.values())))
    .select('SessionDate', 'PagePath')
    .distinct()
)

df_pages = (
    get_spark()
    .table(BQ_PAGES)
    .where(F.col('date') >= SESSION_DATE_START)
    .where(F.col('date') <= SESSION_DATE_END)
    .select('date',
            'UniqueVisitID',
            'PagePath',
            'NextPagePath',
            'FirstTimestamp')
    .withColumnRenamed('date', 'SessionDate')
    .join(df_days_pages, on=['SessionDate', 'PagePath'], how='inner')
    .unionByName(
        (
            get_spark()
            .table(BQ_SCREENS)
            .where(F.col('date') >= SESSION_DATE_START)
            .where(F.col('date') <= SESSION_DATE_END)
            .withColumn('NextPagePath', F.lit(None).cast('string'))
            .select('date',
                    'UniqueVisitID',
                    'ScreenName',
                    'NextPagePath',
                    'FirstTimestamp')
            .withColumnRenamed('date', 'SessionDate')
            .withColumnRenamed('ScreenName', 'PagePath')
            .join(df_days_screens, on=['SessionDate', 'PagePath'], how='inner')
        )
    )
)


# Get session revenue
df_sessions = (
    get_spark()
    .table(BQ_SESSIONS)
    .where(F.col('date') >= SESSION_DATE_START)
    .where(F.col('date') <= SESSION_DATE_END)
    .select('UniqueVisitID',
            'TransactionRevenue',
            'RPID',
            'Device',
            'date')
    .unionByName(
        get_spark()
        .table(BQ_SESSIONS_APP)
        .where(F.col('date') >= SESSION_DATE_START)
        .where(F.col('date') <= SESSION_DATE_END)
        .select('UniqueVisitID',
                'TransactionRevenue',
                'RPID',
                'Device',
                'date')
        )
    .withColumnRenamed('date', 'SessionDate')
    .join(
        get_spark()
        .table(RPID_WITH_ACCOUNTS)
        .withColumnsRenamed({
            'roamingprofileid': 'RPID',
            'account_number': 'AccountNumber'
        })
        .select('AccountNumber', 'RPID')
        .drop_duplicates(),
        on='RPID', how='inner'
    )
    .groupBy('AccountNumber', 'SessionDate', 'UniqueVisitID', 'Device')
    .agg(F.min('TransactionRevenue').alias('Revenue'))
    .withColumn('Converted',
                F.when(F.col('Revenue').isNotNull(), 1).otherwise(0))
    .where(F.col('Device').isNotNull())
    .fillna({'Revenue': 0})
)


df_sessions_ads = (
    df_sessions
    .join(df_pages,
          on=['SessionDate', 'UniqueVisitID'],
          how='inner')
)
df_sessions_ads.cache()


df_fixed_cells = get_spark().table(FIXED_CELLS_LATEST_TABLE)

df_sessions_ads_valid = (
    df_sessions_ads
    .join(
        df_fixed_cells.select('AccountNumber', 'FallowControl'),
        on='AccountNumber', how='inner')
    .join(
        (
            df_valid_assignments
            .select('AccountNumber', 'SessionDate')
            .distinct()
        ),
        on=['AccountNumber', 'SessionDate'], how='inner'
         )
)
df_sessions_ads_valid.cache()

# TODO: EXPORT
df_results_topline = (
    df_sessions_ads_valid
    .groupBy('SessionDate', 'Device', 'FallowControl', 'UniqueVisitID')
    .agg(F.first('Revenue').alias('Revenue'))
    .groupBy('SessionDate', 'Device', 'FallowControl')
    .agg(F.countDistinct('UniqueVisitID').alias('Sessions'),
         F.sum('Revenue').alias('Revenue'))
)


df_valid_assignments_mapped = (
    df_valid_assignments
    .withColumn('PagePath', F.col('Location'))
    .replace(loc2page, subset=['PagePath'])
    .where(F.col('PagePath').isin(list(loc2page.values())))
    .withColumn('Device', F.lit('Desktop'))
    .unionByName(
        df_valid_assignments
        .withColumn('PagePath', F.col('Location'))
        .replace(loc2page, subset=['PagePath'])
        .where(F.col('PagePath').isin(list(loc2page.values())))
        .withColumn('Device', F.lit('Mobile'))
    )
    .unionByName(
        df_valid_assignments
        .withColumn('PagePath', F.col('Location'))
        .replace(loc2screen, subset=['PagePath'])
        .where(F.col('PagePath').isin(list(loc2screen.values())))
        .withColumn('Device', F.lit('App'))
    )
)

df_ad_metadata = (
    get_spark()
    .table(CONTROL_SHEET_TABLE)
    .where(F.col('rundate') >= (SESSION_DATE_START - timedelta(days=1)))
    .where(F.col('rundate') <= (SESSION_DATE_END - timedelta(days=1)))
    .withColumn('SessionDate', F.col('rundate') + timedelta(days=1))
)
df_ad_metadata.cache()

df_sessions_ads_valid_clicks = (
    df_sessions_ads_valid
    .join(df_valid_assignments_mapped,
          on=['AccountNumber', 'SessionDate', 'Device', 'PagePath'],
          how='left')
    .join(
        (
            df_ad_metadata
            .select('SessionDate', 'UniqueAdID', 'Location', 'URL')
            .withColumnRenamed('UniqueAdID', 'UniqueAdIDAssigned')
        ),
        on=['SessionDate', 'Location', 'UniqueAdIDAssigned'], how='left'
    )
    .withColumn(
        'Clicked',
        F.when(F.col('NextPagePath') == F.col('URL'), 1).otherwise(0)
        )
)

df_sessions_master = (
    df_sessions_ads_valid_clicks
    .groupBy('AccountNumber',
             'FallowControl',
             'SessionDate',
             'Device',
             'UniqueVisitID',
             'PagePath',
             'Location',
             'UniqueAdIDBasic',
             'UniqueAdIDBest',
             'UniqueAdIDBestChallenger',
             'Treatment',
             'UniqueAdIDAssigned',
             'Revenue',
             'Converted'
             )
    .agg(
        F.countDistinct('FirstTimestamp').alias('SoftImpressions'),
        F.max('Clicked').alias('SoftClicks'),
        F.min('FirstTimestamp').alias('FirstTimestamp')
        )
)

df_sessions_master.cache()

totals_master = (
    df_sessions_master
    .groupBy('SessionDate', 'UniqueVisitID')
    .agg(F.first('Revenue').alias('Revenue'))
    .groupBy('SessionDate')
    .agg(F.countDistinct('UniqueVisitID').alias('TotalSessions'),
         F.sum('Revenue').alias('TotalRevenue'))
)

totals_topline = (
    df_results_topline
    .groupBy('SessionDate')
    .agg(F.sum('Sessions').alias('TotalSessions'),
         F.sum('Revenue').alias('TotalRevenue'))
)

# Check for consistency in totals after appending ad and treatment data
# < 0.01 threshold used to allow for limitations of floating point arithmetic
sdates = [x[0] for x in totals_topline.select('SessionDate').collect()]
sdates.sort()
for d in sdates:
    d_fmt = d.strftime('%Y-%m-%d')
    log.info(f'Checking consistency of totals for SessionDate: {d_fmt}')
    for c in ['TotalSessions', 'TotalRevenue']:
        tm = (
            totals_master.where(F.col('SessionDate') == d).select(c)
            ).collect()[0][0]
        tt = (
            totals_topline.where(F.col('SessionDate') == d).select(c)
            ).collect()[0][0]
        msg = f'> 0.01 discrepancy found when comparing {c}'
        assert abs(tm - tt) < 0.01, msg


df_sessions_master = (
    df_sessions_master
    .withColumn(
        'TreatmentInferred',
        F.when((F.col('UniqueAdIDAssigned') == 'NoAd'), 'NoAd')
        .when(
            F.col('UniqueAdIDAssigned') == F.col('UniqueAdIDBasic'),
            "2: Basic")
        .when(
            F.col('UniqueAdIDBest') == F.col('UniqueAdIDBest'),
            "1: Best")
        .when(
            (F.col('UniqueAdIDBestChallenger') ==
             F.col('UniqueAdIDBestChallenger')),
            "1: Best (Challenger)")
    )
    .withColumn('Treatment',
                F.when(
                    F.col('Treatment').isNull(),
                    F.col('TreatmentInferred')
                    ).otherwise(F.col('Treatment')))
    .where(~(
        (F.col('FallowControl') == 'Ads')
        & (F.col('UniqueAdIDAssigned') == 'NoAd')
        )
    )
)

# TODO: Remove Order Complete from Data before apportioning revenue
# Calculate Order Complete performance separately

w_session = Window().partitionBy(
    ['SessionDate', 'UniqueVisitID'])
w_session_page = Window().partitionBy(
    ['SessionDate', 'UniqueVisitID', 'PagePath'])

df_apportioned = (
    df_sessions_master
    .withColumn(
        'SessionPages',
        F.size(F.collect_set('PagePath').over(w_session)))
    .withColumn(
        'PageLocations',
        F.size(F.collect_set('Location').over(w_session_page)))
    .withColumn('PageRevenue', F.col('Revenue') / F.col("SessionPages"))
    .withColumn('AdRevenue',
                F.col('PageRevenue') / F.col("PageLocations"))
    .withColumnRenamed('Revenue', 'SessionRevenue')
)

# vs Topline again
totals_apportioned = (
    df_apportioned
    .groupBy('SessionDate')
    .agg(F.countDistinct('UniqueVisitID').alias('TotalSessions'),
         F.sum('AdRevenue').alias('TotalRevenue'))
)

for d in sdates:
    d_fmt = d.strftime('%Y-%m-%d')
    log.info(f'Checking consistency post-apportioning: {d_fmt}')
    tsa = (
        totals_apportioned
        .where(F.col('SessionDate') == d)
        .select('TotalSessions')
        ).collect()[0][0]
    tst = (
        totals_topline
        .where(F.col('SessionDate') == d)
        .select('TotalSessions')
        ).collect()[0][0]
    # Dropping ~10% of sessions from apportioning may be acceptable if
    # page-level controls are being implemented, hence assert <15% dropped
    session_drop_thresh = 0.15
    ts_eval = tst - tsa
    msg = (f'More than {session_drop_thresh:.2%} of sessions dropped ' +
           'while apportioning revenue')
    assert ts_eval < tst * session_drop_thresh, msg

    tra = (
        totals_apportioned
        .where(F.col('SessionDate') == d)
        .select('TotalRevenue')
        ).collect()[0][0]
    trt = (
        totals_topline
        .where(F.col('SessionDate') == d)
        .select('TotalRevenue')
        ).collect()[0][0]
    tr_eval = trt - tra
    tr_eval_pc = tr_eval / trt
    log.warning(f'{tr_eval:,.2f} ({tr_eval_pc:.2%}) revenue dropped ' +
                'during apportioning')


# Ad-PageSet level
w_session_ad = Window().partitionBy('SessionDate',
                                    'UniqueVisitID',
                                    'UniqueAdID')

df_apportioned_location_set = (
    df_apportioned
    .withColumn(
        'UniqueAdID',
        F.when(
            F.col('FallowControl') == 'No Ads',
            F.coalesce('UniqueAdIDBasic', 'UniqueAdIDBest')
            ).otherwise(F.col('UniqueAdIDAssigned')))
    .withColumn('PageSet', F.collect_set('PagePath').over(w_session_ad))
    .where(F.col('UniqueAdID').isNotNull())
)

for fc in ['Ads', 'No Ads']:
    df_pre = df_apportioned.where(F.col('FallowControl') == fc)
    df_post = df_apportioned_location_set.where(F.col('FallowControl') == fc)
    n_diff = df_pre.count() - df_post.count()
    if n_diff != 0:
        log.warning(f'{n_diff:,} cases dropped from {fc} group due to null Ad')


ad_level_pk = [
    'SessionDate', 'UniqueAdID', 'Device', 'PageSet', 'FallowControl'
    ]
df_ad_level_agg = (
    df_apportioned_location_set
    .withColumn('PageSet', F.array_sort(F.col('PageSet')))
    .withColumn('PageSet', F.concat_ws(' + ', F.col('PageSet')))
    .groupBy(*ad_level_pk)
    .agg(
        F.countDistinct('UniqueVisitID').alias('Sessions'),
        F.sum('SoftImpressions').alias('SoftImpressions'),
        F.sum('SoftClicks').alias('SoftClicks'),
        F.countDistinct(
            F.when(F.col('Converted') == 1, F.col('UniqueVisitID'))
                ).alias('Conversions'),
        F.sum('SessionRevenue').alias('SessionRevenue'),
        F.sum('PageRevenue').alias('PageRevenue'),
        F.sum('AdRevenue').alias('AdRevenue'),
        )
)

totals_ad_level = (
    df_ad_level_agg
    .groupBy('SessionDate')
    .agg(
        F.sum('SessionRevenue').alias('SessionRevenue'),
        F.sum('PageRevenue').alias('PageRevenue'),
        F.sum('AdRevenue').alias('AdRevenue')
    )
)

for d in sdates:
    d_fmt = d.strftime('%Y-%m-%d')
    log.info(f'Checking consistency of ad-level totals for: {d_fmt}')

    tr2a = (
        totals_ad_level
        .where(F.col('SessionDate') == d)
        .select('AdRevenue')
        ).collect()[0][0]
    tr2t = (
        totals_topline
        .where(F.col('SessionDate') == d)
        .select('TotalRevenue')
        ).collect()[0][0]
    tr2_eval = tr2t - tr2a
    tr2_eval_pc = tr2_eval / tr2t
    log.warning('Total of apportioned ad-level revenue is ' +
                f'{tr2_eval:,.2f} ({tr2_eval_pc:.2%}) less than topline total')
    assert tr2_eval > 0

# TODO: Export Ad-Level
# Dashboard should be select only one Ad, and Sessions will be summable,
# RPS-able, and t-test-able

# TODO:
# Create latest Ad Metadata view for reporting
# Aggregate by AlgoDivision, by day & by week and do marginal contributions
# Aggregate by Page, by day & by week and do marginal contributions


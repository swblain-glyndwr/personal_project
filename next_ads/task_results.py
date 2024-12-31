import logging
import logging.config
import json
from next_ads.Results import (summarise_sessions,
                              validate_assignments_match_pf)
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import (JobParser,
                                assert_pk, delete_from_and_load,
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

SCHEMA = 'warehouse'
tbls = rsc["tables"]["write"]
FIXED_CELLS_LATEST_TABLE = map_schema(tbls["customer_cells_fixed_latest"],
                                      SCHEMA)
ASSIGNMENTS_TABLE = map_schema(tbls["assignments"], SCHEMA)
TRANSIENT_CELLS_TABLE = map_schema(tbls["customer_cells_transient"],
                                   SCHEMA)
CONTROL_SHEET_TABLE = map_schema(tbls["control_sheet"], SCHEMA)

RESULTS_DEVICE_OS_TABLE = map_schema(tbls["results_device_os"], SCHEMA)
RESULTS_AGGREGATES_TABLE = map_schema(tbls["results_aggregates"], SCHEMA)
RESULTS_AD_WITH_BENCHMARK_TABLE = map_schema(
    tbls["results_ad_with_benchmark"], SCHEMA)
RESULTS_AD_METADATA_TABLE = map_schema(tbls["results_ad_metadata"], SCHEMA)

LOCATIONS = prm['locations']
FIXED_CELLS = prm['fixed_cells']

WEBHOOK_URL = rsc["webhooks"]["DS Warnings"]

if job_env == 'dev':
    SESSION_DATE_START = date(2024, 12, 14)
    SESSION_DATE_END = date(2024, 12, 20)
else:
    SESSION_DATE_START = date.today() - timedelta(days=2)
    SESSION_DATE_END = date.today() - timedelta(days=1)

ndays = (SESSION_DATE_END - SESSION_DATE_START).days + 1
sdates = [SESSION_DATE_END - timedelta(days=x) for x in range(ndays)]
sdates.sort()

log.info(f'Processing results from {SESSION_DATE_START} to {SESSION_DATE_END}')

loc2page = dict()
loc2screen = dict()
loc2pf = dict()
loc2pagegroup = dict()
for k in LOCATIONS:
    if 'page' in LOCATIONS[k]:
        loc2page[k] = LOCATIONS[k]['page']
    if 'screen' in LOCATIONS[k]:
        loc2screen[k] = LOCATIONS[k]['screen']
    if 'pf_col' in LOCATIONS[k]:
        loc2pf[k] = LOCATIONS[k]['pf_col']
    if 'page_group' in LOCATIONS[k]:
        loc2pagegroup[k] = LOCATIONS[k]['page_group']

pf2loc = {v: k for k, v in loc2pf.items()}
pf_cols = list(pf2loc.keys())

# TODO: Generalise
oc_pagepaths = [loc2page['OC1'], loc2screen['OC1']]

# Assignments run the evening before, therefore SessionDate is rundate + 1 day
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

# MASID runs after midnight, therefore SessionDate is rundate
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

sdates_valid = [
    x[0].date() for x in
    df_valid_assignments.select('SessionDate').distinct().collect()
    ]
sdates_valid.sort()

sdates_missing = list(set(sdates).difference(set(sdates_valid)))
if sdates_missing:
    log.warning('No valid assignments found for dates: ' +
                f'{[x.strftime("%Y-%m-%d") for x in sdates_missing]}')

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
    .withColumn('operating_system', F.lit('NA'))
    .select('UniqueVisitID',
            'TransactionRevenue',
            'RPID',
            'Device',
            'operating_system',
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
                'operating_system',
                'date')
        )
    .withColumnRenamed('date', 'SessionDate')
    .withColumnRenamed('operating_system', 'OS')
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
    .groupBy('AccountNumber', 'SessionDate', 'UniqueVisitID', 'Device', 'OS')
    .agg(F.min('TransactionRevenue').alias('Revenue'))
    .where(F.col('Device').isNotNull())
    .fillna({'Revenue': 0})
)


df_sessions_pages = (
    df_sessions
    .join(df_pages,
          on=['SessionDate', 'UniqueVisitID'],
          how='inner')
)
df_sessions_pages.cache()

w_session = Window().partitionBy(
    ['SessionDate', 'UniqueVisitID'])

# Remove the last Order Complete page, and any hits after it from each session
# Rationale: If would be unfair to attribute any Session value to an ad
# on Order Complete when ad is seen after session spend is committed
df_sessions_pages_trimmed = (
    df_sessions_pages
    .withColumn('LastOrderComplete',
                F.max(
                    F.when(
                        F.col('PagePath').isin(oc_pagepaths),
                        F.col('FirstTimestamp'))).over(w_session))
    .withColumn('AfterLastOC',
                F.when(F.col('FirstTimestamp') >= F.col('LastOrderComplete'),
                       1).otherwise(0))
    .where(F.col('AfterLastOC') == 0)
    .select(df_sessions_pages.columns)
)

df_sessions_pre_trim = (
    df_sessions_pages
    .groupBy('SessionDate')
    .agg(F.countDistinct('UniqueVisitID').alias('Sessions'))
)
df_sessions_post_trim = (
    df_sessions_pages_trimmed
    .groupBy('SessionDate')
    .agg(F.countDistinct('UniqueVisitID').alias('Sessions'))
)

for d in sdates_valid:
    nspret = (
        df_sessions_pre_trim
        .where(F.col('SessionDate') == d)
        .select('Sessions')
        .collect()[0][0]
    )
    nspostt = (
        df_sessions_post_trim
        .where(F.col('SessionDate') == d)
        .select('Sessions')
        .collect()[0][0]
    )
    nsdiff = nspret - nspostt
    dfmt = d.strftime('%Y-%m-%d')
    log.info(f'{nsdiff:,} sessions dropped from ' +
             f'{dfmt} due to OrderComplete trimming')

df_fixed_cells = get_spark().table(FIXED_CELLS_LATEST_TABLE)

df_sessions_ads_valid = (
    df_sessions_pages_trimmed
    .join(
        df_fixed_cells.select('AccountNumber', 'FallowControl',
                              'HomePageTest1',
                              'ShoppingBagTest1',
                              'OrderCompleteTest1',
                              'LandingPageTest1'),
        on='AccountNumber', how='inner')
    .join(
        (
            df_valid_assignments
            .select('AccountNumber', 'SessionDate')
            .distinct()
        ),
        on=['AccountNumber', 'SessionDate'], how='inner'
         )
    .where(F.col('Device') != 'App')
)
df_sessions_ads_valid.cache()

df_results_topline = (
    df_sessions_ads_valid
    .groupBy('SessionDate', 'Device', 'OS', 'FallowControl', 'UniqueVisitID')
    .agg(F.first('Revenue').alias('Revenue'))
    .groupBy('SessionDate', 'Device', 'OS', 'FallowControl')
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
    .withColumn(
        'TreatmentInferred',
        F.when((F.col('UniqueAdIDAssigned') == 'NoAd'), 'NoAd')
        .when(
            F.col('UniqueAdIDAssigned') == F.col('UniqueAdIDBasic'),
            "Basic")
        .when(
            F.col('UniqueAdIDBest') == F.col('UniqueAdIDBest'),
            "Best")
        .when(
            (F.col('UniqueAdIDBestChallenger') ==
             F.col('UniqueAdIDBestChallenger')),
            "Best (Challenger)")
    )
    .withColumn('Treatment',
                F.when(
                    F.col('Treatment').isNull(),
                    F.col('TreatmentInferred')
                    ).otherwise(F.col('Treatment')))
    .withColumn(
        'UniqueAdIDMeasurement',
        F.when(F.col('FallowControl') == 'Ads', F.col('UniqueAdIDAssigned'))
        .when(F.col('FallowControl') == 'No Ads', F.col('UniqueAdIDBasic'))
    )
    .join(
        (
            df_ad_metadata
            .select('SessionDate', 'UniqueAdID', 'Location', 'URL')
            .withColumnRenamed('UniqueAdID', 'UniqueAdIDMeasurement')
        ),
        on=['SessionDate', 'Location', 'UniqueAdIDMeasurement'],
        how='left'
    )
    .withColumn(
        'Clicked',
        F.when(F.col('NextPagePath') == F.col('URL'), 1).otherwise(0)
        )
)

df_sessions_master = (
    df_sessions_ads_valid_clicks
    .withColumn('PageGroup', F.col('Location'))
    .replace(loc2pagegroup, subset=['PageGroup'])
    .groupBy('AccountNumber',
             'FallowControl',
             'SessionDate',
             'Device',
             'OS',
             'UniqueVisitID',
             'PagePath',
             'PageGroup',
             'Location',
             'UniqueAdIDBasic',
             'UniqueAdIDBest',
             'UniqueAdIDBestChallenger',
             'Treatment',
             'UniqueAdIDAssigned',
             'UniqueAdIDMeasurement',
             'Revenue'
             )
    .agg(
        F.countDistinct('FirstTimestamp').alias('SoftImpressions'),
        F.max('Clicked').alias('SoftClicks'),
        F.min('FirstTimestamp').alias('FirstTimestamp')
        )
)

df_sessions_master.cache()

reporting_metadata_cols = [
    'PotNumber',
    'CampaignNumber',
    'Title',
    'AlgoDivision',
    'TradeDivision',
    'Brand',
    'MASIDToken',
    'Segment',
    'AdDriver',
    'TemplateName',
    'TargetingCriteria',
    'AdCategory',
    'AdMission',
    'AdTrend',
    'AdSubcategory',
    'AdBrandName',
    'AdCampaign'
]

df_ad_metadata_non_loc = (
    df_ad_metadata
    .select('SessionDate', 'UniqueAdID', *reporting_metadata_cols)
    .distinct()
)

assert_pk(
    df_ad_metadata_non_loc,
    pk_cols=['SessionDate', 'UniqueAdID']
    )


df_sessions_master_meta = (
    df_sessions_master
    .join(
        (
            df_ad_metadata_non_loc
            .select('SessionDate', 'UniqueAdID', *reporting_metadata_cols)
            .distinct()
            .withColumnRenamed('UniqueAdID', 'UniqueAdIDMeasurement')
        ),
        on=['SessionDate', 'UniqueAdIDMeasurement'],
        how='left'
    )
)


totals_master = (
    df_sessions_master
    .groupBy('SessionDate', 'FallowControl', 'UniqueVisitID')
    .agg(F.first('Revenue').alias('Revenue'))
    .groupBy('SessionDate', 'FallowControl')
    .agg(F.countDistinct('UniqueVisitID').alias('TotalSessions'),
         F.sum('Revenue').alias('TotalRevenue'))
)

totals_master_meta = (
    df_sessions_master_meta
    .groupBy('SessionDate', 'FallowControl', 'UniqueVisitID')
    .agg(F.first('Revenue').alias('Revenue'))
    .groupBy('SessionDate', 'FallowControl')
    .agg(F.countDistinct('UniqueVisitID').alias('TotalSessions'),
         F.sum('Revenue').alias('TotalRevenue'))
)


totals_topline = (
    df_results_topline
    .groupBy('SessionDate', 'FallowControl')
    .agg(F.sum('Sessions').alias('TotalSessions'),
         F.sum('Revenue').alias('TotalRevenue'))
)


col_args_dict = {
    'session_id_col': 'UniqueVisitID',
    'page_id_col': 'PagePath',
    'revenue_col': 'Revenue',
    'impressions_col': 'SoftImpressions',
    'clicks_col': 'SoftClicks'
}

session_level_cols = ['SessionDate', 'Device', 'OS']

# Topline view
df_summary_device_os = summarise_sessions(
    df_sessions_master_meta,
    **col_args_dict,
    group_cols=session_level_cols + ['FallowControl']
)

# Aggregate views
agg_cols = [
    'AlgoDivision',
    'TradeDivision',
    'PageGroup',
    'PagePath'
]

agg_summaries = []
for ac in agg_cols:
    df_summary_ac = summarise_sessions(
        df_sessions_master_meta,
        **col_args_dict,
        group_cols=session_level_cols + ['FallowControl', ac]
    )
    df_summary_ac_renamed = (
        df_summary_ac
        .withColumnRenamed(ac, 'AggValue')
        .withColumn('AggColumn', F.lit(ac))
        .where(F.col('AggValue').isNotNull())
    )
    agg_summaries.append(df_summary_ac_renamed)

if agg_summaries:
    df_summary_agg = agg_summaries.pop()
    while agg_summaries:
        df_summary_agg = df_summary_agg.unionByName(agg_summaries.pop())


# Ad-level view
df_summary_ad = (
    summarise_sessions(
        df_sessions_master_meta,
        **col_args_dict,
        group_cols=(
            session_level_cols
            + ['FallowControl', 'UniqueAdIDMeasurement']
            + reporting_metadata_cols
            )
    )
    .where(F.col('UniqueAdIDMeasurement') != 'NoAd')
    .withColumnRenamed('UniqueAdIDMeasurement', 'UniqueAdID')
)


# Additional 'benchmark' to add to ad-level table
# This is added for two reasons
# 1 - At ad-level, control groups can become very small
# 2 - Legacy, the ad-level results for the latter half of 2024 used
# within-division averages as a pseudo control group (due to point 1)
benchmark_col = 'AlgoDivision'
df_ad_benchmarks = (
    df_summary_agg
    .where(F.col('AggColumn') == benchmark_col)
    .drop('AggColumn')
    .where(F.col('FallowControl') == 'Ads')
    .where(F.col('AggValue').isNotNull())
    .withColumnRenamed('AggValue', benchmark_col)
    .drop('FallowControl')
)

join_cols = ['SessionDate', benchmark_col]
kpi_cols = [
    c for c in df_ad_benchmarks.columns if c not in join_cols
]

df_summary_ad_benchmark = (
    df_summary_ad
    .where(F.col('FallowControl') == 'Ads')
    .drop(*kpi_cols)
    .distinct()
    .join(df_ad_benchmarks, on=join_cols, how='left')
    .withColumn('FallowControl', F.lit('Benchmark'))
    .select(df_summary_ad.columns)
)

df_summary_ad_with_benchmark = (
    df_summary_ad
    .unionByName(df_summary_ad_benchmark)
    .withColumnRenamed('FallowControl', 'TestGroup')
)


for d in sdates_valid:
    d_fmt = d.strftime('%Y-%m-%d')
    log.info('Checking consistency of pre- and post-processing ' +
             f'totals for SessionDate: {d_fmt}')
    for fc in ['Ads', 'No Ads']:
        for c in ['Sessions', 'Revenue']:
            tpre = (
                df_results_topline
                .where(F.col('SessionDate') == d)
                .where(F.col('FallowControl') == fc)
                .groupBy('SessionDate', 'FallowControl')
                .agg(F.sum(c).alias(c))
                .select(c)
                ).collect()[0][0]
            tpost = (
                df_summary_device_os
                .where(F.col('SessionDate') == d)
                .where(F.col('FallowControl') == fc)
                .groupBy('SessionDate', 'FallowControl')
                .agg(F.sum(c).alias(c))
                .select(c)
                ).collect()[0][0]
            # Check match to < 0.01 to allow for floating point arithmetic
            msg = f'Pre- and post- total for {c} does not match for {fc}'
            assert abs(tpost - tpre) < 0.01, msg


if job_env == 'prod':
    for d in sdates_valid:
        d_fmt = "\'" + d.strftime('%Y-%m-%d') + "\'"

        log.info(f'Loading results_device_os for {d_fmt} ' +
                 f'to table: {RESULTS_DEVICE_OS_TABLE}')
        delete_from_and_load(
            (
                df_summary_device_os
                .where(F.col('SessionDate') == d)
                .select('SessionDate',
                        'Device',
                        'OS',
                        'FallowControl',
                        'Sessions',
                        'Revenue',
                        'Conversions',
                        'SoftImpressions',
                        'SoftClicks')
            ),
            RESULTS_DEVICE_OS_TABLE,
            pk_cols=['SessionDate', 'Device', 'OS',
                     'FallowControl'],
            del_where={'SessionDate': d_fmt}
        )

        log.info(f'Loading results_aggregates for {d_fmt} ' +
                 f'to table: {RESULTS_AGGREGATES_TABLE}')
        delete_from_and_load(
            (
                df_summary_agg
                .where(F.col('SessionDate') == d)
                .select('SessionDate',
                        'Device',
                        'OS',
                        'AggColumn',
                        'AggValue',
                        'FallowControl',
                        'Sessions',
                        'Revenue',
                        'Conversions',
                        'SoftImpressions',
                        'SoftClicks')
            ),
            RESULTS_AGGREGATES_TABLE,
            pk_cols=['SessionDate', 'Device', 'OS',
                     'AggColumn', 'AggValue', 'FallowControl'],
            del_where={'SessionDate': d_fmt}
        )

        log.info(f'Loading results_ad_with_benchmark for {d_fmt} ' +
                 f'to table: {RESULTS_AD_WITH_BENCHMARK_TABLE}')
        delete_from_and_load(
            (
                df_summary_ad_with_benchmark
                .where(F.col('SessionDate') == d)
                .select('SessionDate',
                        'Device',
                        'OS',
                        'TestGroup',
                        'UniqueAdID',
                        'Sessions',
                        'Revenue',
                        'Conversions',
                        'SoftImpressions',
                        'SoftClicks')
            ),
            RESULTS_AD_WITH_BENCHMARK_TABLE,
            pk_cols=['SessionDate', 'Device', 'OS',
                     'TestGroup', 'UniqueAdID'],
            del_where={'SessionDate': d_fmt}
        )

        log.info(f'Loading results_ad_metadata for {d_fmt} ' +
                 f'to table: {RESULTS_AD_METADATA_TABLE}')
        delete_from_and_load(
            (
                df_ad_metadata_non_loc
                .where(F.col('SessionDate') == d)
                .select('SessionDate',
                        'UniqueAdID',
                        'PotNumber',
                        'CampaignNumber',
                        'Title',
                        'AlgoDivision',
                        'TradeDivision',
                        'Brand',
                        'MASIDToken',
                        'Segment',
                        'AdDriver',
                        'TemplateName',
                        'TargetingCriteria',
                        'AdCategory',
                        'AdMission',
                        'AdTrend',
                        'AdSubcategory',
                        'AdBrandName',
                        'AdCampaign')
            ),
            RESULTS_AD_METADATA_TABLE,
            pk_cols=['SessionDate', 'UniqueAdID'],
            del_where={'SessionDate': d_fmt}
        )


# w_session_ad = Window().partitionBy('SessionDate',
#                                     'FallowControl',
#                                     'UniqueVisitID',
#                                     'UniqueAdIDMeasurement')

# df_summary_ad_page_set = summarise_sessions(
#     (
#         df_sessions_master_meta
#         .withColumn('PageSet', F.collect_set('PagePath').over(w_session_ad))
#     ),
#     **col_args_dict,
#     group_cols=['SessionDate',
#                 'FallowControl',
#                 'UniqueAdIDMeasurement',
#                 'PageSet']
# ).withColumn('PageSetStr', F.col('PageSet').cast('string'))


# df_ad_pageset = (
#     df_sessions_master_meta
#     .withColumn('PageSet', F.collect_set('PagePath').over(w_session_ad))
#     .groupBy(
#         'SessionDate',
#         'FallowControl',
#         'UniqueVisitID',
#         'UniqueAdIDMeasurement',
#         'PageSet'
#         )
#     .agg(
#         F.max('Converted').alias('SessionConverted'),
#         F.sum('SoftImpressions').alias('SoftImpressions'),
#         F.sum('SoftClicks').alias('SoftClicks'),
#         F.sum('Revenue').alias('SessionRevenue')
#         )
#     .groupBy(
#         'SessionDate',
#         'FallowControl',
#         'UniqueAdIDMeasurement',
#         'PageSet'
#     )
#     .agg(
#         F.countDistinct('UniqueVisitID').alias('Sessions'),
#         F.sum('SessionConverted').alias('Conversions'),
#         F.sum('SoftImpressions').alias('SoftImpressions'),
#         F.sum('SoftClicks').alias('SoftClicks'),
#         F.sum('SessionRevenue').alias('TotalSessionRevenue')
#     )
# )
# df_ad_pageset.cache()

# df_ad_agg = (
#     df_ad_pageset
#     .groupBy('SessionDate', 'FallowControl', 'UniqueAdIDMeasurement')
#     .agg(
#         F.sum('Sessions').alias('Sessions'),
#         F.sum('TotalSessionRevenue').alias('Revenue')
#         )
#     .withColumn('RPS', F.col('Revenue')/F.col('Sessions'))
#     .groupBy('SessionDate', 'UniqueAdIDMeasurement')
#     .pivot('FallowControl')
#     .agg(
#         F.first('Sessions').alias('Sessions'),
#         F.first('RPS').alias('RPS')
#         )
#     .withColumn('Inc_RPS', F.col('Ads_RPS') - F.col('No Ads_RPS'))
#     .withColumn('Inc_RPS_Percent', F.col('Ads_RPS'))
#     .withColumn('Estimated_Inc_RPS',
#                 F.col('Inc_RPS') * F.col('Ads_Sessions'))
# )

# # BOOKMARK - Can we get good ad-level results with UniqueAdIDMeasurement
# # concept...or do we need to scrap the page controls and get full
# # UniqueAdIDAssigned coverage?
# # TODO: Resolve fesibility of Shapley approach - this will determine
# # whether page controls can be scrapped

# # Average Ad performance?
# (
#     df_ad_pageset_level_agg
#     .groupBy('SessionDate')
#     .agg(
#         F.mean('Estimated_Inc_RPS'),
#         F.median('Estimated_Inc_RPS'),
#         F.sum('Estimated_Inc_RPS')
#         )
# )


# vvv APPORTIONING - Scrap?

# df_apportioned_location_set = (
#     df_apportioned
#     .withColumn(
#         'UniqueAdID',
#         F.when(
#             F.col('FallowControl') == 'No Ads',
#             F.coalesce('UniqueAdIDBasic', 'UniqueAdIDBest')
#             ).otherwise(F.col('UniqueAdIDAssigned')))
#     .withColumn('PageSet', F.collect_set('PagePath').over(w_session_ad))
#     .where(F.col('UniqueAdID').isNotNull())
# )


# w_session_page = Window().partitionBy(
#     ['SessionDate', 'UniqueVisitID', 'PagePath'])

# df_apportioned = (
#     df_sessions_master
#     .withColumn(
#         'SessionPages',
#         F.size(F.collect_set('PagePath').over(w_session)))
#     .withColumn(
#         'PageLocations',
#         F.size(F.collect_set('Location').over(w_session_page)))
#     .withColumn('PageRevenue', F.col('Revenue') / F.col("SessionPages"))
#     .withColumn('AdRevenue',
#                 F.col('PageRevenue') / F.col("PageLocations"))
#     .withColumnRenamed('Revenue', 'SessionRevenue')
# )

# # vs Topline again
# totals_apportioned = (
#     df_apportioned
#     .groupBy('SessionDate')
#     .agg(F.countDistinct('UniqueVisitID').alias('TotalSessions'),
#          F.sum('AdRevenue').alias('TotalRevenue'))
# )

# for d in sdates_valid:
#     for fc in ['Ads', 'No Ads']:
#         d_fmt = d.strftime('%Y-%m-%d')
#         log.info(f'Checking consistency post-apportioning: {d_fmt}')
#         tsa = (
#             totals_apportioned
#             .where(F.col('SessionDate') == d)
#             .where(F.col('FallowControl') == fc)
#             .select('TotalSessions')
#             ).collect()[0][0]
#         tst = (
#             totals_topline
#             .where(F.col('SessionDate') == d)
#             .where(F.col('FallowControl') == fc)
#             .select('TotalSessions')
#             ).collect()[0][0]
#         # Dropping ~10% of sessions from apportioning may be acceptable if
#         # page-level controls are being implemented, hence assert
#         # <15% dropped
#         session_drop_thresh = 0.15
#         ts_eval = tst - tsa
#         msg = (f'More than {session_drop_thresh:.2%} of sessions dropped ' +
#                f'from {fc} group while apportioning revenue')
#         assert ts_eval < tst * session_drop_thresh, msg

#         tra = (
#             totals_apportioned
#             .where(F.col('SessionDate') == d)
#             .select('TotalRevenue')
#             ).collect()[0][0]
#         trt = (
#             totals_topline
#             .where(F.col('SessionDate') == d)
#             .select('TotalRevenue')
#             ).collect()[0][0]
#         tr_eval = trt - tra
#         tr_eval_pc = tr_eval / trt
#         log.warning(f'{tr_eval:,.2f} ({tr_eval_pc:.2%}) revenue dropped ' +
#                     f'from {fc} group while apportioning')


# # Ad-PageSet level
# w_session_ad = Window().partitionBy('SessionDate',
#                                     'UniqueVisitID',
#                                     'UniqueAdID')

# df_apportioned_location_set = (
#     df_apportioned
#     .withColumn(
#         'UniqueAdID',
#         F.when(
#             F.col('FallowControl') == 'No Ads',
#             F.coalesce('UniqueAdIDBasic', 'UniqueAdIDBest')
#             ).otherwise(F.col('UniqueAdIDAssigned')))
#     .withColumn('PageSet', F.collect_set('PagePath').over(w_session_ad))
#     .where(F.col('UniqueAdID').isNotNull())
# )

# for d in sdates_valid:
#     for fc in ['Ads', 'No Ads']:
#         df_pre = (
#             df_apportioned
#             .where(F.col('SessionDate') == d)
#             .where(F.col('FallowControl') == fc)
#         )
#         df_post = (
#             df_apportioned_location_set
#             .where(F.col('SessionDate') == d)
#             .where(F.col('FallowControl') == fc)
#         )
#         n_diff = df_pre.count() - df_post.count()
#         if n_diff != 0:
#             log.warning(f'{n_diff:,} cases dropped from {fc} group ' +
#                         f'on {d} due to null Ad')


# ad_level_pk = [
#     'SessionDate', 'UniqueAdID', 'Device', 'PageSet', 'FallowControl'
#     ]
# df_results_ad_level = (
#     df_apportioned_location_set
#     .withColumn('PageSet', F.array_sort(F.col('PageSet')))
#     .withColumn('PageSet', F.concat_ws(' + ', F.col('PageSet')))
#     .groupBy(*ad_level_pk)
#     .agg(
#         F.countDistinct('UniqueVisitID').alias('Sessions'),
#         F.sum('SoftImpressions').alias('SoftImpressions'),
#         F.sum('SoftClicks').alias('SoftClicks'),
#         F.countDistinct(
#             F.when(F.col('Converted') == 1, F.col('UniqueVisitID'))
#                 ).alias('Conversions'),
#         F.sum('SessionRevenue').alias('SessionRevenue'),
#         F.sum('PageRevenue').alias('PageRevenue'),
#         F.sum('AdRevenue').alias('AdRevenue'),
#         )
# )

# totals_ad_level = (
#     df_results_ad_level
#     .groupBy('SessionDate')
#     .agg(
#         F.sum('SessionRevenue').alias('SessionRevenue'),
#         F.sum('PageRevenue').alias('PageRevenue'),
#         F.sum('AdRevenue').alias('AdRevenue')
#     )
# )

# for d in sdates_valid:
#     d_fmt = d.strftime('%Y-%m-%d')
#     log.info(f'Checking consistency of ad-level totals for: {d_fmt}')

#     tr2a = (
#         totals_ad_level
#         .where(F.col('SessionDate') == d)
#         .select('AdRevenue')
#         ).collect()[0][0]
#     tr2t = (
#         totals_topline
#         .where(F.col('SessionDate') == d)
#         .select('TotalRevenue')
#         ).collect()[0][0]
#     tr2_eval = tr2t - tr2a
#     tr2_eval_pc = tr2_eval / tr2t
#     log.warning('Total of apportioned ad-level revenue is ' +
#                 f'{tr2_eval:,.2f} ({tr2_eval_pc:.2%}) less than ' +
#                 'topline total')
#     assert tr2_eval > 0

# ^^^ APPORTIONING - Scrap?

# TODO: EXPORT df_results_topline

# TODO: EXPORT df_results_ad_level
# Dashboard should be select only one Ad, and Sessions will be summable,
# RPS-able, and t-test-able
# Select multiple ads and Sessions will not be summable

# TODO:
# Create latest Ad Metadata view for reporting
# Aggregate by Page, by day & by week and do marginal contributions
#   Compare to Page-wise control method
# Aggregate by AlgoDivision, by day & by week and do marginal contributions

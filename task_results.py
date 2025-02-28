import logging
import logging.config
import json
from next_ads.Results import (append_session_overlap_ratio,
                              check_for_missing_dates,
                              patch_missing_dates,
                              summarise_sessions,
                              validate_assignments_match_pf)
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import (JobParser,
                                assert_pk, delete_from_and_load,
                                map_tbl,
                                post_to_webhook)
from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import date, timedelta


logging.config.fileConfig("logging.conf")
log = logging.getLogger("mylog")

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname",
                                        "--datestart",
                                        "--dateend"])
log.info(f"Running in job environment: {job_env}")

DOMAIN = pargs["domain"] if pargs["domain"] else "next_uk"

log.info(f"Configuring run for domain: {DOMAIN}")
with open(f"config/{DOMAIN}.json") as f:
    cfg = json.load(f)

RPID_WITH_ACCOUNTS = cfg["tables"]["read"]["rpid_with_accounts"]
PREFERENCE_FRAMEWORK = cfg["tables"]["read"]["preference_framework"]
BQ_SESSIONS = cfg["tables"]["read"]['bq_sessions']
BQ_SESSIONS_APP = cfg["tables"]["read"]['bq_sessions_app']
BQ_PAGES = cfg["tables"]["read"]['bq_pages']
BQ_SCREENS = cfg["tables"]["read"]['bq_screens']

tbls = cfg["tables"]["write"]
SCHEMA = 'warehouse'
tbl_args = {'schema': SCHEMA, 'domain': DOMAIN}

FIXED_CELLS_LATEST_TABLE = map_tbl(tbls["customer_cells_fixed_latest"],
                                   **tbl_args)
ASSIGNMENTS_TABLE = map_tbl(tbls["assignments"], **tbl_args)
TRANSIENT_CELLS_TABLE = map_tbl(tbls["customer_cells_transient"],
                                **tbl_args)
CONTROL_SHEET_TABLE = map_tbl(tbls["control_sheet"], **tbl_args)

RESULTS_TOPLINE_TABLE = map_tbl(tbls["results_topline"], **tbl_args)
RESULTS_AGGREGATED_TABLE = map_tbl(tbls["results_aggregated"], **tbl_args)
RESULTS_AB_TABLE = map_tbl(tbls["results_ab"], **tbl_args)
RESULTS_ADS_TABLE = map_tbl(tbls["results_ads"], **tbl_args)
RESULTS_ADS_LOCATION_TABLE = map_tbl(tbls["results_ads_location"], **tbl_args)
RESULTS_ADS_PAGE_TABLE = map_tbl(tbls["results_ads_page"], **tbl_args)
RESULTS_DIV_PAGE_TABLE = map_tbl(tbls["results_div_page"], **tbl_args)
RESULTS_ADS_TARGETING_TABLE = map_tbl(tbls["results_ads_targeting"],
                                      **tbl_args)
RESULTS_PAGE_TARGETING_TABLE = map_tbl(tbls["results_page_targeting"],
                                       **tbl_args)
RESULTS_AD_METADATA_TABLE = map_tbl(tbls["results_ad_metadata"], **tbl_args)

LOCATIONS = cfg['locations']
FIXED_CELLS = cfg['fixed_cells']
FALLOW_TRUE = cfg["fallow_control"]["true_label"]
FALLOW_FALSE = cfg["fallow_control"]["false_label"]

VALID_ASSIGNMENT_THRESHOLD = cfg['results_prm']['valid_assignment_threshold']
MASID_REFRESH_HOUR = cfg['results_prm']['masid_refresh_hour']

WEBHOOK_URL = cfg["webhooks"]["DS Warnings"]

dates_provided = True if (pargs['datestart'] and pargs['dateend']) else False

if job_env == 'prod' and not dates_provided:
    # If no date args provided, use default set of recent days
    SESSION_DATE_START = date.today() - timedelta(days=4)
    SESSION_DATE_END = date.today() - timedelta(days=2)
elif dates_provided:
    # If date args are provided (e.g. for backdating)
    ds_num = [int(x) for x in pargs['datestart'].split('-')]
    de_num = [int(x) for x in pargs['dateend'].split('-')]
    SESSION_DATE_START = date(ds_num[0], ds_num[1], ds_num[2])
    SESSION_DATE_END = date(de_num[0], de_num[1], de_num[2])
else:
    # For interactive debugging
    SESSION_DATE_START = date(2025, 2, 24)
    SESSION_DATE_END = date(2025, 2, 24)

assert SESSION_DATE_START <= SESSION_DATE_END, 'Start date after end date'
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
            'UniqueAdIDMeasurement',
            'UniqueAdIDAssigned',
            'MASID')
)

df_ad_metadata = (
    get_spark()
    .table(CONTROL_SHEET_TABLE)
    .where(F.col('rundate') >= (SESSION_DATE_START - timedelta(days=1)))
    .where(F.col('rundate') <= (SESSION_DATE_END - timedelta(days=1)))
    .withColumn('SessionDate', F.to_date(F.col('rundate') + timedelta(days=1)))
)
df_ad_metadata.cache()

# Check for missing dates (e.g. failure in scheduled run) and patch
dates_asgn = [x[0].date() for x in
              df_assignments.select('SessionDate').distinct().collect()]
dates_asgn.sort()
date_patch_asgn = check_for_missing_dates(
    SESSION_DATE_START, SESSION_DATE_END, dates_asgn)
if date_patch_asgn:
    log.warning('Missing dates found in Assignments during results period')
    missing_asgn_dates = [x[0] for x in date_patch_asgn]
    log.warning('Removing affected Assignment dates from Metadata')
    df_ad_metadata = (
        df_ad_metadata
        .where(~F.col('SessionDate').isin(missing_asgn_dates))
    )
    for date_p in date_patch_asgn:
        log.warning(f'Patching missing Assignments date {date_p[0]} ' +
                    f'in Assignments and Metadata '
                    f'with last non-missing date: {date_p[1]}')
    df_asgn_patches = patch_missing_dates(
        date_patch_asgn, df_assignments, date_col='SessionDate')
    df_meta_patches = patch_missing_dates(
        date_patch_asgn, df_ad_metadata, date_col='SessionDate')
    df_assignments = df_assignments.unionByName(df_asgn_patches)
    df_ad_metadata = df_ad_metadata.unionByName(df_meta_patches)

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


# Check for missing PF dates (e.g. failure in scheduled run) and patch
dates_pf = [x[0] for x in
            df_pf.select('SessionDate').distinct().collect()]
dates_pf.sort()
date_patch_pf = check_for_missing_dates(
    SESSION_DATE_START, SESSION_DATE_END, dates_pf)
if date_patch_pf:
    log.warning('Missing dates found in PF during results period')

    # If PF failed, but Assignments didn't Assignments will need to
    # reflect the last date that PF ran in order to match
    missing_pf_dates = [x[0] for x in date_patch_pf]
    log.warning('Removing affected PF dates from Assignments and Metadata')
    df_assignments = (
        df_assignments
        .where(~F.col('SessionDate').isin(missing_pf_dates))
    )
    df_ad_metadata = (
        df_ad_metadata
        .where(~F.col('SessionDate').isin(missing_pf_dates))
    )

    for date_p in date_patch_pf:
        log.warning(f'Patching missing PF date {date_p[0]} ' +
                    'in PF, Assignments and Metadata '
                    f'with last non-missing PF date: {date_p[1]}')

    df_pf_patches = patch_missing_dates(
        date_patch_pf, df_pf, date_col='SessionDate')
    df_assignments_patches = patch_missing_dates(
        date_patch_pf, df_assignments, date_col='SessionDate')
    df_meta_patches = patch_missing_dates(
        date_patch_pf, df_ad_metadata, date_col='SessionDate')

    df_pf = df_pf.unionByName(df_pf_patches)
    df_assignments = df_assignments.unionByName(df_assignments_patches)
    df_ad_metadata = df_ad_metadata.unionByName(df_meta_patches)


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
            post_to_webhook(WEBHOOK_URL, '\n'.join([f'{d}'] + mismatch_msgs))

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
            f'{v:,} customers assigned at least one Ad for {k} ' +
            f'but not found in PF on {k}'
        )
        log.warning(missing_msg)
        if job_env == 'prod':
            post_to_webhook(WEBHOOK_URL, missing_msg)

# Remove cases where ad has been deliberately suppressed
n_pre_supp_removal = df_asgn_pf.count()
df_asgn_pf = df_asgn_pf.where(F.col('Treatment') != 'AdSuppressed')
n_post_supp_removal = df_asgn_pf.count()
n_supp_removals = n_pre_supp_removal - n_post_supp_removal
if n_supp_removals > 0:
    msg_ad_suppressions = (
        f'{n_supp_removals:,} cases removed due to Ad Suppressions '
        + '(this may be due to tests that are currently live)'
    )
    log.warning(msg_ad_suppressions)
    if job_env == 'prod':
        post_to_webhook(WEBHOOK_URL, msg_ad_suppressions)


df_valid_assignments = (
    df_asgn_pf
    .where(F.col('MASID') == F.col('MASIDPF'))
    .drop('MASIDPF')
)

df_valid_proportions = (
    (
        df_asgn_pf
        .where(F.col('MASIDPF').isNotNull())
        .groupBy('SessionDate')
        .agg(F.count('AccountNumber').alias('Cases'))
        .orderBy('SessionDate')
    ).join(
        df_valid_assignments
        .groupBy('SessionDate')
        .agg(F.count('AccountNumber').alias('ValidCases'))
        .orderBy('SessionDate'),
        on='SessionDate', how='left'
    )
    .fillna({'ValidCases': 0})
    .withColumn('ValidCasesPC', F.col('ValidCases')/F.col('Cases'))
)

df_invalid_dates = (
    df_valid_proportions
    .where(F.col('ValidCasesPC') < VALID_ASSIGNMENT_THRESHOLD)
    .select('SessionDate')
)
invalid_dates = [x[0].strftime('%Y-%m-%d') for x in df_invalid_dates.collect()]

if invalid_dates:
    msg_invalid_dates = (
        f'Removing date(s) {", " .join(invalid_dates)} ' +
        'from results processing ' +
        f'(valid case rate < {VALID_ASSIGNMENT_THRESHOLD:.1%})'
    )

    log.warning(msg_invalid_dates)
    if job_env == 'prod':
        post_to_webhook(WEBHOOK_URL, msg_invalid_dates)

    df_valid_assignments = (
        df_valid_assignments
        .join(df_invalid_dates, on='SessionDate', how='leftanti')
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
    .where(
        (F.col('Device').isin('Desktop', 'Mobile'))
        |
        ((F.col('Device') == 'App')
         & (F.col('PagePath').isin('Home', 'Cart')))
    )
)
df_sessions_pages.cache()


# Next Ads measurement cannot currently accomodate sessions associated with
# multiple accounts - check for and remove any cases of this
df_multi_account_sessions = (
    df_sessions_pages
    .groupBy('SessionDate', 'Device', 'OS', 'UniqueVisitID')
    .agg(F.countDistinct('AccountNumber').alias('nAcc'))
    .where(F.col('nAcc') > 1)
)

n_multi_account_sessions = df_multi_account_sessions.count()

if n_multi_account_sessions > 0:
    df_sessions_pages = (
        df_sessions_pages
        .join(
            df_multi_account_sessions.select('SessionDate', 'UniqueVisitID'),
            on=['SessionDate', 'UniqueVisitID'], how='leftanti'
        )
    )
    log.warning(f'{n_multi_account_sessions:,} multi-account sessions removed')

# Next Ads measurement cannot currently accomodate sessions that span
# midnight - check for and remove any cases of this
df_sessions_spanning_midnight = (
    df_sessions_pages
    .groupBy('UniqueVisitID')
    .agg(
        F.to_date(F.min('FirstTimestamp')).alias('SessionStart'),
        F.to_date(F.max('FirstTimestamp')).alias('SessionEnd')
    )
    .where(F.col('SessionStart') != F.col('SessionEnd'))
)

n_sessions_spanning = df_sessions_spanning_midnight.count()

if n_sessions_spanning > 0:
    df_sessions_pages = (
        df_sessions_pages
        .join(
            df_sessions_spanning_midnight.select('UniqueVisitID'),
            on=['UniqueVisitID'], how='leftanti'
        )
    )
    log.warning(f'{n_sessions_spanning:,} sessions spanning midnight removed')

# Next Ads rely on the MASID to be served on site, which is refreshed
# after midnight
# To align with assignments at 'day' level, the decision has been made to
# exclude sessions starting before the refresh on a given date to minimise any
# discrepancy during measurement - check for and remove these cases
df_sessions_pre_masid = (
    df_sessions_pages
    .groupBy('UniqueVisitID')
    .agg(F.min('FirstTimestamp').alias('SessionStart'))
    .withColumn('SessionStartHour', F.hour(F.col('SessionStart')))
    .where(F.col('SessionStartHour') < MASID_REFRESH_HOUR)
)

n_sessions_pre_masid = df_sessions_pre_masid.count()

if n_sessions_pre_masid > 0:
    df_sessions_pages = (
        df_sessions_pages
        .join(
            df_sessions_pre_masid.select('UniqueVisitID'),
            on=['UniqueVisitID'], how='leftanti'
        )
    )
    log.warning(f'{n_sessions_pre_masid:,} sessions pre-MASID refresh removed')


# Remove the last Order Complete page, and any hits after it from each session
# Rationale: If would be unfair to attribute any Session value to an ad
# on Order Complete when ad is seen after session spend is committed

w_session = Window.partitionBy(
    ['SessionDate', 'UniqueVisitID'])

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
             f'{dfmt} due to OrderComplete trimming ' +
             '(i.e. sessions starting at OrderComplete page)')

df_fixed_cells = get_spark().table(FIXED_CELLS_LATEST_TABLE)

df_sessions_ads_valid = (
    df_sessions_pages_trimmed
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


df_sessions_ads_valid_clicks = (
    df_sessions_ads_valid
    .join(df_valid_assignments_mapped,
          on=['AccountNumber', 'SessionDate', 'Device', 'PagePath'],
          how='left')
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
        F.when(
            (F.col('NextPagePath') == F.col('URL'))
            & (F.col('URL').isNotNull()),
            1).otherwise(0)
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

# Prior to introduction of UniqueAdIDMeasurement (2nd Dec 2025),
# impute UniqueAdIDAssigned as UniqueAdIDMeasurement (Ads customers only)
df_sessions_master = (
    df_sessions_master
    .withColumn(
        'UniqueAdIDMeasurement',
        F.when(
            (F.col('UniqueAdIDMeasurement').isNull())
            & (F.col('FallowControl') == FALLOW_FALSE)
            & (F.col('SessionDate') < '2025-01-02')
            & (F.col('UniqueAdIDAssigned') != 'NoAd'),
            F.col('UniqueAdIDAssigned')
            ).otherwise(F.col('UniqueAdIDMeasurement'))
        )
)


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
    'AdCampaign',
    'Tags'
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
df_ad_metadata_non_loc.cache()

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
    .withColumn(
        'AlgoDivision_Brand',
        F.concat(F.col('AlgoDivision'), F.lit('_'), F.col('Brand'))
    )
)

# Remove Seasons Ads from App sessions
# This is a live exclusion
# live_exclusions variable used to modify QA checks that these exclusions
# would cause to fail
live_exclusions = True
excl_seasons_ads_app = [
    'P128_C1676_Seasons_Category_Womens_Footwear_Womens',
    'P128_C1625_Seasons_Category_Womens_Bags_Womens',
    'P128_C1626_Seasons_Solus_Womens_Womens',
    'P128_C1627_Seasons_Solus_Mens_Mens',
    'P128_C1662_Seasons_SolusBrand_Veja_Mens',
    'P128_C1662_Seasons_SolusBrand_Veja_Womens',
    'P131_C1626_Seasons_Womens_Womens_Womens',
    'P131_C1625_Seasons_Womens_Bags'
]
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('Device') == 'App')
            & (F.col('UniqueAdIDMeasurement').isin(excl_seasons_ads_app))
        )
    )
)

# Remove Homepage 'switched off' dates
list_hp_remove_dates = [
    '2024-12-12',
    '2024-12-13',
    '2024-12-14',
    '2024-12-15',
    '2024-12-16',
    '2024-12-17',
    '2024-12-18',
    '2024-12-29',
    '2024-12-30',
    '2024-12-31',
    '2025-01-01',
    '2025-01-02',
    '2025-01-30',
    '2025-01-31',
    '2025-02-01',
    '2025-02-02',
    '2025-02-03'
]
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('PageGroup') == 'HomePage')
            & (F.col('SessionDate').isin(list_hp_remove_dates))
        )
    )
)


session_level_cols = ['SessionDate', 'Device', 'OS']
w_apportion = Window.partitionBy(*session_level_cols, 'UniqueVisitID')

df_sessions_master_meta = (
    df_sessions_master_meta
    .withColumn('SessionPortions',
                F.count('*').over(w_apportion))
    .withColumn('ApportionedRevenue',
                F.col('Revenue')/F.col('SessionPortions'))
    .drop('SessionPortions')
)

df_sessions_master_meta.cache()


col_args_dict = {
    'session_id_col': 'UniqueVisitID',
    'page_id_col': 'PagePath',
    'revenue_col': 'Revenue',
    'impressions_col': 'SoftImpressions',
    'clicks_col': 'SoftClicks',
    'apportioned_revenue_col': 'ApportionedRevenue'
}

# Topline view
df_summary_device_os = summarise_sessions(
    df_sessions_master_meta,
    **col_args_dict,
    group_cols=session_level_cols + ['FallowControl']
)

df_summary_device_os_wide = (
    df_summary_device_os
    .where(F.col('FallowControl').isin(FALLOW_FALSE, FALLOW_TRUE))
    .groupBy('SessionDate', 'Device', 'OS')
    .pivot('FallowControl')
    .agg(
        F.first('Sessions').alias('Sessions'),
        F.first('Revenue').alias('Revenue'),
        F.first('Conversions').alias('Conversions'),
        F.first('SoftImpressions').alias('SoftImpressions'),
        F.first('SoftClicks').alias('SoftClicks'),
        F.first('ApportionedRevenue').alias('ApportionedRevenue')
    )
)

for c in df_summary_device_os_wide.columns:
    df_summary_device_os_wide = (
        df_summary_device_os_wide
        .withColumnRenamed(
            c,
            c.replace(f'{FALLOW_TRUE}_', 'C_').replace(f'{FALLOW_FALSE}_', ''))
    )

df_summary_device_os_wide.cache()

total_r = df_summary_device_os_wide.agg(F.sum('Revenue')).collect()[0][0]
total_apr = (
    df_summary_device_os_wide.agg(F.sum('ApportionedRevenue')).collect()[0][0]
    )
msg = 'Total of ApportionedRevenue != Total Revenue'
# 1p threshold allows for aggregation of floating point errors
assert abs(total_r - total_apr) < 0.01, msg

# Aggregate views
agg_cols = [
    'AlgoDivision',
    'TradeDivision',
    'PageGroup',
    'PagePath',
    'CampaignNumber',
    'PotNumber',
    'TemplateName',
    'Treatment',
    'AlgoDivision_Brand',
    'Segment'
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

all_tags = (
    df_ad_metadata_non_loc
    .withColumn('TagsSplit', F.split('Tags', ', '))
    .select('TagsSplit')
    .distinct()
    .where(F.col('TagsSplit').isNotNull())
).collect()

agg_tags = list(set([x for y in all_tags for x in y[0]]))

if agg_tags:
    for agg_tag in agg_tags:
        df_summary_agg_tag = summarise_sessions(
            (
                df_sessions_master_meta
                .withColumn('TagArr', F.split('Tags', ', '))
                .withColumn(
                    'Tagged',
                    F.when(
                        F.array_contains(F.col('TagArr'), agg_tag),
                        1).otherwise(0))
                .where(F.col('Tagged') == 1)
                .drop('TagArr', 'Tagged')
            ),
            **col_args_dict,
            group_cols=session_level_cols + ['FallowControl']
        )
        df_summary_agg_tag_renamed = (
            df_summary_agg_tag
            .withColumn('AggValue', F.lit(agg_tag))
            .withColumn('AggColumn', F.lit('Tagged'))
        )
        agg_summaries.append(df_summary_agg_tag_renamed)

if agg_summaries:
    df_summary_agg = agg_summaries.pop()
    while agg_summaries:
        df_summary_agg = df_summary_agg.unionByName(agg_summaries.pop())

df_summary_agg_wide = (
    df_summary_agg
    .where(F.col('FallowControl').isin(FALLOW_FALSE, FALLOW_TRUE))
    .groupBy('SessionDate', 'Device', 'OS', 'AggColumn', 'AggValue')
    .pivot('FallowControl')
    .agg(
        F.first('Sessions').alias('Sessions'),
        F.first('Revenue').alias('Revenue'),
        F.first('Conversions').alias('Conversions'),
        F.first('SoftImpressions').alias('SoftImpressions'),
        F.first('SoftClicks').alias('SoftClicks'),
        F.first('ApportionedRevenue').alias('ApportionedRevenue')
    )
)

for c in df_summary_agg_wide.columns:
    df_summary_agg_wide = (
        df_summary_agg_wide
        .withColumnRenamed(
            c,
            c.replace(f'{FALLOW_TRUE}_', 'C_').replace(f'{FALLOW_FALSE}_', ''))
    )

df_summary_agg_wide = (
    append_session_overlap_ratio(
        df_summary_device_os_wide,
        df_summary_agg_wide,
        session_level_cols,
        subtotal_window_cols=['AggColumn']
        )
)

df_summary_agg_wide.cache()

for agg_col in agg_cols:
    total_apr_agg = (
        df_summary_agg_wide
        .where(F.col('AggColumn') == agg_col)
        .agg(F.sum('ApportionedRevenue')).collect()[0][0])
    msg = f'Total of ApportionedRevenue (agg: {agg_col}) > Total Revenue'
    assert total_r*1.001 >= total_apr_agg, msg
    diff_agg = total_apr_agg - total_r
    if diff_agg < -0.01*total_r:
        msg_warn = (f'Total of ApportionedRevenue (agg: {agg_col}) more than '
                    + f'1% below Total Revenue ({diff_agg/total_r:.2%})')
        log.warning(msg_warn)
        if job_env == 'prod':
            post_to_webhook(WEBHOOK_URL, msg_warn)

if agg_tags:
    for agg_tag in agg_tags:
        total_apr_agg_tag = (
            df_summary_agg_wide
            .where(F.col('AggColumn') == 'Tagged')
            .where(F.col('AggValue') == agg_tag)
            .agg(F.sum('ApportionedRevenue')).collect()[0][0])
        msg = f'Total of ApportionedRevenue (tag: {agg_tag}) > Total Revenue'
        assert total_r*1.001 >= total_apr_agg_tag, msg


# AB test aggregates
ab_cols = [
    'HomePageTest1',
    'ShoppingBagTest1',
    'OrderCompleteTest1',
    'LandingPageTest1',
    'AdHocABTest1',
    'AdHocABTest2',
    'AdHocABTest3',
    'AdHocABTest4',
    'AdHocABTest5',
    'AdHocABTest6',
    'AdHocABTest7',
    'AdHocABTest8',
    'AdHocABTest9',
    'ChampionChallenger'
]

df_summary_ab = summarise_sessions(
        (
            df_sessions_master_meta
            .where(F.col('FallowControl') == FALLOW_FALSE)
            .join(df_fixed_cells, on='AccountNumber', how='inner')
        ),
        **col_args_dict,
        group_cols=session_level_cols + ab_cols
    )
df_summary_ab.cache()

total_apr_ab = (
    df_summary_ab.agg(F.sum('ApportionedRevenue')).collect()[0][0]
    )
msg = 'Total of ApportionedRevenue (A/B) != Total Revenue'
assert abs(total_r - total_apr_ab) < 0.001*total_r, msg


# Ad-level view
df_summary_ad = (
    summarise_sessions(
        df_sessions_master_meta,
        **col_args_dict,
        group_cols=(
            session_level_cols
            + ['FallowControl', 'UniqueAdIDMeasurement']
            )
    )
    .where(F.col('UniqueAdIDMeasurement').isNotNull())
    .withColumnRenamed('UniqueAdIDMeasurement', 'UniqueAdID')
)

df_summary_ad_wide = (
    df_summary_ad
    .where(F.col('FallowControl').isin(FALLOW_FALSE, FALLOW_TRUE))
    .groupBy('SessionDate', 'Device', 'OS', 'UniqueAdID')
    .pivot('FallowControl')
    .agg(
        F.first('Sessions').alias('Sessions'),
        F.first('Revenue').alias('Revenue'),
        F.first('Conversions').alias('Conversions'),
        F.first('SoftImpressions').alias('SoftImpressions'),
        F.first('SoftClicks').alias('SoftClicks'),
        F.first('ApportionedRevenue').alias('ApportionedRevenue')
    )
)

for c in df_summary_ad_wide.columns:
    df_summary_ad_wide = (
        df_summary_ad_wide
        .withColumnRenamed(
            c,
            c.replace(f'{FALLOW_TRUE}_', 'C_').replace(f'{FALLOW_FALSE}_', ''))
    )

df_summary_ad_wide = (
    append_session_overlap_ratio(
        df_summary_device_os_wide,
        df_summary_ad_wide,
        session_level_cols
        )
)

df_summary_ad_wide.cache()

total_apr_ad = (
    df_summary_ad_wide.agg(F.sum('ApportionedRevenue')).collect()[0][0])
msg = 'Total of ApportionedRevenue (ads) > Total Revenue'
diff_ad = total_apr_ad - total_r
assert diff_ad < 0.001*total_r, msg
if diff_ad < -0.01*total_r:
    msg_warn = ('Total of ApportionedRevenue (ads) more than 1% '
                + f'below Total Revenue ({diff_ad/total_r:.2%})')
    log.warning(msg_warn)
    if job_env == 'prod':
        post_to_webhook(WEBHOOK_URL, msg_warn)


# Ad x LocationSet view
w_visit_ad = Window.partitionBy('UniqueVisitID', 'UniqueAdIDMeasurement')
df_summary_ad_locset = (
    summarise_sessions(
        (
            df_sessions_master_meta
            .withColumn('LocationSet',
                        F.collect_set('Location').over(w_visit_ad))
        ),
        **col_args_dict,
        group_cols=(
            session_level_cols
            + ['FallowControl', 'UniqueAdIDMeasurement']
            + ['LocationSet']
            )
    )
    .where(F.col('UniqueAdIDMeasurement').isNotNull())
    .where(F.col('LocationSet').isNotNull())
    .withColumnRenamed('UniqueAdIDMeasurement', 'UniqueAdID')
    .withColumn('LocationSet',
                F.concat_ws('+', (F.array_sort(F.col('LocationSet')))))
)

df_summary_ad_locset_wide = (
    df_summary_ad_locset
    .where(F.col('FallowControl').isin(FALLOW_FALSE, FALLOW_TRUE))
    .groupBy('SessionDate', 'Device', 'OS', 'UniqueAdID', 'LocationSet')
    .pivot('FallowControl')
    .agg(
        F.first('Sessions').alias('Sessions'),
        F.first('Revenue').alias('Revenue'),
        F.first('Conversions').alias('Conversions'),
        F.first('SoftImpressions').alias('SoftImpressions'),
        F.first('SoftClicks').alias('SoftClicks'),
        F.first('ApportionedRevenue').alias('ApportionedRevenue')
    )
)

for c in df_summary_ad_locset_wide.columns:
    df_summary_ad_locset_wide = (
        df_summary_ad_locset_wide
        .withColumnRenamed(
            c,
            c.replace(f'{FALLOW_TRUE}_', 'C_').replace(f'{FALLOW_FALSE}_', ''))
    )

df_summary_ad_locset_wide = (
    append_session_overlap_ratio(
        df_summary_device_os_wide,
        df_summary_ad_locset_wide,
        session_level_cols
        )
)

df_summary_ad_locset_wide.cache()

total_apr_ad_locset = (
    df_summary_ad_locset_wide.agg(F.sum('ApportionedRevenue')).collect()[0][0])
msg = 'Total of ApportionedRevenue (ad locset) > Total Revenue'
diff_adlocset = total_apr_ad_locset - total_r
assert diff_adlocset < 0.001*total_r, msg
if diff_adlocset < -0.01*total_r:
    msg_warn = ('Total of ApportionedRevenue (ad locset) more than 1% '
                + f'below Total Revenue ({diff_adlocset/total_r:.2%})')
    log.warning(msg_warn)
    if job_env == 'prod':
        post_to_webhook(WEBHOOK_URL, msg_warn)


# Ad x PageGroupSet view
w_visit_ad = Window.partitionBy('UniqueVisitID', 'UniqueAdIDMeasurement')
df_summary_ad_pagegroupset = (
    summarise_sessions(
        (
            df_sessions_master_meta
            .withColumn('PageGroupSet',
                        F.collect_set('PageGroup').over(w_visit_ad))
        ),
        **col_args_dict,
        group_cols=(
            session_level_cols
            + ['FallowControl', 'UniqueAdIDMeasurement']
            + ['PageGroupSet']
            )
    )
    .where(F.col('UniqueAdIDMeasurement').isNotNull())
    .where(F.col('PageGroupSet').isNotNull())
    .withColumnRenamed('UniqueAdIDMeasurement', 'UniqueAdID')
    .withColumn('PageGroupSet',
                F.concat_ws('+', (F.array_sort(F.col('PageGroupSet')))))
)

df_summary_ad_pagegroupset_wide = (
    df_summary_ad_pagegroupset
    .where(F.col('FallowControl').isin(FALLOW_FALSE, FALLOW_TRUE))
    .groupBy('SessionDate', 'Device', 'OS', 'UniqueAdID', 'PageGroupSet')
    .pivot('FallowControl')
    .agg(
        F.first('Sessions').alias('Sessions'),
        F.first('Revenue').alias('Revenue'),
        F.first('Conversions').alias('Conversions'),
        F.first('SoftImpressions').alias('SoftImpressions'),
        F.first('SoftClicks').alias('SoftClicks'),
        F.first('ApportionedRevenue').alias('ApportionedRevenue')
    )
)

for c in df_summary_ad_pagegroupset_wide.columns:
    df_summary_ad_pagegroupset_wide = (
        df_summary_ad_pagegroupset_wide
        .withColumnRenamed(
            c,
            c.replace(f'{FALLOW_TRUE}_', 'C_').replace(f'{FALLOW_FALSE}_', ''))
    )

df_summary_ad_pagegroupset_wide = (
    append_session_overlap_ratio(
        df_summary_device_os_wide,
        df_summary_ad_pagegroupset_wide,
        session_level_cols
        )
)

df_summary_ad_pagegroupset_wide.cache()


total_apr_ad_pagegroupset = (
    df_summary_ad_pagegroupset_wide
    .agg(F.sum('ApportionedRevenue')).collect()[0][0])
msg = 'Total of ApportionedRevenue (ad page) > Total Revenue'
diff_ad_pgset = total_apr_ad_pagegroupset - total_r
assert diff_ad_pgset < 0.001*total_r, msg
if diff_ad_pgset < -0.01*total_r:
    msg_warn = ('Total of ApportionedRevenue (ad page) more than 1% '
                + f'below Total Revenue ({diff_ad_pgset/total_r:.2%})')
    log.warning(msg_warn)
    if job_env == 'prod':
        post_to_webhook(WEBHOOK_URL, msg_warn)

# Division X PageGroupSet view
w_visit_div = Window.partitionBy('UniqueVisitID', 'AlgoDivision')
df_summary_div_pagegroupset = (
    summarise_sessions(
        (
            df_sessions_master_meta
            .withColumn('PageGroupSet',
                        F.collect_set('PageGroup').over(w_visit_div))
        ),
        **col_args_dict,
        group_cols=(
            session_level_cols
            + ['FallowControl', 'AlgoDivision']
            + ['PageGroupSet']
            )
    )
    .where(F.col('AlgoDivision').isNotNull())
    .where(F.col('PageGroupSet').isNotNull())
    .withColumn('PageGroupSet',
                F.concat_ws('+', (F.array_sort(F.col('PageGroupSet')))))
)

df_summary_div_pagegroupset_wide = (
    df_summary_div_pagegroupset
    .where(F.col('FallowControl').isin(FALLOW_FALSE, FALLOW_TRUE))
    .groupBy('SessionDate', 'Device', 'OS', 'AlgoDivision', 'PageGroupSet')
    .pivot('FallowControl')
    .agg(
        F.first('Sessions').alias('Sessions'),
        F.first('Revenue').alias('Revenue'),
        F.first('Conversions').alias('Conversions'),
        F.first('SoftImpressions').alias('SoftImpressions'),
        F.first('SoftClicks').alias('SoftClicks'),
        F.first('ApportionedRevenue').alias('ApportionedRevenue')
    )
)

for c in df_summary_div_pagegroupset_wide.columns:
    df_summary_div_pagegroupset_wide = (
        df_summary_div_pagegroupset_wide
        .withColumnRenamed(
            c,
            c.replace(f'{FALLOW_TRUE}_', 'C_').replace(f'{FALLOW_FALSE}_', ''))
    )

df_summary_div_pagegroupset_wide = (
    append_session_overlap_ratio(
        df_summary_device_os_wide,
        df_summary_div_pagegroupset_wide,
        session_level_cols
        )
)

df_summary_div_pagegroupset_wide.cache()

total_apr_div_page = (
    df_summary_div_pagegroupset_wide
    .agg(F.sum('ApportionedRevenue')).collect()[0][0])
msg = 'Total of ApportionedRevenue (div page) > Total Revenue'
diff_divp = total_apr_div_page - total_r
assert diff_divp < 0.001*total_r, msg
if diff_divp < -0.01*total_r:
    msg_warn = ('Total of ApportionedRevenue (div page) more than 1% '
                + f'below Total Revenue ({diff_divp/total_r:.2%})')
    log.warning(msg_warn)
    if job_env == 'prod':
        post_to_webhook(WEBHOOK_URL, msg_warn)


# Ad-Targeting view
df_summary_ad_targeting = (
    summarise_sessions(
        (
            df_sessions_master_meta
            .withColumn('Targeting',
                        F.when(
                            F.col('FallowControl') == FALLOW_TRUE,
                            F.lit('Control')
                            ).otherwise(F.col('Treatment'))
                        )
        ),
        **col_args_dict,
        group_cols=(
            session_level_cols
            + ['UniqueAdIDMeasurement', 'PageGroup', 'Targeting']
            )
    )
    .where(F.col('UniqueAdIDMeasurement').isNotNull())
    .where(F.col('PageGroup').isNotNull())
    .where(F.col('Targeting').isNotNull())
    .withColumnRenamed('UniqueAdIDMeasurement', 'UniqueAdID')
)

df_summary_ad_targeting.cache()

total_apr_adtgt = (
    df_summary_ad_targeting
    .where(F.col('Targeting') != 'Control')
    .agg(F.sum('ApportionedRevenue')).collect()[0][0]
    )
msg = 'Total of ApportionedRevenue (ad tgt) > Total Revenue'
diff_adtgt = total_apr_adtgt - total_r
assert diff_adtgt < 0.001*total_r, msg
if diff_adtgt < -0.01*total_r:
    msg_warn = ('Total of ApportionedRevenue (ad tgt) more than 1% '
                + f'below Total Revenue ({diff_adtgt/total_r:.2%})')
    log.warning(msg_warn)
    if job_env == 'prod':
        post_to_webhook(WEBHOOK_URL, msg_warn)


# Page-Targeting view
df_summary_page_targeting = (
    summarise_sessions(
        (
            df_sessions_master_meta
            .withColumn('Targeting',
                        F.when(
                            F.col('FallowControl') == FALLOW_TRUE,
                            F.lit('Control')
                            ).otherwise(F.col('Treatment'))
                        )
        ),
        **col_args_dict,
        group_cols=(
            session_level_cols
            + ['PageGroup', 'Targeting']
            )
    )
    .where(F.col('PageGroup').isNotNull())
    .where(F.col('Targeting').isNotNull())
)

df_summary_page_targeting.cache()

total_apr_pagetgt = (
    df_summary_page_targeting
    .where(F.col('Targeting') != 'Control')
    .agg(F.sum('ApportionedRevenue')).collect()[0][0]
    )
msg = 'Total of ApportionedRevenue (page tgt) > Total Revenue'
diff_pagetgt = total_apr_pagetgt - total_r
assert diff_pagetgt < 0.001*total_r, msg
if diff_pagetgt < -0.01*total_r:
    msg_warn = ('Total of ApportionedRevenue (page tgt) more than 1% '
                + f'below Total Revenue ({diff_pagetgt/total_r:.2%})')
    log.warning(msg_warn)
    if job_env == 'prod':
        post_to_webhook(WEBHOOK_URL, msg_warn)


# Create additional filters for ads table
w_date_ad = Window.partitionBy('SessionDate', 'UniqueAdID')

# EligibleLocations (i.e. those toggled 'on' in the control sheet)
df_ad_elig_locs = (
    df_ad_metadata
    .withColumn('EligibleLocations',
                F.collect_set(F.col('Location')).over(w_date_ad))
    .select('SessionDate', 'UniqueAdID', 'EligibleLocations')
    .distinct()
    .withColumn('EligibleLocations',
                F.concat_ws(' ', (F.array_sort(F.col('EligibleLocations')))))
)
assert_pk(df_ad_elig_locs, pk_cols=['SessionDate', 'UniqueAdID'])
df_ad_elig_locs.cache()

# ServedLocations (those where the ad was actually served)
df_ad_served_locs = (
    df_sessions_master_meta
    .where(F.col('UniqueAdIDMeasurement').isNotNull())
    .withColumnRenamed('UniqueAdIDMeasurement', 'UniqueAdID')
    .withColumn('ServedLocations',
                F.collect_set(F.col('Location')).over(w_date_ad))
    .select('SessionDate', 'UniqueAdID', 'ServedLocations')
    .distinct()
    .withColumn('ServedLocations',
                F.concat_ws(' ', (F.array_sort(F.col('ServedLocations')))))
)
assert_pk(df_ad_served_locs, pk_cols=['SessionDate', 'UniqueAdID'])
df_ad_served_locs.cache()

df_ad_metadata_full = (
    df_ad_metadata_non_loc
    .join(df_ad_elig_locs,
          on=['SessionDate', 'UniqueAdID'], how='left')
    .join(df_ad_served_locs,
          on=['SessionDate', 'UniqueAdID'], how='left')
)
assert_pk(df_ad_metadata_full, pk_cols=['SessionDate', 'UniqueAdID'])
df_ad_metadata_full.cache()


# Not running this check when dates are provided means check is bypassed when
# results are being backdated. This means that dates/sessions can be removed
# from the dashboard retrospectively when known operational issues may
# have biased the results (e.g. MASID or HomePage interruptions). These
# adjustments would otherwise trigger the AsssertionError
# Same is true if there are live exclusions (e.g. excluding Seasons from App)
if not dates_provided and not live_exclusions:
    for d in sdates_valid:
        d_fmt = d.strftime('%Y-%m-%d')
        log.info('Checking consistency of pre- and post-processing ' +
                 f'totals for SessionDate: {d_fmt}')
        for fc in [FALLOW_FALSE, FALLOW_TRUE]:
            for c in ['Sessions', 'Revenue']:
                tpre = (
                    df_results_topline
                    .where(F.col('SessionDate') == d)
                    .where(F.col('FallowControl') == fc)
                    .groupBy('SessionDate', 'FallowControl')
                    .agg(F.sum(c).alias(c))
                    .select(c)
                    ).collect()[0][0]
                if fc == FALLOW_TRUE:
                    c_piv = 'C_' + c
                else:
                    c_piv = c
                tpost = (
                    df_summary_device_os_wide
                    .where(F.col('SessionDate') == d)
                    .groupBy('SessionDate')
                    .agg(F.sum(c_piv).alias(c_piv))
                    .select(c_piv)
                    ).collect()[0][0]
                # Check match to < 0.01 to allow for floating point arithmetic
                msg = (f'Pre- and post- total for {c} does not match for {fc} '
                       + f'(pre: {tpre:,}; post: {tpost:,}; '
                       + f'change: {tpost-tpre:,})')
                assert abs(tpost - tpre) < 0.01, msg


if job_env == 'prod':
    for d in sdates_valid:
        d_fmt = "\'" + d.strftime('%Y-%m-%d') + "\'"

        log.info(f'Loading results_topline for {d_fmt} ' +
                 f'to table: {RESULTS_TOPLINE_TABLE}')
        delete_from_and_load(
            (
                df_summary_device_os_wide
                .where(F.col('SessionDate') == d)
                .select('SessionDate',
                        'Device',
                        'OS',
                        'Sessions',
                        'Revenue',
                        'Conversions',
                        'SoftImpressions',
                        'SoftClicks',
                        'C_Sessions',
                        'C_Revenue',
                        'C_Conversions',
                        'C_SoftImpressions',
                        'C_SoftClicks')
            ),
            RESULTS_TOPLINE_TABLE,
            pk_cols=['SessionDate', 'Device', 'OS'],
            del_where={'SessionDate': d_fmt}
        )

        log.info(f'Loading results_aggregated for {d_fmt} ' +
                 f'to table: {RESULTS_AGGREGATED_TABLE}')
        delete_from_and_load(
            (
                df_summary_agg_wide
                .where(F.col('SessionDate') == d)
                .select('SessionDate',
                        'Device',
                        'OS',
                        'AggColumn',
                        'AggValue',
                        'Sessions',
                        'Revenue',
                        'Conversions',
                        'SoftImpressions',
                        'SoftClicks',
                        'ApportionedRevenue',
                        'C_Sessions',
                        'C_Revenue',
                        'C_Conversions',
                        'C_SoftImpressions',
                        'C_SoftClicks',
                        'C_ApportionedRevenue',
                        'SessionOverlapRatio')
            ),
            RESULTS_AGGREGATED_TABLE,
            pk_cols=['SessionDate', 'Device', 'OS',
                     'AggColumn', 'AggValue'],
            del_where={'SessionDate': d_fmt}
        )

        log.info(f'Loading results_ab for {d_fmt} ' +
                 f'to table: {RESULTS_AB_TABLE}')
        delete_from_and_load(
            (
                df_summary_ab
                .where(F.col('SessionDate') == d)
                .select('SessionDate',
                        'Device',
                        'OS',
                        *ab_cols,
                        'Sessions',
                        'Revenue',
                        'Conversions',
                        'SoftImpressions',
                        'SoftClicks')
            ),
            RESULTS_AB_TABLE,
            pk_cols=['SessionDate', 'Device', 'OS', *ab_cols],
            del_where={'SessionDate': d_fmt}
        )

        log.info(f'Loading results_ads for {d_fmt} ' +
                 f'to table: {RESULTS_ADS_TABLE}')
        delete_from_and_load(
            (
                df_summary_ad_wide
                .where(F.col('SessionDate') == d)
                .select('SessionDate',
                        'Device',
                        'OS',
                        'UniqueAdID',
                        'Sessions',
                        'Revenue',
                        'Conversions',
                        'SoftImpressions',
                        'SoftClicks',
                        'ApportionedRevenue',
                        'C_Sessions',
                        'C_Revenue',
                        'C_Conversions',
                        'C_SoftImpressions',
                        'C_SoftClicks',
                        'C_ApportionedRevenue',
                        'SessionOverlapRatio')
            ),
            RESULTS_ADS_TABLE,
            pk_cols=['SessionDate', 'Device', 'OS', 'UniqueAdID'],
            del_where={'SessionDate': d_fmt}
        )

        log.info(f'Loading results_ads_location for {d_fmt} ' +
                 f'to table: {RESULTS_ADS_LOCATION_TABLE}')
        delete_from_and_load(
            (
                df_summary_ad_locset_wide
                .where(F.col('SessionDate') == d)
                .select('SessionDate',
                        'Device',
                        'OS',
                        'UniqueAdID',
                        'LocationSet',
                        'Sessions',
                        'Revenue',
                        'Conversions',
                        'SoftImpressions',
                        'SoftClicks',
                        'ApportionedRevenue',
                        'C_Sessions',
                        'C_Revenue',
                        'C_Conversions',
                        'C_SoftImpressions',
                        'C_SoftClicks',
                        'C_ApportionedRevenue',
                        'SessionOverlapRatio')
            ),
            RESULTS_ADS_LOCATION_TABLE,
            pk_cols=['SessionDate', 'Device', 'OS',
                     'UniqueAdID', 'LocationSet'],
            del_where={'SessionDate': d_fmt}
        )

        log.info(f'Loading results_ads_page for {d_fmt} ' +
                 f'to table: {RESULTS_ADS_PAGE_TABLE}')
        delete_from_and_load(
            (
                df_summary_ad_pagegroupset_wide
                .where(F.col('SessionDate') == d)
                .select('SessionDate',
                        'Device',
                        'OS',
                        'UniqueAdID',
                        'PageGroupSet',
                        'Sessions',
                        'Revenue',
                        'Conversions',
                        'SoftImpressions',
                        'SoftClicks',
                        'ApportionedRevenue',
                        'C_Sessions',
                        'C_Revenue',
                        'C_Conversions',
                        'C_SoftImpressions',
                        'C_SoftClicks',
                        'C_ApportionedRevenue',
                        'SessionOverlapRatio')
            ),
            RESULTS_ADS_PAGE_TABLE,
            pk_cols=['SessionDate', 'Device', 'OS',
                     'UniqueAdID', 'PageGroupSet'],
            del_where={'SessionDate': d_fmt}
        )

        log.info(f'Loading results_div_page for {d_fmt} ' +
                 f'to table: {RESULTS_DIV_PAGE_TABLE}')
        delete_from_and_load(
            (
                df_summary_div_pagegroupset_wide
                .where(F.col('SessionDate') == d)
                .select('SessionDate',
                        'Device',
                        'OS',
                        'AlgoDivision',
                        'PageGroupSet',
                        'Sessions',
                        'Revenue',
                        'Conversions',
                        'SoftImpressions',
                        'SoftClicks',
                        'ApportionedRevenue',
                        'C_Sessions',
                        'C_Revenue',
                        'C_Conversions',
                        'C_SoftImpressions',
                        'C_SoftClicks',
                        'C_ApportionedRevenue',
                        'SessionOverlapRatio')
            ),
            RESULTS_DIV_PAGE_TABLE,
            pk_cols=['SessionDate', 'Device', 'OS',
                     'AlgoDivision', 'PageGroupSet'],
            del_where={'SessionDate': d_fmt}
        )

        log.info(f'Loading results_ads_targeting for {d_fmt} ' +
                 f'to table: {RESULTS_ADS_TARGETING_TABLE}')
        delete_from_and_load(
            (
                df_summary_ad_targeting
                .where(F.col('SessionDate') == d)
                .select('SessionDate',
                        'Device',
                        'OS',
                        'UniqueAdID',
                        'PageGroup',
                        'Targeting',
                        'Sessions',
                        'Revenue',
                        'Conversions',
                        'SoftImpressions',
                        'SoftClicks')
            ),
            RESULTS_ADS_TARGETING_TABLE,
            pk_cols=['SessionDate', 'Device', 'OS',
                     'UniqueAdID', 'PageGroup', 'Targeting'],
            del_where={'SessionDate': d_fmt}
        )

        log.info(f'Loading results_page_targeting for {d_fmt} ' +
                 f'to table: {RESULTS_PAGE_TARGETING_TABLE}')
        delete_from_and_load(
            (
                df_summary_page_targeting
                .where(F.col('SessionDate') == d)
                .select('SessionDate',
                        'Device',
                        'OS',
                        'PageGroup',
                        'Targeting',
                        'Sessions',
                        'Revenue',
                        'Conversions',
                        'SoftImpressions',
                        'SoftClicks')
            ),
            RESULTS_PAGE_TARGETING_TABLE,
            pk_cols=['SessionDate', 'Device', 'OS',
                     'PageGroup', 'Targeting'],
            del_where={'SessionDate': d_fmt}
        )

        log.info(f'Loading results_ad_metadata for {d_fmt} ' +
                 f'to table: {RESULTS_AD_METADATA_TABLE}')
        delete_from_and_load(
            (
                df_ad_metadata_full
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
                        'AdCampaign',
                        'Tags',
                        'EligibleLocations',
                        'ServedLocations')
            ),
            RESULTS_AD_METADATA_TABLE,
            pk_cols=['SessionDate', 'UniqueAdID'],
            del_where={'SessionDate': d_fmt}
        )

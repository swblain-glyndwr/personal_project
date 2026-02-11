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
from datetime import date, timedelta
from pyspark.sql import functions as F
from pyspark.sql import Window
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (assert_pk,
                         build_spark_schema,
                         map_tbl,
                         post_to_webhook)
from dsutils.argparser import get_job_parser
from next_ads.Results import (check_for_missing_dates,
                              patch_missing_dates,
                              validate_assignments_match_pf)


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

logger.info(f"Configuring run for client: {CLIENT}")
with open(PROJECT_ROOT / f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

DATESTART = jobparser.get_typed_arg('--datestart', str)
DATEEND = jobparser.get_typed_arg('--dateend', str)
HISTORY_CELLS_DATE = jobparser.get_arg('--history_cells_from_date')
if HISTORY_CELLS_DATE:
    logger.info(
        f'Use of fixed customer cells from {HISTORY_CELLS_DATE} requested')

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"]["prod"]
logger.info(f'Read schema set to {SCHEMA} (always read from prod for results)')

# Map write schema to parameterised read table names
tbl_args = {'schema': SCHEMA, 'client': CLIENT}
FIXED_CELLS_LATEST_TABLE = map_tbl(tbls["customer_cells_fixed_latest"],
                                   **tbl_args)
FIXED_CELLS_HISTORY_TABLE = map_tbl(tbls["customer_cells_fixed_history"],
                                    **tbl_args)
ASSIGNMENTS_TABLE = map_tbl(tbls["assignments"], **tbl_args)
TRANSIENT_CELLS_TABLE = map_tbl(tbls["customer_cells_transient"],
                                **tbl_args)
CONTROL_SHEET_TABLE = map_tbl(tbls["control_sheet"], **tbl_args)
MULTIPAGE_LOCATIONS_TABLE = map_tbl(tbls["multipage_locations"], **tbl_args)

# Get read tables
RPID_WITH_ACCOUNTS = cfg["tables"]["read"]["rpid_with_accounts"]
PREFERENCE_FRAMEWORK = cfg["tables"]["read"]["preference_framework"]
BQ_SESSIONS = cfg["tables"]["read"]['bq_sessions']
BQ_SESSIONS_APP = cfg["tables"]["read"]['bq_sessions_app']
BQ_PAGES = cfg["tables"]["read"]['bq_pages']
BQ_SCREENS = cfg["tables"]["read"]['bq_screens']

CREDIT_AD_ACCOUNTS_TABLE = cfg["tables"]["read"]['credit_ad_accounts']

LOCATIONS = cfg['locations']
FIXED_CELLS = cfg['fixed_cells']
FALLOW_TRUE = cfg["fallow_control"]["true_label"]
FALLOW_FALSE = cfg["fallow_control"]["false_label"]

VALID_ASSIGNMENT_THRESHOLD = cfg['results_prm']['valid_assignment_threshold']
MASID_REFRESH_HOUR = cfg['results_prm']['masid_refresh_hour']

WEBHOOK_URL = cfg["webhooks"]["DS Warnings"]

TMP_RESULTS_LOCATION = f'{cfg["dbfs_base_path"]}/{JOB_ENV}/tmp'

dates_provided = True if (DATESTART and DATEEND) else False

if JOB_ENV.lower() != 'dev' and not dates_provided:
    # If no date args provided, use default set of recent days
    OFFSET_START_DAYS = cfg['results_prm']['default_offset_start']
    OFFSET_END_DAYS = cfg['results_prm']['default_offset_end']
    SESSION_DATE_START = date.today() - timedelta(days=OFFSET_START_DAYS)
    SESSION_DATE_END = date.today() - timedelta(days=OFFSET_END_DAYS)
    logger.info('No dates provided to job')
    logger.info('Default date offsets assumed')
    logger.info(f'Running from {SESSION_DATE_START} to {SESSION_DATE_END}')
elif dates_provided:
    # If date args are provided (e.g. for backdating)
    ds_num = [int(x) for x in DATESTART.split('-')]
    de_num = [int(x) for x in DATEEND.split('-')]
    SESSION_DATE_START = date(ds_num[0], ds_num[1], ds_num[2])
    SESSION_DATE_END = date(de_num[0], de_num[1], de_num[2])
    logger.info('Start Date and End Date provided')
    logger.info(f'Running from {SESSION_DATE_START} to {SESSION_DATE_END})')
else:
    # For interactive debugging
    SESSION_DATE_START = date(2026, 1, 21)
    SESSION_DATE_END = date(2026, 1, 22)
    logger.warning(
        f'Start Date not specified (defaulting to {SESSION_DATE_START})')
    logger.warning(
        f'End Date not specified (defaulting to {SESSION_DATE_END})')

assert SESSION_DATE_START <= SESSION_DATE_END, 'Start date after end date'
ndays = (SESSION_DATE_END - SESSION_DATE_START).days + 1
sdates = [SESSION_DATE_END - timedelta(days=x) for x in range(ndays)]
sdates.sort()

logger.info(
    f'Processing results from {SESSION_DATE_START} to {SESSION_DATE_END}')


loc2pf = dict()
for k in LOCATIONS:
    if 'pf_col' in LOCATIONS[k]:
        loc2pf[k] = LOCATIONS[k]['pf_col']

pf2loc = {v: k for k, v in loc2pf.items()}
pf_cols = list(pf2loc.keys())

# Assignments run the evening before, therefore SessionDate is rundate + 1 day
df_assignments = (
    spark
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
    spark
    .table(CONTROL_SHEET_TABLE)
    .where(F.col('rundate') >= (SESSION_DATE_START - timedelta(days=1)))
    .where(F.col('rundate') <= (SESSION_DATE_END - timedelta(days=1)))
    .withColumn('SessionDate', F.to_date(F.col('rundate') + timedelta(days=1)))
)

# add PLX PagePaths and Screen to control sheet data
df_multipage_lookup = (
    spark
    .table(MULTIPAGE_LOCATIONS_TABLE)
    .where(F.col('rundate') >= (SESSION_DATE_START - timedelta(days=1)))
    .where(F.col('rundate') <= (SESSION_DATE_END - timedelta(days=1)))
    .withColumn('SessionDate', F.to_date(F.col('rundate') + timedelta(days=1)))
)

# Force deletion of SessionDate 29th May from ads tables
# MASID failed on 29th, but was not re-run, so need to force copying forward
# of ads tables from previous rundate (these will have inhereted from the
# previous day under the new MASID process)
# Note: This will cause the run to fail if 29th May is re-run in isolation
# i.e. without 28th May also being included in the date range
df_assignments = df_assignments.where(F.col('SessionDate') != '2025-05-29')
df_ad_metadata = df_ad_metadata.where(F.col('SessionDate') != '2025-05-29')

# Force deletion of SessionDate 30th Jan 2026 from ads tables
# Assignment job failed with rundate 2026-01-29, so no assignments exist for
# SessionDate 2026-01-30 (SessionDate = rundate + 1). MASID served almost all
# 'Z' (NoAd) with no valid test/control split. Results processing would fail
# and even if it ran, data would be invalid.
df_assignments = df_assignments.where(F.col('SessionDate') != '2026-01-30')
df_ad_metadata = df_ad_metadata.where(F.col('SessionDate') != '2026-01-30')

# remove PLP locations prior to 20th Sept (pre-launch period)
plp_locs = ['PL' + str(num) for num in range(1, 62, 1)]
df_assignments = (
    df_assignments
    .where(~(F.col('Location').isin(plp_locs)
             & (F.col('SessionDate') < '2025-09-20')))
)
df_ad_metadata = (
    df_ad_metadata
    .where(~(F.col('Location').isin(plp_locs)
             & (F.col('rundate') < '2025-09-19')))
)

loc2page = {}
loc2screen = {}

# get temporal page mappings and screen mappings from ad metadata
page_mappings = (
    df_ad_metadata
    .select('SessionDate', 'Location', 'Page')
    .filter(F.col('Page').isNotNull())
    .distinct()
    .collect()
)
for row in page_mappings:
    key = (row['SessionDate'], row['Location'])
    loc2page[key] = row['Page']

screen_mappings = (
    df_ad_metadata
    .select('SessionDate', 'Location', 'Screen')
    .filter(F.col('Screen').isNotNull())
    .distinct()
    .collect()
)
for row in screen_mappings:
    key = (row['SessionDate'], row['Location'])
    loc2screen[key] = row['Screen']

# Create OC pagepaths dynamically from available mappings
oc_pagepaths = []
for (session_date, location), page in loc2page.items():
    if location == 'OC1':
        oc_pagepaths.append(page)
for (session_date, location), screen in loc2screen.items():
    if location == 'OC1':
        oc_pagepaths.append(screen)
oc_pagepaths = list(set(oc_pagepaths))  # Remove duplicates

# Remove Homepage assignments for SessionDate 22nd-27th Aug
# (inadvertently left switched off)
df_assignments = (
    df_assignments
    .where(
        ~(
            (F.col('SessionDate') >= '2025-08-22')
            & (F.col('SessionDate') <= '2025-08-27')
            & (F.col('Location').startswith('PH'))
        )
    )
)
df_ad_metadata = (
    df_ad_metadata
    .where(
        ~(
            (F.col('SessionDate') >= '2025-08-22')
            & (F.col('SessionDate') <= '2025-08-27')
            & (F.col('Location').startswith('PH'))
        )
    )
)

df_ad_metadata.cache()

# Check for missing dates (e.g. failure in scheduled run) and patch
dates_asgn = [x[0].date() for x in
              df_assignments.select('SessionDate').distinct().collect()]
dates_asgn.sort()
date_patch_asgn = check_for_missing_dates(
    SESSION_DATE_START, SESSION_DATE_END, dates_asgn)
if date_patch_asgn:
    logger.warning('Missing dates found in Assignments during results period')
    missing_asgn_dates = [x[0] for x in date_patch_asgn]
    logger.warning('Removing affected Assignment dates from Metadata')
    df_ad_metadata = (
        df_ad_metadata
        .where(~F.col('SessionDate').isin(missing_asgn_dates))
    )
    for date_p in date_patch_asgn:
        logger.warning(f'Patching missing Assignments date {date_p[0]} ' +
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
    spark
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
    logger.warning('Missing dates found in PF during results period')

    # If PF failed, but Assignments didn't Assignments will need to
    # reflect the last date that PF ran in order to match
    missing_pf_dates = [x[0] for x in date_patch_pf]
    logger.warning('Removing affected PF dates from Assignments and Metadata')
    df_assignments = (
        df_assignments
        .where(~F.col('SessionDate').isin(missing_pf_dates))
    )
    df_ad_metadata = (
        df_ad_metadata
        .where(~F.col('SessionDate').isin(missing_pf_dates))
    )

    for date_p in date_patch_pf:
        logger.warning(f'Patching missing PF date {date_p[0]} ' +
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
        logger.warning(f'Mismatches in MASID found for SessionDate: {d}')
        for msg in mismatch_msgs:
            logger.warning(msg)
        if JOB_ENV == 'prod':
            post_to_webhook(WEBHOOK_URL, '\n'.join([f'{d}'] + mismatch_msgs))

df_asgn_pf_nulls = (
    df_asgn_pf
    .where(F.col('MASIDPF').isNull())
    .groupBy('SessionDate', 'Location')
    .agg(F.countDistinct('AccountNumber').alias('Accounts'))
)

if df_asgn_pf_nulls.count() > 0:
    df_nulls_dict = {f'{x[0].strftime("%Y-%m-%d")} ({x[1]})': x[2]
                     for x in df_asgn_pf_nulls.collect()}
    for k, v in df_nulls_dict.items():
        missing_msg = (
            f'{k}: {v:,} customers assigned an Ad but not found in PF'
        )
        if missing_msg:
            logger.warning(missing_msg)
            if JOB_ENV == 'prod':
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
    logger.warning(msg_ad_suppressions)
    if JOB_ENV == 'prod':
        post_to_webhook(WEBHOOK_URL, msg_ad_suppressions)

# Remove cases where no ad was found for customer (both test and control)
n_pre_naf_removal = df_asgn_pf.count()
df_asgn_pf = df_asgn_pf.where(F.col('UniqueAdIDMeasurement') != 'NoAdFound')
n_post_naf_removal = df_asgn_pf.count()
n_naf_removals = n_pre_naf_removal - n_post_naf_removal
if n_naf_removals > 0:
    msg_noadfound = (
        f'{n_naf_removals:,} cases removed due to "NoAdFound" '
        + '(this may be due to tests that are currently live)'
    )
    logger.warning(msg_noadfound)
    if JOB_ENV == 'prod':
        post_to_webhook(WEBHOOK_URL, msg_noadfound)


df_valid_assignments = (
    df_asgn_pf
    .where(F.col('MASID') == F.col('MASIDPF'))
    .drop('MASIDPF')
)

# Invalid teasers stripped from the assignments as part of read into MASID
# These cases are excluded from the results because:
# MASID (assignments) != MASIDPF (pf)
# This step does not exclude these cases in the control group however; if
# someone only gets one teaser in the control group, their teasers are set to
# three Zs anyway, which is a valid teaser assignment
# This is why the invalid teaser removal is replicated here, but based on
# UniqueAdIDMeasurement, so as to account for this removal in the control group

teaser_locs = ['PH3', 'PH4']
teaser_locs_fmt = ["'" + tl + "'" for tl in teaser_locs]
w_dt_acc = Window.partitionBy('SessionDate', 'AccountNumber')

df_invalid_teasers_adid = (
    df_valid_assignments
    .where(F.col('Location').isin(teaser_locs))
    .withColumn(
        'TeaserAssigned',
        F.when(
            F.col('UniqueAdIDMeasurement').isin('AdSuppressed', 'NoAdFound'),
            F.lit(0)
            ).otherwise(F.lit(1))
        )
    .withColumn('TeasersAssigned', F.sum('TeaserAssigned').over(w_dt_acc))
    .drop('TeaserAssigned')
    .withColumn('AdSet',
                F.collect_set(F.col('UniqueAdIDMeasurement')).over(w_dt_acc))
    .withColumn('UniqueAds', F.array_size('AdSet'))
    .where(
        (F.col('TeasersAssigned') < len(teaser_locs))
        | (F.col('UniqueAds') < len(teaser_locs))
        )
    .where(F.col('AdSet') != F.array(F.lit('AdSuppressed')))
    .where(F.col('AdSet') != F.array(F.lit('NoAdFound')))
)

df_invalid_teaser_accounts = (
    df_invalid_teasers_adid
    .select('SessionDate', 'AccountNumber')
    .distinct()
)

n_it = df_invalid_teaser_accounts.count()
if n_it > 0:
    for sdate in sdates:
        n_it_sdate = (
            df_invalid_teaser_accounts
            .where(F.col('SessionDate') == sdate)
        ).count()
        msg_it = (f'{n_it_sdate:,} accounts found with invalid HomePage '
                  + f'Teasers while processing results for {sdate}; '
                  + 'removing affected cases from valid assignments')
        logger.warning(msg_it)
        if JOB_ENV == "prod":
            post_to_webhook(WEBHOOK_URL, msg_it)

    df_teaser_locs = (
        spark.createDataFrame(
            data=[tuple([x]) for x in teaser_locs],
            schema=build_spark_schema(
                [['Location', 'string', 'not null']]
            )
        )
    )

    df_invalid_teasers_rm = (
        df_invalid_teaser_accounts
        .crossJoin(df_teaser_locs)
        .withColumn('IT', F.lit(1))
    )

    df_valid_assignments = (
        df_valid_assignments
        .join(df_invalid_teasers_rm,
              on=['SessionDate', 'AccountNumber', 'Location'],
              how='left')
        .where(F.col('IT').isNull())
        .drop('IT')
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
    .select('SessionDate', 'ValidCasesPC')
)
invalid_dates = [
    (x[0].strftime('%Y-%m-%d'), x[1]) for x in df_invalid_dates.collect()
    ]

if invalid_dates:
    for invalid_date in invalid_dates:
        msg_invalid_dates = (
            f'Removing {invalid_date[0]} from results processing '
            + f'as valid case rate ({invalid_date[1]:.1%}) '
            + f'< threshold ({VALID_ASSIGNMENT_THRESHOLD:.1%})'
        )

        logger.warning(msg_invalid_dates)
        if JOB_ENV == 'prod':
            post_to_webhook(WEBHOOK_URL, msg_invalid_dates)

    df_valid_assignments = (
        df_valid_assignments
        .join(df_invalid_dates.select('SessionDate'),
              on='SessionDate', how='leftanti')
    )


sdates_valid = [
    x[0].date() for x in
    df_valid_assignments.select('SessionDate').distinct().collect()
    ]
assert sdates_valid, f'No valid sessions found for any dates between {SESSION_DATE_START} and {SESSION_DATE_END}'  # noqa
sdates_valid.sort()

sdates_missing = list(set(sdates).difference(set(sdates_valid)))
if sdates_missing:
    logger.warning('No valid assignments found for dates: ' +
                   f'{[x.strftime("%Y-%m-%d") for x in sdates_missing]}')

# Get pages visited, limiting to pages showing Ads on given SessionDate
df_days_locations = (
    df_valid_assignments
    .select('SessionDate', 'Location')
    .distinct()
)

# Expand df_ad_metadata to accommodate multi-page/screen Locations
df_ad_metadata_mult = (
    df_ad_metadata
    .join(
        (
            df_multipage_lookup
            .select(
                'SessionDate',
                'Location',
                F.col('Page').alias('mult_page'),
                F.col('Screen').alias('mult_screen')
            )
        ),
        on=['SessionDate', 'Location'],
        how='left'
        )
    .withColumn('Page', F.coalesce(F.col('mult_page'), F.col('Page')))
    .withColumn('Screen', F.coalesce(F.col('mult_screen'), F.col('Screen')))
    .drop('mult_page', 'mult_screen')
)

# Create date-aware page mappings for filtering
df_page_mapping = (
    df_ad_metadata_mult
    .select('SessionDate', 'Location', F.col('Page').alias('PagePath'))
    .filter(F.col('PagePath').isNotNull())
    .distinct()
)

df_days_pages = (
    df_days_locations
    .join(df_page_mapping, on=['SessionDate', 'Location'], how='inner')
    .select('SessionDate', 'PagePath')
    .distinct()
)

# Create date-aware screen mappings for filtering
df_screen_mapping = (
    df_ad_metadata_mult
    .withColumn(
        'Screen',
        F.when(
            F.col('Screen') == 'PLP',
            F.col('Page')).otherwise(F.col('Screen'))
    )
    .select('SessionDate', 'Location', F.col('Screen').alias('PagePath'))
    .filter(F.col('PagePath').isNotNull())
    .filter(F.col('PagePath') != '')
    .distinct()
)

df_days_screens = (
    df_days_locations
    .join(df_screen_mapping, on=['SessionDate', 'Location'], how='inner')
    .select('SessionDate', 'PagePath')
    .distinct()
)

df_pages = (
    spark
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
            spark
            .table(BQ_SCREENS)
            .where(F.col('date') >= SESSION_DATE_START)
            .where(F.col('date') <= SESSION_DATE_END)
            .withColumn('NextPagePath', F.lit(None).cast('string'))
            .withColumn(
                'ScreenName',
                F.when(
                    F.col('ScreenName') == 'PLP',
                    F.col('PagePath')).otherwise(F.col('ScreenName'))
            )
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
).distinct()
assert df_pages.count() > 0, (
    'No broswing data (pages) found between' +
    f' {SESSION_DATE_START} and {SESSION_DATE_END}'
    f' in table {BQ_PAGES}')

# Get session revenue
df_sessions = (
    spark
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
        spark
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
        spark
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
assert df_sessions.count() > 0, (
    'No broswing data (sessions) found between' +
    f' {SESSION_DATE_START} and {SESSION_DATE_END}'
    f' in table {BQ_SESSIONS}')

# Filter by device and page/screen to reflect where ads can be served
# TODO - reference a table in the control sheet where trade can toggle
# device and page/screen combinations on/off rather than hard-coding here
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
    logger.warning(
        f'{n_multi_account_sessions:,} multi-account sessions removed')

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
    logger.warning(
        f'{n_sessions_spanning:,} sessions spanning midnight removed')

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
    logger.warning(
        f'{n_sessions_pre_masid:,} sessions pre-MASID refresh removed')


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
    logger.info(f'{nsdiff:,} sessions dropped from ' +
                f'{dfmt} due to OrderComplete trimming ' +
                '(i.e. sessions starting at OrderComplete page)')

last_control_refresh = (
    spark
    .table(FIXED_CELLS_HISTORY_TABLE)
    .select('RunDateEnd')
    .distinct()
    .agg(F.max('RunDateEnd').alias('last_refresh_date'))
).collect()[0][0]

# Add one day to the last refresh date to account for rundate being
# one day before the corresponding session date
last_control_refresh += timedelta(days=1)

# Which of the valid session dates pre-date the last control refresh
history_dates = [x for x in sdates_valid if x <= last_control_refresh]

# If there are any history date and no history cells date has been provided
# stop execution as a failsafe
if history_dates and not HISTORY_CELLS_DATE:
    logger.error(
        'At least one requested date pre-dates the last control refresh' +
        ' - Please specify required RunEndDate from fixed_cells_history' +
        ' table as --history_cells_from_date arg and re-run script')
    raise Exception(
        'One or more requested dates pre-date last control refresh')

all_dates_from_history = len(history_dates) == len(sdates_valid)
if history_dates and not all_dates_from_history:
    msg_span_refresh = (
        'Requested dates span control refresh - please re-run dates' +
        ' before and after control refresh separately (for pre-refresh' +
        ' dates ensure that --history_cells_from_date is provided)'
    )
    raise Exception(msg_span_refresh)
elif history_dates and all_dates_from_history and HISTORY_CELLS_DATE:
    logger.info('Getting fixed customer cells from history')
    df_fixed_cells = (
        spark
        .table(FIXED_CELLS_HISTORY_TABLE)
        .where(F.col('specialaccountindicator').isNull())
        .where(F.col('RunDateEnd') == HISTORY_CELLS_DATE)
        .withColumnRenamed('RunDateEnd', 'rundate')
    )
    msg_no_history_found = (
        f'No records found in {FIXED_CELLS_HISTORY_TABLE}' +
        f' where RunDateEnd matches {HISTORY_CELLS_DATE}')
    history_rows = df_fixed_cells.count()
    assert history_rows > 0, msg_no_history_found
    logger.info(
        f'{history_rows} records found in {FIXED_CELLS_HISTORY_TABLE}' +
        f' where RunDateEnd matches {HISTORY_CELLS_DATE}')
    assert_pk(df_fixed_cells, pk_cols=['AccountNumber'])
else:
    logger.info('Getting fixed customer cells from latest table')
    df_fixed_cells = (
        spark
        .table(FIXED_CELLS_LATEST_TABLE)
        .where(F.col('specialaccountindicator').isNull())
    )

df_valid_assignments_pages = (
    df_valid_assignments
    .join(df_page_mapping, on=['SessionDate', 'Location'], how='inner')
    .select('AccountNumber', 'SessionDate', 'PagePath')
    .unionByName(
        df_valid_assignments
        .join(df_screen_mapping, on=['SessionDate', 'Location'], how='inner')
        .select('AccountNumber', 'SessionDate', 'PagePath')
    )
    .distinct()
)

df_sessions_ads_valid = (
    df_sessions_pages_trimmed
    .join(
        df_fixed_cells.select('AccountNumber', 'FallowControl'),
        on='AccountNumber', how='inner')
    .join(
        df_valid_assignments_pages,
        on=['AccountNumber', 'SessionDate', 'PagePath'], how='inner'
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
    .join(df_page_mapping, on=['SessionDate', 'Location'], how='inner')
    .withColumn('Device', F.lit('Desktop'))
    .unionByName(
        df_valid_assignments
        .join(df_page_mapping, on=['SessionDate', 'Location'], how='inner')
        .withColumn('Device', F.lit('Mobile'))
    )
    .unionByName(
        df_valid_assignments
        .join(df_screen_mapping, on=['SessionDate', 'Location'], how='inner')
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

# Collect page group mappins from df_ad_metadata
df_pagegroup_mapping = (
    df_ad_metadata
    .select('SessionDate', 'Location', 'PageGroup')
    .filter(F.col('PageGroup').isNotNull())
    .distinct()
)

df_sessions_master = (
    df_sessions_ads_valid_clicks
    .join(df_pagegroup_mapping, on=['SessionDate', 'Location'], how='left')
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
    .select('SessionDate', 'UniqueAdID', 'AudienceOnly',
            *reporting_metadata_cols)
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
            .select('SessionDate', 'UniqueAdID', 'AudienceOnly',
                    *reporting_metadata_cols)
            .distinct()
            .withColumnRenamed('UniqueAdID', 'UniqueAdIDMeasurement')
        ),
        on=['SessionDate', 'UniqueAdIDMeasurement'],
        how='left'
    )
    .fillna({'AudienceOnly': 0})
    .where(
        ~(
            (F.col('UniqueAdIDMeasurement') == F.col('Treatment'))
            & (F.col('AudienceOnly') != 1)
        )
    )
    .withColumn(
        'AlgoDivision_Brand',
        F.concat(F.col('AlgoDivision'), F.lit('_'), F.col('Brand'))
    )
)

# Dropping AudienceOnly column after use avoids downstream schema changes
df_ad_metadata_non_loc = df_ad_metadata_non_loc.drop('AudienceOnly')
df_sessions_master_meta = df_sessions_master_meta.drop('AudienceOnly')

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
    'P131_C1625_Seasons_Womens_Bags',
    'P132_C1735_Seasons_Womens_Athleisure_Womens',
    'P132_C1737_Seasons_Womens_NewIn_Womens',
    'P133_C1782_Seasons_Womens_Coach_Womens',
    'P133_C1777_Seasons_Womens_Rixo_Womens',
    'P133_C1781_Seasons_Womens_Ganni_Womens',
    'P133_C1780_Seasons_Womens_MarcJacobs_Womens',
    'P133_C1778_Seasons_Mixed_PoloRalphLauren_Womens',
    'P133_C1779_Seasons_Womens_Missoma_Womens',
    'P133_C1776_Seasons_Womens_Varley_Womens'
]
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('Device') == 'App')
            & (F.col('UniqueAdIDMeasurement').isin(excl_seasons_ads_app))
            & (F.col('SessionDate') <= '2025-03-04')
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
    '2025-02-03',
    '2025-11-05'
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

# Remove Homepage (Desktop only) for affected dates
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('PageGroup') == 'HomePage')
            & (F.col('Device') == 'Desktop')
            & (F.col('SessionDate') >= date(2025, 3, 11))
            & (F.col('SessionDate') <= date(2025, 3, 19))
        )
    )
)

# Remove Homepage (App only) for affected dates
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('PageGroup') == 'HomePage')
            & (F.col('Device') == 'App')
            & (F.col('SessionDate') >= date(2025, 6, 21))
            & (F.col('SessionDate') <= date(2025, 6, 22))
        )
    )
)

# Remove Shopping Bag (App only) for affected dates
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('PageGroup') == 'ShoppingBag')
            & (F.col('Device') == 'App')
            & (F.col('SessionDate') >= date(2025, 7, 8))
            & (F.col('SessionDate') <= date(2025, 7, 17))
        )
    )
)

# Remove Homepage (Desktop only) for affected dates
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('PageGroup') == 'HomePage')
            & (F.col('Device') == 'Desktop')
            & (F.col('SessionDate') >= date(2025, 5, 1))
            & (F.col('SessionDate') <= date(2025, 5, 5))
        )
    )
)

# Remove OrderComplete (Desktop only) for affected dates
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('PageGroup') == 'OrderComplete')
            & (F.col('Device') == 'Desktop')
            & (F.col('SessionDate') >= date(2025, 8, 29))
            & (F.col('SessionDate') <= date(2025, 9, 25))
        )
    )
)

# Remove Shopping bag (desktop/ mobile) for affected dates
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('PageGroup') == 'ShoppingBag')
            & (F.col('Device').isin(['Desktop', 'Mobile']) &
               F.col("PageGroup").isNotNull())
            & (F.col('SessionDate') >= date(2025, 4, 2))
            & (F.col('SessionDate') <= date(2025, 4, 22))
        )
    )
)


# Remove Order Complete (desktop, mobile, app) for affected dates
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('PageGroup') == 'OrderComplete')
            & (F.col('Device').isin(['Desktop', 'Mobile', 'App']) &
               F.col("PageGroup").isNotNull())
            & (F.col('SessionDate') >= date(2025, 4, 2))
            & (F.col('SessionDate') <= date(2025, 4, 22))
        )
    )
)

# Remove SB2 assignments from results prior to 2025-03-07 as
# content wasn't live in CMS
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('Location') == 'SB2')
            & (F.col('SessionDate') < '2025-03-07')
        )
    )
)

# Remove TheSet ads from results as audiance overrides
# over-ran the lifecycle of these ads in the control sheet.
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('UniqueAdIDMeasurement').isin(
                ['P136_C788_Next_Womens_Multipacks_Womens',
                 'P136_C873_Next_Womens_TheSet_Womens']))
            & (F.col('SessionDate') >= '2025-05-30')
        )
    )
)

# Shopping Bag switch off (App only) - Aug 2025
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('PageGroup') == 'ShoppingBag')
            & (F.col('Device') == 'App')
            & (F.col('FirstTimestamp') > '2025-08-01 16:00:00')
            & (F.col('FirstTimestamp') < '2025-08-06 10:00:00')
        )
    )
)

# Homepage switch off across all devices - Aug 2025
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('PageGroup') == 'HomePage')
            & (F.col('FirstTimestamp') > '2025-08-07 15:00:00')
            & (F.col('FirstTimestamp') < '2025-08-08 14:00:00')
        )
    )
)

# Homepage not live yet - Jan/Feb 2026
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('PageGroup') == 'HomePage')
            & (F.col('SessionDate') >= date(2026, 1, 20))
            # Update when we have the end date for this HomePage exclusion
            # & (F.col('FirstTimestamp') <= date(2026, 2, XX))
        )
    )
)

# MASID switch off - Aug 2025
df_sessions_master_meta = (
    df_sessions_master_meta
    .where(
        ~(
            (F.col('SessionDate') >= '2025-08-18')
            & (F.col('SessionDate') <= '2025-08-22')
        )
    )
)


# Credit Ad test - account exclusions
df_exclude_credit_accounts = (
    spark
    .table(CREDIT_AD_ACCOUNTS_TABLE)
    .select('account_number')
    .withColumnRenamed('account_number', 'AccountNumber')
)

df_exclude_credit_dates = (
    df_sessions_master_meta
    .select('SessionDate')
    .distinct()
    .where((F.col('SessionDate') >= '2025-09-16'))
    .where((F.col('SessionDate') <= '2025-11-10'))
)

df_credit_account_date_combinations = (
    df_exclude_credit_accounts
    .join(df_exclude_credit_dates, how='cross')
)

df_sessions_master_meta = (
    df_sessions_master_meta
    .join(
        df_credit_account_date_combinations,
        on=['AccountNumber', 'SessionDate'],
        how='leftanti'
    )
)

session_level_cols = ['SessionDate', 'Device', 'OS']  # Move to config?
w_apportion = Window.partitionBy(*session_level_cols, 'UniqueVisitID')

df_sessions_master_meta = (
    df_sessions_master_meta
    .withColumn('SessionPortions',
                F.count('*').over(w_apportion))
    .withColumn('ApportionedRevenue',
                F.col('Revenue')/F.col('SessionPortions'))
    .drop('SessionPortions')
)

# Drop Page and Screen columns as only needed for temporal join
df_sessions_master_meta = (
    df_sessions_master_meta
    .drop('Page', 'Screen')
)
df_ad_metadata = (
    df_ad_metadata
    .drop('Page', 'Screen', 'PageGroup')
)

# df_sessions_master_meta.cache()

# Writing out key dataframes to modularise results scripting into:
# - results
# - results_agg

logger.info(f'Writing: df_sessions_master_meta to -> '
            f'{TMP_RESULTS_LOCATION}/df_sessions_master_meta')
(
    df_sessions_master_meta
    .write
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .parquet(f'{TMP_RESULTS_LOCATION}/df_sessions_master_meta')
)

logger.info(f'Writing: df_ad_metadata_non_loc to -> '
            f'{TMP_RESULTS_LOCATION}/df_ad_metadata_non_loc')
(
    df_ad_metadata_non_loc
    .write
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .parquet(f'{TMP_RESULTS_LOCATION}/df_ad_metadata_non_loc')
)

logger.info('Writing: df_ad_metadata to -> '
            f'{TMP_RESULTS_LOCATION}/df_ad_metadata')
(
    df_ad_metadata
    .write
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .parquet(f'{TMP_RESULTS_LOCATION}/df_ad_metadata')
)

logger.info(f'Writing: df_fixed_cells to -> '
            f'{TMP_RESULTS_LOCATION}/df_fixed_cells')
(
    df_fixed_cells
    .write
    .mode('overwrite')
    .option('overwriteSchema', 'true')
    .parquet(f'{TMP_RESULTS_LOCATION}/df_fixed_cells')
)

logger.info('Cleaning up cached dataframes')
df_ad_metadata.unpersist()
df_asgn_pf.unpersist()
df_sessions_pages.unpersist()
df_sessions_ads_valid.unpersist()
df_ad_metadata_non_loc.unpersist()

logger.info("Run Complete")

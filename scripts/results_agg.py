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
from pyspark.sql import Window
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (assert_pk,
                         delete_from_and_load,
                         map_tbl,
                         post_to_webhook)
from dsutils.argparser import get_job_parser
from next_ads.Results import (append_session_overlap_ratio,
                              summarise_sessions)


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

tbl_args_op = {'schema': cfg["schema"][JOB_ENV], 'client': CLIENT}
RESULTS_TOPLINE_TABLE = map_tbl(tbls["results_topline"], **tbl_args_op)
RESULTS_AGGREGATED_TABLE = map_tbl(tbls["results_aggregated"], **tbl_args_op)
RESULTS_AB_TABLE = map_tbl(tbls["results_ab"], **tbl_args_op)
RESULTS_ADS_TABLE = map_tbl(tbls["results_ads"], **tbl_args_op)
RESULTS_ADS_LOCATION_TABLE = map_tbl(tbls["results_ads_location"],
                                     **tbl_args_op)
RESULTS_ADS_PAGE_TABLE = map_tbl(tbls["results_ads_page"], **tbl_args_op)
RESULTS_DIV_PAGE_TABLE = map_tbl(tbls["results_div_page"], **tbl_args_op)
RESULTS_TDIV_PAGE_TABLE = map_tbl(tbls["results_tdiv_page"], **tbl_args_op)
RESULTS_ADS_TARGETING_TABLE = map_tbl(tbls["results_ads_targeting"],
                                      **tbl_args_op)
RESULTS_PAGE_TARGETING_TABLE = map_tbl(tbls["results_page_targeting"],
                                       **tbl_args_op)
RESULTS_AD_METADATA_TABLE = map_tbl(tbls["results_ad_metadata"], **tbl_args_op)

WEBHOOK_URL = cfg["webhooks"]["DS Warnings"]

FALLOW_TRUE = cfg["fallow_control"]["true_label"]
FALLOW_FALSE = cfg["fallow_control"]["false_label"]
SESSION_LEVEL_COLS = cfg["results_processing"]["session_level_cols"]

TMP_RESULTS_LOCATION = f'{cfg["dbfs_base_path"]}/{JOB_ENV}/tmp'

logger.info('Reading: df_sessions_master_meta')
df_sessions_master_meta = (
    spark
    .read
    .format('parquet')
    .load(f'{TMP_RESULTS_LOCATION}/df_sessions_master_meta')
)
df_sessions_master_meta.cache()

process_dates = [
    x[0] for x in
    df_sessions_master_meta.select('SessionDate').distinct().collect()
    ]

logger.info('Reading: df_ad_metadata_non_loc')
df_ad_metadata_non_loc = (
    spark
    .read
    .format('parquet')
    .load(f'{TMP_RESULTS_LOCATION}/df_ad_metadata_non_loc')
)
df_ad_metadata_non_loc.cache()

logger.info('Reading: df_ad_metadata')
df_ad_metadata = (
    spark
    .read
    .format('parquet')
    .load(f'{TMP_RESULTS_LOCATION}/df_ad_metadata')
)
df_ad_metadata.cache()

logger.info('Reading: df_fixed_cells')
df_fixed_cells = (
    spark
    .read
    .format('parquet')
    .load(f'{TMP_RESULTS_LOCATION}/df_fixed_cells')
)


# Start of split from results script

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
    group_cols=SESSION_LEVEL_COLS + ['FallowControl']
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
        group_cols=SESSION_LEVEL_COLS + ['FallowControl', ac]
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

# Filter out 'special' tags from reporting
# e.g. those used for filtering subsets of ads during assignment
# such as "[Test Group] Variant A"
agg_tags = [x for x in agg_tags if not x.startswith('[')]

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
            group_cols=SESSION_LEVEL_COLS + ['FallowControl']
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
        SESSION_LEVEL_COLS,
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
        msg_warn = (f'Total of ApportionedRevenue (agg: {agg_col}) more than'
                    + f' 1% below Total Revenue ({diff_agg/total_r:.2%})')
        logger.warning(msg_warn)
        if JOB_ENV == 'prod':
            post_to_webhook(WEBHOOK_URL, msg_warn)

# if agg_tags:
#     for agg_tag in agg_tags:
#         total_apr_agg_tag = (
#             df_summary_agg_wide
#             .where(F.col('AggColumn') == 'Tagged')
#             .where(F.col('AggValue') == agg_tag)
#             .agg(F.sum('ApportionedRevenue')).collect()[0][0])
#         if total_apr_agg_tag is None:
#             continue
#         msg = f'Total of ApportionedRevenue (tag: {agg_tag}) > Total Revenue'
#         assert total_r*1.001 >= total_apr_agg_tag, msg

# Switched off 20250711 - output not getting used
# # AB test aggregates
# ab_cols = [
#     'HomePageTest1',
#     'ShoppingBagTest1',
#     'OrderCompleteTest1',
#     'LandingPageTest1',
#     'AdHocABTest1',
#     'AdHocABTest2',
#     'AdHocABTest3',
#     'AdHocABTest4',
#     'AdHocABTest5',
#     'AdHocABTest6',
#     'AdHocABTest7',
#     'AdHocABTest8',
#     'AdHocABTest9',
#     'ChampionChallenger'
# ]

# df_summary_ab = summarise_sessions(
#         (
#             df_sessions_master_meta
#             .where(F.col('FallowControl') == FALLOW_FALSE)
#             .join(df_fixed_cells, on='AccountNumber', how='inner')
#         ),
#         **col_args_dict,
#         group_cols=SESSION_LEVEL_COLS + ab_cols
#     )
# df_summary_ab.cache()

# total_apr_ab = (
#     df_summary_ab.agg(F.sum('ApportionedRevenue')).collect()[0][0]
#     )
# msg = 'Total of ApportionedRevenue (A/B) != Total Revenue'
# assert abs(total_r - total_apr_ab) < 0.001*total_r, msg


# Ad-level view
df_summary_ad = (
    summarise_sessions(
        df_sessions_master_meta,
        **col_args_dict,
        group_cols=(
            SESSION_LEVEL_COLS
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
        SESSION_LEVEL_COLS
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
    logger.warning(msg_warn)
    if JOB_ENV == 'prod':
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
            SESSION_LEVEL_COLS
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
        SESSION_LEVEL_COLS
        )
)

df_summary_ad_locset_wide.cache()

# total_apr_ad_locset = (
#     df_summary_ad_locset_wide.agg(F.sum('ApportionedRevenue')).collect()[0][0])
# msg = 'Total of ApportionedRevenue (ad locset) > Total Revenue'
# diff_adlocset = total_apr_ad_locset - total_r
# assert diff_adlocset < 0.001*total_r, msg
# if diff_adlocset < -0.01*total_r:
#     msg_warn = ('Total of ApportionedRevenue (ad locset) more than 1% '
#                 + f'below Total Revenue ({diff_adlocset/total_r:.2%})')
#     logger.warning(msg_warn)
#     if JOB_ENV == 'prod':
#         post_to_webhook(WEBHOOK_URL, msg_warn)


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
            SESSION_LEVEL_COLS
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
        SESSION_LEVEL_COLS
        )
)

df_summary_ad_pagegroupset_wide.cache()


# total_apr_ad_pagegroupset = (
#     df_summary_ad_pagegroupset_wide
#     .agg(F.sum('ApportionedRevenue')).collect()[0][0])
# msg = 'Total of ApportionedRevenue (ad page) > Total Revenue'
# diff_ad_pgset = total_apr_ad_pagegroupset - total_r
# assert diff_ad_pgset < 0.001*total_r, msg
# if diff_ad_pgset < -0.01*total_r:
#     msg_warn = ('Total of ApportionedRevenue (ad page) more than 1% '
#                 + f'below Total Revenue ({diff_ad_pgset/total_r:.2%})')
#     logger.warning(msg_warn)
#     if JOB_ENV == 'prod':
#         post_to_webhook(WEBHOOK_URL, msg_warn)

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
            SESSION_LEVEL_COLS
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
        SESSION_LEVEL_COLS
        )
)

df_summary_div_pagegroupset_wide.cache()

# total_apr_div_page = (
#     df_summary_div_pagegroupset_wide
#     .agg(F.sum('ApportionedRevenue')).collect()[0][0])
# msg = 'Total of ApportionedRevenue (div page) > Total Revenue'
# diff_divp = total_apr_div_page - total_r
# assert diff_divp < 0.001*total_r, msg
# if diff_divp < -0.01*total_r:
#     msg_warn = ('Total of ApportionedRevenue (div page) more than 1% '
#                 + f'below Total Revenue ({diff_divp/total_r:.2%})')
#     logger.warning(msg_warn)
#     if JOB_ENV == 'prod':
#         post_to_webhook(WEBHOOK_URL, msg_warn)

# Trade Division X PageGroupSet view - added 20250711
w_visit_tdiv = Window.partitionBy('UniqueVisitID', 'TradeDivision')
df_summary_tdiv_pagegroupset = (
    summarise_sessions(
        (
            df_sessions_master_meta
            .withColumn('PageGroupSet',
                        F.collect_set('PageGroup').over(w_visit_tdiv))
        ),
        **col_args_dict,
        group_cols=(
            SESSION_LEVEL_COLS
            + ['FallowControl', 'TradeDivision']
            + ['PageGroupSet']
            )
    )
    .where(F.col('TradeDivision').isNotNull())
    .where(F.col('PageGroupSet').isNotNull())
    .withColumn('PageGroupSet',
                F.concat_ws('+', (F.array_sort(F.col('PageGroupSet')))))
)

df_summary_tdiv_pagegroupset_wide = (
    df_summary_tdiv_pagegroupset
    .where(F.col('FallowControl').isin(FALLOW_FALSE, FALLOW_TRUE))
    .groupBy('SessionDate', 'Device', 'OS', 'TradeDivision', 'PageGroupSet')
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

for c in df_summary_tdiv_pagegroupset_wide.columns:
    df_summary_tdiv_pagegroupset_wide = (
        df_summary_tdiv_pagegroupset_wide
        .withColumnRenamed(
            c,
            c.replace(f'{FALLOW_TRUE}_', 'C_').replace(f'{FALLOW_FALSE}_', ''))
    )

df_summary_tdiv_pagegroupset_wide = (
    append_session_overlap_ratio(
        df_summary_device_os_wide,
        df_summary_tdiv_pagegroupset_wide,
        SESSION_LEVEL_COLS
        )
)

df_summary_tdiv_pagegroupset_wide.cache()


# Switched off 20250711 - output not getting used
# # Ad-Targeting view
# df_summary_ad_targeting = (
#     summarise_sessions(
#         (
#             df_sessions_master_meta
#             .withColumn('Targeting',
#                         F.when(
#                             F.col('FallowControl') == FALLOW_TRUE,
#                             F.lit('Control')
#                             ).otherwise(F.col('Treatment'))
#                         )
#         ),
#         **col_args_dict,
#         group_cols=(
#             SESSION_LEVEL_COLS
#             + ['UniqueAdIDMeasurement', 'PageGroup', 'Targeting']
#             )
#     )
#     .where(F.col('UniqueAdIDMeasurement').isNotNull())
#     .where(F.col('PageGroup').isNotNull())
#     .where(F.col('Targeting').isNotNull())
#     .withColumnRenamed('UniqueAdIDMeasurement', 'UniqueAdID')
# )

# df_summary_ad_targeting.cache()

# total_apr_adtgt = (
#     df_summary_ad_targeting
#     .where(F.col('Targeting') != 'Control')
#     .agg(F.sum('ApportionedRevenue')).collect()[0][0]
#     )
# msg = 'Total of ApportionedRevenue (ad tgt) > Total Revenue'
# diff_adtgt = total_apr_adtgt - total_r
# assert diff_adtgt < 0.001*total_r, msg
# if diff_adtgt < -0.01*total_r:
#     msg_warn = ('Total of ApportionedRevenue (ad tgt) more than 1% '
#                 + f'below Total Revenue ({diff_adtgt/total_r:.2%})')
#     logger.warning(msg_warn)
#     if JOB_ENV == 'prod':
#         post_to_webhook(WEBHOOK_URL, msg_warn)

# Switched off 20250711 - output not getting used
# # Page-Targeting view
# df_summary_page_targeting = (
#     summarise_sessions(
#         (
#             df_sessions_master_meta
#             .withColumn('Targeting',
#                         F.when(
#                             F.col('FallowControl') == FALLOW_TRUE,
#                             F.lit('Control')
#                             ).otherwise(F.col('Treatment'))
#                         )
#         ),
#         **col_args_dict,
#         group_cols=(
#             SESSION_LEVEL_COLS
#             + ['PageGroup', 'Targeting']
#             )
#     )
#     .where(F.col('PageGroup').isNotNull())
#     .where(F.col('Targeting').isNotNull())
# )

# df_summary_page_targeting.cache()

# total_apr_pagetgt = (
#     df_summary_page_targeting
#     .where(F.col('Targeting') != 'Control')
#     .agg(F.sum('ApportionedRevenue')).collect()[0][0]
#     )
# msg = 'Total of ApportionedRevenue (page tgt) > Total Revenue'
# diff_pagetgt = total_apr_pagetgt - total_r
# assert diff_pagetgt < 0.001*total_r, msg
# if diff_pagetgt < -0.01*total_r:
#     msg_warn = ('Total of ApportionedRevenue (page tgt) more than 1% '
#                 + f'below Total Revenue ({diff_pagetgt/total_r:.2%})')
#     logger.warning(msg_warn)
#     if JOB_ENV == 'prod':
#         post_to_webhook(WEBHOOK_URL, msg_warn)


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
# if not dates_provided and not live_exclusions:
#     for d in process_dates:
#         d_fmt = d.strftime('%Y-%m-%d')
#         logger.info('Checking consistency of pre- and post-processing ' +
#                     f'totals for SessionDate: {d_fmt}')
#         for fc in [FALLOW_FALSE, FALLOW_TRUE]:
#             for c in ['Sessions', 'Revenue']:
#                 tpre = (
#                     df_results_topline
#                     .where(F.col('SessionDate') == d)
#                     .where(F.col('FallowControl') == fc)
#                     .groupBy('SessionDate', 'FallowControl')
#                     .agg(F.sum(c).alias(c))
#                     .select(c)
#                     ).collect()[0][0]  # Nested collect inefficient - rewrite
#                 if fc == FALLOW_TRUE:
#                     c_piv = 'C_' + c
#                 else:
#                     c_piv = c
#                 tpost = (
#                     df_summary_device_os_wide
#                     .where(F.col('SessionDate') == d)
#                     .groupBy('SessionDate')
#                     .agg(F.sum(c_piv).alias(c_piv))
#                     .select(c_piv)
#                     ).collect()[0][0]
#                 # Check match < 0.01 to allow for floating point arithmetic
#                 msg = (f'Pre- and post- total for {c} does not match for '
#                        + '{fc} '
#                        + f'(pre: {tpre:,}; post: {tpost:,}; '
#                        + f'change: {tpost-tpre:,})')
#                 assert abs(tpost - tpre) < 0.01, msg


for d in process_dates:
    d_fmt = "\'" + d.strftime('%Y-%m-%d') + "\'"

    logger.info(f'Loading results_topline for {d_fmt} ' +
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

    logger.info(f'Loading results_aggregated for {d_fmt} ' +
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

    # logger.info(f'Loading results_ab for {d_fmt} ' +
    #             f'to table: {RESULTS_AB_TABLE}')
    # delete_from_and_load(
    #     (
    #         df_summary_ab
    #         .where(F.col('SessionDate') == d)
    #         .select('SessionDate',
    #                 'Device',
    #                 'OS',
    #                 *ab_cols,
    #                 'Sessions',
    #                 'Revenue',
    #                 'Conversions',
    #                 'SoftImpressions',
    #                 'SoftClicks')
    #     ),
    #     RESULTS_AB_TABLE,
    #     pk_cols=['SessionDate', 'Device', 'OS', *ab_cols],
    #     del_where={'SessionDate': d_fmt}
    # )

    logger.info(f'Loading results_ads for {d_fmt} ' +
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

    logger.info(f'Loading results_ads_location for {d_fmt} ' +
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

    logger.info(f'Loading results_ads_page for {d_fmt} ' +
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

    logger.info(f'Loading results_div_page for {d_fmt} ' +
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

    logger.info(f'Loading results_tdiv_page for {d_fmt} ' +
                f'to table: {RESULTS_TDIV_PAGE_TABLE}')
    delete_from_and_load(
        (
            df_summary_tdiv_pagegroupset_wide
            .where(F.col('SessionDate') == d)
            .select('SessionDate',
                    'Device',
                    'OS',
                    'TradeDivision',
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
        RESULTS_TDIV_PAGE_TABLE,
        pk_cols=['SessionDate', 'Device', 'OS',
                 'TradeDivision', 'PageGroupSet'],
        del_where={'SessionDate': d_fmt}
    )

    # logger.info(f'Loading results_ads_targeting for {d_fmt} ' +
    #             f'to table: {RESULTS_ADS_TARGETING_TABLE}')
    # delete_from_and_load(
    #     (
    #         df_summary_ad_targeting
    #         .where(F.col('SessionDate') == d)
    #         .select('SessionDate',
    #                 'Device',
    #                 'OS',
    #                 'UniqueAdID',
    #                 'PageGroup',
    #                 'Targeting',
    #                 'Sessions',
    #                 'Revenue',
    #                 'Conversions',
    #                 'SoftImpressions',
    #                 'SoftClicks')
    #     ),
    #     RESULTS_ADS_TARGETING_TABLE,
    #     pk_cols=['SessionDate', 'Device', 'OS',
    #              'UniqueAdID', 'PageGroup', 'Targeting'],
    #     del_where={'SessionDate': d_fmt}
    # )

    # logger.info(f'Loading results_page_targeting for {d_fmt} ' +
    #             f'to table: {RESULTS_PAGE_TARGETING_TABLE}')
    # delete_from_and_load(
    #     (
    #         df_summary_page_targeting
    #         .where(F.col('SessionDate') == d)
    #         .select('SessionDate',
    #                 'Device',
    #                 'OS',
    #                 'PageGroup',
    #                 'Targeting',
    #                 'Sessions',
    #                 'Revenue',
    #                 'Conversions',
    #                 'SoftImpressions',
    #                 'SoftClicks')
    #     ),
    #     RESULTS_PAGE_TARGETING_TABLE,
    #     pk_cols=['SessionDate', 'Device', 'OS',
    #              'PageGroup', 'Targeting'],
    #     del_where={'SessionDate': d_fmt}
    # )

    logger.info(f'Loading results_ad_metadata for {d_fmt} ' +
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

logger.info("Run Complete")

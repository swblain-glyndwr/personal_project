import json
from pyspark.sql import functions as F
from pyspark.sql import Window

from dsutils.dbc import configure_spark
from dsutils.argparser import get_job_parser
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import truncate_and_load, delete_from_and_load, map_tbl
from dsutils.etl import post_to_webhook
from next_ads.Assignment import get_ad_feedback_scores, greedy_assignment


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

APPLY_AD_FEEDBACK = jobparser.has_arg('--apply-ad-feedback')
AD_FEEDBACK_WEIGHT = jobparser.get_arg('--ad-feedback-weight') or 0.05
TOP_ADS_PER_LOCATION = jobparser.get_arg('--top-ads-per-location') or 3

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

tbl_args = {'schema': SCHEMA, 'client': CLIENT}
# Read tables
NEXT_THEME_SCORES_LATEST = map_tbl(tbls["next_theme_scores_latest"], **tbl_args)  # noqa
CONTROL_SHEET_LATEST = map_tbl(tbls["control_sheet_latest"], **tbl_args)
CUSTOMER_CELLS_LATEST = map_tbl(tbls["customer_cells_latest"], **tbl_args)
# Write tables
THEME_SCORE_COMPONENTS_LATEST = map_tbl(tbls["theme_score_components_latest"], **tbl_args)  # noqa
THEME_SCORE_COMPONENTS = map_tbl(tbls["theme_score_components"], **tbl_args)  # noqa
PRERANKED_ADS_FROM_THEMES_LATEST = map_tbl(tbls["preranked_ads_from_themes_latest"], **tbl_args)  # noqa

WEBHOOK_URL = cfg['webhooks']['DS Warnings']

# Force read from prod results tables for ad feedback scores
AD_RESULTS = map_tbl(
    cfg["tables"]["write"]["results_ads"],
    schema=cfg["schema"]["prod"],
    client=CLIENT
)

spark = configure_spark()

logger.info(f'Getting theme to ad mappings from {CONTROL_SHEET_LATEST}')
df_theme2ad = (
    spark
    .table(CONTROL_SHEET_LATEST)
    .where(F.col('AudienceOnly') != 1)
    .select('Themes', 'UniqueAdID')
    .where(F.col('Themes').isNotNull())
    .where(F.col('Themes') != '')
    .distinct()
)

logger.info(f'Getting customer base from {CUSTOMER_CELLS_LATEST}')
df_cust = spark.table(CUSTOMER_CELLS_LATEST).select('AccountNumber')

logger.info(f'Getting theme scores from {NEXT_THEME_SCORES_LATEST}')
# Limit to customers with cells so we don't waste processing accounts
# that will be dropped downstream
df_theme_scores = (
    spark
    .table(NEXT_THEME_SCORES_LATEST)
    .join(df_cust, on='AccountNumber', how='inner')
)

logger.info('Normalising theme scores')
min_score = df_theme_scores.agg(F.min('ProbAggRebased')).collect()[0][0]
max_score = df_theme_scores.agg(F.max('ProbAggRebased')).collect()[0][0]
score_range = max_score - min_score
logger.info(f'Norm min/max/range: {min_score}/{max_score}/{score_range}')

GREEDY_CFG = cfg.get('greedy_themes', {})

# Validate greedy quota format
gcfg_isdict = isinstance(GREEDY_CFG.get('quotas', None), dict)
if gcfg_isdict:
    gcfg_val_int = all(
        [isinstance(k, int) for k in GREEDY_CFG['quotas'].values()]
        )
else:
    gcfg_val_int = False

if all([gcfg_isdict, gcfg_val_int]):
    # Process greedy config
    greedy_quotas = GREEDY_CFG.get('quotas')
    max_quota = max(greedy_quotas.values())
    logger.info(f'Greedy quotas: {greedy_quotas}')
    # Default options for switching ntiles and switching behaviour
    switch_tiles = GREEDY_CFG.get('switch_tiles', True)
    tiles = GREEDY_CFG.get('tiles', 1000)
    logger.info(f'Greedy tiles: {tiles} (switching: {switch_tiles})')
    switch_multiplier = -1 if switch_tiles else 1

    # Rank themes from most niche to least niche
    df_theme_order = (
        df_theme_scores
        .where(F.col('NextTheme').isin(list(greedy_quotas.keys())))
        .groupBy('NextTheme')
        .agg(F.first('ProbBase').alias('ProbBase'))
        .orderBy(F.col('ProbBase'))
        .withColumn('ThemeOrder', F.monotonically_increasing_id() + 1)
    )

    df_theme_scores_global_rank = (
        df_theme_scores
        .join(df_theme_order, on='NextTheme', how='inner')
        .withColumn(
            'RankInTheme',
            F.row_number().over(
                Window
                .partitionBy('NextTheme')
                .orderBy(F.col('ProbAggRebased').desc())
            )
        )
        .where(F.col('RankInTheme') <= (len(greedy_quotas.keys()) * max_quota))
        .withColumn(
            'nTile',
            F.ntile(1000).over(
                Window
                .partitionBy('NextTheme')
                .orderBy(F.col('ProbAggRebased').desc())
            )
        )
        .withColumn(
            'SwitchRank',
            F.when(
                F.col('nTile') % 2 == 0,
                F.col('ThemeOrder') * F.lit(switch_multiplier)
            ).otherwise(F.col('ThemeOrder'))
        )
        .orderBy(
            F.col('nTile'),
            F.col('SwitchRank'),
            F.col('ProbAggRebased').desc()
        )
        .withColumn('GlobalRank', F.monotonically_increasing_id() + 1)
    )

    df_theme_scores_global_rank.cache()
    gr_records = df_theme_scores_global_rank.count()
    logger.info(f'{gr_records:,} records passed to greedy assignment')

    df_theme_scores_greedy = greedy_assignment(
        df_theme_scores_global_rank,
        greedy_quotas,
        item_col='NextTheme',
        user_col='AccountNumber',
        rank_col='GlobalRank'
    )

    df_theme_scores = (
        df_theme_scores
        .join(df_theme_scores_greedy.withColumn('GreedyScore', F.lit(1)),
              on=['AccountNumber', 'NextTheme'], how='left')
        .fillna(0, subset=['GreedyScore'])
    )
else:
    if GREEDY_CFG:
        bad_gcfg_msg = 'Invalid greedy theme config, skipping greedy assignment' # noqa
        logger.warning(bad_gcfg_msg)
        if JOB_ENV == "prod":
            post_to_webhook(WEBHOOK_URL, bad_gcfg_msg)
    logger.info('Greedy assignment not enabled')
    logger.info('Defaulting to greedy score of 0 for all themes')
    df_theme_scores = df_theme_scores.withColumn('GreedyScore', F.lit(0))


if APPLY_AD_FEEDBACK:
    logger.info(f'Getting ad feedback scores (weight: {AD_FEEDBACK_WEIGHT})')
    df_ad_feedback_scores = get_ad_feedback_scores(
        ad_results_table=AD_RESULTS,
        control_sheet_latest_table=CONTROL_SHEET_LATEST,
        ad_feedback_weight=AD_FEEDBACK_WEIGHT
    )

    if not df_ad_feedback_scores or df_ad_feedback_scores.isEmpty():
        logger.warning('No ad feedback scores returned')
        logger.info('Defaulting to incremental score of 1.0 for all ads')
        df_theme2ad = df_theme2ad.withColumn('IncrementalScore', F.lit(1.0))
    else:
        n_afs = df_ad_feedback_scores.count()
        logger.info(f'{n_afs:,} ad feedback scores returned, appending')
        df_theme2ad = (
            df_theme2ad
            .join(df_ad_feedback_scores, on='UniqueAdID', how='left')
            .withColumnRenamed('AdFeedbackScore', 'IncrementalScore')
            .fillna(1.0, subset=['IncrementalScore'])
        )

else:
    logger.info('Ad feedback loop not enabled')
    logger.info('Defaulting to incremental score of 1.0 for all ads')
    df_theme2ad = df_theme2ad.withColumn('IncrementalScore', F.lit(1.0))

logger.info('Normalising theme scores and mapping to ads')
# Add GreedyScore after normalisation so greedy assignments exist in
# range [1, 2), which normal scores fall in range [0, 1)
df_score_components = (
    df_theme_scores
    .withColumn(
        'RelevanceScore',
        ((F.col('ProbAggRebased') - F.lit(min_score)) / F.lit(score_range))
        + F.col('GreedyScore')
    )
    .join(df_theme2ad.withColumnRenamed('Themes', 'NextTheme'),
          on='NextTheme', how='inner')
    .withColumn('Score',
                F.col('RelevanceScore') * F.col('IncrementalScore'))
    .select('AccountNumber',
            F.col('NextTheme').alias('Theme'),
            'UniqueAdID',
            'RelevanceScore',
            'IncrementalScore',
            'Score')
)
df_score_components.cache()

logger.info(f'Loading score components to {THEME_SCORE_COMPONENTS_LATEST}')
truncate_and_load(
    df_score_components,
    THEME_SCORE_COMPONENTS_LATEST,
    pk_cols=['AccountNumber', 'Theme', 'UniqueAdID']
)

logger.info(f'Loading score components to {THEME_SCORE_COMPONENTS}')
delete_from_and_load(
    df_score_components,
    THEME_SCORE_COMPONENTS,
    pk_cols=['AccountNumber', 'Theme', 'UniqueAdID'],
    del_where={"rundate": "current_date()"}
)


# Locations commonly have the same set of eligible ads, so to avoid repeating
# ranking processes multiple times (which is computationally expensive), we
# identify distinct ad sets across locations and rank ads per ad set first, and
# then map back to locations.
# Also, we can't just perform a global ranking and select the top ad per loc
# as the top ad according to the global ranking may not be eligible for that
# location. Another solution would be to take the max score per location during
# the task_build_page step, which would be less computationally expensive, but
# could be limiting if we needed to assign more than one ads per location.
logger.info('Fetching ad location mappings')
df_ad2loc = (
    spark
    .table(CONTROL_SHEET_LATEST)
    .where(F.col('AudienceOnly') != 1)
    .select('UniqueAdID', 'Location')
    .distinct()
)

logger.info(
    'Finding distinct ad sets across locations to minimise repeated ranking')
# Use string of sorted ad IDs as ad set identifier (effectively a hash key)
df_adsets = (
    df_ad2loc
    .groupBy('Location')
    .agg(F.array_sort(F.collect_list('UniqueAdID')).alias('AdSetSorted'))
    .withColumn('AdSet', F.concat_ws('|', F.col('AdSetSorted')))
    .groupBy('AdSet')
    .agg(F.collect_set('Location').alias('LocationSet'))
    .withColumn(
        'AdSetID',
        F.row_number().over(
            Window.partitionBy(F.lit(1)).orderBy(F.lit(1))
            )
        )
    .select('AdSetID', 'LocationSet')
)

df_adset2loc = (
    df_adsets
    .select('AdSetID', F.explode('LocationSet').alias('Location'))
)

nLocs = df_adset2loc.select('Location').distinct().count()
nAdSets = df_adsets.count()
logger.info(f'{nAdSets:,} distinct ad sets found across {nLocs:,} locations')

adsets_rows = df_adsets.collect()
for row in adsets_rows:
    locations = ', '.join(sorted(row['LocationSet']))
    logger.info(f"AdSetID {row['AdSetID']}: Locations [{locations}]")

df_ad2adset = (
    df_ad2loc
    .join(df_adset2loc, on='Location', how='inner')
    .select('UniqueAdID', 'AdSetID')
    .distinct()
)

logger.info(f'Ranking and returning top {TOP_ADS_PER_LOCATION} ads per ad set')
# De-duplicate multi-ad themes (random uniform selection)
df_adset_scores = (
    df_score_components
    .withColumn('Rand', F.rand())
    .withColumn(
        'AdPerThemeRank',
        F.rank().over(
            Window
            .partitionBy('AccountNumber', 'Theme')
            .orderBy(F.col('Rand'))
        ))
    .where(F.col('AdPerThemeRank') == 1)
    .select('AccountNumber', 'UniqueAdID', 'Score')
    .join(df_ad2adset, on='UniqueAdID', how='inner')
    .withColumn('TieBreaker', F.rand())
    .withColumn(
        'Rank',
        F.rank().over(
            Window
            .partitionBy('AccountNumber', 'AdSetID')
            .orderBy(F.col('Score').desc(), F.desc('TieBreaker'))
        )
    )
    .where(F.col('Rank') <= TOP_ADS_PER_LOCATION)
)
df_adset_scores.cache()

logger.info('Mapping ranked ads back to locations')
df_ad_scores = (
    df_adset_scores
    .join(df_adset2loc, on='AdSetID', how='inner')
    .select('AccountNumber', 'UniqueAdID', 'Location', 'Score', 'Rank')
)

logger.info('Checking for ads assigned to ineligible locations')
df_violations = (
    df_ad_scores
    .join(df_ad2loc, on=['Location', 'UniqueAdID'], how='left_anti')
)
assert df_violations.count() == 0, 'Ads assigned to ineligible locations'

logger.info(
    f'Loading preranked theme ads to {PRERANKED_ADS_FROM_THEMES_LATEST}')
truncate_and_load(
    df_ad_scores,
    PRERANKED_ADS_FROM_THEMES_LATEST,
    pk_cols=['AccountNumber', 'UniqueAdID', 'Location']
)

logger.info('Unpersisting cached dataframes')
df_score_components.unpersist()
df_adset_scores.unpersist()

logger.info('Run complete')

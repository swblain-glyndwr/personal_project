import json
from pyspark.sql import functions as F
from pyspark.sql import Window

from dsutils.dbc import configure_spark
from dsutils.argparser import get_job_parser
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import truncate_and_load, delete_from_and_load, map_tbl

from next_ads.Assignment import get_ad_feedback_scores


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
# Write tables
THEME_SCORE_COMPONENTS_LATEST = map_tbl(tbls["theme_score_components_latest"], **tbl_args)  # noqa
THEME_SCORE_COMPONENTS = map_tbl(tbls["theme_score_components"], **tbl_args)  # noqa
PRERANKED_ADS_FROM_THEMES_LATEST = map_tbl(tbls["preranked_ads_from_themes_latest"], **tbl_args)  # noqa

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

logger.info(f'Getting theme scores from {NEXT_THEME_SCORES_LATEST}')
df_theme_scores = spark.table(NEXT_THEME_SCORES_LATEST)

logger.info('Normalising theme scores')
min_score = df_theme_scores.agg(F.min('ProbAggRebased')).collect()[0][0]
max_score = df_theme_scores.agg(F.max('ProbAggRebased')).collect()[0][0]
score_range = max_score - min_score
logger.info(f'Norm min/max/range: {min_score}/{max_score}/{score_range}')

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
df_score_components = (
    df_theme_scores
    .withColumn(
        'RelevanceScore',
        (F.col('ProbAggRebased') - F.lit(min_score)) / F.lit(score_range)
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
df_adset_scores = (
    df_score_components
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

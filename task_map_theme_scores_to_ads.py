import json
from pyspark.sql import functions as F
from pyspark.sql import Window

from dsutils.dbc import configure_spark
from dsutils.argparser import get_job_parser
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import truncate_and_load, map_tbl

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

APPLY_AD_FEEDBACK = jobparser.has_arg('--apply_ad_feedback')

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
    logger.info('Getting ad feedback scores')
    df_ad_feedback_scores = get_ad_feedback_scores(
        ad_results_table=AD_RESULTS,
        control_sheet_latest_table=CONTROL_SHEET_LATEST,
        ad_feedback_weight=0.5
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

logger.info('Normalising theme scores, mapping to ads, applying ad feedback')
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

logger.info(f'Loading score components to {THEME_SCORE_COMPONENTS_LATEST}')
truncate_and_load(
    df_score_components,
    THEME_SCORE_COMPONENTS_LATEST,
    pk_cols=['AccountNumber', 'Theme', 'UniqueAdID']
)

logger.info(f'Loading score components to {THEME_SCORE_COMPONENTS}')
truncate_and_load(
    df_score_components,
    THEME_SCORE_COMPONENTS,
    pk_cols=['AccountNumber', 'Theme', 'UniqueAdID']
)

logger.info('Ranking ads by final score (with tie breaker)')
df_ad_scores = (
    df_score_components
    .withColumn('TieBreaker', F.rand())
    .withColumn(
        'Rank',
        F.rank().over(
            Window
            .partitionBy('AccountNumber')
            .orderBy(F.col('Score').desc(), F.desc('TieBreaker'))
        )
    )
    .select('AccountNumber', 'UniqueAdID', 'Score', 'Rank')
)

logger.info(
    f'Loading preranked theme ads to {PRERANKED_ADS_FROM_THEMES_LATEST}')
truncate_and_load(
    df_ad_scores,
    PRERANKED_ADS_FROM_THEMES_LATEST,
    pk_cols=['AccountNumber', 'UniqueAdID']
)

logger.info('Run complete')

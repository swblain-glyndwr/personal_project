import json
from pyspark.sql import functions as F
from pyspark.sql import Window

from dsutils.dbc import configure_spark
from dsutils.argparser import get_job_parser
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import truncate_and_load, map_tbl


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

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

tbl_args = {'schema': SCHEMA, 'client': CLIENT}
# Read tables
NEXT_THEME_SCORES_LATEST = map_tbl(tbls["next_theme_scores_latest"], **tbl_args)  # noqa
CONTROL_SHEET_LATEST = map_tbl(tbls["control_sheet_latest"], **tbl_args)
# Write tables
PRERANKED_ADS_FROM_THEMES_LATEST = map_tbl(
    tbls["preranked_ads_from_themes_latest"], **tbl_args)

spark = configure_spark()

df_theme2ad = (
    spark
    .table(CONTROL_SHEET_LATEST)
    .select('Themes', 'UniqueAdID')
    .where(F.col('Themes').isNotNull())
    .distinct()
)

df_theme_scores = spark.table(NEXT_THEME_SCORES_LATEST)

min_score = df_theme_scores.agg(F.min('ProbAggRebased')).collect()[0][0]
max_score = df_theme_scores.agg(F.max('ProbAggRebased')).collect()[0][0]
score_range = max_score - min_score

# Normalise scores globally
df_theme_scores_norm = (
    df_theme_scores
    .withColumn(
        'Score',
        (F.col('ProbAggRebased') - F.lit(min_score)) / F.lit(score_range)
    )
    .select('AccountNumber', F.col('NextTheme').alias('Themes'), 'Score')
)

df_ad_scores = (
    df_theme_scores_norm
    .join(df_theme2ad, on='Themes', how='inner')
    .select('AccountNumber', 'UniqueAdID', 'Score')
    .withColumn(
        'Rank',
        F.rank().over(
            Window
            .partitionBy('AccountNumber')
            .orderBy(F.col('Score').desc(), F.col('UniqueAdID'))
            )
    )
)

logger.info(
    f'Loading preranked theme ads to {PRERANKED_ADS_FROM_THEMES_LATEST}')
truncate_and_load(
    df_ad_scores,
    PRERANKED_ADS_FROM_THEMES_LATEST,
    pk_cols=['AccountNumber', 'UniqueAdID']
)

logger.info('Run complete')

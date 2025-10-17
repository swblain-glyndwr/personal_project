import json
from pyspark.sql import functions as F
from pyspark.sql import Window
from next_ads.Attributes import parse_ad_attributes
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (map_tbl,
                         delete_from_and_load,
                         truncate_and_load)
from dsutils.argparser import get_job_parser
from dsutils import gcp


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

SET_THEME_ATTRIBUTES = jobparser.has_arg('--set') or False
THEME_RANKING_MODE = jobparser.get_arg('--theme-ranking-mode')
if not THEME_RANKING_MODE:
    THEME_RANKING_MODE = 'adtype-themetype'
    logger.info('THEME_RANKING_MODE not specified, defaulting to:'
                + f' {THEME_RANKING_MODE}')

THEME_MAPPING_URL = cfg['theme_mapping']['url']
THEME_MAPPING_SHEET = cfg['theme_mapping']['sheet']
THEME_MAPPING_READ_SCHEMA = cfg['theme_mapping']['read_schema']

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

# Map write schema to parameterised table names
tbl_args = {'schema': SCHEMA, 'client': CLIENT}
THEME_MAPPING = map_tbl(tbls["theme_mapping"], **tbl_args)
THEME_MAPPING_LATEST = map_tbl(tbls["theme_mapping_latest"], **tbl_args)
ITEM_ATTRIBUTES_LATEST = map_tbl(tbls["item_attributes_latest"], **tbl_args)
ITEM_THEMES_LATEST = map_tbl(tbls["item_themes_latest"], **tbl_args)
ITEM_THEMES = map_tbl(tbls["item_themes"], **tbl_args)


if SET_THEME_ATTRIBUTES:
    logger.info('Parsing theme mapping from control sheet tab:'
                + f' {cfg["theme_mapping"]["sheet"]}')

    df_themes = gcp.spark_df_from_sheets(
        url=THEME_MAPPING_URL,
        worksheet_name=THEME_MAPPING_SHEET,
        gcp_scope=cfg["gcp"]["scope"],
        gcp_key=cfg["gcp"]["key"],
        schema=THEME_MAPPING_READ_SCHEMA
    ).withColumn('Theme', F.trim(F.lower(F.col('Theme'))))

    theme_attributes = parse_ad_attributes(
        df=df_themes.select('Theme', 'TargetingAttributes'),
        ad_id_col='Theme'
    ).distinct()

    n_themes = theme_attributes.select('Theme').distinct().count()
    n_rows = theme_attributes.count()
    logger.info(f'Parsed {n_themes:,} themes ({n_rows:,} rows)')

    logger.info('Writing theme mapping to output tables')
    truncate_and_load(
        theme_attributes,
        THEME_MAPPING_LATEST,
        pk_cols=['Theme', 'attribute', 'value']
    )

    delete_from_and_load(
        theme_attributes,
        THEME_MAPPING,
        pk_cols=['Theme', 'attribute', 'value'],
        del_where={'rundate': 'current_date()'}
    )


if not SET_THEME_ATTRIBUTES:
    logger.info('Reading existing theme mapping for item-theme mapping')
    theme_attributes = spark.table(THEME_MAPPING_LATEST)

    df_themes = gcp.spark_df_from_sheets(
        url=THEME_MAPPING_URL,
        worksheet_name=THEME_MAPPING_SHEET,
        gcp_scope=cfg["gcp"]["scope"],
        gcp_key=cfg["gcp"]["key"],
        schema=THEME_MAPPING_READ_SCHEMA
    ).withColumn('Theme', F.trim(F.lower(F.col('Theme'))))

else:
    logger.info('Using newly refreshed theme mapping for item-theme mapping')

item_attributes = spark.table(ITEM_ATTRIBUTES_LATEST)

# An item belongs to a theme if it matches at least one value from each
# of the theme's attributes.
# e.g. Theme "men's tops" {gender:mens, category:t-shirts, category:shirts}
# matches every item that is a "mens t-shirt" OR a "mens shirt"
item_theme_joined = (
    item_attributes.alias('i')
    .join(theme_attributes.alias('t'),
          on='attribute', how='inner')
    .where(F.col('i.value') == F.col('t.value'))
)
matched_counts = (
    item_theme_joined.groupBy('pid', 'Theme')
    .agg(F.countDistinct('attribute').alias('n_matched'))
    )
required_counts = (
    item_theme_joined.groupBy('Theme')
    .agg(F.countDistinct('attribute').alias('n_required'))
    )
item_themes = (
    matched_counts
    .join(required_counts, on='Theme', how='inner')
    .where(F.col('n_matched') == F.col('n_required'))
    .select(F.col('pid'), F.col('Theme').alias('theme'))
    )

logger.info('Ranking themes for each item')
if THEME_RANKING_MODE == 'adtype-themefreq':
    # Calculate theme frequencies in item base for ranking
    theme_freq = (
        item_themes
        .groupBy('theme')
        .agg(F.count('pid').alias('MatchingItems'))
    )
    item_themes_ranked = (
        item_themes
        .join(theme_freq, on='theme', how='left')
        .join(
            (
                df_themes
                .select('Theme', 'AdTypeRank')
                .withColumnRenamed('Theme', 'theme')
            ), on='theme', how='left'
        )
        .withColumn('AdTypeScore',
                    F.lit(1.0) / F.col('AdTypeRank').cast('float'))
        .withColumn('FreqScore',
                    F.lit(1.0) / F.col('MatchingItems').cast('float'))
        .fillna({'AdTypeScore': -1.0, 'AdTypeRank': -1.0})
        .withColumn(
            'theme_rank',
            F.dense_rank().over(
                Window
                .partitionBy('pid').orderBy(
                    F.desc(F.col('AdTypeScore')),
                    F.desc(F.col('FreqScore')),
                    'theme'
                )
            )
        )
    )
elif THEME_RANKING_MODE == 'adtype-themetype':
    item_themes_ranked = (
        item_themes
        .join(
            (
                df_themes
                .select('Theme', 'AdTypeRank', 'ThemeTypeRank')
                .withColumnRenamed('Theme', 'theme')
            ), on='theme', how='left'
        )
        .withColumn('AdTypeScore',
                    F.lit(1.0) / F.col('AdTypeRank').cast('float'))
        .withColumn('ThemeTypeScore',
                    F.lit(1.0) / F.col('ThemeTypeRank').cast('float'))
        .fillna({'AdTypeScore': -1.0, 'AdTypeRank': -1.0})
        .withColumn(
            'theme_rank',
            F.dense_rank().over(
                Window
                .partitionBy('pid').orderBy(
                    F.desc(F.col('AdTypeScore')),
                    F.desc(F.col('ThemeTypeScore')),
                    'theme'
                )
            )
        )
    )
else:
    raise ValueError(f'Unknown THEME_RANKING_MODE: {THEME_RANKING_MODE}')

logger.info('Writing item-theme mapping to output tables')
truncate_and_load(
    item_themes_ranked.select('pid', 'theme', 'theme_rank'),
    ITEM_THEMES_LATEST,
    pk_cols=['pid', 'theme']
)

delete_from_and_load(
    item_themes_ranked.select('pid', 'theme', 'theme_rank'),
    ITEM_THEMES,
    pk_cols=['pid', 'theme'],
    del_where={'rundate': 'current_date()'}
)

logger.info('Run complete')

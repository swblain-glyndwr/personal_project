import json
from pyspark.sql import functions as F
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

THEME_MAPPING_URL = cfg['theme_mapping']['url']
THEME_MAPPING_SHEET = cfg['theme_mapping']['sheet']
THEME_MAPPING_READ_SCHEMA = cfg['theme_mapping']['read_schema']

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

# Map write schema to parameterised write table names
tbl_args = {'schema': SCHEMA, 'client': CLIENT}
THEME_MAPPING = map_tbl(tbls["theme_mapping"], **tbl_args)
THEME_MAPPING_LATEST = map_tbl(tbls["theme_mapping_latest"], **tbl_args)


logger.info('Parsing theme mapping from control sheet tab:'
            + f' {cfg["theme_mapping"]["sheet"]}')

df_themes_raw = gcp.spark_df_from_sheets(
    url=THEME_MAPPING_URL,
    worksheet_name=THEME_MAPPING_SHEET,
    gcp_scope=cfg["gcp"]["scope"],
    gcp_key=cfg["gcp"]["key"],
    schema=THEME_MAPPING_READ_SCHEMA
)

df_themes = (
    df_themes_raw
    .withColumn('Theme', F.trim(F.lower(F.col('Theme'))))
)

df_themes_parsed = parse_ad_attributes(
    df=df_themes,
    ad_id_col='Theme'
).distinct()

logger.info('Writing theme mapping to output tables')
delete_from_and_load(
    df_themes_parsed,
    THEME_MAPPING,
    pk_cols=['Theme', 'attribute', 'value'],
    del_where={'rundate': 'current_date()'}
)

truncate_and_load(
    df_themes_parsed,
    THEME_MAPPING_LATEST,
    pk_cols=['Theme', 'attribute', 'value']
)

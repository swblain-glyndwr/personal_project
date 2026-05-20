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
import re

from pyspark.sql import functions as F
from pyspark.sql import Window

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.etl import truncate_and_load
from dsutils.logtools import configure_logging, get_logger
from next_ads.utils import config_manager
from next_ads.utils import etl


PAGE_SLOT_SUFFIX_PATTERN = re.compile(r"_slot_[^_]+$", flags=re.IGNORECASE)
LOCATION_SUFFIX_PATTERN = re.compile(r"\d+$")
PAGE_PREFIX_TO_PAGE_TYPE = {
    "HN": "HomePage",
    "SB": "ShoppingBag",
    "OC": "OrderComplete",
    "PL": "ProductListingPage",
    "FY": "ForYouPage",
}


def derive_page_type(location: str, location_cfg: dict) -> str | None:
    """Derive the V2 PageType from pf_col, falling back to Location."""
    pf_col = location_cfg.get("pf_col")
    if pf_col:
        page_prefix = PAGE_SLOT_SUFFIX_PATTERN.sub("", pf_col).upper()
    else:
        page_prefix = LOCATION_SUFFIX_PATTERN.sub("", location).upper()
    return PAGE_PREFIX_TO_PAGE_TYPE.get(page_prefix)


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
    CLIENT = 'next_uk'
    logger.warning(f'Client not specified (defaulting to {CLIENT})')

top_ads_arg = jobparser.get_arg('--top-ads-per-page-type')
TOP_ADS_PER_PAGE_TYPE = int(top_ads_arg or 100)
assert TOP_ADS_PER_PAGE_TYPE > 0, \
    'top-ads-per-page-type must be greater than zero'

# load configuration
config = config_manager.load_config(JOB_ENV)
logger.info(f"Configuring run for client: {CLIENT}")
with open(PROJECT_ROOT / f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

tbls = cfg["tables"]["write"]
SCHEMA = config.schema_write
logger.info(f'Write schema set to {SCHEMA}')

tbl_args = {'catalog': config.catalog_write, 'schema': SCHEMA, 'client': CLIENT}
PRERANKED_ADS_FROM_THEMES_LATEST = etl.map_tbl(
    tbls["preranked_ads_from_themes_latest"],
    **tbl_args
)
PRERANKED_ADS_FROM_THEMES_V2_LATEST = etl.map_tbl(
    tbls["preranked_ads_from_themes_v2_latest"],
    **tbl_args
)

logger.info('Building Location to PageType mapping from client config')
location_page_type_rows = []
skipped_locations = []
for location, location_cfg in cfg["locations"].items():
    page_type = derive_page_type(location, location_cfg)
    if page_type:
        location_page_type_rows.append((location, page_type))
    else:
        skipped_locations.append(location)

assert location_page_type_rows, 'No V2 PageType locations found in client config'

if skipped_locations:
    logger.info(
        'Skipping locations outside the V2 PageType build: '
        + ', '.join(sorted(skipped_locations)))

df_location_page_type = spark.createDataFrame(
    location_page_type_rows,
    schema='Location string, PageType string'
)

logger.info(f'Reading slot-level ranked ads from '
            f'{PRERANKED_ADS_FROM_THEMES_LATEST}')
df_preranked = (
    spark.table(PRERANKED_ADS_FROM_THEMES_LATEST)
    .select(
        'AccountNumber',
        'UniqueAdID',
        'Location',
        'Score',
        'TriggerScore',
        'Rank')
)

logger.info('Mapping slot-level Location to V2 PageType')
df_ranked_with_page_type = (
    df_preranked
    .join(F.broadcast(df_location_page_type), on='Location', how='inner')
)

logger.info('Deduplicating ads within each AccountNumber and PageType')
w_dedupe = (
    Window
    .partitionBy('AccountNumber', 'PageType', 'UniqueAdID')
    .orderBy(
        F.col('Score').desc(),
        F.col('Rank').asc(),
        F.col('Location').asc()
    )
)
df_deduped = (
    df_ranked_with_page_type
    .withColumn('DedupRank', F.row_number().over(w_dedupe))
    .where(F.col('DedupRank') == 1)
    .drop('DedupRank')
    .withColumnRenamed('Rank', 'SlotRank')
)

logger.info(f'Re-ranking top {TOP_ADS_PER_PAGE_TYPE} ads per PageType')
w_page_type_rank = (
    Window
    .partitionBy('AccountNumber', 'PageType')
    .orderBy(
        F.col('Score').desc(),
        F.col('SlotRank').asc(),
        F.col('UniqueAdID').asc()
    )
)
df_v2_ranked = (
    df_deduped
    .withColumn('Rank', F.row_number().over(w_page_type_rank))
    .where(F.col('Rank') <= TOP_ADS_PER_PAGE_TYPE)
    .select(
        'AccountNumber',
        'UniqueAdID',
        'PageType',
        'Score',
        'TriggerScore',
        'Rank')
)

logger.info('Persisting V2 PageType ranks before write')
df_v2_ranked = df_v2_ranked.persist()
row_count = df_v2_ranked.count()
logger.info(f'Materialized {row_count:,} V2 PageType ranked rows')

logger.info(f'Loading V2 PageType ranked ads to '
            f'{PRERANKED_ADS_FROM_THEMES_V2_LATEST}')
truncate_and_load(
    df_v2_ranked,
    PRERANKED_ADS_FROM_THEMES_V2_LATEST,
    pk_cols=['AccountNumber', 'PageType', 'UniqueAdID']
)

df_v2_ranked.show()

logger.info('Unpersisting cached dataframes')
df_v2_ranked.unpersist()

logger.info('Run complete')

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
import requests
import re
import pyspark.sql.types as T
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from dsutils.etl import map_tbl, post_to_webhook, delete_from_and_load


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
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

# Map write schema to parameterised write table names
tbl_args = {'schema': SCHEMA, 'client': CLIENT}
CONTROL_SHEET_LATEST = map_tbl(tbls["control_sheet_latest"], **tbl_args)
TARGET_TABLE = map_tbl(tbls["ad_items"], **tbl_args)

WEBHOOK_URL = cfg["webhooks"]["Input Warnings"]

SEARCH_ENDPOINT = cfg['apis']['search_endpoint']
SEARCH_TYPE = cfg['apis']['search_type']
DOMAIN = cfg['domain']
OSA_SUBSTRING = cfg['osa_url_substring']
TOP_N = cfg['top_n_lister_items']


# Reference for AdID key, for convenience
ak = 'UniqueAdID'

df_ctrl = (
    spark
    .table(CONTROL_SHEET_LATEST)
    .select(ak, 'URL', 'Items')
    .distinct()
)

ad_list = [row.asDict() for row in df_ctrl.collect()]

ads = {x[ak]: {y: x[y] for y in x.keys()} for x in ad_list}

logger.info(f'Processing ad items for {len(ads.keys()):,} ads')
for ad_i in list(ads.keys()):
    logger.debug(f'Processing items for ad: {ad_i}')
    ad = ads[ad_i]

    if ad['Items']:
        items_list = re.split(r'[^a-zA-Z\d]+', ad['Items'])
        ad['ItemsAd'] = [x for x in items_list if x != '']

    if ad['URL'] is None:
        msg = 'No URL supplied'
        ad["Error"] = msg
        logger.warning(f"{msg} for {ad_i}")
        continue
    if 'shop/' not in ad['URL']:
        msg = 'URL does not contain substring `shop/`'
        ad["Error"] = msg
        logger.warning(f"{msg} for {ad_i}")
        continue
    if OSA_SUBSTRING not in ad['URL']:
        msg = f'URL does not contain substring `{OSA_SUBSTRING}`'
        ad["Warning"] = msg
        logger.warning(f'{msg} for {ad_i}')

    url_path = ad['URL'].replace(" ", "").split(DOMAIN)[-1]
    full_url = DOMAIN + url_path

    query_params = {"criteria": full_url, "type": SEARCH_TYPE}
    resp = requests.request("GET", SEARCH_ENDPOINT, params=query_params)
    resp_json = json.loads(resp.text)
    ad['ItemsLister'] = [x['itemNumber'] for x in resp_json['items']][0:TOP_N]
    try:
        # If statement before assignment handles case of empty list
        if ad['ItemsLister']:
            ad['RepresentativeItems'] = ad['ItemsLister']
        else:
            raise KeyError
    except KeyError:
        # If statement before assignment handles case of empty list
        if ad['ItemsAd']:
            ad['RepresentativeItems'] = ad['ItemsAd']
        else:
            raise KeyError
    except KeyError:
        msg = 'No items found to represent ad'
        logger.warning(f'{msg} for {ad_i}')
        ad["Error"] = msg
        continue

ads_with_errors = {k: v for k, v in ads.items() if 'Error' in v.keys()}
logger.info(
    f'Errors occurred while getting items for {len(ads_with_errors):,} ads')
ads_with_warnings = {k: v for k, v in ads.items() if 'Warning' in v.keys()}
logger.info(
    f'Warnings occurred while getting for {len(ads_with_warnings):,} ads')

bot_messages = []
if ads_with_errors:
    bot_messages.append("Ad URL errors:")
    for awe in ads_with_errors.keys():
        bot_messages.append(awe + ' - ' + ads[awe]['Error'])
if ads_with_warnings:
    bot_messages.append("Ad URL warnings:")
    for aww in ads_with_warnings.keys():
        bot_messages.append(aww + ' - ' + ads[aww]['Warning'])
if JOB_ENV == 'prod' and bot_messages:
    bot_string = "\n".join(bot_messages)
    post_to_webhook(WEBHOOK_URL, bot_string)


output_list = [ads[k] for k in ads.keys() if k not in ads_with_errors.keys()]

logger.info(f'RepresentativeItems were found for {len(output_list):,} ads')
col_keys = [ak, 'RepresentativeItems']
output_list_cols = [{k: x[k] for k in col_keys} for x in output_list]

ad_item_schema = T.StructType([
    T.StructField("UniqueAdID", T.StringType(), False),
    T.StructField("RepresentativeItems", T.ArrayType(T.StringType()), True)
    ])

df_ad_items = spark.createDataFrame(output_list_cols, ad_item_schema)

logger.info("Loading output to table")
delete_from_and_load(df_ad_items.select(*col_keys),
                     TARGET_TABLE,
                     pk_cols=["UniqueAdID"],
                     del_where={"rundate": "current_date()"})

logger.info("Run complete")

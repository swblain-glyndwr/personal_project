import logging
import logging.config
import json
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import map_tbl, JobParser


logging.config.fileConfig("logging.conf")
log = logging.getLogger("mylog")

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")

DOMAIN = pargs["domain"] if pargs["domain"] else "next_uk"

log.info(f"Configuring run for domain: {DOMAIN}")
with open(f"config/{DOMAIN}.json") as f:
    cfg = json.load(f)

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][job_env]
tbl_args = {'schema': SCHEMA, 'domain': DOMAIN}
ASSIGNMENTS_TABLE_LATEST = map_tbl(tbls["assignments_latest"], **tbl_args)

log.info(f'Truncating {ASSIGNMENTS_TABLE_LATEST} ' +
         'to remove any discontinued assignments')
get_spark().sql(f'truncate table {ASSIGNMENTS_TABLE_LATEST}')

log.info('Run Complete')

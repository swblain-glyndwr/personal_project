import logging
import logging.config
import json
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import map_schema, JobParser


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)


parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")

SCHEMA = rsc["schema"][job_env]

tbls = rsc["tables"]["write"]
ASSIGNMENTS_TABLE_LATEST = map_schema(tbls["assignments_latest"], SCHEMA)

log.info(f'Truncating {ASSIGNMENTS_TABLE_LATEST} ' +
         'to remove any discontinued assignments')
get_spark().sql(f'truncate table {ASSIGNMENTS_TABLE_LATEST}')

log.info('Run Complete')

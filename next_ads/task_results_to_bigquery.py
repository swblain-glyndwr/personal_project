import logging
import logging.config
import json
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import (JobParser,
                                map_schema)


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")

# SCHEMA = rsc["schema"][job_env]
SCHEMA = 'warehouse'
tbls = rsc["tables"]["write"]
RESULTS_TOPLINE_TABLE = map_schema(tbls["results_topline"], SCHEMA)
BQ_OPTIONS = rsc['big_query']

log.info('Exporting results (topline) to Big Query')
df_results_topline = get_spark().table(RESULTS_TOPLINE_TABLE)

(
    df_results_topline
    .write.format('bigquery')
    .mode('overwrite')
    .option('temporaryGcsBucket', BQ_OPTIONS['temporaryGcsBucket'])
    .option('parentProject', BQ_OPTIONS['parentProject'])
    .option('table', BQ_OPTIONS['tables']['results_topline'])
    .save()
)

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

SCHEMA = 'warehouse'
tbls = rsc["tables"]["write"]

BQ_OPTIONS = rsc['big_query']
RESULTS_EXPORTS = [
    'results_topline',
    'results_aggregated',
    'results_ads',
    'results_ads_location',
    'results_ads_targeting',
    'results_ad_metadata'
    ]

for results_export in RESULTS_EXPORTS:
    results_table = map_schema(tbls[results_export], SCHEMA)
    log.info(f'Exporting {results_export} to Big Query')
    df_export = get_spark().table(results_table)

    (
        df_export
        .write.format('bigquery')
        .mode('overwrite')
        .option('temporaryGcsBucket', BQ_OPTIONS['temporaryGcsBucket'])
        .option('parentProject', BQ_OPTIONS['parentProject'])
        .option('table', BQ_OPTIONS['tables'][results_export])
        .save()
    )

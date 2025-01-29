import logging
import logging.config
import json
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import (JobParser,
                                map_tbl)


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

BQ_OPTIONS = cfg['big_query']
RESULTS_EXPORTS = list(BQ_OPTIONS['tables'].keys())

if job_env == 'prod':
    for results_export in RESULTS_EXPORTS:
        results_table = map_tbl(tbls[results_export], **tbl_args)
        log.info(f'Exporting {results_export} to Big Query')
        df_export = get_spark().table(results_table)

        (
            df_export
            .write.format('bigquery')
            .mode('overwrite')
            .option('temporaryGcsBucket', BQ_OPTIONS['temporaryGcsBucket'])
            .option('parentProject', BQ_OPTIONS['parentProject'])
            .option('table',
                    map_tbl(BQ_OPTIONS['tables'][results_export], **tbl_args))
            .save()
        )

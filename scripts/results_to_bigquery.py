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
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import map_tbl
from dsutils.argparser import get_job_parser


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
    CLIENT = 'next_uk'  # Client can be specified for interactive debugging
    logger.warning(f'Client not specified (defaulting to {CLIENT})')

logger.info(f"Configuring run for client: {CLIENT}")
with open(PROJECT_ROOT / f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

tbl_args = {'schema': SCHEMA, 'client': CLIENT}

BQ_OPTIONS = cfg['big_query']
RESULTS_EXPORTS = list(BQ_OPTIONS['tables'].keys())

if JOB_ENV == 'prod':
    for results_export in RESULTS_EXPORTS:
        results_table = map_tbl(tbls[results_export], **tbl_args)
        logger.info(f'Exporting {results_export} to Big Query')
        df_export = spark.table(results_table)

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

logger.info("Run Complete")

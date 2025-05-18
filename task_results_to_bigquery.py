import json
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import map_tbl
from dsutils.argparser import get_job_parser


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

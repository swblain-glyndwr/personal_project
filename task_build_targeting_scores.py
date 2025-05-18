import json
from pyspark.sql import functions as F
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import truncate_and_load, map_tbl
from dsutils.argparser import get_job_parser
from next_ads.Scoring import aggregate_model_scores


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

# Map write schema to parameterised write table names
tbl_args = {'schema': SCHEMA, 'client': CLIENT}
TARGETING_SCORES_TABLE = map_tbl(tbls["targeting_scores_latest"], **tbl_args)
CONTROL_SHEET_LATEST = map_tbl(tbls["control_sheet_latest"], **tbl_args)

# Get read tables
MODEL_SCORE_TABLE = cfg["tables"]["read"]["model_scores_latest"]

df_scores_required = (
    spark
    .table(CONTROL_SHEET_LATEST)
    .select("Models",
            "ModelCombination")
    .where(F.col("Models").isNotNull())
    .distinct()
    .fillna({"ModelCombination": "and"})
)


df_ms_agg = aggregate_model_scores(
    df_scores_required.select("Models", "ModelCombination"),
    model_score_table=MODEL_SCORE_TABLE
)
df_ms_agg.cache()


truncate_and_load(df_ms_agg,
                  table=TARGETING_SCORES_TABLE,
                  pk_cols=["AccountNumber", "TargetingCriteria"])

df_ms_agg.unpersist()

logger.info("Run complete")

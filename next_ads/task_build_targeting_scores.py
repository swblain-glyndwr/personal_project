import logging
import logging.config
import json
from pyspark.sql import functions as F
from Scoring import aggregate_model_scores
from next_ads.utils.etl import truncate_and_load, JobParser, map_schema
from next_ads.utils.dbc import get_spark


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")

MODEL_SCORE_TABLE = rsc["tables"]["read"]["model_scores_latest"]

SCHEMA = rsc["schema"][job_env]
tbls = rsc["tables"]["write"]
TARGETING_SCORES_TABLE = map_schema(tbls["targeting_scores_latest"], SCHEMA)
CONTROL_SHEET_LATEST = map_schema(tbls["control_sheet_latest"], SCHEMA)

df_scores_required = (
    get_spark()
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

log.info("Run complete")

import logging
import logging.config
import json
from pyspark.sql import functions as F
from next_ads.Scoring import aggregate_model_scores
from next_ads.utils.etl import truncate_and_load, JobParser, map_tbl
from next_ads.utils.dbc import get_spark


logging.config.fileConfig("logging.conf")
log = logging.getLogger("mylog")

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")

DOMAIN = pargs["domain"] if pargs["domain"] else "next_uk"

log.info(f"Configuring run for domain: {DOMAIN}")
with open(f"config/{DOMAIN}.json") as f:
    cfg = json.load(f)
MODEL_SCORE_TABLE = cfg["tables"]["read"]["model_scores_latest"]

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][job_env]
tbl_args = {'schema': SCHEMA, 'domain': DOMAIN}
TARGETING_SCORES_TABLE = map_tbl(tbls["targeting_scores_latest"], **tbl_args)
CONTROL_SHEET_LATEST = map_tbl(tbls["control_sheet_latest"], **tbl_args)

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

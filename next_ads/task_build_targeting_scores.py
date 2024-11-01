import argparse
import logging
import logging.config
import json
from pyspark.sql import functions as F
from AdRetrieval import get_latest_ads
from Scoring import aggregate_model_scores
from next_ads.utils.etl import truncate_and_load, get_job_env, map_schema


logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

log.info("Configuring run")
with open("config/resources.json") as f:
    rsc = json.load(f)

parser = argparse.ArgumentParser()
parser.add_argument("--f", help="dummy arg enabling interactive debugging")
parser.add_argument("--jobname", nargs="?", const="dev_", type=str)
known_args, unknown_args = parser.parse_known_args()
pargs = vars(known_args)
job_env = get_job_env(pargs)
log.info(f"Running in job environment: {job_env}")

MODEL_SCORE_TABLE = rsc["tables"]["read"]["model_scores_latest"]

SCHEMA = rsc["schema"][job_env]
tbls = rsc["tables"]["write"]
TARGETING_SCORES_TABLE = map_schema(tbls["targeting_scores_latest"], SCHEMA)


df_scores_required = (
    get_latest_ads()
    .select("Models",
            "ModelCombination")
    .where(F.col("Models").isNotNull())
    .distinct()
    .fillna({"ModelCombination": "and"})
)


df_ms_agg = aggregate_model_scores(
    df_scores_required.select("Models", "ModelCombination"),
    model_score_table=MODEL_SCORE_TABLE,
    patch_model_refs=True
)
df_ms_agg.cache()


truncate_and_load(df_ms_agg,
                  table=TARGETING_SCORES_TABLE,
                  job_env=job_env,
                  pk_cols=["AccountNumber", "TargetingCriteria"])

df_ms_agg.unpersist()

log.info("Run complete")

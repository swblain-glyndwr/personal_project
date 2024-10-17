import json
from pyspark.sql import functions as F
from AdRetrieval import get_latest_ads
from Scoring import aggregate_model_scores
from next_ads.utils.etl import create_or_replace


with open("config/resources.json") as f:
    rsc = json.load(f)


MODEL_SCORE_TABLE = rsc["tables"]["model_scores_latest"]
TARGETING_SCORES_TABLE = rsc["tables"]["targeting_scores"]


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


create_or_replace(df_ms_agg,
                  table=TARGETING_SCORES_TABLE,
                  pk_cols=["AccountNumber", "TargetingCriteria"])

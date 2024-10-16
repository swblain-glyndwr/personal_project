import json
from pyspark.sql import functions as F
from AdRetrieval import get_latest_ads
from Assignment import aggregate_model_scores
from next_ads.utils.etl import create_or_replace


with open("config/resources.json") as f:
    rsc = json.load(f)

# Constants
MODEL_SCORE_TABLE = rsc["tables"]["model_scores_latest"]
TARGETING_SCORES_TABLE = rsc["tables"]["targeting_scores"]


# Get Ad metadata where there is an associated model
# Model Combination defaults to "and", if missing
# Distinct flattens to unique TargetingCriteria
df_scores_required = (
    get_latest_ads()
    .select("Models",
            "ModelCombination")
    .where(F.col("Models").isNotNull())
    .distinct()
    .fillna({"ModelCombination": "and"})
)
# TODO: Remove renaming AlgoDivision once fully migrated to new control sheet


# Aggregate scores for all active TargetingCriteria
df_ms_agg = aggregate_model_scores(
    df_scores_required.select("Models", "ModelCombination"),
    model_score_table=MODEL_SCORE_TABLE,
    patch_model_refs=True
)
df_ms_agg.cache()


# Write to table (overwrite - large table, only keep current run)
create_or_replace(df_ms_agg,
                  table=TARGETING_SCORES_TABLE)

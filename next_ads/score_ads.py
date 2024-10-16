import json
from AdRetrieval import get_latest_ads
from pyspark.sql import functions as F
from Assignment import assign_scores_to_entity


with open("config/resources.json") as f:
    rsc = json.load(f)


# Get Ad metadata where there is an associated model
# Model Combination defaults to "and", if missing
df_ads = (
    get_latest_ads()
    .select("UniqueAdID",
            "Models",
            "ModelCombination")
    .where(F.col("Models").isNotNull())
    .fillna({"ModelCombination": "and"})
)
# TODO: Remove renaming AlgoDivision once fully migrated to new control sheet

# Score all ads for all customers
df_adscores = assign_scores_to_entity(
    df_ads.select("UniqueAdID", "Models", "ModelCombination"),
    entity_col="UniqueAdID",
    model_score_table=rsc["tables"]["model_scores_latest"],
    patch_model_refs=True
    )

df_adscores.cache()

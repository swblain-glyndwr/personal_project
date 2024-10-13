import json
from AdRetrieval import get_live_ads
from Assignment import assign_scores_to_entity


with open("config/resources.json") as f:
    rsc = json.load(f)

# Get Ads
df_ads = (
    get_live_ads(filter_underperforming=False)
    .select("UniqueAdID", "Models", "ModelCombination")
    .distinct()
)

# Score Ads
df_ad_scores = assign_scores_to_entity(
    df_ads,
    entity_col="UniqueAdID",
    model_score_table=rsc["tables"]["model_scores_latest"]
    )

# Export to table

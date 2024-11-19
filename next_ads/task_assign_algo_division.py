import json
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import build_spark_schema
import pyspark.sql.functions as F

with open("config/parameters.json") as f:
    prm = json.load(f)

div_dict = prm["algo_divisions"]

df_div_models = (
    get_spark()
    .createDataFrame(
        list([[k, v["model"]] for k, v in div_dict.items()]),
        schema=build_spark_schema([
            ["AlgoDivision", "string", "not null"],
            ["Models", "string", "not null"]
            ])
        )
).withColumn("ModelCombination", F.lit("and"))

# df_div_scores = assign_scores_to_entity(
#     df_div_models,
#     entity_col="AlgoDivision",
#     model_score_table=MODEL_SCORES_LATEST,
#     patch_model_refs=False
#     )

# BOOKMARK
# TODO: Adjust for buying rates?

# Store Division Scores (used for Landing Pages)
# Assign best Division

# Create Control Assignemnt Table
# Create Division Assignment

import logging
import logging.config
import json
from utils.dbcutils import get_display
from PageBuilder import (
    get_underperforming_ads,
    get_live_ads,
    get_pscores
    )
from pyspark.sql import functions as F

logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")


with open("config/resources.json") as f:
    rsc = json.load(f)


# ARGUMENTS
location = "HN1"

# Get Ad data
df_live_ads = get_live_ads(location)


# Get underperforming Ads
df_under_perf = get_underperforming_ads(location)


# Remove underperforming from Ads to process
df_ads = (
    df_live_ads
    .join(
        df_under_perf,
        on=["UniqueAdID", "Division"],
        how="leftanti"
    )
)


# Append Scores to Ads according to associated models
# Split out model column on delimiter
split_model_col_asterisk = F.split(F.col("model"), r"\*")
split_model_col_period = F.split(F.col("model"), r"\.")
max_models_assigned = (
    df_ads
    .withColumn("models_assigned", F.size(split_model_col_asterisk))
    .agg(F.max(F.col("models_assigned")).alias("models_max"))
    .collect()[0]["models_max"]
)

for n in range(1, max_models_assigned+1):
    df_ads = (
        df_ads
        .withColumn(
            f"model{str(str(n).zfill(2))}",
            split_model_col_asterisk.getItem(n-1)
        )
    )

df_ads = df_ads.drop("model")

# Melt models down to rows per
df_ads = (
    df_ads
    .melt(
        ids=[c for c in df_ads.columns if not c.startswith("model")],
        values=[c for c in df_ads.columns if c.startswith("model")],
        variableColumnName="modeln",
        valueColumnName="model"
    )
    .drop("modeln")
    .where(F.col("model").isNotNull())
)

df_ad_score_lookup = (
    df_ads
    .select("UniqueAdID", "Division", "model")
    .withColumn("model_split", split_model_col_period)
    .withColumn(
        "model_score_col",
        F.col("model_split").getItem(F.size(F.col("model_split"))-1)
    )
    .drop("model", "model_split")
)
get_display(df_ad_score_lookup)

model_score_cols = [
    x[0] for x in (
        df_ad_score_lookup
        .select("model_score_col")
        .distinct()
        .collect()
        )
    ]

df_pscores = get_pscores(division="womens", col_subset=model_score_cols)
get_display(df_pscores)

# Combine scores
# TODO: Function

# Standardise scores
# TODO: Function

# Determine Best Score (model combination) for each customer

# Determine Random Score for each customer
# Could we do this by subbing division in as the model combination

# Randomise within model combination


# Write output to table
# TODO: Create output table in ds_sandbox
# account,
# algodivision,
# location,
# overall_cell,
# page_cell,
# best_ad,
# best_masid_token,
# random_ad,
# random_masid,
# assigned_masid_token

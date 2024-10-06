import logging
import logging.config
import json
from utils.dbcutils import get_spark

logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")


with open("config/resources.json") as f:
    rsc = json.load(f)


# Get Ad, Location, AlgoDivision, Models from ControlSheet
# TODO: Create controlsheet table in ds_sandbox
df_ctrl = get_spark().table(rsc["table"]["control_sheet"])


# Filter out underperforming Ads
# TODO: Get from results file

# Append Scores to Ads according to associated models
# TODO: Lookups for each divisions model scores tables

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

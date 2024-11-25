import logging
import logging.config
import json
from next_ads.utils.dbc import get_spark
from pyspark.sql import functions as F
from next_ads.utils.etl import (truncate_and_load,
                                JobParser,
                                map_schema)


# Configure logging
logging.config.fileConfig("config/logging.conf")
log = logging.getLogger("mylog")

# Configure run
with open("config/resources.json") as f:
    rsc = json.load(f)
with open("config/parameters.json") as f:
    prm = json.load(f)

parser = JobParser()
pargs, job_env = parser.parse_job_args(["--jobname"])
log.info(f"Running in job environment: {job_env}")

CELL_ASSIGNMENT = rsc["files"]["cell_assignment"]
DIVISION_ASSIGNMENTS = rsc["files"]["division_assignments"]

SCHEMA = rsc["schema"][job_env]
FIXED_CELLS_TABLE = map_schema(rsc["tables"]["write"]["fixed_cells"], SCHEMA)


log.info("Fetching existing assignments")
# Import existing file for test cell assignment
df_cells_raw = (
    get_spark()
    .read.format("delta").load(CELL_ASSIGNMENT)
    .select(
        "account_number",
        "OverallTestControl",
        "Test",
        "HPTest",
        "OCTest",
        "VariantTest",
        "VariantTest2",
        "VariantTest3",
        "VariantTest4"
    )
)

# Rename columns to fit new schmea
df_cells_fmt = (
    df_cells_raw
    .withColumnsRenamed(
        {
            "account_number": "AccountNumber",
            "OverallTestControl": "FallowControl",
            "HPTest": "HN",
            "Test": "SB",
            "OCTest": "OC",
            "VariantTest": "AdHocAB1",
            "VariantTest2": "AdHocAB2",
            "VariantTest3": "AdHocAB3",
            "VariantTest4": "AdHocAB4"
        }
    )
    .withColumns({
        "ChampionChallenger": F.lit("Champion")
    })
    .select(
        "AccountNumber",
        "FallowControl",
        "HN",
        "SB",
        "OC",
        "AdHocAB1",
        "AdHocAB2",
        "AdHocAB3",
        "AdHocAB4",
        "ChampionChallenger"
    )
)


div_asgn_list = []
for div_k in DIVISION_ASSIGNMENTS.keys():

    df_div = (
        get_spark()
        .read.format("delta")
        .load(DIVISION_ASSIGNMENTS[div_k])
        .select("account_number")
        .withColumnRenamed("account_number", "AccountNumber")
        .withColumn("AlgoDivision", F.lit(div_k))
    )

    div_asgn_list.append(df_div)

df_cust_div = div_asgn_list.pop()
for df_asgn in div_asgn_list:
    df_cust_div = df_cust_div.unionByName(df_asgn)


df_cells = (
    df_cells_fmt
    .join(df_cust_div, on="AccountNumber", how="left")
)

# count_null_by_column(df_cells)
# df_cells.where(F.col("AlgoDivision").isNull())
# TODO: Why are some customers not assigned a Division? Remove for now...
df_cells = df_cells.where(F.col("AlgoDivision").isNotNull())


log.info(f"Writing assignments to {FIXED_CELLS_TABLE}")
truncate_and_load(df_cells,
                  FIXED_CELLS_TABLE,
                  pk_cols=["AccountNumber"])


# vvvv New file dev below vvvv

# RPID_WITH_ACCOUNTS = rsc["tables"]["read"]["rpid_with_accounts"]
# MODEL_SCORES_LATEST = rsc["tables"]["read"]["model_scores_latest"]

# LEGACY_EXCL = rsc["legacy"]["account_exclusions"]

# FALLOW_PC = prm["fallow_control"]["proportion"]
# FALLOW_SEED = prm["fallow_control"]["seed"]
# MACRO_LOCATIONS = prm["macro_locations"]
# CHALLENGER_PC = prm["challenger"]["proportion"]
# CHALLENGER_SEED = prm["challenger"]["seed"]


# # GET CUSTOMER BASE
# # Read in RPID with Accounts
# # Query inherited from Gill's script
# # TODO: Should we take lastest updated record to de-dup instead?
# df_rpid_w_acc = (
#         get_spark()
#         .table(RPID_WITH_ACCOUNTS)
#         .select("account_number", "roamingprofileid")
#         .where(~F.col("account_number").isin(LEGACY_EXCL))
#         .distinct()
# )

# # Read in SVOC table
# # SVOC table used because it contains older accounts too
# # Where clause inherited from Gill's script
# df_cust = (
#     get_spark()
#     .table("pii.svoccust_pii")
#     .where(
#         (F.col("countrycode").isin("GB"))
#         & (F.col("client") == "NEXT")
#         & (F.col("AccountIsCurrent") == "Y")
#         & (F.col("LatestAccountKeyIndicator") == 1)
#         )
#     .join(df_rpid_w_acc, on="account_number")
#     .select("account_number")
#     .withColumnRenamed("account_number", "AccountNumber")
# )

# assert_pk(df_cust, ["AccountNumber"])
# df_cust.cache()
# n_cust = df_cust.count()


# # CREATE TABLE - ONLY ON INITIAL RUN

# # Fallow assignment
# # OrderBy first for deterministic

# df_fallow = (
#     df_cust
#     .orderBy(F.col("AccountNumber"))
#     .withColumn("RandomFallow", F.rand(seed=FALLOW_SEED))
#     .withColumn("FallowControl", F.col("RandomFallow") <= FALLOW_PC)
#     )
# n_cust_fallow = df_fallow.where(F.col("FallowControl")).count()
# # TODO: Check/calibrate spend per customer of fallow and test group?


# # Get macro locations and seeds
# macro_locs = list(MACRO_LOCATIONS.keys())
# macro_loc_seeds = {k: MACRO_LOCATIONS[k]["seed"] for k in macro_locs}

# # Convert to dataframe
# schema = build_spark_schema([["MacroLocation", "string", "not null"]])
# df_macro_loc = get_spark().createDataFrame([(c,) for c in macro_locs],
#                                            schema)

# # Append column of Random variable for each Macro Location
# df_test_rdm = (
#     df_fallow
#     .where(~F.col("FallowControl"))
#     .drop("RandomFallow", "FallowControl")
# )
# for macro_loc in macro_locs:
#     df_test_rdm = (
#         df_test_rdm
#         .withColumn(f"Random{macro_loc}",
#                     F.rand(seed=macro_loc_seeds[macro_loc]))
#     )

# # Build case-when to map cell references
# macro_loc_cell_assignments = []
# for macro_loc in macro_locs:
#     cells = MACRO_LOCATIONS[macro_loc]["cells"]
#     when_strs = []
#     for cell in cells:
#         when_str = f"when Random{macro_loc} <= {cell[0]} then '{cell[1]}'"
#         when_strs.append(when_str)
#     macro_loc_cell_assignment = (
#         "case "
#         + " ".join(when_strs)
#         + f" else null end as Cell{macro_loc}"
#     )
#     macro_loc_cell_assignments.append(macro_loc_cell_assignment)

# macro_loc_cell_case_when = ",\n".join(macro_loc_cell_assignments)

# # Map test cell references
# df_test_rdm.createOrReplaceTempView("df_test_rdm_tmp")
# df_test_cells = (
#     get_spark()
#     .sql(f"""
#          select a.*,
#          {macro_loc_cell_case_when} from df_test_rdm_tmp a
#          """)
# )

# # Champion-Challenger assignment
# df_test_cells_champ = (
#     df_test_cells
#     .withColumn("RandomChallenger", F.rand(seed=CHALLENGER_SEED))
#     .withColumn("Challenger", F.col("RandomChallenger") <= CHALLENGER_PC)
#     )

# df_test_ctrl = (
#     df_cust
#     .join(df_fallow, on="AccountNumber", how="left")
#     .join(df_test_cells_champ, on="AccountNumber", how="left")
# )
# df_test_ctrl = (
#     df_test_ctrl
#     .drop(*[c for c in df_test_ctrl.columns if c.startswith("Random")])
# )
# assert_pk(df_test_ctrl, ["AccountNumber"])


# # Overall Division
# # Get Divisions
# div_dict = prm["divisions"]
# df_div = (
#     get_spark()
#     .createDataFrame(
#         list([[k, v["model"]] for k, v in div_dict.items()]),
#         schema=build_spark_schema([
#             ["Division", "string", "not null"],
#             ["Models", "string", "not null"]
#             ])
#         )
# ).withColumn("ModelCombination", F.lit("and"))

# df_div_scores = assign_scores_to_entity(
#     df_div,
#     entity_col="Division",
#     model_score_table=MODEL_SCORES_LATEST,
#     patch_model_refs=False
#     )

# # BOOKMARK
# # TODO: Adjust for buying rates?

# # Store Division Scores (used for Landing Pages)
# # Assign best Division

# # Create Control Assignemnt Table
# # Create Division Assignment

log.info("Run complete")

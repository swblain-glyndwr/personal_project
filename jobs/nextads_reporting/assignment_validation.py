import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from pyspark.sql import functions as F
from pyspark.sql import Window
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import assert_pk, get_table_pk_cols, post_to_webhook
from dsutils.argparser import get_job_parser
from next_ads.common import config_manager
from next_ads.common.paths import load_client_config
from next_ads.common import etl
from pyspark.sql.types import BooleanType
from next_ads.data.schemas.CMS import cms_schema

jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client")
LOG_LEVEL = jobparser.get_arg("--log_level")
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert JOB_ENV.lower() == "dev", (
        f"Client must be specified when running in {JOB_ENV}"
    )
    CLIENT = "next_uk"  # Client can be specified for interactive debugging
    logger.warning(f"Client not specified (defaulting to {CLIENT})")

# load configuration
config = config_manager.load_config(JOB_ENV, client=CLIENT)
logger.info(f"Configuring run for client: {CLIENT}")
cfg = load_client_config(CLIENT)

LOCATIONS = cfg["locations"]

PRODUCT_CATALOG_TABLE = cfg["tables"]["read"]["product_catalog"]

tbls = cfg["tables"]["write"]
SCHEMA = config.schema_write
logger.info(f"Write schema set to {SCHEMA}")

# Map write schema to parameterised write table names
tbl_args = {
    "catalog": config.catalog_write,
    "schema": SCHEMA,
    "client": CLIENT,
}
ASSIGNMENTS_TABLE_LATEST = etl.map_tbl(tbls["assignments_latest"], **tbl_args)
CELLS_TABLE_LATEST = etl.map_tbl(tbls["customer_cells_latest"], **tbl_args)
ITEM_THEMES_TABLE_LATEST = etl.map_tbl(tbls["item_themes_latest"], **tbl_args)

FALLOW_TRUE = cfg["fallow_control"]["true_label"]
FIXED_CELLS = cfg["fixed_cells"]

MAX_THEMES_PER_PID = cfg["themes_qa"]["max_themes_per_pid"]
MIN_THEME_PIDS = cfg["themes_qa"]["min_theme_pids"]
MAX_ZERO_THEMES_PC = cfg["themes_qa"]["max_zero_themes_pc"]
MAX_MULTI_THEMES_PC = cfg["themes_qa"]["max_multi_themes_pc"]
PID_LOOKBACK_DAYS = cfg["attributes"]["lookback_days"]

WEBHOOK_URL = cfg["webhooks"]["DS Warnings"]

errors = []  # Collect all assertion errors and raise at end of script
warnings = []  # Collect all warnings and post to webhook at end of script (but don't fail the job)

df_assigned = spark.table(ASSIGNMENTS_TABLE_LATEST)
df_cells = spark.table(CELLS_TABLE_LATEST)

logger.info("Checking the CMS data vs the Control Sheet (v1)")

cms = spark.table(config.tables_write.cms_content_latest).filter(
    "CMSPageID <> '' "
)
ctrl_sheet = spark.table(config.tables_write.control_sheet_raw_latest)


cms_check = (
    ctrl_sheet.join(cms, ["CMSPageID"])
    .withColumn("cms_data", F.from_json("cms_data", cms_schema))
    .selectExpr(
        "UniqueAdID",
        "cms_data.data.externalPageId as CMSPageID",
        "cms_data.data.title as cms_Title",
        "Status as ctrl_status",
        "url as ctrl_url",
        "cms_data.data.placements[0].content[0].items[0].target as cms_target_url",
        "from_unixtime(cms_data.data.lastChangedTimestamp  / 1000) as cms_LastChangedTimestamp",
        "case when (cms_Title is null and ctrl_status = 'Active') then 'Ad active but not found in CMS' else null end as error1",
        "case when (cms_target_url <> ctrl_url)                   then 'Ad target urls do not match between CMS and Control Sheet' else null end as error2",
        "case when (left(UniqueAdID,10) <> left(cms_Title, 10)) then 'Ad matches on CMSContentId but the UniqueAdID is different' else null end as error3",
        "concat_ws('-',error1, error2, error3) as errors",
    )
    .filter(
        "(cms_Title is null and ctrl_status = 'Active') or (cms_target_url <> ctrl_url ) or (left(UniqueAdID,10) <> left(cms_Title, 10) ) "
    )
    .drop("error1", "error2", "error3")
    .select(
        "UniqueAdID",
        "CMSPageID",
        "errors",
        "cms_Title",
        "ctrl_status",
        "ctrl_url",
        "cms_target_url",
        "cms_LastChangedTimestamp",
    )
    .selectExpr(
        "concat_ws('|', errors, UniqueAdID, CMSPageID, cms_Title, ctrl_status, ctrl_url, cms_target_url, cms_LastChangedTimestamp) as error_msg"
    )
)

for row in cms_check.select("error_msg").collect():
    warnings.append(row.error_msg)


logger.info("Checking the Sort Order item types vs the Product Catalog")


def has_common_substring(s1, s2, min_length=4):
    if s1 is None or s2 is None:
        return False

    s1 = s1.lower()
    s2 = s2.lower()

    # Iterate over the shorter string for efficiency
    if len(s1) > len(s2):
        s1, s2 = s2, s1

    n = len(s1)

    for length in range(min_length, n + 1):
        for start in range(n - length + 1):
            if s1[start : start + length] in s2:
                return True

    return False


has_common_substring_udf = F.udf(has_common_substring, BooleanType())

so_latest = (
    spark.table(config.tables_write.sort_order_v2_latest)
    .selectExpr(
        "UniqueAdID",
        "items as pid",
        "item_pos",
        "AlgoDivision",
        "TradeDivision",
        "Url",
    )
    .filter("item_pos <= 1")
)
pc = (
    spark.table(config.tables_read.product_catalog_latest)
    .select("pid", "brand", "gender", "department", "use")
    .distinct()
)

comb = so_latest.join(pc, ["pid"])

comb = comb.withColumn(
    "div_string",
    F.concat_ws(
        "", F.lower(F.col("AlgoDivision")), F.lower(F.col("TradeDivision"))
    ),
).withColumn(
    "attr_string",
    F.concat_ws(
        "",
        F.lower(F.col("brand")),
        F.lower(F.col("gender")),
        F.lower(F.col("department")),
        F.lower(F.col("use")),
    ),
)

sort_order_check = (
    (
        comb.filter(
            ~has_common_substring_udf(
                F.col("div_string"), F.col("attr_string")
            )
        )
    )
    .selectExpr(
        "'sort order data items not associated with ad' as error_msg",
        "pid as itemnum",
        "UniqueAdID",
        "item_pos",
        "AlgoDivision as ctrl_AlgoDivision",
        "TradeDivision as ctrl_TradeDivision",
        "Url as target_url",
        "brand as item_brand",
        "gender as item_gender",
        "department as item_department",
        "use as item_use",
    )
    .selectExpr(
        "struct(error_msg, itemnum, UniqueAdID, item_pos, ctrl_AlgoDivision, ctrl_TradeDivision, target_url) as ads_data",
        "struct(item_brand, item_gender, item_department, item_use) as item_data",
    )
    .selectExpr("to_json(struct(ads_data, item_data)) as error ")
)

for row in sort_order_check.select("error").collect():
    warnings.append(row.error)


logger.info("Checking for invalid Homepage Teaser assignments")

teaser_locs = ["PH3", "PH4"]
w_acc = Window.partitionBy("AccountNumber")

df_invalid_teasers = (
    df_assigned.where(F.col("Location").isin(teaser_locs))
    .withColumn(
        "TeaserAssigned",
        F.when(F.col("MASID").endswith("_Z"), F.lit(0)).otherwise(F.lit(1)),
    )
    .withColumn("TeasersAssigned", F.sum("TeaserAssigned").over(w_acc))
    .drop("TeaserAssigned")
    .withColumn("MASIDToken", F.split("MASID", "_")[1])
    .withColumn("TokenSet", F.collect_set(F.col("MASIDToken")).over(w_acc))
    .withColumn("UniqueTokens", F.array_size("TokenSet"))
    .where(
        (F.col("TeasersAssigned") < len(teaser_locs))
        | (F.col("UniqueTokens") < len(teaser_locs))
    )
    .where(F.col("TokenSet") != F.array(F.lit("Z")))
)

if df_invalid_teasers.count() > 0:
    df_invalid_teaser_accounts = df_invalid_teasers.select(
        "AccountNumber"
    ).distinct()

    n_it = df_invalid_teaser_accounts.count()
    msg_it = (
        f"{n_it:,} accounts found with invalid HomePage Teasers. "
        "Assignments have not been modified."
    )
    logger.warning(msg_it)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, msg_it)


df_assigned_dt = df_assigned.select("rundate").distinct()
df_cells_dt = df_cells.select("rundate").distinct()
assigned_dts = [x[0] for x in df_assigned_dt.collect()]
cells_dts = [x[0] for x in df_cells_dt.collect()]

try:
    assert len(assigned_dts) == 1, (
        f"Multiple dates in {ASSIGNMENTS_TABLE_LATEST}"
    )  # noqa
except AssertionError as e:
    errors.append(str(e))

try:
    assert len(cells_dts) == 1, f"Multiple dates in {CELLS_TABLE_LATEST}"
except AssertionError as e:
    errors.append(str(e))

try:
    assert assigned_dts == cells_dts, (
        "Assignment dates do not equal Cells dates"
    )  # noqa
except AssertionError as e:
    errors.append(str(e))

logger.info("Checking integrity of Fallow Control")
df_assignments_w_cells = df_assigned.join(
    df_cells, on="AccountNumber", how="inner"
)

df_fallow_with_ads = df_assignments_w_cells.where(
    F.col("FallowControl") == FALLOW_TRUE
).where(F.col("UniqueAdIDAssigned") != "NoAd")

ads_in_control = df_fallow_with_ads.count()

try:
    assert ads_in_control == 0, "Ads assigned to Fallow Control customers"
except AssertionError as e:
    errors.append(str(e))


logger.info("Checking integrity of Local Controls")
local_control_labels = dict()
for fc in FIXED_CELLS:
    for i in FIXED_CELLS[fc]["cells"]:
        if "control" in i["then"]["lit"].lower():
            local_control_labels[fc] = i["then"]["lit"]

lc_to_location = dict()
for local_control in local_control_labels:
    lc_to_location[local_control] = []

for lc, lc_val in local_control_labels.items():
    for location in LOCATIONS:
        for m in LOCATIONS[location]["map"]:
            for i in m["when"]:
                if i["col"] == lc and i["val"] == lc_val:
                    lc_to_location[lc].append(location)

for lc in lc_to_location:
    for location in lc_to_location[lc]:
        logger.info(f"Checking {lc} local control for location {location}")
        df_lc_with_ads = (
            df_assignments_w_cells.where(F.col("Location") == location)
            .where(F.col(lc) == local_control_labels[lc])
            .where(F.col("UniqueAdIDAssigned") != "NoAd")
        )
        ads_in_lc = df_lc_with_ads.count()
        try:
            assert ads_in_lc == 0, (
                f"Ads assigned to {lc} at location: {location}"
            )  # noqa
        except AssertionError as e:
            errors.append(str(e))


logger.info("Checking that all NoAd assignments map to MASID ending _Z")
df_noad_nonz = df_assignments_w_cells.where(
    F.col("UniqueAdIDAssigned") == "NoAd"
).where(~F.col("MASID").endswith("_Z"))
df_noad_nonz_n = df_noad_nonz.count()

try:
    assert df_noad_nonz_n == 0, (
        "Non _Z-ending MASIDs found for NoAd assignments"
    )  # noqa
except AssertionError as e:
    errors.append(str(e))

logger.info("Checking for excessive NoAdFound assignments")
df_avg_no_ad_found = (
    df_assigned.withColumn(
        "is_no_ad_found",
        F.when(F.col("UniqueAdIDAssigned") == "NoAdFound", 1).otherwise(0),
    )
    .groupBy("AccountNumber")
    .agg(F.sum("is_no_ad_found").alias("no_ad_count_per_account"))
    .agg(
        F.round(F.avg("no_ad_count_per_account"), 2).alias(
            "avg_no_ad_found_per_account"
        )
    )
)

avg_no_ad_found = df_avg_no_ad_found.first()["avg_no_ad_found_per_account"]

if avg_no_ad_found is not None and avg_no_ad_found > 5.0:
    warning_msg = (
        f"Warning: Average count of 'NoAdFound' in UniqueAdIDAssigned "
        f"per account is {avg_no_ad_found} (threshold: 5.0)"
    )
    logger.warning(warning_msg)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, warning_msg)


logger.info("Checking Primary Key validity of latest process tables")
# Checking history tables too would progressively increase process runtime
for tbl in tbls:
    if not tbl.endswith("_latest"):
        continue
    tbl_mapped = etl.map_tbl(tbls[tbl], **tbl_args)
    if not spark.catalog.tableExists(tbl_mapped):
        logger.info(
            f"  ↳ Table {tbl_mapped} does not exist, skipping PK check."
        )
        continue
    pk_cols = get_table_pk_cols(tbl_mapped)
    if not pk_cols:
        logger.info(f"  ↳ Skipping: {tbl_mapped}, due to no PK's defined.")
        continue
    logger.info(f"  ↳ Asserting {pk_cols} as PK for {tbl_mapped}")
    df_tbl_pk = spark.table(tbl_mapped)

    try:
        assert_pk(df_tbl_pk, pk_cols), f"Primary Key invalid: {tbl_mapped}"
    except AssertionError as e:
        errors.append(str(e))


# Themes checks
themes = spark.table(ITEM_THEMES_TABLE_LATEST).where(F.col("theme_rank") == 1)

logger.info("Checking maximum themes per PID")
themes_per_pid = (
    themes.groupBy("pid")
    .agg(F.countDistinct("theme").alias("n_themes"))
    .where(F.col("n_themes") > MAX_THEMES_PER_PID)
)
n_err_themes_per_pid = themes_per_pid.count()
msg_themes_per_pid = (
    f"{n_err_themes_per_pid:,} PIDs have"
    + f" > {MAX_THEMES_PER_PID} themes assigned"
)
if n_err_themes_per_pid > 0:
    logger.warning(msg_themes_per_pid)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, msg_themes_per_pid)


logger.info("Checking theme coverage of all PIDs")
all_pids = (
    spark.table(PRODUCT_CATALOG_TABLE)
    .where(F.col("end_date") > F.date_sub(F.current_date(), PID_LOOKBACK_DAYS))
    .select("pid")
    .distinct()
)

n_all_pids = all_pids.count()
n_theme_pids = themes.select("pid").distinct().count()
logger.info("Checking count of distinct PIDs with themes assigned")
if n_theme_pids < MIN_THEME_PIDS:
    msg_min_pids = (
        f"Only {n_theme_pids:,} distinct PIDs with themes"
        + " associated returned from product catalog"
        + f" (expected >= {MIN_THEME_PIDS:,})"
    )
    logger.warning(msg_min_pids)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, msg_min_pids)

all_pids_themes = (
    all_pids.join(themes.select("pid", "theme"), on="pid", how="left")
    .groupBy("pid")
    .agg(F.countDistinct("theme").alias("n_themes"))
    .groupBy("n_themes")
    .agg(F.countDistinct("pid").alias("n_pids"))
    .withColumn("pc_pids", F.col("n_pids") / F.lit(n_all_pids))
)

pc_zero_themes = (
    (
        all_pids_themes.where(F.col("n_themes") == 0)
        .select("pc_pids")
        .collect()[0][0]
    )
    or 0
)

pc_multi_themes = (
    (
        all_pids_themes.where(F.col("n_themes") > 1)
        .agg(F.sum("pc_pids").alias("pc_pids"))
        .select("pc_pids")
        .collect()[0][0]
    )
    or 0
)

logger.info("Checking proportion of PIDs without a theme")
if pc_zero_themes > MAX_ZERO_THEMES_PC:
    msg_zero_themes = (
        f"{pc_zero_themes:.1%} PIDs found with zero themes"
        + f" (expected <= {MAX_ZERO_THEMES_PC:.1%})"
    )
    logger.warning(msg_zero_themes)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, msg_zero_themes)

logger.info("Checking proportion of PIDs with multiple themes")
if pc_multi_themes > MAX_MULTI_THEMES_PC:
    msg_multi_themes = (
        f"{pc_multi_themes:.1%} PIDs found with multiple"
        + f" themes (expected <= {MAX_MULTI_THEMES_PC:.1%})"
    )
    logger.warning(msg_multi_themes)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, msg_multi_themes)

if warnings:
    msg_warnings = "\n".join(warnings)
    if JOB_ENV == "prod":
        # don't raise an exception for warnings, but post to webhook
        post_to_webhook(WEBHOOK_URL, msg_warnings)

if errors:
    msg_finalassertion = "\n".join(errors)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, msg_finalassertion)
    # Raise a combined AssertionError with all messages
    raise AssertionError(msg_finalassertion)

logger.info("Run Complete")

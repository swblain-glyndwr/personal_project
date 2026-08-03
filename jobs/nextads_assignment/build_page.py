import atexit
import sys
from datetime import date, timedelta
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
from next_ads.decisioning.assignment import (
    assign_nextgenads,
    assign_preranked_ads,
    assign_random_ads,
    assign_random_ads_with_exclusions,
)
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (
    build_spark_schema,
    chain_when_thens,
    post_to_webhook,
)
from dsutils.argparser import get_job_parser
from jobs.nextads_assignment.publish_build import (
    ScopeManifestEntry,
    build_assignment_scope_contract,
    parse_scope_manifest_json,
    resolve_assignment_tables,
)
from next_ads.common import config_manager
from next_ads.common.paths import load_client_config
from next_ads.common import etl
from next_ads.decisioning.assignment_manifest import (
    split_assignment_scope_manifest,
    validate_configured_v1_scope_manifest,
)
from next_ads.decisioning.assignment_publication import (
    AssignmentColumnContract,
    stage_assignment_scope,
)
from next_ads.ranking.scoring_inputs import read_delta_version


def _get_required_job_arg(job_parser, name):
    value = job_parser.get_arg(name)
    if value is None or not str(value).strip():
        raise ValueError(f"{name} must be provided")
    return str(value).strip()


def _get_integer_job_arg(job_parser, name, *, minimum):
    raw_value = _get_required_job_arg(job_parser, name)
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return value


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg("--job_env")
CLIENT = jobparser.get_arg("--client")
LOG_LEVEL = jobparser.get_arg("--log_level")
RAW_SCOPE_MANIFEST = _get_required_job_arg(
    jobparser,
    "--scope_manifest_json",
)
split_assignment_scope_manifest(RAW_SCOPE_MANIFEST)
SCOPE_MANIFEST = parse_scope_manifest_json(
    RAW_SCOPE_MANIFEST
)
RUN_DATE_RAW = _get_required_job_arg(jobparser, "--run_date")
try:
    RUN_DATE = date.fromisoformat(RUN_DATE_RAW)
except ValueError as exc:
    raise ValueError("--run_date must use ISO format YYYY-MM-DD") from exc
BUILD_RUN_ID = _get_required_job_arg(jobparser, "--build_run_id")
if not BUILD_RUN_ID.startswith("v1_") or BUILD_RUN_ID == "v1_":
    raise ValueError(
        "--build_run_id must start with 'v1_' and include a run identifier"
    )
TASK_RUN_ID = _get_integer_job_arg(
    jobparser,
    "--task_run_id",
    minimum=1,
)
EXECUTION_COUNT = _get_integer_job_arg(
    jobparser,
    "--execution_count",
    minimum=0,
)
CUSTOMER_CELLS_TABLE = _get_required_job_arg(
    jobparser,
    "--customer_cells_table",
)
CUSTOMER_CELLS_DELTA_VERSION = _get_integer_job_arg(
    jobparser,
    "--customer_cells_delta_version",
    minimum=0,
)
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

cached_assignment_frames = []
cache_cleanup_registered = False


def _release_cached_assignment_frames():
    global cache_cleanup_registered
    while cached_assignment_frames:
        cached_frame = cached_assignment_frames.pop()
        try:
            cached_frame.unpersist()
        except Exception as exc:
            logger.warning(f"Unable to release cached assignment frame: {exc}")
    cache_cleanup_registered = False


def _cache_assignment_frame(df):
    global cache_cleanup_registered
    if not cache_cleanup_registered:
        # A Python-file task exits after an uncaught failure. Registering the
        # cleanup after Spark starts makes it run before Spark's own exit hook.
        atexit.register(_release_cached_assignment_frames)
        cache_cleanup_registered = True
    cached_frame = df.cache()
    cached_assignment_frames.append(cached_frame)
    return cached_frame


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

LOCATION = jobparser.get_arg("--location")
INHERIT_BASIC_FROM = jobparser.get_arg("--inherit_basic_from")
if INHERIT_BASIC_FROM is not None:
    INHERIT_BASIC_FROM = str(INHERIT_BASIC_FROM).strip() or None
if not LOCATION:
    assert JOB_ENV.lower() == "dev", (
        f"Location must be specified when running in {JOB_ENV}"
    )
    LOCATION = "SB1"  # Location can be specified for interactive debugging
    logger.warning(f"Location not specified (defaulting to {LOCATION})")

LOCATIONS = cfg["locations"]
validate_configured_v1_scope_manifest(SCOPE_MANIFEST, config.locations)
MANIFEST_SCOPES = tuple(entry.scope for entry in SCOPE_MANIFEST)
if LOCATION not in MANIFEST_SCOPES:
    raise ValueError(
        f"V1 location {LOCATION!r} is not present in the scope manifest"
    )

CURRENT_SCOPE_ENTRY = next(
    entry for entry in SCOPE_MANIFEST if entry.scope == LOCATION
)
CONFIGURED_INHERIT_BASIC_FROM = LOCATIONS[LOCATION].get(
    "inherit_basic_from"
)
if (
    CURRENT_SCOPE_ENTRY.inherit_basic_from
    != CONFIGURED_INHERIT_BASIC_FROM
):
    raise ValueError(
        f"V1 location {LOCATION!r} inheritance does not match config: "
        f"manifest={CURRENT_SCOPE_ENTRY.inherit_basic_from!r}, "
        f"configured={CONFIGURED_INHERIT_BASIC_FROM!r}"
    )
if INHERIT_BASIC_FROM != CURRENT_SCOPE_ENTRY.inherit_basic_from:
    raise ValueError(
        f"V1 location {LOCATION!r} inheritance argument does not match "
        f"the scope manifest: argument={INHERIT_BASIC_FROM!r}, "
        f"manifest={CURRENT_SCOPE_ENTRY.inherit_basic_from!r}"
    )
LOCATION_SCOPE_MANIFEST: tuple[ScopeManifestEntry, ...] = (
    CURRENT_SCOPE_ENTRY,
)

MIN_C_SESSIONS = cfg["results_prm"]["min_c_sessions"]
INCREMENTAL_LOOKBACK = cfg["incrementality"]["incremental_lookback"]
CHECK_SESSIONS_FROM = RUN_DATE - timedelta(
    days=INCREMENTAL_LOOKBACK + 1
)

# Switch to turn incrementality on or off
INCREMENTALITY_ADS_SUPPRESSION_SWITCH = cfg["incrementality"][
    "incrementality_ads_suppression_switch"
]
ADS_SWITCH_LABEL = cfg["incrementality"]["ads_switch_label"]
INCREMENTALITY_LOCATIONS = cfg["incrementality"]["locations"]
INCREMENTALITY_TREATMENTS = cfg["incrementality"]["treatments"]
AD_SUPPRESSION_MASID_TOKEN = cfg["incrementality"]["masid_test_token"]
INC_AD_SUPPRESSION_THRESHOLD = cfg["incrementality"][
    "incremental_value_threshold"
]
INC_ADS_SUFFIX = cfg["incrementality"]["incremental_ads_suffix"]


tbls = cfg["tables"]["write"]
SCHEMA = config.schema_write
logger.info(f"Write schema set to {SCHEMA}")

# Map write schema to parameterised write table names
tbl_args = {
    "catalog": config.catalog_write,
    "schema": SCHEMA,
    "client": CLIENT,
}
CONTROL_SHEET_LATEST = etl.map_tbl(tbls["control_sheet_latest"], **tbl_args)
TARGETING_SCORES_TABLE = etl.map_tbl(
    tbls["targeting_scores_latest"], **tbl_args
)
ASSIGNMENT_TABLES = resolve_assignment_tables(config, "v1")
ASSIGNMENT_COLUMNS = AssignmentColumnContract()
ASSIGNMENT_SCOPE_CONTRACT = build_assignment_scope_contract(
    "v1",
    LOCATION_SCOPE_MANIFEST,
)
ASSIGNMENT_INPUT_COLUMNS = tuple(
    column
    for column in ASSIGNMENT_SCOPE_CONTRACT.public_columns
    if column != ASSIGNMENT_SCOPE_CONTRACT.publication_date_column
)
PRERANKED_THEMES_TABLE = etl.map_tbl(
    tbls["preranked_ads_from_themes_latest"], **tbl_args
)
NEXTGENADS_ASSIGNMENTS_TABLE = cfg["tables"]["read"][
    "nextgenads_assignments_latest"
]

# Read results data from prod schema dataset
tbl_args_results = {
    "catalog": config.catalog_read,
    "schema": config.schema_read,
    "client": CLIENT,
}
AD_RESULTS_TABLE = etl.map_tbl(tbls["results_ads"], **tbl_args_results)

FALLOW_TRUE_LABEL = cfg["fallow_control"]["true_label"]

_pt_isolation_cfg = cfg["page_type_isolation"]
PAGE_TYPE_ISOLATION_ENABLED = _pt_isolation_cfg["enabled"]
PAGE_TYPE_ALLOWED_GROUPS = (
    [
        grp
        for grp, locs in _pt_isolation_cfg["page_type_map"].items()
        if LOCATION in locs
    ]
    + ["AllPages"]
    if PAGE_TYPE_ISOLATION_ENABLED
    else []
)

WEBHOOK_URL = cfg["webhooks"]["DS Warnings"]

try:
    CELL_MAP = LOCATIONS[LOCATION]
except KeyError as ke:
    loc_key_msg = f"{LOCATION} build requested but not in config"
    logger.warning(loc_key_msg)
    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, loc_key_msg)
    raise ke

df_inherited_scope = None
CASCADE_INHERITED_NO_ADS = False
if INHERIT_BASIC_FROM:
    df_inherited_scope = _cache_assignment_frame(
        spark.table(ASSIGNMENT_TABLES.staging_table)
        .where(F.col(ASSIGNMENT_COLUMNS.build_run_id) == BUILD_RUN_ID)
        .where(F.col("Location") == INHERIT_BASIC_FROM)
        .where(F.col("rundate") == F.lit(RUN_DATE))
        .select("AccountNumber", "UniqueAdIDBasic")
    )
    CASCADE_INHERITED_NO_ADS = df_inherited_scope.isEmpty()
    if CASCADE_INHERITED_NO_ADS:
        logger.info(
            f"Current-build primary scope {INHERIT_BASIC_FROM} has no "
            f"assignments; cascading NO_ADS to {LOCATION}"
        )

logger.info(f"Assigning Ads for Location: {LOCATION}")

logger.info("Getting Ads")
df_ads = (
    spark.table(CONTROL_SHEET_LATEST)
    .where(F.col("Location") == LOCATION)
    .select(
        "UniqueAdID",
        "UniqueAdIDPremium",
        "AlgoDivision",
        "MASIDToken",
        "TargetingCriteria",
        "AudienceOnly",
        "Tags",
        "Themes",
        "ClusterID",
    )
)

if INCREMENTALITY_ADS_SUPPRESSION_SWITCH:
    # Aggregate the results table to an ad level view
    df_incremental = (
        spark.table(AD_RESULTS_TABLE)
        .where(
            (F.col("SessionDate") >= CHECK_SESSIONS_FROM)
            & (F.col("UniqueAdID").rlike(INC_ADS_SUFFIX + "$"))
        )
        .groupBy("UniqueAdID")
        .agg(
            F.sum("ApportionedRevenue").alias("ApportionedRevenue"),
            F.sum("Sessions").alias("Sessions"),
            F.sum("C_ApportionedRevenue").alias("C_ApportionedRevenue"),
            F.sum("C_Sessions").alias("C_Sessions"),
            F.when(
                F.sum("Sessions") > 0,
                F.sum(F.col("SessionOverlapRatio") * F.col("Sessions"))
                / F.sum("Sessions"),
            )
            .otherwise(F.lit(None))
            .alias("SessionOverlapRatio"),
        )
        .withColumn(
            "ARPS",
            F.when(
                F.col("Sessions") > 0,
                F.col("ApportionedRevenue") / F.col("Sessions"),
            ).otherwise(F.lit(None)),
        )
        .withColumn(
            "C_ARPS",
            F.when(
                F.col("C_Sessions") > 0,
                F.col("C_ApportionedRevenue") / F.col("C_Sessions"),
            ).otherwise(F.lit(None)),
        )
        .withColumn("IncARPS", F.col("ARPS") - F.col("C_ARPS"))
        .withColumn(
            "IncARPSAdj",
            F.when(
                F.col("SessionOverlapRatio").isNotNull()
                & (F.col("SessionOverlapRatio") > 0),
                F.col("IncARPS") / F.col("SessionOverlapRatio"),
            ).otherwise(F.lit(None)),
        )
        .withColumn("EstContribution", F.col("IncARPSAdj") * F.col("Sessions"))
        .withColumn(
            "IncPct",
            F.when(
                F.col("C_ARPS").isNotNull() & (F.col("C_ARPS") != 0),
                F.col("IncARPS") / F.col("C_ARPS"),
            ).otherwise(F.lit(None)),
        )
    )

    df_incremental = df_incremental.select(
        F.col("UniqueAdID").alias("UniqueAdIDAssigned"),
        F.col("C_Sessions"),
        "EstContribution",
    )

df_ads_tgt = df_ads.fillna(0, subset=["AudienceOnly"]).where(
    (F.col("AudienceOnly") != 1)
)

# Create subset of ads for Best
df_ads_tgt_best = df_ads_tgt.where(F.col("Themes").isNotNull()).where(
    F.col("Themes") != ""
)

df_ads_tgt_nextgenads = df_ads.filter(
    F.col("ClusterID").isNotNull() & F.col("Themes").isNull()
)

# Drop unneeded columns following processing dataframe
ads_required_cols = [
    "UniqueAdID",
    "UniqueAdIDPremium",
    "AlgoDivision",
    "MASIDToken",
    "TargetingCriteria",
]
df_ads = df_ads.select(ads_required_cols)
df_ads_tgt = df_ads_tgt.select(ads_required_cols)
df_ads_tgt_best = df_ads_tgt_best.select(ads_required_cols)

if CASCADE_INHERITED_NO_ADS:
    HAS_TARGETED_ADS = False
    NO_ASSIGNABLE_ADS = True
else:
    HAS_TARGETED_ADS = not df_ads_tgt.isEmpty()
    NO_ASSIGNABLE_ADS = (
        not HAS_TARGETED_ADS and df_ads_tgt_nextgenads.isEmpty()
    )

if NO_ASSIGNABLE_ADS:
    if CASCADE_INHERITED_NO_ADS:
        no_ads_msg = (
            f"No assignments found for current-build primary scope "
            f"{INHERIT_BASIC_FROM}; staging NO_ADS for {LOCATION}"
        )
    else:
        no_ads_msg = f"No ads found for Location: {LOCATION}"
    logger.warning(no_ads_msg)

    if JOB_ENV == "prod":
        post_to_webhook(WEBHOOK_URL, no_ads_msg)

    df_ad_assigned_masid_output = spark.createDataFrame(
        [],
        schema=(
            spark.table(ASSIGNMENT_TABLES.staging_table)
            .select(*ASSIGNMENT_INPUT_COLUMNS)
            .schema
        ),
    )

else:
    logger.info("Getting customer cell assignments")
    df_cells = _cache_assignment_frame(
        read_delta_version(
            spark,
            CUSTOMER_CELLS_TABLE,
            CUSTOMER_CELLS_DELTA_VERSION,
        ).drop("rundate")
    )

    logger.info("Assigning Ads with Basic Targeting")

    if not HAS_TARGETED_ADS:
        logger.info("No non-AudienceOnly ads - skipping basic/best")
        df_assigned_basic = spark.createDataFrame(
            [], schema="AccountNumber STRING, UniqueAdID STRING"
        )
        df_assigned_best = spark.createDataFrame(
            [], schema="AccountNumber STRING, UniqueAdID STRING"
        )
        df_assigned_best_challenger = df_assigned_best
        basic_within = LOCATIONS[LOCATION]["basic_within"]
        best_kwargs = {"return_ranks": [1]}
    else:
        if "basic_within" in LOCATIONS[LOCATION]:
            basic_within = LOCATIONS[LOCATION]["basic_within"]
        else:
            basic_default_warn_msg = (
                f"`basic_within` not specified in config for {LOCATION}"
                + ' - defaulting to "global"'
            )
            logger.warning(basic_default_warn_msg)
            basic_within = "global"

        # 'global' is a dummy column name used within the assign_random_ads
        # function when there are no grouping columns. This was done to minimise
        # refactoring the required, but a more generalisable assign_random_ads
        # function would be beneficial to remove this restriction
        if "global" in df_ads_tgt.columns:
            protected_colname_msg = (
                'Protected column name "global" was found in df_ads_tgt'
            )
            raise Exception(protected_colname_msg)
        if "global" in df_cells.columns:
            protected_colname_msg = (
                'Protected column name "global" was found in df_cells'
            )
            raise Exception(protected_colname_msg)

        # Secondary locations inherit only from the primary scope staged by
        # this build. They must never read yesterday's serving snapshot.
        if INHERIT_BASIC_FROM:
            inherit_location = INHERIT_BASIC_FROM
            logger.info(
                f"Inheriting basic assignments from {inherit_location} - "
                "excluding already-assigned ads"
            )

            # Reuse the current-build scope already checked before allocation.
            df_inherited_assignments = (
                df_inherited_scope
                .where(F.col("UniqueAdIDBasic").isNotNull())
                .select(
                    "AccountNumber",
                    F.col("UniqueAdIDBasic").alias("ExcludedAdID"),
                )
            )

            # Join to cells to get excluded ads per customer
            df_cells_with_exclusions = df_cells.join(
                df_inherited_assignments, on="AccountNumber", how="left"
            )

            # Assign random ads excluding the already-assigned ones
            if basic_within == "global":
                df_assigned_basic = assign_random_ads_with_exclusions(
                    df_ads_tgt.select("UniqueAdID"),
                    df_cells_with_exclusions.select(
                        "AccountNumber", "ExcludedAdID"
                    ),
                )
            else:
                df_assigned_basic = assign_random_ads_with_exclusions(
                    df_ads_tgt.select("UniqueAdID", basic_within),
                    df_cells_with_exclusions.select(
                        "AccountNumber", basic_within, "ExcludedAdID"
                    ),
                    grp_col=basic_within,
                )
        else:
            # Original logic for locations without basic inheritance
            if basic_within == "global":
                df_assigned_basic = assign_random_ads(
                    df_ads_tgt.select("UniqueAdID"),
                    df_cells.select("AccountNumber"),
                )
            else:
                df_assigned_basic = assign_random_ads(
                    df_ads_tgt.select("UniqueAdID", basic_within),
                    df_cells.select("AccountNumber", basic_within),
                    grp_col=basic_within,
                )

        df_assigned_basic = _cache_assignment_frame(df_assigned_basic)

        logger.info("Assigning Ads with Best Targeting")

        if "best_kwargs" in LOCATIONS[LOCATION]:
            best_kwargs = LOCATIONS[LOCATION]["best_kwargs"]
        else:
            best_kwargs = {"return_ranks": [1]}

        df_assigned_best = assign_preranked_ads(
            df_ads=df_ads_tgt_best,
            preranked_ads_table=PRERANKED_THEMES_TABLE,
            location=LOCATION,
            df_cust=df_cells.select("AccountNumber"),
            **best_kwargs,
        )
        df_assigned_best = _cache_assignment_frame(df_assigned_best)

        df_assigned_best_challenger = df_assigned_best

    USE_NEXTGENADS = any(
        step.get("then", {}).get("col") == "UniqueAdIDNextGenAds"
        for step in CELL_MAP.get("map", [])
    )
    if USE_NEXTGENADS:
        logger.info(
            f"NextGenAds enabled for {LOCATION} - assigning cluster ads"
        )
        df_assigned_nextgenads = assign_nextgenads(
            df_ads=df_ads_tgt_nextgenads,
            customer_to_cluster_table=NEXTGENADS_ASSIGNMENTS_TABLE,
            df_cust=df_cells.select("AccountNumber"),
            return_ranks=best_kwargs["return_ranks"],
        )
    else:
        logger.info(f"NextGenAds not referenced in {LOCATION} map - skipping")
        df_assigned_nextgenads = spark.createDataFrame(
            [], schema="AccountNumber STRING, UniqueAdID STRING"
        )
    df_assigned_nextgenads = _cache_assignment_frame(
        df_assigned_nextgenads
    )

    logger.info("Determining Ad to show based on assignments and fixed cells")
    df_assignments = (
        df_cells.withColumn("AdSuppressed", F.lit("AdSuppressed"))
        .join(
            (
                df_assigned_basic.select(
                    "AccountNumber", "UniqueAdID"
                ).withColumnRenamed("UniqueAdID", "UniqueAdIDBasic")
            ),
            on="AccountNumber",
            how="left",
        )
        .join(
            (
                df_assigned_best.select(
                    "AccountNumber", "UniqueAdID"
                ).withColumnRenamed("UniqueAdID", "UniqueAdIDBest")
            ),
            on="AccountNumber",
            how="left",
        )
        .join(
            (
                df_assigned_best_challenger.select(
                    "AccountNumber", "UniqueAdID"
                ).withColumnRenamed("UniqueAdID", "UniqueAdIDBestChallenger")
            ),
            on="AccountNumber",
            how="left",
        )
        .join(
            (
                df_assigned_nextgenads.select(
                    "AccountNumber", "UniqueAdID"
                ).withColumnRenamed("UniqueAdID", "UniqueAdIDNextGenAds")
            ),
            on="AccountNumber",
            how="left",
        )
    )
    df_assignments = _cache_assignment_frame(df_assignments)

    df_ad_assigned = (
        df_assignments.withColumn(
            "UniqueAdIDMeasurement", chain_when_thens(CELL_MAP["map"])
        )
        .join(
            (
                df_ads.select(
                    "UniqueAdID", "UniqueAdIDPremium"
                ).withColumnRenamed("UniqueAdID", "UniqueAdIDMeasurement")
            ),
            on="UniqueAdIDMeasurement",
            how="left",
        )
        .withColumn(
            "UniqueAdIDMeasurement",
            F.when(
                (
                    (F.col("IsPremium") == 1)
                    & (F.col("UniqueAdIDPremium").isNotNull())
                ),
                F.col("UniqueAdIDPremium"),
            ).otherwise(F.col("UniqueAdIDMeasurement")),
        )
        .fillna("NoAdFound", subset=["UniqueAdIDMeasurement"])
        .withColumn(
            "UniqueAdIDAssigned",
            F.when(
                F.col("FallowControl") == FALLOW_TRUE_LABEL, F.lit("NoAd")
            ).otherwise(F.col("UniqueAdIDMeasurement")),
        )
    )

    df_ad_treatments = (
        df_assignments.drop(
            "AdSuppressed",
            "UniqueAdIDBasicUniqueAdIDBestUniqueAdIDBestChallenger",
            "UniqueAdIDNextGenAds",
        )
        .withColumns(
            {
                "AdSuppressed": F.lit("AdSuppressed"),
                "UniqueAdIDBasic": F.lit("Basic"),
                "UniqueAdIDBest": F.lit("Best"),
                "UniqueAdIDBestChallenger": F.lit("BestChallenger"),
                "UniqueAdIDNextGenAds": F.lit("NextGenAds"),
            }
        )
        .withColumn("Treatment", chain_when_thens(CELL_MAP["map"]))
        .select("AccountNumber", "Treatment")
    )

    df_ad_assigned = df_ad_assigned.join(
        df_ad_treatments, on="AccountNumber", how="left"
    ).withColumn(
        "Treatment",
        F.when(
            (
                (F.col("IsPremium") == 1)
                & (F.col("UniqueAdIDPremium").isNotNull())
            ),
            F.concat(F.col("Treatment"), F.lit("Prem")),
        ).otherwise(F.col("Treatment")),
    )

    # --- Page-type isolation suppression ---
    # Customers in a page-type isolation bucket (e.g. HP_Only) should only
    # receive ads on locations that belong to their assigned page type.
    # For any other location we overwrite UniqueAdIDAssigned with 'NoAd'.
    if PAGE_TYPE_ISOLATION_ENABLED:
        logger.info(
            f"Page-type isolation enabled. "
            f"Allowed groups for {LOCATION}: {PAGE_TYPE_ALLOWED_GROUPS}"
        )
        df_ad_assigned = df_ad_assigned.withColumn(
            "UniqueAdIDAssigned",
            F.when(
                # Suppress if this customer's isolation cell is set AND
                # their group is not in the permitted list for this location
                F.col("PageTypeIsolation").isNotNull()
                & ~F.col("PageTypeIsolation").isin(PAGE_TYPE_ALLOWED_GROUPS),
                F.lit("NoAd"),
            ).otherwise(F.col("UniqueAdIDAssigned")),
        )

    df_ad_masid = (
        df_ads.select("UniqueAdID", "MASIDToken")
        .withColumn("Location", F.lit(LOCATION))
        .withColumn(
            "MASID",
            F.concat(F.col("Location"), F.lit("_"), F.col("MASIDToken")),
        )
        .drop("Location", "MASIDToken")
        .distinct()
    )

    ctrl_masid_cols = ["UniqueAdID", "MASID"]
    ctrl_masid_vals = [
        ("NoAd", f"{LOCATION}_Z"),
        ("AdSuppressed", f"{LOCATION}_Z"),
        ("NoAdFound", f"{LOCATION}_Z"),
    ]

    df_control_masid = spark.createDataFrame(
        data=ctrl_masid_vals,
        schema=build_spark_schema(
            [
                ["UniqueAdID", "string", "not null"],
                ["MASID", "string", "not null"],
            ]
        ),
    )
    df_ad_masid = df_ad_masid.unionByName(df_control_masid)

    df_ad_assigned_masid = df_ad_assigned.join(
        df_ad_masid,
        on=df_ad_assigned.UniqueAdIDAssigned == df_ad_masid.UniqueAdID,
        how="left",
    ).drop("UniqueAdID")
    df_ad_assigned_masid = _cache_assignment_frame(df_ad_assigned_masid)

    # Check and warn if null Treatments exist
    n_null_treatment = df_ad_assigned_masid.where(
        F.col("Treatment").isNull()
    ).count()
    if n_null_treatment > 0:
        null_treatment_msg = (
            f"{n_null_treatment:,} accounts removed during "
            + f"assignment of {LOCATION} due to null Treatment"
        )
        logger.warning(null_treatment_msg)
        if JOB_ENV == "prod":
            post_to_webhook(WEBHOOK_URL, null_treatment_msg)
        df_ad_assigned_masid = df_ad_assigned_masid.where(
            F.col("Treatment").isNotNull()
        )

    # Check and warn if null MASID assignments exist
    n_null_masid = df_ad_assigned_masid.where(F.col("MASID").isNull()).count()
    if n_null_masid > 0:
        null_masid_msg = (
            f"{n_null_masid:,} accounts removed during "
            + f"assignment of {LOCATION} due to null MASID"
        )
        logger.warning(null_masid_msg)
        if JOB_ENV == "prod":
            post_to_webhook(WEBHOOK_URL, null_masid_msg)
        df_ad_assigned_masid = df_ad_assigned_masid.where(
            F.col("MASID").isNotNull()
        )

    # Check and warn if UniqueAdIDMeasurement is null
    n_null_measure = (
        df_ad_assigned_masid.where(F.col("UniqueAdIDMeasurement").isNull())
    ).count()
    if n_null_measure > 0:
        null_measure_msg = (
            f"{n_null_measure:,} accounts removed during "
            + f"assignment of {LOCATION} due to null "
            + "UniqueAdIDMeasurement"
        )
        logger.warning(null_measure_msg)
        if JOB_ENV == "prod":
            post_to_webhook(WEBHOOK_URL, null_measure_msg)
        df_ad_assigned_masid = df_ad_assigned_masid.where(
            F.col("UniqueAdIDMeasurement").isNotNull()
        )

    if INCREMENTALITY_ADS_SUPPRESSION_SWITCH:
        suppression_cond = (
            (F.col("Location").isin(INCREMENTALITY_LOCATIONS))
            & (F.col("Treatment").isin(INCREMENTALITY_TREATMENTS))
            & (F.col("EstContribution") < INC_AD_SUPPRESSION_THRESHOLD)
            & (F.col("EstContribution") < 0)
            & (F.col("C_Sessions") >= MIN_C_SESSIONS)
        )

        df_ad_assigned_masid = (
            df_ad_assigned_masid.join(
                df_incremental, on=["UniqueAdIDAssigned"], how="left"
            )
            .withColumn(
                "UniqueAdIDAssigned",
                F.when(suppression_cond, F.lit(ADS_SWITCH_LABEL)).otherwise(
                    F.col("UniqueAdIDAssigned")
                ),
            )
            .withColumn(
                "MASID",
                F.when(
                    suppression_cond,
                    F.concat(
                        F.col("Location"),
                        F.lit("_"),
                        F.lit(AD_SUPPRESSION_MASID_TOKEN),
                    ),
                ).otherwise(F.col("MASID")),
            )
        )

        df_ad_assigned_masid_output = df_ad_assigned_masid.withColumn(
            "Location", F.lit(LOCATION)
        ).select(
            "AccountNumber",
            "Location",
            "UniqueAdIDBasic",
            "UniqueAdIDBest",
            "UniqueAdIDBestChallenger",
            "UniqueAdIDNextGenAds",
            "Treatment",
            "UniqueAdIDMeasurement",
            "UniqueAdIDAssigned",
            "MASID",
        )

    else:
        df_ad_assigned_masid_output = df_ad_assigned_masid.withColumn(
            "Location", F.lit(LOCATION)
        ).select(
            "AccountNumber",
            "Location",
            "UniqueAdIDBasic",
            "UniqueAdIDBest",
            "UniqueAdIDBestChallenger",
            "UniqueAdIDNextGenAds",
            "Treatment",
            "UniqueAdIDMeasurement",
            "UniqueAdIDAssigned",
            "MASID",
        )

df_ad_assigned_masid_output = df_ad_assigned_masid_output.select(
    *ASSIGNMENT_INPUT_COLUMNS
)
try:
    logger.info(
        f"Staging assignments for {LOCATION} in build {BUILD_RUN_ID}"
    )
    stage_result = stage_assignment_scope(
        spark,
        df_ad_assigned_masid_output,
        tables=ASSIGNMENT_TABLES,
        columns=ASSIGNMENT_COLUMNS,
        scope_contract=ASSIGNMENT_SCOPE_CONTRACT,
        build_run_id=BUILD_RUN_ID,
        build_date=RUN_DATE,
        scope=LOCATION,
        task_run_id=TASK_RUN_ID,
        execution_count=EXECUTION_COUNT,
    )
    logger.info(
        f"Staged {stage_result.row_count:,} assignments for {LOCATION}; "
        f"completion status: {stage_result.status}"
    )
finally:
    _release_cached_assignment_frames()
    atexit.unregister(_release_cached_assignment_frames)

logger.info("Run complete")

import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    from dsutils.dbc import get_dbutils

    dbutils = get_dbutils()
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

import json
import pyspark.sql.functions as F
from datetime import date, timedelta

# from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (
    assert_pk,
    truncate_and_load,
    delete_from_and_load,
    post_to_webhook,
)
from dsutils.argparser import get_job_parser
import dsutils.gcp as gcp
from next_ads.Scoring import append_targeting_criteria
from next_ads.utils import config_manager
from dsutils.dbc import configure_spark
from next_ads.data_validation import schemas


def check_primary_key(df, logger, JOB_ENV, WEBHOOK_URL):
    logger.info("Checking input Primary Key")

    assert_pk(df, ["UniqueAdID", "PageType"])

    df_dup_masids = (
        df.groupBy("AlgoDivision", "PageType", "MASIDToken")
        .agg(F.countDistinct("UniqueAdID").alias("AdsPerMASID"))
        .where(F.col("AdsPerMASID") > 1)
    )
    if df_dup_masids.count() >= 1:
        dup_masid_list = list(
            set(
                [
                    row[0]
                    for row in (df_dup_masids.select("MASIDToken").collect())
                ]
            )
        )

        warn_dup_masid = (
            "Duplicate MASID suffixes assigned to Ads"
            + f" in same AlgoDivision: {dup_masid_list}"
        )
        logger.warning(warn_dup_masid)

        warn_dup_masid = []

        for m in dup_masid_list:
            res_conflict = f"Resolving conflict for MASID suffix: {m}"
            logger.info(res_conflict)

            warn_dup_masid.append(res_conflict)

            df_dups_m = (
                df.where(F.col("MASIDToken") == m).select("UniqueAdID")
            ).collect()

            clashing_ids = list(set([row[0] for row in df_dups_m]))
            clashing_ids.sort()  # Sort alphabetically as proxy for latest
            try:
                keep_ad = f"Keeping ad: {clashing_ids[-1]}"
                logger.info(keep_ad)
                warn_dup_masid.append(keep_ad)

                ids_to_del = clashing_ids[:-1]

                for id_del in ids_to_del:
                    drop_ad = f"Dropping conflicting ad: {id_del}"
                    logger.warning(drop_ad)
                    warn_dup_masid.append(drop_ad)

                    df = df.where(F.col("UniqueAdID") != id_del)
            except IndexError as e:
                logger.error(f"Error resolving MASID conflict: {e}")
                logger.warning(f"Unable to resolve conflict for suffix: {m}")
                logger.warning(f"Removing all ads associated with suffix: {m}")
                issue_ad = (
                    "Issue resolving conflict for ads with MASID suffix:"
                    + f" {m} - all {m} ads removed"
                )
                warn_dup_masid += "\n" + issue_ad
                df = df.where(F.col("MASIDToken") != m)

        if JOB_ENV == "prod":
            post_to_webhook(WEBHOOK_URL, "/n".join(warn_dup_masid))

    return df


def report_invalid_dates(
    df_ctrl_raw, date_fmt, date_regex, logger, JOB_ENV, WEBHOOK_URL
):
    df_ctrl_valid_date_fmt = df_ctrl_raw.where(
        (F.col("StartDate").rlike(date_regex))
        & (F.col("EndDate").rlike(date_regex))
    )

    df_ctrl_not_empty = df_ctrl_raw.where(F.col("UniqueAdID") != "")

    df_invalid_date_ads = df_ctrl_not_empty.join(
        df_ctrl_valid_date_fmt, on="UniqueAdID", how="leftanti"
    ).select("UniqueAdID")

    invalid_date_ads = [x[0] for x in df_invalid_date_ads.collect()]

    if len(invalid_date_ads) > 0:
        date_fmt_msg = (
            "Start or End date of the following ads was invlaid\n"
            + "\n".join(invalid_date_ads)
            + f"\nDate must be entered in the format: {date_fmt}"
        )
        logger.warning(date_fmt_msg)
        if JOB_ENV == "prod":
            post_to_webhook(WEBHOOK_URL, date_fmt_msg)

    return df_ctrl_valid_date_fmt


################################################################################
# SECTION 1: SETUP AND CONFIGURATION
################################################################################


def main(JOB_ENV: str, CLIENT: str, LOG_LEVEL: str):
    if LOG_LEVEL:
        configure_logging(log_level=LOG_LEVEL)
    else:
        configure_logging()

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
    config = config_manager.load_config(JOB_ENV)
    logger.info(f"Configuring run for client: {CLIENT}")

    VALID_PAGE_TYPES = [
        "ProductListingPage",
        "ForYouPage",
        "CheckoutPage",
        "ShoppingBagPage",
        "HomePage",
    ]

    CONTROL_SHEET = config.control_sheet_v2
    EXCLUSIONS_SHEET = config.exclusions_sheet

    SCHEMA = config.schema_write
    logger.info(f"Write schema set to {SCHEMA}")

    # Map write schema to parameterised write table names
    TARGET_TABLE = config.tables_write.control_sheet_v2
    TARGET_TABLE_LATEST = config.tables_write.control_sheet_latest_v2
    TARGET_EXCLUSIONS = config.tables_write.exclusions
    TARGET_EXCLUSIONS_LATEST = config.tables_write.exclusions_latest

    WEBHOOK_URL = config.webhooks.input_warnings

    DATE_FMT = CONTROL_SHEET["date_format"]
    DATE_REGEX = CONTROL_SHEET["date_regex"]

    # log all params
    logger.info(
        f"Configuration - "
        f"ENV: {JOB_ENV}, "
        f"SCHEMA: {SCHEMA}, "
        f"CLIENT: {CLIENT}, "
        f"TARGET_TABLE: {TARGET_TABLE}, "
        f"TARGET_TABLE_LATEST: {TARGET_TABLE_LATEST}, "
        f"TARGET_EXCLUSIONS: {TARGET_EXCLUSIONS}, "
        f"TARGET_EXCLUSIONS_LATEST: {TARGET_EXCLUSIONS_LATEST}, "
    )

    ################################################################################
    # SECTION 2: DATA EXTRACTION - must be on Next VPN for this to work
    ################################################################################

    logger.info("Reading Control Sheet from Google Sheets")

    df_ctrl_raw = gcp.spark_df_from_sheets(
        url=CONTROL_SHEET["url"],
        worksheet_name=CONTROL_SHEET["sheet"],
        gcp_scope=config.gcp.scope,
        gcp_key=config.gcp.key,
        schema=CONTROL_SHEET["read_schema"],
    )

    logger.info("Reading Exclusions Sheet from Google Sheets")

    df_exclusions = gcp.spark_df_from_sheets(
        url=EXCLUSIONS_SHEET["url"],
        worksheet_name=EXCLUSIONS_SHEET["sheet"],
        gcp_scope=config.gcp.scope,
        gcp_key=config.gcp.key,
        schema=EXCLUSIONS_SHEET["read_schema"],
    )

    ################################################################################
    # SECTION 3: INITIAL DATA LOADING
    ################################################################################

    df_ctrl_raw_filtered = df_ctrl_raw.filter(df_ctrl_raw.UniqueAdID != "")

    delete_from_and_load(
        df=df_ctrl_raw_filtered,
        table=config.tables_write.control_sheet_raw_v2,
        pk_cols=["UniqueAdID"],
        del_where={"rundate": "current_date()"},
    )

    logger.info(
        f"Writing Control Sheet to {config.tables_write.control_sheet_raw_latest_v2}"
    )
    truncate_and_load(
        df=df_ctrl_raw_filtered,
        table=config.tables_write.control_sheet_raw_latest_v2,
        pk_cols=["UniqueAdID"],
    )

    logger.info(
        f"Writing Exclusions Sheet to {config.tables_write.exclusions_latest}"
    )
    truncate_and_load(
        df=df_exclusions,
        table=config.tables_write.exclusions_latest,
        pk_cols=["PageType", "Page", "Exclude_Campaign"],
    )

    delete_from_and_load(
        df=df_exclusions,
        table=config.tables_write.exclusions,
        pk_cols=["PageType", "Page", "Exclude_Campaign"],
        del_where={"rundate": "current_date()"},
    )

    ################################################################################
    # SECTION 4: DATA VALIDATION USING PANDERA SCHEMAS
    ################################################################################

    # Data Validation
    # NOTE: soft validation (no assert)
    logger.info("Validating Control Sheet data schema")
    df_ctrl_raw_filtered = df_ctrl_raw.filter(
        df_ctrl_raw.UniqueAdID != ""
    ).filter(df_ctrl_raw.CMSPageID != "")

    df_ctrl_raw_filtered = schemas.ControlSheetInputModelv2.validate(
        df_ctrl_raw_filtered, lazy=True
    )
    errors_json = json.dumps(
        dict(df_ctrl_raw_filtered.pandera.errors),
        indent=2,
    )
    logger.info(f"Data validation errors: {errors_json}")

    logger.info("Validating Control Sheet Exclusions data schema")
    df_exclusions = schemas.ControlSheetExclusionsInputModel.validate(
        df_exclusions, lazy=True
    )
    errors_json = json.dumps(
        dict(df_exclusions.pandera.errors),
        indent=2,
    )
    logger.info(f"Data validation errors: {errors_json}")

    ################################################################################
    # SECTION 5: TRANSFORMATION LOGIC
    ################################################################################

    logger.info("Stripping empty UniqueAdID entries")

    ################################################################################
    # REPORT INVALID DATES
    ################################################################################
    df_ctrl_valid_date_fmt = report_invalid_dates(
        df_ctrl_raw, DATE_FMT, DATE_REGEX, logger, JOB_ENV, WEBHOOK_URL
    )

    logger.info("Getting active status of ads based on StartDate and EndDate")

    date_tomorrow = date.today() + timedelta(days=1)

    df_ctrl_active = (  # active ads
        df_ctrl_valid_date_fmt.drop("Status")
        .withColumn("StartDate", F.to_date(F.col("StartDate"), DATE_FMT))
        .withColumn("EndDate", F.to_date(F.col("EndDate"), DATE_FMT))
        .where(F.col("StartDate") <= date_tomorrow)
        .where(F.col("EndDate") >= date_tomorrow)
        .withColumn(  # Legacy coercion of item codes to upper case and replace("-","")
            "Items", F.regexp_replace(F.upper(F.col("Items")), "-", "")
        )
        .withColumn(
            "AudienceOnlyInt",
            F.when(F.col("AudienceOnly") == "TRUE", 1).otherwise(0),
        )
        .drop("AudienceOnly")
        .withColumnRenamed("AudienceOnlyInt", "AudienceOnly")
    )

    logger.info(f"Active Ads: {df_ctrl_active.count():,}")

    ################################################################################
    # Active Ad-PageTypes
    ################################################################################
    df_id_loc = (
        df_ctrl_active.unpivot(
            ids="UniqueAdID",
            values=VALID_PAGE_TYPES,  # type: ignore - we can pass a list of string here
            variableColumnName="PageType",
            valueColumnName="Requested",
        )
        .where(F.col("Requested") == "TRUE")
        .drop_duplicates()
        .drop("Requested")
    )

    active_locs = set(
        [row[0] for row in df_id_loc.select("PageType").collect()]
    )
    logger.info(f"Active PageType: {len(active_locs):,} {sorted(active_locs)}")

    logger.info(f"Active Ad-PageTypes: {df_id_loc.count():,}")

    ################################################################################
    # Join in Ad attributes
    # ################################################################################

    df_ad_attributes = (
        df_ctrl_active.drop(*VALID_PAGE_TYPES)
        .drop_duplicates()
        .replace("", None)
    )

    df_processed = df_id_loc.join(
        df_ad_attributes, on="UniqueAdID", how="left"
    )

    # these aren't populated in the control sheet anymore, but are needed for downstream targeting criteria construction,
    # so we add them in as nulls here
    df_processed = df_processed.withColumn(
        "ModelCombination", F.lit(None)
    ).withColumn("Models", F.lit(None))

    ################################################################################
    # Final steps
    ################################################################################

    df_processed = append_targeting_criteria(df_processed)

    # Ensure UniqueAdIDPremium is only present on PageTypes in sibling ad
    logger.info("Constraining Premium Ads to Only Show on Sibling PageTypes")

    page_type_lookup_df = (
        df_processed.groupBy("UniqueAdID")
        .agg(F.collect_set("PageType").alias("ValidPageTypes"))
        .withColumnRenamed("UniqueAdID", "LookupAdID")
    )

    df_processed = (
        df_processed.join(
            page_type_lookup_df,
            df_processed["UniqueAdIDPremium"]
            == page_type_lookup_df["LookupAdID"],
            "left",
        ).withColumn(
            "UniqueAdIDPremium",
            F.when(
                (F.col("UniqueAdIDPremium").isNotNull())
                & (
                    F.col("ValidPageTypes").isNull()
                    | ~F.array_contains(
                        F.col("ValidPageTypes"), F.col("PageType")
                    )
                ),
                F.lit(None),
            ).otherwise(F.col("UniqueAdIDPremium")),
        )
    ).drop("LookupAdID", "ValidPageTypes")

    logger.info("Cleaning theme strings (lowercase, strip whitespace)")

    df_valid_ad_ids = df_processed.select(
        F.col("UniqueAdID").alias("valid_id")
    ).distinct()

    df_processed = (
        df_processed.withColumn(
            "Themes",
            F.when(
                F.col("Themes").isNotNull(), F.trim(F.lower(F.col("Themes")))
            ).otherwise(F.col("Themes")),
        )
        .join(
            df_valid_ad_ids,
            F.col("UniqueAdIDPremium") == F.col("valid_id"),
            "left_outer",
        )
        .withColumn(
            "UniqueAdIDPremium",
            F.when(F.col("valid_id").isNull(), F.lit(None)).otherwise(
                F.col("UniqueAdIDPremium")
            ),
        )
    )

    ################################################################################
    # Checking input Primary Key
    ################################################################################
    df_processed = check_primary_key(
        df_processed, logger, JOB_ENV, WEBHOOK_URL
    )

    ################################################################################
    # SECTION 6: FINAL DATA LOADING
    ################################################################################

    target_cols = (spark.table(TARGET_TABLE).drop("rundate")).columns

    if set(target_cols) == set(df_processed.columns):
        logger.info("Control Sheet columns match Target table columns")
    elif set(target_cols).issubset(set(df_processed.columns)):
        logger.warning("Target table cols are subset of Control Sheet cols")
        extra_cols = set(df_processed.columns).difference(set(target_cols))
        logger.warning(
            "Dropping superfluous columns: %s", ", ".join(extra_cols)
        )
        df_processed = df_processed.drop(*list(extra_cols))
    else:
        raise Exception("Target table cols not a subset of Control Sheet cols")

    logger.info("Loading output to table")
    delete_from_and_load(
        df_processed.select(*target_cols),
        TARGET_TABLE,
        pk_cols=["UniqueAdID", "PageType"],
        del_where={"rundate": "current_date()"},
    )

    logger.info("Loading output to table (latest)")
    truncate_and_load(
        df_processed.select(*target_cols),
        TARGET_TABLE_LATEST,
        pk_cols=["UniqueAdID", "PageType"],
    )

    ################################################################################
    # SECTION 7: COMPLETION
    ################################################################################

    logger.info("Run complete")


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    JOB_ENV = jobparser.get_arg("--job_env")
    CLIENT = jobparser.get_arg("--client")
    LOG_LEVEL = jobparser.get_arg("--log_level")
    main(JOB_ENV, CLIENT, LOG_LEVEL)

import sys
from pathlib import Path
import pyspark.sql.functions as F

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
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
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

import dsutils.gcp as gcp
from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.etl import post_to_webhook
from dsutils.logtools import configure_logging, get_logger

from next_ads.control.load_control_sheet import (
    align_control_sheet_to_read_schema,
    build_control_sheet_run_context,
    build_multipage_locations,
    process_control_sheet,
    validate_control_sheet_inputs,
)
from next_ads.common import config_manager, etl
from next_ads.common.paths import load_client_config
from next_ads.common.snapshot_writes import (
    capture_run_date,
    publish_history_and_latest,
)


def write_control_sheet_input_tables(
    df_control_sheet,
    df_placements,
    output_tables,
    *,
    spark,
    run_date,
):
    publish_history_and_latest(
        spark,
        df_control_sheet,
        history_table=output_tables.control_sheet_raw,
        latest_table=output_tables.control_sheet_raw_latest,
        key_columns=["Realm", "Territory", "UniqueAdID"],
        run_date=run_date,
        columns=[*df_control_sheet.columns, "rundate"],
    )
    publish_history_and_latest(
        spark,
        df_placements,
        history_table=output_tables.control_sheet_plp_raw,
        latest_table=output_tables.control_sheet_plp_raw_latest,
        key_columns=["Location"],
        run_date=run_date,
        columns=[*df_placements.columns, "rundate"],
    )


def write_multipage_location_tables(
    df_multipage_locations,
    run_context,
    *,
    spark,
    run_date,
):
    publish_history_and_latest(
        spark,
        df_multipage_locations,
        history_table=run_context.target_multipage_locations_table,
        latest_table=run_context.target_multipage_locations_latest_table,
        key_columns=["Location", "Page"],
        run_date=run_date,
        columns=["Location", "Page", "Screen", "rundate"],
    )


def write_processed_control_sheet_tables(
    df_processed,
    run_context,
    target_columns,
    *,
    spark,
    run_date,
):
    publish_history_and_latest(
        spark,
        df_processed.select(*target_columns),
        history_table=run_context.target_table,
        latest_table=run_context.target_table_latest,
        key_columns=["UniqueAdID", "Location"],
        run_date=run_date,
        columns=[*target_columns, "rundate"],
    )


def main(JOB_ENV, CLIENT, LOG_LEVEL):
    if LOG_LEVEL:
        configure_logging(log_level=LOG_LEVEL)
    else:
        configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    run_date = capture_run_date(spark)
    logger.info(f"Running in job environment: {JOB_ENV}")

    if not CLIENT:
        assert JOB_ENV.lower() == "dev", (
            f"Client must be specified when running in {JOB_ENV}"
        )
        CLIENT = "next_uk"
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    logger.info(f"Configuring run for client: {CLIENT}")
    cfg = load_client_config(CLIENT)

    run_context = build_control_sheet_run_context(
        client=CLIENT,
        client_config=cfg,
        config=config,
    )
    location_config = run_context.location_config
    output_tables = run_context.output_tables
    logger.info(f"Write schema set to {run_context.schema_write}")

    tbls = cfg["tables"]["write"]
    tbl_args = {
        "catalog": config.catalog_write,
        "schema": config.schema_write,
        "client": CLIENT,
    }
    UNDERPERFORMING_ADS = etl.map_tbl(
        tbls["results_underperforming_ads"], **tbl_args
    )

    logger.info(
        f"Valid locations: {' '.join(location_config.valid_locations)}"
    )
    logger.info(
        f"Locations to read: {' '.join(location_config.read_locations)}"
    )
    logger.info(
        f"Locations with inherited ads: {location_config.inherited_locations}"
    )

    logger.info(
        f"Configuration - "
        f"ENV: {JOB_ENV}, "
        f"SCHEMA: {run_context.schema_write}, "
        f"CLIENT: {CLIENT}, "
        f"TARGET_TABLE: {run_context.target_table}, "
        f"TARGET_TABLE_LATEST: {run_context.target_table_latest}, "
        f"TARGET_MPL_TABLE: {run_context.target_multipage_locations_table}, "
        f"TARGET_MPL_TABLE_LATEST: "
        f"{run_context.target_multipage_locations_latest_table}, "
    )

    logger.info("Reading Control Sheet from Google Sheets")
    df_ctrl_raw = gcp.spark_df_from_sheets(
        url=run_context.control_sheet["url"],
        worksheet_name=run_context.control_sheet["sheet"],
        gcp_scope=cfg["gcp"]["scope"],
        gcp_key=cfg["gcp"]["key"],
        schema=run_context.control_sheet_read_schema,
    )
    control_sheet_read_alignment = align_control_sheet_to_read_schema(
        df_ctrl_raw,
        run_context.control_sheet_read_schema,
    )
    df_ctrl_raw = control_sheet_read_alignment.df
    if control_sheet_read_alignment.extra_columns:
        logger.warning(
            "Dropping Control Sheet columns outside configured read schema: %s",
            ", ".join(control_sheet_read_alignment.extra_columns),
        )

    logger.info("Reading Placements Sheet from Google Sheets")
    df_placements = gcp.spark_df_from_sheets(
        url=run_context.placements_sheet["url"],
        worksheet_name=run_context.placements_sheet["sheet"],
        gcp_scope=cfg["gcp"]["scope"],
        gcp_key=cfg["gcp"]["key"],
        schema=run_context.placements_sheet["read_schema"],
    )

    logger.info("Reading PLX URLs Sheet from Google Sheets")
    try:
        df_plx_urls = gcp.spark_df_from_sheets(
            url=run_context.plx_urls_sheet["url"],
            worksheet_name=run_context.plx_urls_sheet["sheet"],
            gcp_scope=cfg["gcp"]["scope"],
            gcp_key=cfg["gcp"]["key"],
            schema=run_context.plx_urls_sheet["read_schema"],
        )
    except Exception as e:
        df_plx_urls = None
        plx_load_msg = "Error loading PLX URLs sheet - URLs not refreshed"
        logger.warning(plx_load_msg)
        logger.error(e)
        if JOB_ENV == "prod":
            post_to_webhook(run_context.webhook_url, plx_load_msg)

    df_ctrl_raw_filtered = df_ctrl_raw.filter(df_ctrl_raw.UniqueAdID != "")

    logger.info(
        "Writing Control Sheet to history and latest tables"
    )
    logger.info(
        "Writing Placements Sheet to history and latest tables"
    )
    write_control_sheet_input_tables(
        df_ctrl_raw_filtered,
        df_placements,
        output_tables,
        spark=spark,
        run_date=run_date,
    )

    if df_plx_urls:
        try:
            logger.info("Updating PLX URLs in multipage locations table")
            df_multipage_locs = build_multipage_locations(df_plx_urls)
            logger.info(
                "Loading multipage locations to history and latest tables"
            )
            write_multipage_location_tables(
                df_multipage_locs,
                run_context,
                spark=spark,
                run_date=run_date,
            )

        except Exception as e:
            plx_write_msg = (
                "Error writing to multipage locations table; "
                "URLs not refreshed"
            )
            logger.warning(plx_write_msg)
            logger.error(e)
            if JOB_ENV == "prod":
                post_to_webhook(run_context.webhook_url, plx_write_msg)

    logger.info("Validating Control Sheet data schema")
    logger.info("Validating Control Sheet PLP data schema")
    logger.info("Validating Control Sheet PLX data schema")
    validation_result = validate_control_sheet_inputs(
        df_control_sheet=df_ctrl_raw,
        df_placements=df_placements,
        df_plx_urls=df_plx_urls,
    )
    df_placements = validation_result.df_placements
    for (
        input_name,
        errors_json,
    ) in validation_result.errors_json_by_input.items():
        logger.info(f"{input_name} data validation errors: {errors_json}")

    logger.info("Stripping empty UniqueAdID entries")
    target_cols = (
        spark.table(run_context.target_table).drop("rundate")
    ).columns
    logger.info(f"Reading underperforming ads from {UNDERPERFORMING_ADS}")
    df_underperforming_ads = spark.table(UNDERPERFORMING_ADS)
    processed_control_sheet = process_control_sheet(
        df_control_sheet=df_ctrl_raw,
        df_placements=df_placements,
        valid_locations=location_config.valid_locations,
        inherited_locations=location_config.inherited_locations,
        date_format=run_context.date_format,
        date_regex=run_context.date_regex,
        target_cols=target_cols,
        reference_date=run_date,
        df_underperforming_ads=df_underperforming_ads,
    )
    df_processed = processed_control_sheet.df

    underperforming_ads = (
        df_processed.filter(F.col("IsUnderperforming"))
        .select("UniqueAdID")
        .distinct()
    )
    logger.info(
        f"IsUnderperforming: {underperforming_ads.count():,} ads flagged"
    )
    logger.info(
        f"IsUnderperforming: {underperforming_ads.show(truncate=False)}"
    )

    if len(processed_control_sheet.invalid_date_ad_ids) > 0:
        date_fmt_msg = (
            "Start or End date of the following ads was invlaid\n"
            + "\n".join(processed_control_sheet.invalid_date_ad_ids)
            + f"\nDate must be entered in the format: {run_context.date_format}"
        )
        logger.warning(date_fmt_msg)
        if JOB_ENV == "prod":
            post_to_webhook(run_context.webhook_url, date_fmt_msg)

    logger.info(f"Active Ads: {processed_control_sheet.active_ad_count:,}")
    logger.info(
        "Active Locations: "
        f"{len(processed_control_sheet.active_locations):,} "
        f"{processed_control_sheet.active_locations}"
    )
    logger.info(
        f"Active Ad-Locations: "
        f"{processed_control_sheet.active_ad_location_count:,}"
    )

    duplicate_masid_resolution = (
        processed_control_sheet.duplicate_masid_resolution
    )
    if duplicate_masid_resolution.warning_message:
        logger.warning(duplicate_masid_resolution.warning_message)
        if JOB_ENV == "prod":
            post_to_webhook(
                run_context.webhook_url,
                duplicate_masid_resolution.warning_message,
            )

    target_alignment = processed_control_sheet.target_alignment
    if not target_alignment.extra_columns:
        logger.info("Control Sheet columns match Target table columns")
    else:
        logger.warning("Target table cols are subset of Control Sheet cols")
        logger.warning(
            "Dropping superfluous columns: %s",
            ", ".join(target_alignment.extra_columns),
        )

    logger.info("Loading output to history and latest tables")
    write_processed_control_sheet_tables(
        df_processed,
        run_context,
        target_cols,
        spark=spark,
        run_date=run_date,
    )

    logger.info("Run complete")


def parse_args():
    jobparser = get_job_parser()
    jobparser._parse_args()
    return {
        "JOB_ENV": jobparser.get_arg("--job_env"),
        "CLIENT": jobparser.get_arg("--client"),
        "LOG_LEVEL": jobparser.get_arg("--log_level"),
    }


if __name__ == "__main__":
    main(**parse_args())

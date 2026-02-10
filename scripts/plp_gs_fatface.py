import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    from dsutils.dbc import get_dbutils

    dbutils = get_dbutils()
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from typing import Dict
import json
from pyspark.sql.functions import col, concat, lit, when
from pyspark.sql.dataframe import DataFrame

from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from next_ads.utils import gs_helpers
from next_ads.utils import config_manager
from dynaconf import Dynaconf
import pandera.pyspark as pa
from next_ads.data_validation import schemas


logger = get_logger(__name__)
spark = configure_spark()


def load_control_sheet_config(cfg: Dynaconf, client: str, territory: str) -> Dict:
    CONTROL_SHEET_LOOKUP = cfg.task_plp_gs_per_client.control_sheet_lookup.to_dict()  # noqa
    CONTROL_SHEET_URL = CONTROL_SHEET_LOOKUP[client][territory]["url"]
    CONTROL_SHEET_TAB = CONTROL_SHEET_LOOKUP[client][territory]["control_sheet_tab_name"]
    PLP_PLACEMENTS_TAB = CONTROL_SHEET_LOOKUP[client][territory]["plp_placements_tab_name"]
    ADDITIONAL_PLP_PLACEMENTS_TAB = CONTROL_SHEET_LOOKUP[client][territory][
        "additional_plp_placements_tab_name"
    ]

    return {
        "CONTROL_SHEET_LOOKUP": CONTROL_SHEET_LOOKUP,
        "CONTROL_SHEET_URL": CONTROL_SHEET_URL,
        "CONTROL_SHEET_TAB": CONTROL_SHEET_TAB,
        "PLP_PLACEMENTS_TAB": PLP_PLACEMENTS_TAB,
        "ADDITIONAL_PLP_PLACEMENTS_TAB": ADDITIONAL_PLP_PLACEMENTS_TAB,
    }


@pa.check_output(schemas.GlobalSolutionOutputModel, lazy=True)
def process_control_sheet(control_sheet_config: Dict) -> "DataFrame":
    CONTROL_SHEET_URL = control_sheet_config["CONTROL_SHEET_URL"]
    CONTROL_SHEET_TAB = control_sheet_config["CONTROL_SHEET_TAB"]
    PLP_PLACEMENTS_TAB = control_sheet_config["PLP_PLACEMENTS_TAB"]
    ADDITIONAL_PLP_PLACEMENTS_TAB = control_sheet_config["ADDITIONAL_PLP_PLACEMENTS_TAB"]

    logger.info(f"Reading control sheet from Google sheet: {CONTROL_SHEET_URL}")
    latest_control_sheet = gs_helpers.read_from_google_sheets_to_dataframe(
        sheet_url=CONTROL_SHEET_URL, worksheet_name=CONTROL_SHEET_TAB
    )
    # Data validation.
    # NOTE: soft validation (no assert) due to some inconsistency between
    # fatface and next
    latest_control_sheet = latest_control_sheet.filter(latest_control_sheet.UniqueAdID != "")
    latest_control_sheet = schemas.ControlSheetInputModel.validate(latest_control_sheet, lazy=True)
    errors_json = json.dumps(
        dict(latest_control_sheet.pandera.errors),
        indent=2,
    )
    logger.info(f"Data validation errors: {errors_json}")

    latest_control_sheet.createOrReplaceTempView("control_sheet")

    plp_placements = gs_helpers.read_from_google_sheets_to_dataframe(
        sheet_url=CONTROL_SHEET_URL, worksheet_name=PLP_PLACEMENTS_TAB
    )
    plp_placements = (
        plp_placements.where(col("Page") != "")
        .where(col("Location").startswith("PL"))
        .withColumnsRenamed({"Location": "PLP_slot", "Page": "URL"})
        .select("PLP_slot", "URL")
    )
    plp_placements.createOrReplaceTempView("plp_placements")

    try:
        plx_placements = gs_helpers.read_from_google_sheets_to_dataframe(
            sheet_url=CONTROL_SHEET_URL,
            worksheet_name=ADDITIONAL_PLP_PLACEMENTS_TAB,
        )

        plx_placements = plx_placements.drop("Sales").withColumn("PLP_slot", lit("PLX"))
        plx_placements = plx_placements.where(col("URL") != "")
        plx_placements = plx_placements.join(
            plp_placements.select("URL"), how="left_anti", on=["URL"]
        )

        plp_placements = plp_placements.unionByName(plx_placements)

    except IndexError:
        logger.error("No additional PLP placements found")

    # Derive Trade name from URL and join to search test cells
    plp_slots = [i for i in latest_control_sheet.columns if i.lower().startswith("pl")] + ["PLX"]

    latest_control_sheet = latest_control_sheet.select(
        "uniqueadid",
        "realm",
        "territory",
        "status",
        "CMSPageID",
        "MASIDToken",
        *plp_slots,
    )

    latest_control_sheet = latest_control_sheet.withColumn(
        "action",
        lit("upsert"),
    )
    # This needs to be done at a URL level not an ad level.
    # For now we're upserting everything.
    # when(lower(col("status")) == "active", lit("upsert"))
    #     .when(lower(col("status")) == "inactive", lit("delete"))
    #     .otherwise(lit("na"))

    latest_control_sheet_melt = latest_control_sheet.melt(
        [
            "uniqueadid",
            "MASIDToken",
            "CMSPageID",
            "action",
            "realm",
            "territory",
        ],
        plp_slots,
        "PLP_slot",
        "PLP_bools",
    )

    latest_control_sheet_melt = latest_control_sheet_melt.join(plp_placements, on=["PLP_slot"])
    latest_control_sheet_melt = latest_control_sheet_melt.dropDuplicates(
        subset=["URL", "PLP_slot", "CMSPageID"]
    )

    # filter for ticked PL slots
    latest_control_sheet_melt = latest_control_sheet_melt.filter(
        latest_control_sheet_melt.PLP_bools == "TRUE"
    )

    # When masid token is not null then add PLP_slot_MASIDToken-CMSPageID,
    # else -CMSPageID
    latest_control_sheet_melt = latest_control_sheet_melt.withColumn(
        "MASIDCMSid",
        when(
            (col("MASIDToken").isNotNull()) & (col("MASIDToken") != ""),
            concat(
                col("PLP_slot"),
                lit("_"),
                col("MASIDToken"),
                lit("-"),
                col("CMSPageID"),
            ),
        ).otherwise(
            concat(
                lit("-"),
                col("CMSPageID"),
            )
        ),
    )

    output_df = latest_control_sheet_melt.groupby("action", "realm", "territory", "URL").apply(
        gs_helpers.get_masid_csmid_columns_udf
    )

    output_df = gs_helpers.format_output_col_names(
        output_df,
        output_schema_mapping={
            "action": "Action",
            "realm": "realm",
            "territory": "territory",
            "URL": "url",
            "MASIDCMSid": "masIdSlotsAndCMSContent",
        },
    )

    return output_df


if __name__ == "__main__":
    # parse parameters
    jobparser = get_job_parser()
    jobparser._parse_args()
    JOB_ENV = jobparser.get_arg("--job_env")
    TERRITORY = jobparser.get_arg("--territory")
    CLIENT = jobparser.get_arg("--client")
    LOG_LEVEL = jobparser.get_arg("--log_level")

    if LOG_LEVEL:
        configure_logging(log_level=LOG_LEVEL)
    else:
        configure_logging()

    # load configuration
    config = config_manager.load_config(JOB_ENV)

    output_table_name_map = config.task_plp_gs_per_client.output_table_name_plp_gs
    OUTPUT_TABLE_NAME = output_table_name_map[CLIENT][TERRITORY]
    WAREHOUSE = config.warehouse
    SCHEMA = config.schema

    # final output write
    # TABLES_TO_COMBINE = config.task_plp_gs_combiner.tables_to_combine.to_list()
    GS_FINAL_OUTPUT_TABLE_NAME = config.task_plp_gs_combiner.output_table_name_latest
    ACCOUNT_NAME = config.az_st_account
    ACCOUNT_URL = config.az_st_account_url
    CONTAINER_NAME = config.az_st_container_name
    DBUTILS_SECRET_SCOPE = config.dbutils_secret_scope
    SECRET_KEY_SPN_CLIENTID = config.secret_key_spn_clientid
    SECRET_KEY_SPN_SECRET = config.secret_key_spn_secret
    TENANT_ID = config.az_tenant_id
    AZ_OUTPUT_ABFSS_PATH = config.task_plp_gs_combiner.az_output_abfss_path

    # log all params
    logger.info(
        f"Configuration - "
        f"ENV: {JOB_ENV}, "
        f"WAREHOUSE: {WAREHOUSE}, "
        f"SCHEMA: {SCHEMA}, "
        f"CLIENT: {CLIENT}, "
        f"TERRITORY: {TERRITORY}, "
        f"OUTPUT_TABLE_NAME: {OUTPUT_TABLE_NAME}, "
        # f"TABLES_TO_COMBINE: {TABLES_TO_COMBINE}, "
        f"GS_FINAL_OUTPUT_TABLE_NAME: {GS_FINAL_OUTPUT_TABLE_NAME}, "
        f"ACCOUNT_NAME: {ACCOUNT_NAME}, "
        f"ACCOUNT_URL: {ACCOUNT_URL}, "
        f"CONTAINER: {CONTAINER_NAME}, "
        f"SCOPE: {DBUTILS_SECRET_SCOPE}, "
        f"TENANT_ID: {TENANT_ID}, "
        f"AZ_OUTPUT_ABFSS_PATH: {AZ_OUTPUT_ABFSS_PATH}"
    )

    spark.sql(f"USE CATALOG {WAREHOUSE}")

    control_sheet_config = load_control_sheet_config(cfg=config, client=CLIENT, territory=TERRITORY)
    CONTROL_SHEET_LOOKUP = control_sheet_config["CONTROL_SHEET_LOOKUP"]
    output_df = process_control_sheet(control_sheet_config=control_sheet_config)

    # Data validation
    pandera_errors = output_df.pandera.errors
    errors_json = json.dumps(dict(pandera_errors), indent=2)
    logger.info(f"Data validation errors: {errors_json}")
    logger.info(output_df.show(5, truncate=False))
    assert not pandera_errors, "Data validation failed!"

    # Writing output
    output_count = output_df.count()
    logger.info(f"Writing to {OUTPUT_TABLE_NAME} with {output_count} records")
    output_df.write.mode("overwrite").saveAsTable(OUTPUT_TABLE_NAME)

    # Write to final GS table and Azure Storage
    gs_helpers.create_dl_table(
        spark_df=output_df,
        OUTPUT_TABLE=GS_FINAL_OUTPUT_TABLE_NAME,
        limit_history=True,
        limit_history_days=365,
        join_condition="(source.rundate=dest.rundate AND source.realm=dest.realm AND source.territory=dest.territory)",
    )

    gs_helpers.configure_abfs(
        spark=spark,
        dbutils=dbutils,
        account_name=ACCOUNT_NAME,
        tenant_id=TENANT_ID,
        dbutils_secret_scope=DBUTILS_SECRET_SCOPE,
        secret_key_spn_clientid=SECRET_KEY_SPN_CLIENTID,
        secret_key_spn_secret=SECRET_KEY_SPN_SECRET,
    )

    (
        output_df.repartition(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(AZ_OUTPUT_ABFSS_PATH)
    )
    logger.info(f"Written output_df with {output_count} records to {AZ_OUTPUT_ABFSS_PATH}")

    logger.info("Run complete")

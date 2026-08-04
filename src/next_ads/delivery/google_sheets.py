"""Global solution helpers"""

import ast
import json
from dataclasses import dataclass
from datetime import date

import gspread as gs
import pandera.pyspark as pa
import pandas as pd
import pyspark
from dynaconf import Dynaconf
from pyspark import StorageLevel
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    PandasUDFType,
    col,
    concat,
    lit,
    pandas_udf,
    when,
)
from pyspark.sql.types import StringType, StructField, StructType
from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import configure_logging, get_logger

from next_ads.common import config_manager
from next_ads.common.delta_writes import (
    replace_scope_by_name,
    replace_table_by_name,
    validate_unique_non_null_keys,
)
from next_ads.data.validation import schemas

spark = None
dbutils = None
logger = get_logger(__name__)

PLP_GS_OUTPUT_COLUMNS = [
    "Action",
    "realm",
    "territory",
    "url",
    "masIdSlotsAndCMSContent",
]
PLP_GS_OUTPUT_KEY_COLUMNS = ["Action", "realm", "territory", "url"]
PLP_ASSIGNMENT_KEY_COLUMNS = ["URL", "PLP_slot", "uniqueadid"]
PLP_GS_HISTORY_COLUMNS = [*PLP_GS_OUTPUT_COLUMNS, "rundate"]


@dataclass(frozen=True)
class PlpGsDeliveryConfig:
    """Resolved PLP GS output route for a job run."""

    output_table_name: str
    final_output_table_name: str
    az_output_abfss_path: str
    catalog_write: str
    schema_write: str


def _get_spark():
    global spark
    if spark is None:
        spark = configure_spark()
    return spark


def _get_dbutils():
    global dbutils
    if dbutils is None:
        dbutils = get_dbutils()
    return dbutils


def resolve_plp_gs_delivery_config(
    config,
    client: str,
    territory: str,
) -> PlpGsDeliveryConfig:
    """Resolve the table and storage outputs used by the PLP GS task."""
    output_table_name_map = config.tables_write.nextads_plp_gs
    client_map = output_table_name_map[client]
    territory_key = (
        territory
        if territory in client_map
        else territory.lower()
    )

    return PlpGsDeliveryConfig(
        output_table_name=client_map[territory_key]["latest"],
        final_output_table_name=config.tables_write.nextads_plp_gs_latest,
        az_output_abfss_path=(
            config.task_plp_gs_combiner.az_output_abfss_path
        ),
        catalog_write=config.catalog_write,
        schema_write=config.schema_write,
    )


def load_plp_gs_runtime_config(
    *,
    job_env: str,
    config_client: str,
    delivery_client: str,
    territory: str,
):
    """Load the repo client config separately from the delivery realm."""
    config = config_manager.load_config(job_env, client=config_client)
    delivery_config = resolve_plp_gs_delivery_config(
        config=config,
        client=delivery_client,
        territory=territory,
    )
    return config, delivery_config


@pa.check_output(schemas.GlobalSolutionOutputModel, lazy=True)
def process_control_sheet(
    config: Dynaconf,
    spark_session=None,
    run_logger=None,
) -> "DataFrame":
    """Process PLP Google Sheets delivery rows from configured source tables."""
    spark_session = spark_session or _get_spark()
    run_logger = run_logger or logger

    run_logger.info(
        "Loading control sheet from tables: "
        f"{config.tables_write.control_sheet_raw_latest}, "
        f"{config.tables_write.control_sheet_plp_raw_latest}, "
        f"{config.tables_write.multipage_locations_latest}"
    )
    latest_control_sheet = spark_session.table(
        config.tables_write.control_sheet_raw_latest
    )
    plp_placements = spark_session.table(
        config.tables_write.control_sheet_plp_raw_latest
    )
    plx_placements = spark_session.table(
        config.tables_write.multipage_locations_latest
    )

    latest_control_sheet = latest_control_sheet.filter(
        latest_control_sheet.UniqueAdID != ""
    ).filter(latest_control_sheet.CMSPageID != "")

    latest_control_sheet.createOrReplaceTempView("control_sheet")

    plp_placements = (
        plp_placements.where(col("Page") != "")
        .where(col("Location").startswith("PL"))
        .withColumnsRenamed({"Location": "PLP_slot", "Page": "URL"})
        .select("PLP_slot", "URL")
    )
    plp_placements.createOrReplaceTempView("plp_placements")

    try:
        plx_placements = (
            plx_placements.withColumnsRenamed(
                {"Location": "PLP_slot", "Page": "URL"}
            )
            .where(col("URL") != "")
            .select("PLP_slot", "URL")
        )
        plx_placements = plx_placements.join(
            plp_placements.select("URL"), how="left_anti", on=["URL"]
        )

        plp_placements = plp_placements.unionByName(plx_placements)

    except IndexError:
        run_logger.error("No additional PLP placements found")

    plp_slots = [
        i for i in latest_control_sheet.columns if i.lower().startswith("pl")
    ]

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

    latest_control_sheet_melt = latest_control_sheet_melt.join(
        plp_placements, on=["PLP_slot"]
    )
    latest_control_sheet_melt = latest_control_sheet_melt.filter(
        latest_control_sheet_melt.PLP_bools == "TRUE"
    )
    validate_unique_non_null_keys(
        latest_control_sheet_melt,
        PLP_ASSIGNMENT_KEY_COLUMNS,
    )

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

    output_df = latest_control_sheet_melt.groupby(
        "action", "realm", "territory", "URL"
    ).apply(get_masid_csmid_columns_udf)

    return format_output_col_names(
        output_df,
        output_schema_mapping={
            "action": "Action",
            "realm": "realm",
            "territory": "territory",
            "URL": "url",
            "MASIDCMSid": "masIdSlotsAndCMSContent",
        },
    )


def publish_plp_tables(
    output_df: DataFrame,
    *,
    history_table: str,
    latest_table: str,
    run_date: date,
    realm: str,
    territory: str,
    spark_session,
):
    """Validate once, then publish PLP history before its serving snapshot."""
    validation = validate_unique_non_null_keys(
        output_df,
        PLP_GS_OUTPUT_KEY_COLUMNS,
    )
    history_df = output_df.withColumn(
        "rundate",
        lit(run_date).cast("date"),
    ).select(*PLP_GS_HISTORY_COLUMNS)
    replace_scope_by_name(
        history_df,
        history_table,
        {
            "rundate": run_date,
            "realm": realm,
            "territory": territory,
        },
        PLP_GS_HISTORY_COLUMNS,
        spark=spark_session,
    )
    replace_table_by_name(
        output_df,
        latest_table,
        PLP_GS_OUTPUT_COLUMNS,
        spark=spark_session,
    )
    return validation


def resolve_run_date(run_date: str | date) -> date:
    """Require one explicit ISO date for every PLP output in a logical run."""
    if isinstance(run_date, date):
        return run_date
    if not isinstance(run_date, str):
        raise ValueError("--run_date must use ISO format YYYY-MM-DD")
    run_date_text = run_date.strip()
    try:
        parsed_run_date = date.fromisoformat(run_date_text)
    except ValueError as exc:
        raise ValueError(
            "--run_date must use ISO format YYYY-MM-DD"
        ) from exc
    if parsed_run_date.isoformat() != run_date_text:
        raise ValueError("--run_date must use ISO format YYYY-MM-DD")
    return parsed_run_date


def run_plp_gs_delivery(
    job_env: str,
    territory: str,
    client: str,
    config_client: str,
    run_date: str | date,
    log_level: str | None,
    spark_session=None,
    dbutils_obj=None,
) -> None:
    """Run the PLP Google Sheets delivery task."""
    logical_run_date = resolve_run_date(run_date)
    if log_level:
        configure_logging(log_level=log_level)
    else:
        configure_logging()

    run_logger = get_logger(__name__)
    spark_session = spark_session or configure_spark()
    dbutils_obj = dbutils_obj or get_dbutils()

    config, delivery_config = load_plp_gs_runtime_config(
        job_env=job_env,
        config_client=config_client,
        delivery_client=client,
        territory=territory,
    )

    run_logger.info(
        f"Configuration - "
        f"ENV: {job_env}, "
        f"CATALOG_WRITE: {delivery_config.catalog_write}, "
        f"WAREHOUSE: {config.catalog_read}, "
        f"SCHEMA: {delivery_config.schema_write}, "
        f"CLIENT: {client}, "
        f"TERRITORY: {territory}, "
        f"OUTPUT_TABLE_NAME: {delivery_config.output_table_name}, "
        f"GS_FINAL_OUTPUT_TABLE_NAME: {delivery_config.final_output_table_name}, "
        f"ACCOUNT_NAME: {config.az_st_account}, "
        f"ACCOUNT_URL: {config.az_st_account_url}, "
        f"CONTAINER: {config.az_st_container_name}, "
        f"SCOPE: {config.dbutils_secret_scope}, "
        f"TENANT_ID: {config.az_tenant_id}, "
        f"AZ_OUTPUT_ABFSS_PATH: {delivery_config.az_output_abfss_path}"
    )

    spark_session.sql(f"USE CATALOG {config.catalog_read}")

    output_df = process_control_sheet(
        config=config,
        spark_session=spark_session,
        run_logger=run_logger,
    )

    pandera_errors = output_df.pandera.errors
    errors_json = json.dumps(dict(pandera_errors), indent=2)
    run_logger.info(f"Data validation errors: {errors_json}")
    assert not pandera_errors, "Data validation failed!"

    output_df = output_df.select(*PLP_GS_OUTPUT_COLUMNS).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    try:
        validation = publish_plp_tables(
            output_df,
            history_table=delivery_config.final_output_table_name,
            latest_table=delivery_config.output_table_name,
            run_date=logical_run_date,
            realm=client.title(),
            territory=territory.upper(),
            spark_session=spark_session,
        )
        output_count = validation.row_count
        run_logger.info(
            f"Published {delivery_config.output_table_name} "
            f"with {output_count} records"
        )

        configure_abfs(
            spark=spark_session,
            dbutils=dbutils_obj,
            account_name=config.az_st_account,
            tenant_id=config.az_tenant_id,
            dbutils_secret_scope=config.dbutils_secret_scope,
            secret_key_spn_clientid=config.secret_key_spn_clientid,
            secret_key_spn_secret=config.secret_key_spn_secret,
        )

        (
            output_df.repartition(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(delivery_config.az_output_abfss_path)
        )
        run_logger.info(
            f"Written output_df with {output_count} records "
            f"to {delivery_config.az_output_abfss_path}"
        )
    finally:
        output_df.unpersist()
    run_logger.info("Run complete")


def get_service_account_dict(secret="mktg-gcp-service-account-b64-encoded"):
    import base64

    file = _get_dbutils().secrets.get("mktg_gcp", secret)
    d = base64.b64decode(file).decode("utf-8")
    return ast.literal_eval(d)


def read_from_google_sheets_to_dataframe(
    gcp_key_json_file=None, sheet_url=None, worksheet_name=None
):
    """Function to read from google sheets to spark data frame

    Prerequisites:
    For the function to work, user needs to share the spreadsheet with the
    service account user (e.g.
    discovery@big-query-156009.iam.gserviceaccount.com).
    Following parameter should be set to point to key associated with the
    service account:
    gcp_key_json_file = '/dbfs/gcp/service-account-key.json'

    Inputs:
        - gcp_key_json_file : Service account Key file used for GCP access.
            Google Drive API and Google Sheets API needs to be enabled.
        - sheet_url: url of google sheet eg:
            https://docs.google.com/spreadsheets/d/1udrmu2yhUbwMHff4Ku74tKvFJHPqkEU4e6WCYiDkh9U/edit#gid=0
    - worksheet_name: name of the worksheet in the sheet eg. Sheet2

    """
    if gcp_key_json_file is None:
        gcp_credentials_dict = get_service_account_dict()
    else:
        try:
            file = open(gcp_key_json_file).read()
            gcp_credentials_dict = ast.literal_eval(file)
        except FileNotFoundError:
            logger.info(
                f"Could not find gcp credentials file: {gcp_key_json_file}!"
            )
            return

    if sheet_url is None:
        raise Exception("sheet_url not provided")
        return

    if worksheet_name is None:
        logger.info(
            "worksheet_name is not provided. So defaulting to "
            "worksheet_name = Sheet1"
        )
        worksheet_name = "Sheet1"

    google_spread_conn = gs.service_account_from_dict(gcp_credentials_dict)

    google_sheet = google_spread_conn.open_by_url(sheet_url)
    worksheet = google_sheet.worksheet(worksheet_name)
    pandas_df = pd.DataFrame(worksheet.get_all_records())

    # check if sheet is empty
    if pandas_df.empty:
        logger.warning(f"Empty worksheet '{worksheet_name}' from {sheet_url}")

        # Get column names from header row
        try:
            columns = worksheet.row_values(1)
            if not columns:
                raise ValueError(
                    f"No columns found in worksheet '{worksheet_name}'"
                )

            # Create schema with all StringType columns
            schema = StructType([
                StructField(col, StringType(), True)
                for col in columns
            ])
            schema_cols = [field.name for field in schema.fields]
            logger.info(
                f"Returning empty DataFrame with schema: {schema_cols}"
            )
            return _get_spark().createDataFrame([], schema=schema)
        except Exception as e:
            logger.error(f"Could not infer schema from empty worksheet: {e}")
            raise

    for col_name in pandas_df.columns:
        if pandas_df[col_name].dtype == "object":
            # Column has mixed types (strings, nulls, numbers)
            # Convert to string, replacing NaN/None with empty string
            pandas_df[col_name] = pandas_df[col_name].fillna("").astype(str)

    df_return = _get_spark().createDataFrame(pandas_df)

    return df_return


def format_output_col_names(
    df: pyspark.sql.dataframe.DataFrame,
    output_schema_mapping: dict = {
        "action": "Action",
        "realm": "realm",
        "territory": "territory",
        "URL": "url",
        "MASIDCMSid": "masIdSlotsAndCMSContent",
    },
) -> pyspark.sql.dataframe.DataFrame:
    """Fuction will take in a pyspark data frame with columns:
    ['action', 'realm', 'territory', 'URL', 'MASIDCMSid']
    and renames them to what is expected in the output schema defined by the
    output_schema_mapping param.

    @params
    df: pyspark.sql.dataframe.DataFrame
    output_schema_mapping defining input: out column names

    @returns
    pyspark.sql.dataframe.DataFrame
    """
    for on, nn in output_schema_mapping.items():
        df = df.withColumnRenamed(on, nn)
    return df


schema = StructType(
    [
        StructField("action", StringType(), True),
        StructField("realm", StringType(), True),
        StructField("territory", StringType(), True),
        StructField("URL", StringType(), True),
        StructField("MASIDCMSid", StringType(), True),
    ]
)


@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def get_masid_csmid_columns_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    """Function takes in a data frame where we have multiple rows for the same
    PLP url, realm, action and territory but differing MASIDCMSid values.
    The function will return a single row for each PLP url, realm, action and
    territory and combine all the values of MASIDCMSid into a single string
    separated by a pipe character.

    @params
    pdf: pandas dataframe with columns
    ['action', 'realm', 'territory', 'URL', 'MASIDCMSid']

    @returns
    pandas dataframe with columns
    ['action', 'realm', 'territory', 'URL', 'MASIDCMSid']
    """
    masid_cms_list = "|".join(sorted(pdf["MASIDCMSid"].tolist()))
    pdf = pdf.iloc[0]
    pdf["MASIDCMSid"] = masid_cms_list
    return pd.DataFrame(
        [pdf[["action", "realm", "territory", "URL", "MASIDCMSid"]]]
    )


def configure_abfs(
    spark,
    dbutils,
    account_name: str,
    tenant_id: str,
    dbutils_secret_scope: str,
    secret_key_spn_clientid: str,
    secret_key_spn_secret: str,
) -> None:
    """Configure Spark for ABFS authentication and write DataFrame to CSV.

    Args:
        spark: SparkSession instance
        dbutils: Databricks utilities instance
        account_name: Azure storage account name
        tenant_id: Azure tenant ID
        dbutils_secret_scope: Databricks secret scope name
        secret_key_spn_clientid: Secret key for Service Principal client ID
        secret_key_spn_secret: Secret key for Service Principal secret
    """
    logger.info("Configuring ABFS authentication...")

    # Get credentials from Databricks secrets
    client_id = dbutils.secrets.get(
        scope=dbutils_secret_scope, key=secret_key_spn_clientid
    )
    client_secret = dbutils.secrets.get(
        scope=dbutils_secret_scope, key=secret_key_spn_secret
    )

    # Configure Spark for ABFS OAuth authentication
    spark.conf.set(
        f"fs.azure.account.auth.type.{account_name}"
        ".dfs.core.windows.net",
        "OAuth",
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{account_name}"
        ".dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{account_name}"
        ".dfs.core.windows.net",
        client_id,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{account_name}"
        ".dfs.core.windows.net",
        client_secret,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{account_name}"
        ".dfs.core.windows.net",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    )

    logger.info("ABFS authentication configured")

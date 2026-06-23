"""Global solution helpers"""

import ast
from dataclasses import dataclass

import gspread as gs
import pandas as pd
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.functions import PandasUDFType, pandas_udf, current_date
import pyspark
from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import get_logger

spark = None
dbutils = None
logger = get_logger(__name__)


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


def data_quality_check(df, table, **kwargs):
    total_count = df.count()
    print(f"Total count of table: {total_count}")
    if total_count == 0:
        raise Exception(f"Found Empty dataset from {table}")
    total_distinct_count = df.distinct().count()
    print(f"Total distinct count of table: {total_distinct_count}")
    if total_count != total_distinct_count:
        raise Exception(f"Duplicates found in the {table}")
    print("No duplicates found in table")
    if kwargs:
        for key, column in kwargs.items():
            count_col = df.select(f"{column}").count()
            print(f"Total Count by {column}: {count_col}")
            count_col_distinct = df.select(f"{column}").distinct().count()
            print(f"Total distinct Count by {column}: {count_col_distinct}")
            if count_col != count_col_distinct:
                raise Exception(f"Duplicates by {column} found in {table}")
            print(f"No duplicates found in table by {column}")


def optimize_delta_table(TABLE_NAME, vacuum_hours=0, zorderby=None):
    spark_session = _get_spark()
    spark_session.sql(
        "SET spark.databricks.delta.retentionDurationCheck.enabled = false"
    )
    if zorderby is None:
        print(f"Optimizing {TABLE_NAME} without ZORDERBY clause")
        spark_session.sql(f"""OPTIMIZE {TABLE_NAME}""")
        print("Optimize step complete")
    else:
        zorderby_string = ",".join(zorderby)
        print(
            f"Optimizing {TABLE_NAME} with ZORDERBY clause : {zorderby_string}"
        )
        spark_session.sql(f"""OPTIMIZE {TABLE_NAME}
                        ZORDER BY {zorderby_string}""")
        print("Optimize with ZORDERBY complete")
    spark_session.sql(f"""VACUUM {TABLE_NAME} RETAIN {vacuum_hours} hours""")
    return None


def f_limit_history(OUTPUT_TABLE, limit_history_days):
    _get_spark().sql(
        (
            f"DELETE FROM {OUTPUT_TABLE} "
            f"where rundate <= current_date()-{limit_history_days}"
        )
    )
    optimize_delta_table(OUTPUT_TABLE)


def create_dl_table(
    spark_df,
    limit_history=True,
    limit_history_days=731,
    merge_schema=False,
    join_condition="(source.rundate=dest.rundate)",
    OUTPUT_TABLE=None,
):
    # Add rundate
    model_output = spark_df.withColumn("rundate", current_date())

    # run dq check
    data_quality_check(model_output, OUTPUT_TABLE)

    # Create a table from dataframe
    model_output.createOrReplaceTempView("model_output_table")

    print("Delta processing")
    if merge_schema:
        print(
            "merge_schema is set to True - Turning on AutoMerge Option "
            "before performing merge operation"
        )
        _get_spark().sql(
            "SET spark.databricks.delta.schema.autoMerge.enabled = true"
        )
    _get_spark().sql(f"""
MERGE INTO {OUTPUT_TABLE} dest
USING model_output_table source ON {join_condition}
WHEN NOT MATCHED THEN INSERT *
            """)
    print(f"Table {OUTPUT_TABLE} is now updated")

    if limit_history:
        f_limit_history(OUTPUT_TABLE, limit_history_days)

    return None


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

from next_ads.utils.etl import build_spark_schema
import base64
import ast
import gspread as gs
from pyspark.sql import DataFrame
from next_ads.utils.dbc import get_dbutils, get_spark


GCP_SCOPE = "mktg_gcp"
GCP_KEY = "mktg-gcp-service-account-b64-encoded"

# Legacy? - gcp_key_json_file should be set to point to the key
# associated with the serivce acccount
# GCP_KEY_JSON_FILE = '/dbfs/gcp/service-account-key.json'


def gcp_conn():
    """
    Returns `gspread` connection object
    """
    file = get_dbutils().secrets.get(
        scope=GCP_SCOPE,
        key=GCP_KEY)
    decoded = base64.b64decode(file.encode("utf-8")).decode("utf-8")
    service_account_dict = ast.literal_eval(decoded)
    return gs.service_account_from_dict(service_account_dict)


def spark_df_from_sheets(
        url: str,
        worksheet_name: str,
        schema: list[list[str]] = []
        ) -> DataFrame:
    """
    Function to read from Google Sheets to Spark dataframe. Only seems to
    work importing everything as a string(??).

    Prerequisites:
        Share Google Sheet to be read with service account user
        e.g. azure-databricks@big-query-156009.iam.gserviceaccount.com

    Arguments:
        url - URL of Google Sheet
        worksheet_name - Name of worksheet to be read
        schema - List of lists representing schema to be read
            e.g. `[["ID","string","not null"],["Name","string","nullable"]]`
    """
    google_sheet = gcp_conn().open_by_url(url)
    worksheet = google_sheet.worksheet(worksheet_name)

    if schema:
        df = get_spark().createDataFrame(
            worksheet.get_all_records(),
            build_spark_schema(schema)
            )
    else:
        df = get_spark().createDataFrame(worksheet.get_all_records())

    return df

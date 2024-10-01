from utils.sparkutils import build_spark_schema
import base64
import ast
import gspread as gs
from pyspark.sql import DataFrame
from utils.dbcutils import get_dbutils, get_spark


def gcp_conn():
    """
    Function returns gspread connection object.
    """
    file = get_dbutils().secrets.get(
        scope="mktg_gcp",
        key="mktg-gcp-service-account-b64-encoded")
    decoded = base64.b64decode(file.encode("utf-8")).decode("utf-8")
    service_account_dict = ast.literal_eval(decoded)
    return gs.service_account_from_dict(service_account_dict)


def spark_df_from_sheets(
        sheets_url: str,
        worksheet_name: str,
        schema: list = []
        ) -> DataFrame:
    '''
    Function to read from google sheets to spark data frame

    Prerequisites:
    For the function to work, user needs to share the spreadsheet with service
    account user e.g. azure-databricks@big-query-156009.iam.gserviceaccount.com

    # Legacy?
    # Following parameter should be set to point to key associated with the
    # serivce acccount gcp_key_json_file = '/dbfs/gcp/service-account-key.json'

    Inputs:
    - gcp_key_json_file : Service account Key file used for GCP access,
        Google Drive API and Google Sheets API needs to be enabled.
    - sheet_url: url of google sheet
    - worksheet_name: name of the worksheet in the sheet eg. Sheet2
    - schema: list of lists to convert to spark schema,
        e.g. [
            ["id", "string", "notnullable"],
            ["name", "string", "nullable"],
            ["age", "string", nullable]
            ]
        N.B. gspread read only seems to work when importing all as string type
    '''

    google_sheet = gcp_conn().open_by_url(sheets_url)
    worksheet = google_sheet.worksheet(worksheet_name)

    if schema:
        df = get_spark().createDataFrame(
            worksheet.get_all_records(),
            build_spark_schema(schema)
            )
    else:
        df = get_spark().createDataFrame(worksheet.get_all_records())

    return df

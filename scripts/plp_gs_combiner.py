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

from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser

from next_ads.utils import gs_helpers
from next_ads.utils import config_manager
from next_ads.data_validation import schemas

logger = get_logger(__name__)

spark = configure_spark()
dbutils = get_dbutils()


def _configure_abfs(
    spark,
    dbutils,
    account_name: str,
    tenant_id: str,
    dbutils_secret_scope: str,
    secret_key_spn_clientid: str,
    secret_key_spn_secret: str,
) -> None:
    """
    Configure Spark for ABFS authentication and write DataFrame to CSV.

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


if __name__ == "__main__":
    # parse parameters
    jobparser = get_job_parser()
    jobparser._parse_args()
    JOB_ENV = jobparser.get_arg("--job_env")
    LOG_LEVEL = jobparser.get_arg("--log_level")

    if LOG_LEVEL:
        configure_logging(log_level=LOG_LEVEL)
    else:
        configure_logging()

    # load configuration
    config = config_manager.load_config(JOB_ENV)

    WAREHOUSE = config.warehouse
    SCHEMA = config.schema
    TABLES_TO_COMBINE = config.task_plp_gs_combiner.tables_to_combine.to_list()
    OUTPUT_TABLE_NAME = config.task_plp_gs_combiner.output_table_name_latest
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
        f"TABLES_TO_COMBINE: {TABLES_TO_COMBINE}, "
        f"OUTPUT_TABLE_NAME: {OUTPUT_TABLE_NAME}, "
        f"ACCOUNT_NAME: {ACCOUNT_NAME}, "
        f"ACCOUNT_URL: {ACCOUNT_URL}, "
        f"CONTAINER: {CONTAINER_NAME}, "
        f"SCOPE: {DBUTILS_SECRET_SCOPE}, "
        f"TENANT_ID: {TENANT_ID}, "
        f"AZ_OUTPUT_ABFSS_PATH: {AZ_OUTPUT_ABFSS_PATH}"
    )

    spark.sql(f"USE CATALOG {WAREHOUSE}")

    for tableName in TABLES_TO_COMBINE:
        table_count = spark.table(tableName).count()
        logger.info(f"{tableName} table count: {table_count}")

    # combine tables
    base_table = spark.sql(f"""select * from {TABLES_TO_COMBINE.pop(0)}""")
    base_columns = base_table.columns

    for tableName in TABLES_TO_COMBINE:
        tableName = spark.sql(f"""select * from {tableName}""")
        base_table = base_table.unionByName(tableName)

    output_df = base_table.select(*base_columns)

    # Data validation
    output_df = schemas.GlobalSolutionOutputModel.validate(
        output_df,
        lazy=True,
    )
    pandera_errors = output_df.pandera.errors
    errors_json = json.dumps(dict(pandera_errors), indent=2)
    logger.info(f"Data validation errors: {errors_json}")
    assert not pandera_errors, "Data validation failed!"

    output_count = output_df.count()
    logger.info(f"Combined output_df with {output_count} records")
    gs_helpers.create_dl_table(
        spark_df=output_df,
        OUTPUT_TABLE=OUTPUT_TABLE_NAME,
        limit_history=True,
        limit_history_days=365,
    )

    _configure_abfs(
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
    logger.info(
        f"Written output_df with {output_count} records "
        f"to {AZ_OUTPUT_ABFSS_PATH}"
    )

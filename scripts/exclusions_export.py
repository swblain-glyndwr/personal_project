import sys
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
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
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from next_ads.utils import config_manager
from next_ads.utils.cosmos import get_cosmos_config, sdk_write_to_cosmos


def main(JOB_ENV, CLIENT, LOG_LEVEL):
    logger = get_logger(__name__)
    configure_logging(LOG_LEVEL)
    config = config_manager.load_config(JOB_ENV)

    spark = configure_spark()
    dbutils = get_dbutils()
    logger.info(f"Running in job environment: {JOB_ENV}")

    EXCLUSIONS_TABLE = config.tables_write.exclusions_latest
    exclusions_df = spark.table(EXCLUSIONS_TABLE)
    exclusions_df.createOrReplaceTempView("exclusions")

    sql = """
        with a as (
        select
            'next-uk' as id,
            Page as url,
            Exclude_Campaign as Ad
        from
            exclusions
        ),
        b as (
        select
            id,
            url,
            collect_list(distinct Ad) as excludedAds
        from
            a
        group by
            id,
            url
        )
        select
        id,
        collect_list(struct(url, excludedAds)) as mappings
        from
        b
        group by
        id
  """

    payload = spark.sql(sql)

    clientId = dbutils.secrets.get(
        config.dbutils_secret_scope, config.secret_key_spn_clientid
    )
    clientSecret = dbutils.secrets.get(
        config.dbutils_secret_scope,
        config.secret_key_spn_secret,
    )

    cosmos_config_args = {
        "url": config.cosmos_url,
        "db_name": config.cosmos_database,
        "container": config.cosmos_container,
        "subscriptionid": config.cosmos_subscriptionId,
        "rg_name": config.cosmos_resource_group,
        "tenantId": config.az_tenant_id,
        "clientId": clientId,
        "clientSecret": clientSecret,
    }

    cosmosconfig_upsert = get_cosmos_config("upsert", **cosmos_config_args)
    cosmosconfig_read = get_cosmos_config("read", **cosmos_config_args)

    excount = payload.count()

    logger.info(
        f"Try Writing {excount} exclusions to {config.cosmos_url} using spark connector with upsert mode, and fallback to the SDK if it fails."
    )

    write_success = False

    try:
        payload.write.format("cosmos.oltp").options(
            **cosmosconfig_upsert
        ).mode("APPEND").save()
        write_success = True

        logger.info("Reading back top 10 records from Cosmos DB")
        cosmos_preview_df = (
            spark.read.format("cosmos.oltp")
            .options(**cosmosconfig_read)
            .load()
        )
        cosmos_preview_df.filter("id = 'next-uk'").show(10, truncate=False)

    except Exception as c_e:
        logger.error(
            f"Failed writing to Cosmos DB using spark connector: {c_e}"
        )

    if not write_success:
        try:
            sdk_write_to_cosmos(config, JOB_ENV, payload)
            write_success = True
        except Exception as sdk_e:
            logger.error(f"Failed writing to Cosmos DB using SDK: {sdk_e}")
            raise sdk_e

    logger.info("Run complete")


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    JOB_ENV = jobparser.get_arg("--job_env")
    CLIENT = jobparser.get_arg("--client")
    LOG_LEVEL = jobparser.get_arg("--log_level")
    main(JOB_ENV, CLIENT, LOG_LEVEL)

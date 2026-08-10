import sys
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

from dsutils.dbc import configure_spark, get_dbutils
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from next_ads.common import config_manager
from next_ads.delivery.cosmos import get_cosmos_config, sdk_write_to_cosmos
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import pyspark.sql.functions as F


def main(JOB_ENV, CLIENT, LOG_LEVEL):
    logger = get_logger(__name__)
    configure_logging(LOG_LEVEL)
    config = config_manager.load_config(JOB_ENV, client=CLIENT)

    spark = configure_spark()
    dbutils = get_dbutils()
    logger.info(f"Running in job environment: {JOB_ENV}")

    REALM_TERRITORY = "next-gb"
    EXCLUSIONS_TABLE = config.tables_write.exclusions_latest
    exclusions_df = spark.table(EXCLUSIONS_TABLE)
    exclusions_df.createOrReplaceTempView("exclusions")

    payload_schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField(
                "mappings",
                ArrayType(
                    StructType(
                        [
                            StructField("url", StringType(), False),
                            StructField(
                                "excludedAds",
                                ArrayType(StringType(), containsNull=False),
                                False,
                            ),
                        ]
                    ),
                    containsNull=False,
                ),
                False,
            ),
        ]
    )

    # need to always return a dataframe even if there are no exclusions,
    # so existing exclusions can be cleared in cosmos
    payload = spark.createDataFrame(
        [(REALM_TERRITORY, [])], schema=payload_schema
    )

    exclusions_payload = (
        exclusions_df.select(
            F.lit(REALM_TERRITORY).alias("id"),
            F.col("url"),
            F.col("CMSPageID").alias("Ad"),
        )
        .groupBy("id", "url")
        .agg(F.collect_set("Ad").alias("excludedAds"))
        .groupBy("id")
        .agg(F.collect_set(F.struct("url", "excludedAds")).alias("mappings"))
    )

    if exclusions_payload.schema != payload_schema:
        raise ValueError(
            f"Schema mismatch:\npayload={payload_schema.simpleString()}\n"
            f"exclusions={exclusions_payload.schema.simpleString()}"
        )

    payload = exclusions_payload if exclusions_payload.take(1) else payload

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

        logger.info("Reading back document from Cosmos DB")
        cosmos_preview_df = (
            spark.read.format("cosmos.oltp")
            .options(**cosmosconfig_read)
            .load()
            .filter(f"id = '{REALM_TERRITORY}'")
        )

        cosmos_preview_df.show(truncate=False)

        if cosmos_preview_df.count() == 1:
            write_success = True

    except Exception as c_e:
        logger.error(
            f"Failed writing to Cosmos DB using spark connector (clientId: {clientId}): {c_e}"
        )

    if not write_success:
        try:
            sdk_write_to_cosmos(config, JOB_ENV, payload)
            write_success = True
        except Exception as sdk_e:
            logger.error(
                f"Failed writing to Cosmos DB using SDK (clientId: {clientId}): {sdk_e}"
            )
            raise sdk_e

    logger.info("Run complete")


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    JOB_ENV = jobparser.get_arg("--job_env")
    CLIENT = jobparser.get_arg("--client")
    LOG_LEVEL = jobparser.get_arg("--log_level")
    main(JOB_ENV, CLIENT, LOG_LEVEL)

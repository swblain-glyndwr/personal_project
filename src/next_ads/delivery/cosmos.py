import logging
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError
from azure.identity import ClientSecretCredential
from dsutils.dbc import get_dbutils

logger = logging.getLogger(__name__)


def get_logger(name=__name__):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        force=True,  # ensures it resets config in Databricks
    )
    return logging.getLogger(name)


def _resolve_tenant_id(dbutils, config, env):
    configured_tenant = getattr(config, "az_tenant_id", None)
    if configured_tenant:
        return configured_tenant

    senv = env.capitalize()
    return dbutils.secrets.get(
        scope="realtime", key=f"DataPlatform-{senv}-TenantId"
    )


def _raise_cosmos_error_with_context(operation, error, config):
    error_text = str(error)

    if (
        error.status_code == 403
        and "databaseAccounts/readMetadata" in error_text
    ):
        message = (
            "Cosmos RBAC denied metadata access (databaseAccounts/readMetadata). "
            "Grant the service principal a Cosmos data-plane role that includes metadata read "
            "(for writes, typically 'Cosmos DB Built-in Data Contributor') at account/database/container scope. "
            f"Operation={operation}, Endpoint={config.cosmos_url}, Database={config.cosmos_database}, Container={config.cosmos_container}."
        )
        logger.error(message)
        raise PermissionError(message) from error

    if error.status_code == 404:
        message = (
            f"Cosmos resource not found during {operation}. "
            f"Endpoint={config.cosmos_url}, Database={config.cosmos_database}, Container={config.cosmos_container}."
        )
        logger.error(message)
        raise FileNotFoundError(message) from error

    logger.error(f"Error during Cosmos operation '{operation}': {error}")
    raise error


def get_cosmos_config(
    ctype,
    url,
    db_name,
    container,
    subscriptionid,
    rg_name,
    tenantId,
    clientId,
    clientSecret,
    TC=False,
    gateway_mode=True,
):
    """Generate Azure Cosmos DB Spark connector configuration.

    This function creates a Spark configuration dictionary for connecting to Azure Cosmos DB
    with service principal authentication and operation type setting

    Parameters
    ----------
    ctype : str
        Type of operation. Must be one of: 'read', 'upsert', 'delete', or 'default_upsert'.
    url : str
        Azure Cosmos DB account endpoint URL.
    db_name : str
        Name of the database.
    container : str
        Name of the container.
    subscriptionid : str
        Azure subscription ID for the Cosmos DB account.
    rg_name : str
        Azure resource group name.
    tenantId : str
        Azure Entra (AAD) tenant ID for service principal authentication.
    clientId : str
        Azure Entra client ID (application ID) for service principal authentication.
    clientSecret : str
        Azure Entra client secret for service principal authentication.
    TC : bool, optional
        Enable throughput control. Default is False.
    gateway_mode : bool, optional
        Use gateway mode connection. Default is True. Set to False for direct mode.

    Returns:
    -------
    dict
        Spark configuration dictionary with Azure Cosmos DB connector settings
        specific to the requested operation type.

    Raises:
    ------
    Exception
        If ctype is not one of: 'read', 'upsert', 'delete', or 'default_upsert'.

    Notes:
    -----
    - Service Principal authentication is used via Azure Entra.
    - When TC is True, throughput control is set to 30000 RUs with global control enabled - needs a throughput control container
      (called "ThroughputControl") set up in the Cosmos DB account.
    - Schema inference is enabled for all operation types.
    - Write strategies vary by operation type: ItemOverwrite for upsert/default_upsert,
      ItemDelete for delete operations.
    """
    base_cosmosconfig = {
        # General settings for all modes
        "spark.cosmos.accountEndpoint": url,
        "spark.cosmos.database": db_name,
        "spark.cosmos.container": container,
        "spark.cosmos.account.subscriptionId": subscriptionid,
        "spark.cosmos.account.resourceGroupName": rg_name,
        # Entra authentication settings
        "spark.cosmos.auth.type": "ServicePrincipal",
        "spark.cosmos.account.tenantId": tenantId,
        "spark.cosmos.auth.aad.clientId": clientId,
        "spark.cosmos.auth.aad.clientSecret": clientSecret,
        # Additional Tuning
        "spark.cosmos.useGatewayMode": "True",
        # "spark.cosmos.read.partitioning.strategy": "Restrictive",
        # Write config
        "spark.cosmos.write.bulk.enabled": "false",
    }

    if TC:  # ThroughputControl
        base_cosmosconfig["spark.cosmos.throughputControl.enabled"] = "true"
        base_cosmosconfig["spark.cosmos.throughputControl.name"] = (
            "IntelRecsThroughputControl"
        )
        # base_cosmosconfig["spark.cosmos.throughputControl.targetThroughputThreshold"] = "0.2" # This isn't working/not appropriate if we aren't autoscaling
        # It is sensible to allow 20% overhead in terms of RUs
        # Reads are quite cheap in terms of RU usage, upserts cost more due to needing an internal read
        # The targetThroughput here will double in terms of the effective RUs when running upserts - upsert is a read + write operation, so double the operations of append
        base_cosmosconfig[
            "spark.cosmos.throughputControl.targetThroughput"
        ] = "30000"
        base_cosmosconfig[
            "spark.cosmos.throughputControl.globalControl.database"
        ] = db_name
        base_cosmosconfig[
            "spark.cosmos.throughputControl.globalControl.container"
        ] = "ThroughputControl"

    if not gateway_mode:
        base_cosmosconfig["spark.cosmos.useGatewayMode"] = "false"

    if ctype == "read":
        cosmosconfig_read = base_cosmosconfig.copy()
        cosmosconfig_read["spark.cosmos.read.inferSchema.enabled"] = "true"
        return cosmosconfig_read
    elif ctype == "upsert":
        cosmosconfig_upsert = base_cosmosconfig.copy()
        cosmosconfig_upsert["spark.cosmos.read.inferSchema.enabled"] = "true"
        cosmosconfig_upsert["spark.cosmos.write.strategy"] = "ItemOverwrite"
        return cosmosconfig_upsert
    elif ctype == "delete":
        cosmosconfig_delete = base_cosmosconfig.copy()
        cosmosconfig_delete["spark.cosmos.read.inferSchema.enabled"] = "true"
        cosmosconfig_delete["spark.cosmos.write.strategy"] = "ItemDelete"
        return cosmosconfig_delete
    elif ctype == "default_upsert":
        cosmosconfig_default_upsert = base_cosmosconfig.copy()
        cosmosconfig_default_upsert[
            "spark.cosmos.read.inferSchema.enabled"
        ] = "true"
        cosmosconfig_default_upsert["spark.cosmos.write.strategy"] = (
            "ItemOverwrite"
        )
        return cosmosconfig_default_upsert
    else:
        raise Exception("ctype must be read, upsert, delete or default_upsert")


def sdk_write_to_cosmos(config, env, dataframe):
    """Write a Spark DataFrame to Azure Cosmos DB using the Cosmos SDK.

    This function authenticates to Azure Cosmos DB using ClientSecretCredential,
    collects the DataFrame to the driver, and upserts all documents to the
    specified container. The client connection is closed after the operation.

    Args:
        COSMOS_ENDPOINT (str): The connection endpoint URL for the Cosmos DB account.
        DATABASE_NAME (str): The name of the target database in Cosmos DB.
        CONTAINER_NAME (str): The name of the target container in the database.
        dataframe (pyspark.sql.DataFrame): The Spark DataFrame to write to Cosmos DB.
            Each row will be converted to a document and upserted.

    Raises:
        Exception: If an error occurs during the upsert operation. The exception
            is logged and re-raised.

    Returns:
        None

    Note:
        - Authentication is performed using Azure ClientSecretCredential.
        - All DataFrame rows are collected to the driver memory before writing.
        - Documents are upserted individually; use with caution for large DataFrames.
        - Logs the number of documents successfully upserted.
    """
    logger.info("Starting Cosmos SDK write workflow")
    dbutils = get_dbutils()

    logger.info("Resolving Cosmos tenant/client credentials")
    TENANT_ID = _resolve_tenant_id(dbutils, config, env)

    CLIENT_ID = dbutils.secrets.get(
        config.dbutils_secret_scope, config.secret_key_spn_clientid
    )
    CLIENT_SECRET = dbutils.secrets.get(
        config.dbutils_secret_scope, config.secret_key_spn_secret
    )

    credential = ClientSecretCredential(
        tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
    )

    logger.info(f"Creating Cosmos client for endpoint {config.cosmos_url}")
    client = CosmosClient(url=config.cosmos_url, credential=credential)

    try:
        logger.info("Checking Cosmos account connectivity")
        next(client.list_databases(max_item_count=1), None)
        logger.info("Cosmos account connectivity check passed")

        logger.info(f"Checking database access: {config.cosmos_database}")
        database_client = client.get_database_client(config.cosmos_database)
        database_client.read()
        logger.info("Database check passed")

        logger.info(f"Checking container access: {config.cosmos_container}")
        container = database_client.get_container_client(
            config.cosmos_container
        )
        container.read()
        logger.info("Container check passed")

        logger.info("Collecting dataframe records for SDK upsert")
        documents = [
            row.asDict(recursive=True) for row in dataframe.collect()
        ]
        logger.info(f"Collected {len(documents)} documents")

        # Upsert documents
        logger.info("Starting document upserts")
        for doc in documents:
            container.upsert_item(doc)
        logger.info(
            f"Upserted {len(documents)} documents using Cosmos Python SDK"
        )
    except CosmosHttpResponseError as e:
        _raise_cosmos_error_with_context("sdk_write_to_cosmos", e, config)
    except Exception as e:
        logger.error(f"Error writing to Cosmos DB: {e}")
        raise
    finally:
        client.close()

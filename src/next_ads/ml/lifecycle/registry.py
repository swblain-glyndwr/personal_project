def configure_mlflow(mlflow_module) -> None:
    mlflow_module.set_tracking_uri("databricks")
    mlflow_module.set_registry_uri("databricks-uc")


def model_uri_for_alias(registered_model_name: str, alias: str) -> str:
    return f"models:/{registered_model_name}@{alias}"


def model_uri_for_version(registered_model_name: str, version) -> str:
    return f"models:/{registered_model_name}/{version}"


def set_model_alias(mlflow_client, registered_model_name: str, version, alias: str):
    mlflow_client.set_registered_model_alias(
        name=registered_model_name,
        alias=alias,
        version=str(version),
    )


def copy_model_alias_to_registered_model(
    mlflow_module,
    source_registered_model_name: str,
    source_alias: str,
    target_registered_model_name: str,
    target_alias: str,
):
    source_uri = model_uri_for_alias(source_registered_model_name, source_alias)
    registered_model = mlflow_module.register_model(
        model_uri=source_uri,
        name=target_registered_model_name,
    )
    client = mlflow_module.tracking.MlflowClient()
    set_model_alias(
        client,
        target_registered_model_name,
        registered_model.version,
        target_alias,
    )
    return registered_model


def copy_model_version_to_registered_model(
    mlflow_module,
    source_registered_model_name: str,
    source_version,
    target_registered_model_name: str,
    target_alias: str,
):
    source_uri = model_uri_for_version(
        source_registered_model_name,
        source_version,
    )
    registered_model = mlflow_module.register_model(
        model_uri=source_uri,
        name=target_registered_model_name,
    )
    client = mlflow_module.tracking.MlflowClient()
    set_model_alias(
        client,
        target_registered_model_name,
        registered_model.version,
        target_alias,
    )
    client.set_model_version_tag(
        name=target_registered_model_name,
        version=str(registered_model.version),
        key="source_registered_model_name",
        value=source_registered_model_name,
    )
    client.set_model_version_tag(
        name=target_registered_model_name,
        version=str(registered_model.version),
        key="source_model_version",
        value=str(source_version),
    )
    return registered_model

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


def resolve_model_version_for_alias(
    mlflow_client,
    registered_model_name: str,
    alias: str,
) -> str:
    model_version = mlflow_client.get_model_version_by_alias(
        name=registered_model_name,
        alias=alias,
    )
    return str(model_version.version)


def _already_exists(error: Exception) -> bool:
    return str(getattr(error, "error_code", "")).upper() in {
        "ALREADY_EXISTS",
        "RESOURCE_ALREADY_EXISTS",
    }


def _ensure_registered_model(mlflow_client, registered_model_name: str) -> None:
    try:
        mlflow_client.create_registered_model(registered_model_name)
    except Exception as error:
        if not _already_exists(error):
            raise


def _copy_exact_model_version(
    mlflow_client,
    *,
    source_uri: str,
    source_model_version,
    target_registered_model_name: str,
):
    """Copy an exact artifact without relying on a logged-model ID."""
    _ensure_registered_model(mlflow_client, target_registered_model_name)
    return mlflow_client.create_model_version(
        name=target_registered_model_name,
        source=source_uri,
        run_id=getattr(source_model_version, "run_id", None) or None,
        tags=dict(getattr(source_model_version, "tags", {}) or {}),
        run_link=None,
        description=getattr(source_model_version, "description", None),
        model_id=None,
    )


def copy_model_alias_to_registered_model(
    mlflow_module,
    source_registered_model_name: str,
    source_alias: str,
    target_registered_model_name: str,
    target_alias: str,
):
    client = mlflow_module.tracking.MlflowClient()
    source_model_version = client.get_model_version_by_alias(
        name=source_registered_model_name,
        alias=source_alias,
    )
    source_uri = model_uri_for_version(
        source_model_version.name,
        source_model_version.version,
    )
    registered_model = _copy_exact_model_version(
        client,
        source_uri=source_uri,
        source_model_version=source_model_version,
        target_registered_model_name=target_registered_model_name,
    )
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
    client = mlflow_module.tracking.MlflowClient()
    source_model_version = client.get_model_version(
        name=source_registered_model_name,
        version=str(source_version),
    )
    source_uri = model_uri_for_version(
        source_model_version.name,
        source_model_version.version,
    )
    registered_model = _copy_exact_model_version(
        client,
        source_uri=source_uri,
        source_model_version=source_model_version,
        target_registered_model_name=target_registered_model_name,
    )
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

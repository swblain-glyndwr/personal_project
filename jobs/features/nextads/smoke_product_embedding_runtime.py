import json
from pathlib import Path
import sys

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
except NameError:
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
    PROJECT_ROOT = Path(notebook_path).parents[3]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger

from next_ads.features.embedding_contract import (
    load_approved_dev_smoke_binding,
    load_product_embedding_definition,
)
from next_ads.features.embedding_runtime import (
    installed_package_versions,
    load_approved_sentence_transformer,
    resolve_runtime_version,
    validate_cpu_torch,
    validate_embedding_vector,
    validate_runtime_environment,
    validate_threadpool_runtime,
)


SMOKE_MANIFEST_PREFIX = "PRODUCT_EMBEDDING_RUNTIME_SMOKE="
SMOKE_TEXT = "black running trainers womens footwear"


def run_smoke(
    spark,
    *,
    mlflow_module=None,
    torch_module=None,
    threadpoolctl_module=None,
    model_loader=None,
):
    if mlflow_module is None:
        import mlflow as mlflow_module
    if torch_module is None:
        import torch as torch_module
    if threadpoolctl_module is None:
        import threadpoolctl as threadpoolctl_module
    if model_loader is None:
        model_loader = load_approved_sentence_transformer

    definition = load_product_embedding_definition()
    binding = load_approved_dev_smoke_binding()

    runtime_version = resolve_runtime_version(spark)
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    expected_packages = dict(definition.runtime_profile.package_versions)
    installed_packages = installed_package_versions(expected_packages)
    validate_runtime_environment(
        definition,
        runtime_version=runtime_version,
        python_version=python_version,
        package_versions=installed_packages,
    )
    cpu_evidence = validate_cpu_torch(torch_module)
    threadpool_evidence = validate_threadpool_runtime(threadpoolctl_module)

    mlflow_module.set_registry_uri("databricks-uc")
    model, model_evidence = model_loader(
        mlflow_module,
        binding,
        definition,
    )
    model_dimension = int(model.get_sentence_embedding_dimension())
    if model_dimension != definition.embedding_dimension:
        raise ValueError(
            "Registered embedding model dimension must be "
            f"{definition.embedding_dimension}; found {model_dimension}"
        )
    encoded = model.encode(
        [SMOKE_TEXT],
        batch_size=1,
        normalize_embeddings=definition.normalize_embeddings,
        show_progress_bar=False,
    )
    vector_dimension, vector_norm = validate_embedding_vector(
        encoded[0],
        definition,
    )
    return {
        "status": "PASS",
        "contract_version": definition.contract_version,
        "source_model_name": definition.source_model_name,
        "model_uri": binding.model.model_uri,
        "registered_model_version": binding.model.registered_model_version,
        "runtime_version": runtime_version,
        "python_version": python_version,
        "expected_runtime_engine": definition.runtime_profile.runtime_engine,
        "expected_accelerator": definition.runtime_profile.accelerator,
        "package_versions": installed_packages,
        "embedding_dimension": vector_dimension,
        "embedding_norm": vector_norm,
        "normalize_embeddings": definition.normalize_embeddings,
        "allow_runtime_registration": (
            definition.allow_runtime_registration
        ),
        **cpu_evidence,
        **threadpool_evidence,
        **model_evidence,
        "writes_performed": False,
    }


def main():
    logger = get_logger(__name__)
    spark = configure_spark()
    logger.info("Running read-only product-embedding runtime smoke")
    manifest = run_smoke(spark)
    logger.info(
        "%s%s",
        SMOKE_MANIFEST_PREFIX,
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
    )
    logger.info(
        "Product-embedding runtime smoke passed without altering tables or "
        "model aliases"
    )
    return manifest


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    LOG_LEVEL = jobparser.get_arg("--log_level")
    configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()

    main()

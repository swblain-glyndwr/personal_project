import importlib.metadata
import hashlib
import json
import os
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


SMOKE_MANIFEST_PREFIX = "PRODUCT_EMBEDDING_RUNTIME_SMOKE="
SMOKE_TEXT = "black running trainers womens footwear"
EXPECTED_SENTENCE_TRANSFORMER_MODULES = [
    {
        "idx": 0,
        "name": "0",
        "path": "",
        "type": "sentence_transformers.models.Transformer",
    },
    {
        "idx": 1,
        "name": "1",
        "path": "1_Pooling",
        "type": "sentence_transformers.models.Pooling",
    },
    {
        "idx": 2,
        "name": "2",
        "path": "2_Normalize",
        "type": "sentence_transformers.models.Normalize",
    },
]


def _runtime_version(spark) -> str:
    candidates = []
    try:
        candidates.append(
            spark.conf.get(
                "spark.databricks.clusterUsageTags.sparkVersion",
                "",
            )
        )
    except Exception:
        pass
    candidates.append(os.environ.get("DATABRICKS_RUNTIME_VERSION", ""))
    for candidate in candidates:
        value = str(candidate or "").strip()
        if value:
            return value
    raise ValueError("Could not determine the Databricks runtime version")


def _runtime_matches(actual: str, expected: str) -> bool:
    if actual == expected:
        return True
    expected_release = expected.split(".x-", 1)[0]
    return actual == expected_release or actual.startswith(
        expected_release + "."
    )


def _installed_package_versions(package_names):
    return {
        package_name: importlib.metadata.version(package_name)
        for package_name in package_names
    }


def validate_runtime_environment(
    definition,
    *,
    runtime_version: str,
    python_version: str,
    package_versions,
) -> None:
    profile = definition.runtime_profile
    if not _runtime_matches(runtime_version, profile.spark_version):
        raise ValueError(
            "Product embedding runtime must use "
            f"{profile.spark_version}; found {runtime_version}"
        )
    if python_version != profile.python_version:
        raise ValueError(
            "Product embedding runtime must use Python "
            f"{profile.python_version}; found {python_version}"
        )

    expected_packages = dict(profile.package_versions)
    mismatches = []
    for package_name, expected_version in expected_packages.items():
        actual_version = package_versions.get(package_name)
        if actual_version != expected_version:
            mismatches.append(
                f"{package_name}: expected {expected_version}, "
                f"found {actual_version or 'missing'}"
            )
    if mismatches:
        raise ValueError(
            "Product embedding dependency versions do not match: "
            + "; ".join(mismatches)
        )


def validate_cpu_torch(torch_module) -> dict[str, object]:
    cuda_version = getattr(torch_module.version, "cuda", None)
    cuda_available = bool(torch_module.cuda.is_available())
    if cuda_version is not None or cuda_available:
        raise ValueError(
            "Product embedding runtime requires a CPU-only Torch build; "
            f"torch.version.cuda={cuda_version!r}, "
            f"torch.cuda.is_available()={cuda_available}"
        )
    return {
        "observed_accelerator": "CPU",
        "torch_cuda_version": None,
        "torch_cuda_available": False,
    }


def validate_model_metadata(metadata, definition) -> dict[str, object]:
    values = dict(metadata or {})
    source_model_name = str(values.get("source_model_name", "")).strip()
    if source_model_name != definition.source_model_name:
        raise ValueError(
            "Registered embedding model source_model_name must be "
            f"{definition.source_model_name}; found "
            f"{source_model_name or 'missing'}"
        )
    normalized_value = values.get("normalised_embeddings")
    if isinstance(normalized_value, str):
        normalized_value = normalized_value.strip().lower() == "true"
    if normalized_value is not True:
        raise ValueError(
            "Registered embedding model metadata must declare "
            "normalised_embeddings=true"
        )
    return {
        "model_metadata_source_model_name": source_model_name,
        "model_metadata_normalised_embeddings": True,
    }


def validate_model_version_provenance(version_info, binding) -> None:
    actual_run_id = str(getattr(version_info, "run_id", "") or "")
    if actual_run_id != binding.source_run_id:
        raise ValueError(
            "Registered embedding model run ID does not match the approved "
            f"source: expected {binding.source_run_id}, found "
            f"{actual_run_id or 'missing'}"
        )
    actual_name = str(getattr(version_info, "name", "") or "")
    actual_version = str(getattr(version_info, "version", "") or "")
    if actual_name != binding.model.registered_model_name or actual_version != str(
        binding.model.registered_model_version
    ):
        raise ValueError(
            "Registered embedding model metadata does not match the approved "
            "name and version"
        )


def _json_without_duplicate_keys(path: Path):
    def reject_duplicate_keys(pairs):
        values = {}
        for key, value in pairs:
            if key in values:
                raise ValueError(
                    f"JSON artifact contains duplicate key {key!r}: {path}"
                )
            values[key] = value
        return values

    try:
        return json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=reject_duplicate_keys,
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not safely parse JSON artifact: {path}") from exc


def validate_sentence_transformer_module_graph(model_data_path: Path) -> None:
    modules_path = model_data_path / "modules.json"
    if not modules_path.is_file():
        raise ValueError(
            "Registered embedding model is missing modules.json"
        )
    modules = _json_without_duplicate_keys(modules_path)
    if modules != EXPECTED_SENTENCE_TRANSFORMER_MODULES:
        raise ValueError(
            "Registered embedding model modules.json must contain only the "
            "approved Transformer, Pooling, and Normalize graph"
        )
    for module in modules:
        module_path = str(module["path"])
        if not module_path:
            continue
        resolved_module_path = (model_data_path / module_path).resolve()
        if model_data_path not in resolved_module_path.parents:
            raise ValueError(
                "SentenceTransformer module path resolves outside the "
                "approved artifact"
            )


def _artifact_sha256(model_data_path: Path, files: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in sorted(files):
        relative_path = path.relative_to(model_data_path).as_posix()
        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as artifact_file:
            for chunk in iter(lambda: artifact_file.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def validate_safe_model_artifacts(model_data_path: Path) -> dict[str, object]:
    if not model_data_path.is_dir():
        raise ValueError(
            f"SentenceTransformer model data is missing: {model_data_path}"
        )
    entries = list(model_data_path.rglob("*"))
    symbolic_links = sorted(
        str(path.relative_to(model_data_path))
        for path in entries
        if path.is_symlink()
    )
    if symbolic_links:
        raise ValueError(
            "Registered embedding model contains symbolic links: "
            + ", ".join(symbolic_links)
        )
    files = [path for path in entries if path.is_file()]
    unsafe_suffixes = {
        ".bat",
        ".bin",
        ".cmd",
        ".dll",
        ".dylib",
        ".egg",
        ".exe",
        ".joblib",
        ".jar",
        ".pickle",
        ".pkl",
        ".ps1",
        ".pt",
        ".pth",
        ".py",
        ".pyc",
        ".pyd",
        ".pyo",
        ".sh",
        ".so",
        ".whl",
    }
    unsafe_files = sorted(
        str(path.relative_to(model_data_path))
        for path in files
        if path.suffix.lower() in unsafe_suffixes
    )
    if unsafe_files:
        raise ValueError(
            "Registered embedding model contains disallowed executable or "
            "pickle-based artifacts: "
            + ", ".join(unsafe_files)
        )
    safetensor_count = sum(
        path.suffix.lower() == ".safetensors" for path in files
    )
    if safetensor_count <= 0:
        raise ValueError(
            "Registered embedding model must contain safetensors weights"
        )
    validate_sentence_transformer_module_graph(model_data_path)
    return {
        "artifact_file_count": len(files),
        "artifact_sha256": _artifact_sha256(model_data_path, files),
        "module_graph_verified": True,
        "safetensor_file_count": safetensor_count,
    }


def load_approved_sentence_transformer(
    mlflow_module,
    binding,
    definition,
):
    from sentence_transformers import SentenceTransformer

    client = mlflow_module.MlflowClient()
    version_info = client.get_model_version(
        name=binding.model.registered_model_name,
        version=str(binding.model.registered_model_version),
    )
    validate_model_version_provenance(version_info, binding)

    artifact_root = Path(
        mlflow_module.artifacts.download_artifacts(
            artifact_uri=binding.model.model_uri
        )
    ).resolve()
    model_info = mlflow_module.models.Model.load(str(artifact_root))
    metadata_evidence = validate_model_metadata(
        model_info.metadata,
        definition,
    )
    flavor = dict(model_info.flavors.get("sentence_transformers") or {})
    if not flavor:
        raise ValueError(
            "Registered model does not contain a SentenceTransformers flavor"
        )
    if flavor.get("code"):
        raise ValueError(
            "Registered embedding model must not contain custom model code"
        )
    model_data = str(flavor.get("model_data", "") or "").strip()
    if not model_data:
        raise ValueError(
            "SentenceTransformers flavor does not declare model_data"
        )
    model_data_path = (artifact_root / model_data).resolve()
    if artifact_root not in model_data_path.parents:
        raise ValueError(
            "SentenceTransformers model_data resolves outside the artifact"
        )
    artifact_evidence = validate_safe_model_artifacts(model_data_path)
    model = SentenceTransformer(
        str(model_data_path),
        device="cpu",
        trust_remote_code=False,
    )
    return model, {
        **metadata_evidence,
        **artifact_evidence,
        "approved_source_run_id": binding.source_run_id,
        "custom_model_code": False,
    }


def validate_embedding_vector(vector, definition) -> tuple[int, float]:
    import numpy as np

    values = np.asarray(vector, dtype="float64").reshape(-1)
    if values.size != definition.embedding_dimension:
        raise ValueError(
            "Product embedding dimension must be "
            f"{definition.embedding_dimension}; found {values.size}"
        )
    if not bool(np.isfinite(values).all()):
        raise ValueError("Product embedding contains a non-finite value")
    vector_norm = float(np.linalg.norm(values))
    if definition.normalize_embeddings and not bool(
        np.isclose(vector_norm, 1.0, rtol=1e-5, atol=1e-5)
    ):
        raise ValueError(
            "Product embedding must be L2-normalised; "
            f"found norm {vector_norm}"
        )
    return int(values.size), vector_norm


def run_smoke(
    spark,
    *,
    mlflow_module=None,
    torch_module=None,
    model_loader=None,
):
    if mlflow_module is None:
        import mlflow as mlflow_module
    if torch_module is None:
        import torch as torch_module
    if model_loader is None:
        model_loader = load_approved_sentence_transformer

    definition = load_product_embedding_definition()
    binding = load_approved_dev_smoke_binding()

    runtime_version = _runtime_version(spark)
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    expected_packages = dict(definition.runtime_profile.package_versions)
    installed_packages = _installed_package_versions(expected_packages)
    validate_runtime_environment(
        definition,
        runtime_version=runtime_version,
        python_version=python_version,
        package_versions=installed_packages,
    )
    cpu_evidence = validate_cpu_torch(torch_module)

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

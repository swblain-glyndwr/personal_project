"""Runtime and artifact validation for approved product embedding models."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path


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


def resolve_runtime_version(spark) -> str:
    """Resolve the active Databricks runtime version from Spark or the environment."""
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


def runtime_matches(actual: str, expected: str) -> bool:
    """Return whether an observed runtime matches the pinned DBR release."""
    if actual == expected:
        return True
    expected_release = expected.split(".x-", 1)[0]
    return actual == expected_release or actual.startswith(
        expected_release + "."
    )


def installed_package_versions(package_names) -> dict[str, str]:
    """Read installed versions for the packages required by the runtime contract."""
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
    """Require the exact DBR, Python, and dependency versions in the contract."""
    profile = definition.runtime_profile
    if not runtime_matches(runtime_version, profile.spark_version):
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
    """Require the CPU-only Torch build declared by the runtime profile."""
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


def validate_threadpool_runtime(threadpoolctl_module) -> dict[str, object]:
    """Confirm that native threadpool inspection works in the active runtime."""
    try:
        libraries = threadpoolctl_module.threadpool_info()
    except Exception as exc:
        raise ValueError(
            "Product embedding threadpool runtime inspection failed"
        ) from exc
    if not isinstance(libraries, list):
        raise ValueError(
            "Product embedding threadpool runtime inspection must return a "
            "list"
        )
    return {"threadpool_library_count": len(libraries)}


def validate_model_metadata(metadata, definition) -> dict[str, object]:
    """Validate the encoder identity and normalisation metadata."""
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
    """Require the registered version to match the approved immutable binding."""
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
    """Reject custom or path-traversing SentenceTransformer module graphs."""
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
    """Validate the artifact contents and return immutable digest evidence."""
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


def resolve_validated_sentence_transformer_artifact(
    mlflow_module,
    binding,
    definition,
) -> tuple[Path, dict[str, object]]:
    """Download and validate an approved model without constructing it."""
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
    sentence_transformers_flavor = dict(
        model_info.flavors.get("sentence_transformers") or {}
    )
    if not sentence_transformers_flavor:
        raise ValueError(
            "Registered model does not contain a SentenceTransformers flavor"
        )
    if sentence_transformers_flavor.get("code"):
        raise ValueError(
            "Registered embedding model must not contain custom model code"
        )
    logged_sentence_transformers_version = str(
        sentence_transformers_flavor.get(
            "sentence_transformers_version",
            "",
        )
        or ""
    ).strip()
    expected_sentence_transformers_version = dict(
        definition.runtime_profile.package_versions
    )["sentence-transformers"]
    if (
        logged_sentence_transformers_version
        != expected_sentence_transformers_version
    ):
        raise ValueError(
            "Registered model SentenceTransformers flavor version must be "
            f"{expected_sentence_transformers_version}; found "
            f"{logged_sentence_transformers_version or 'missing'}"
        )

    python_function_flavor = dict(
        model_info.flavors.get("python_function") or {}
    )
    if not python_function_flavor:
        raise ValueError(
            "Registered model does not contain a python_function flavor"
        )
    if python_function_flavor.get("code"):
        raise ValueError(
            "Registered embedding model python_function flavor must not "
            "contain custom model code"
        )
    loader_module = str(
        python_function_flavor.get("loader_module", "") or ""
    ).strip()
    if loader_module != "mlflow.sentence_transformers":
        raise ValueError(
            "Registered embedding model python_function loader_module must "
            "be mlflow.sentence_transformers"
        )
    model_data = str(
        python_function_flavor.get("data", "") or ""
    ).strip()
    if model_data != "model.sentence_transformer":
        raise ValueError(
            "Registered embedding model python_function data must be "
            "model.sentence_transformer"
        )
    model_data_path = (artifact_root / model_data).resolve()
    if artifact_root not in model_data_path.parents:
        raise ValueError(
            "SentenceTransformers model_data resolves outside the artifact"
        )
    artifact_evidence = validate_safe_model_artifacts(model_data_path)
    if artifact_evidence["artifact_sha256"] != binding.artifact_sha256:
        raise ValueError(
            "Registered embedding model artifact digest does not match the "
            f"approved SHA-256: expected {binding.artifact_sha256}, found "
            f"{artifact_evidence['artifact_sha256']}"
        )
    return model_data_path, {
        **metadata_evidence,
        **artifact_evidence,
        "approved_artifact_sha256": binding.artifact_sha256,
        "approved_source_run_id": binding.source_run_id,
        "custom_model_code": False,
    }


def load_approved_sentence_transformer(
    mlflow_module,
    binding,
    definition,
):
    """Download, validate, and load one approved SentenceTransformer version."""
    from sentence_transformers import SentenceTransformer

    model_data_path, evidence = resolve_validated_sentence_transformer_artifact(
        mlflow_module,
        binding,
        definition,
    )
    model = SentenceTransformer(
        str(model_data_path),
        device="cpu",
        trust_remote_code=False,
    )
    return model, evidence


def validate_embedding_vector(vector, definition) -> tuple[int, float]:
    """Require a finite vector with the contract dimension and L2 norm."""
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


__all__ = [
    "EXPECTED_SENTENCE_TRANSFORMER_MODULES",
    "installed_package_versions",
    "load_approved_sentence_transformer",
    "resolve_validated_sentence_transformer_artifact",
    "resolve_runtime_version",
    "runtime_matches",
    "validate_cpu_torch",
    "validate_embedding_vector",
    "validate_model_metadata",
    "validate_model_version_provenance",
    "validate_runtime_environment",
    "validate_safe_model_artifacts",
    "validate_sentence_transformer_module_graph",
    "validate_threadpool_runtime",
]

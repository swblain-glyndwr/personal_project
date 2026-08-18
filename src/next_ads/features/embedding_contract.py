"""Immutable runtime and model contracts for product embeddings."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Mapping

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PRODUCT_EMBEDDING_CONTRACT_PATH = (
    PROJECT_ROOT / "configs" / "features" / "product_embeddings.yaml"
)
DEFAULT_DEV_SMOKE_BINDING_PATH = (
    PROJECT_ROOT
    / "configs"
    / "features"
    / "product_embedding_smoke_dev.yaml"
)
DEFAULT_DEV_MATERIALIZATION_BINDING_PATH = (
    PROJECT_ROOT
    / "configs"
    / "features"
    / "product_embedding_materialization_dev.yaml"
)
DEFAULT_PERSONAL_DEV_MATERIALIZATION_BINDING_PATH = (
    PROJECT_ROOT
    / "configs"
    / "features"
    / "product_embedding_materialization_personal_dev.yaml"
)

EXPECTED_CONTRACT_VERSION = 1
EXPECTED_SOURCE_MODEL_NAME = "sentence-transformers/all-MiniLM-L12-v2"
EXPECTED_EMBEDDING_DIMENSION = 384
EXPECTED_SPARK_VERSION = "15.4.x-scala2.12"
EXPECTED_RUNTIME_ENGINE = "STANDARD"
EXPECTED_ACCELERATOR = "CPU"
EXPECTED_PYTHON_VERSION = "3.11"
EXPECTED_REQUIREMENTS_FILE = "requirements-feature-store-embeddings.txt"
EXPECTED_PIP_OPTIONS = (
    "--extra-index-url https://download.pytorch.org/whl/cpu",
)
EXPECTED_PACKAGE_VERSIONS = (
    ("mlflow", "3.11.1"),
    ("numpy", "1.26.4"),
    ("protobuf", "4.24.1"),
    ("sentence-transformers", "2.4.0"),
    ("threadpoolctl", "3.6.0"),
    ("torch", "2.10.0+cpu"),
    ("transformers", "4.41.2"),
)

_ROOT_FIELDS = {
    "contract_version",
    "source_model_name",
    "embedding_dimension",
    "normalize_embeddings",
    "allow_runtime_registration",
    "runtime_profile",
}
_RUNTIME_FIELDS = {
    "spark_version",
    "runtime_engine",
    "accelerator",
    "python_version",
    "requirements_file",
    "pip_options",
    "package_versions",
}
_SMOKE_BINDING_FIELDS = {
    "environment",
    "purpose",
    "registered_model_name",
    "registered_model_version",
    "model_uri",
    "source_run_id",
    "artifact_sha256",
}
_MATERIALIZATION_BINDING_FIELDS = {
    "environment",
    "purpose",
    "registered_model_name",
    "registered_model_version",
    "model_uri",
    "source_registered_model_name",
    "source_registered_model_version",
    "source_run_id",
    "artifact_sha256",
    "model_staging_root",
    "inference_partitions",
    "inference_batch_size",
}
_MODEL_URI_PATTERN = re.compile(
    r"models:/(?P<name>[^/@\s]+)/(?P<version>[1-9][0-9]*)"
)
_MODEL_VERSION_PATTERN = re.compile(r"[1-9][0-9]*")
_RUN_ID_PATTERN = re.compile(r"[0-9a-f]{32}")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


def _required_mapping(raw: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping):
        raise ValueError(f"{context} must be a mapping")
    return raw


def _require_exact_fields(
    raw: Mapping[str, Any],
    required_fields: set[str],
    context: str,
) -> None:
    missing = sorted(required_fields.difference(raw))
    unknown = sorted(set(raw).difference(required_fields))
    if missing:
        raise ValueError(
            f"{context} is missing required fields: {', '.join(missing)}"
        )
    if unknown:
        raise ValueError(
            f"{context} contains unsupported fields: {', '.join(unknown)}"
        )


def _required_text(raw: Any, field_name: str, context: str) -> str:
    if not isinstance(raw, str):
        raise ValueError(f"{context} {field_name} must be text")
    value = raw.strip()
    if not value:
        raise ValueError(f"{context} has an empty {field_name}")
    return value


def _require_value(
    actual: Any,
    expected: Any,
    field_name: str,
    context: str,
) -> None:
    if actual != expected:
        raise ValueError(
            f"{context} {field_name} must be {expected!r}, got {actual!r}"
        )


def _package_versions(raw: Any) -> tuple[tuple[str, str], ...]:
    packages = _required_mapping(
        raw,
        "Product embedding runtime package_versions",
    )
    values = []
    for name, version in packages.items():
        package_name = _required_text(
            name,
            "package name",
            "Product embedding runtime",
        )
        package_version = _required_text(
            version,
            f"version for {package_name}",
            "Product embedding runtime",
        )
        values.append((package_name, package_version))
    return tuple(sorted(values))


def _text_values(raw: Any, context: str) -> tuple[str, ...]:
    if not isinstance(raw, list):
        raise ValueError(f"{context} must be a list")
    return tuple(
        _required_text(value, "value", context)
        for value in raw
    )


def _read_exact_requirements(
    path: Path,
) -> tuple[tuple[tuple[str, str], ...], tuple[str, ...]]:
    if not path.is_file():
        raise ValueError(f"Embedding requirements file does not exist: {path}")

    packages: dict[str, str] = {}
    pip_options = []
    for line_number, raw_line in enumerate(
        path.read_text().splitlines(),
        start=1,
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("--"):
            pip_options.append(line)
            continue
        if line.count("==") != 1:
            raise ValueError(
                "Embedding requirements must use exact package pins; "
                f"invalid line {line_number}: {raw_line!r}"
            )
        name, version = (value.strip() for value in line.split("==", 1))
        if not name or not version:
            raise ValueError(
                "Embedding requirements contain an incomplete pin at line "
                f"{line_number}"
            )
        if name in packages:
            raise ValueError(
                f"Embedding requirements contain duplicate package {name}"
            )
        packages[name] = version
    return tuple(sorted(packages.items())), tuple(pip_options)


@dataclass(frozen=True)
class EmbeddingRuntimeProfile:
    """Supported DBR and dependency profile for embedding materialisation."""

    spark_version: str
    runtime_engine: str
    accelerator: str
    python_version: str
    requirements_file: str
    pip_options: tuple[str, ...]
    package_versions: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        """Prevent unsupported profiles even when constructed directly."""
        self._validate_supported_profile()

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "EmbeddingRuntimeProfile":
        """Build and validate the one supported product-embedding runtime."""
        values = _required_mapping(raw, "Product embedding runtime_profile")
        _require_exact_fields(
            values,
            _RUNTIME_FIELDS,
            "Product embedding runtime_profile",
        )
        profile = cls(
            spark_version=_required_text(
                values["spark_version"],
                "spark_version",
                "Product embedding runtime",
            ),
            runtime_engine=_required_text(
                values["runtime_engine"],
                "runtime_engine",
                "Product embedding runtime",
            ).upper(),
            accelerator=_required_text(
                values["accelerator"],
                "accelerator",
                "Product embedding runtime",
            ).upper(),
            python_version=_required_text(
                values["python_version"],
                "python_version",
                "Product embedding runtime",
            ),
            requirements_file=_required_text(
                values["requirements_file"],
                "requirements_file",
                "Product embedding runtime",
            ),
            pip_options=_text_values(
                values["pip_options"],
                "Product embedding runtime pip_options",
            ),
            package_versions=_package_versions(values["package_versions"]),
        )
        return profile

    def _validate_supported_profile(self) -> None:
        expected = {
            "spark_version": EXPECTED_SPARK_VERSION,
            "runtime_engine": EXPECTED_RUNTIME_ENGINE,
            "accelerator": EXPECTED_ACCELERATOR,
            "python_version": EXPECTED_PYTHON_VERSION,
            "requirements_file": EXPECTED_REQUIREMENTS_FILE,
            "pip_options": EXPECTED_PIP_OPTIONS,
            "package_versions": EXPECTED_PACKAGE_VERSIONS,
        }
        for field_name, expected_value in expected.items():
            _require_value(
                getattr(self, field_name),
                expected_value,
                field_name,
                "Product embedding runtime",
            )


@dataclass(frozen=True)
class ProductEmbeddingDefinition:
    """Environment-neutral encoder and vector-shape definition."""

    contract_version: int
    source_model_name: str
    embedding_dimension: int
    normalize_embeddings: bool
    allow_runtime_registration: bool
    runtime_profile: EmbeddingRuntimeProfile

    def __post_init__(self) -> None:
        """Prevent inconsistent definitions outside the YAML loader."""
        if isinstance(self.contract_version, bool) or not isinstance(
            self.contract_version, int
        ):
            raise ValueError(
                "Product embedding definition contract_version must be an "
                "integer"
            )
        if isinstance(self.embedding_dimension, bool) or not isinstance(
            self.embedding_dimension, int
        ):
            raise ValueError(
                "Product embedding definition embedding_dimension must be an "
                "integer"
            )
        if not isinstance(self.normalize_embeddings, bool):
            raise ValueError(
                "Product embedding definition normalize_embeddings must be a "
                "boolean"
            )
        if not isinstance(self.allow_runtime_registration, bool):
            raise ValueError(
                "Product embedding definition allow_runtime_registration must "
                "be a boolean"
            )
        if not isinstance(self.runtime_profile, EmbeddingRuntimeProfile):
            raise ValueError(
                "Product embedding definition runtime_profile must be an "
                "EmbeddingRuntimeProfile"
            )
        self._validate_supported_definition()

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ProductEmbeddingDefinition":
        """Build the logical definition and reject unsupported drift."""
        values = _required_mapping(raw, "Product embedding definition")
        _require_exact_fields(
            values,
            _ROOT_FIELDS,
            "Product embedding definition",
        )
        if isinstance(values["contract_version"], bool) or not isinstance(
            values["contract_version"], int
        ):
            raise ValueError(
                "Product embedding definition contract_version must be an "
                "integer"
            )
        if isinstance(values["embedding_dimension"], bool) or not isinstance(
            values["embedding_dimension"], int
        ):
            raise ValueError(
                "Product embedding definition embedding_dimension must be an "
                "integer"
            )
        for field_name in (
            "normalize_embeddings",
            "allow_runtime_registration",
        ):
            if not isinstance(values[field_name], bool):
                raise ValueError(
                    f"Product embedding definition {field_name} must be a "
                    "boolean"
                )

        definition = cls(
            contract_version=values["contract_version"],
            source_model_name=_required_text(
                values["source_model_name"],
                "source_model_name",
                "Product embedding definition",
            ),
            embedding_dimension=values["embedding_dimension"],
            normalize_embeddings=values["normalize_embeddings"],
            allow_runtime_registration=values["allow_runtime_registration"],
            runtime_profile=EmbeddingRuntimeProfile.from_dict(
                values["runtime_profile"]
            ),
        )
        return definition

    def _validate_supported_definition(self) -> None:
        expected = {
            "contract_version": EXPECTED_CONTRACT_VERSION,
            "source_model_name": EXPECTED_SOURCE_MODEL_NAME,
            "embedding_dimension": EXPECTED_EMBEDDING_DIMENSION,
            "normalize_embeddings": True,
            "allow_runtime_registration": False,
        }
        for field_name, expected_value in expected.items():
            _require_value(
                getattr(self, field_name),
                expected_value,
                field_name,
                "Product embedding definition",
            )


@dataclass(frozen=True)
class EmbeddingModelBinding:
    """Exact registered MLflow model version supplied by an environment."""

    registered_model_name: str
    registered_model_version: int
    model_uri: str

    def __post_init__(self) -> None:
        """Reject aliases and inconsistent name, version, or URI values."""
        name = _required_text(
            self.registered_model_name,
            "registered_model_name",
            "Embedding model binding",
        )
        if (
            "/" in name
            or "@" in name
            or any(character.isspace() for character in name)
        ):
            raise ValueError(
                "Embedding model binding registered_model_name cannot contain "
                "slashes, aliases, or whitespace"
            )
        name_parts = name.split(".")
        if len(name_parts) != 3 or any(not part for part in name_parts):
            raise ValueError(
                "Embedding model binding registered_model_name must be a "
                "three-part Unity Catalog name"
            )

        raw_version = self.registered_model_version
        if isinstance(raw_version, bool):
            version_text = ""
        elif isinstance(raw_version, int):
            version_text = str(raw_version)
        elif isinstance(raw_version, str):
            version_text = raw_version.strip()
        else:
            version_text = ""
        if not _MODEL_VERSION_PATTERN.fullmatch(version_text):
            raise ValueError(
                "Embedding model binding registered_model_version must be a "
                "positive numeric version"
            )

        model_uri = _required_text(
            self.model_uri,
            "model_uri",
            "Embedding model binding",
        )
        if "@" in model_uri:
            raise ValueError(
                "Embedding model binding model_uri cannot use an MLflow alias"
            )
        match = _MODEL_URI_PATTERN.fullmatch(model_uri)
        if match is None:
            raise ValueError(
                "Embedding model binding model_uri must use exact numeric "
                "MLflow form models:/<registered-model>/<version>"
            )
        if match.group("name") != name:
            raise ValueError(
                "Embedding model binding model_uri does not match "
                "registered_model_name"
            )
        if match.group("version") != version_text:
            raise ValueError(
                "Embedding model binding model_uri does not match "
                "registered_model_version"
            )

        object.__setattr__(self, "registered_model_name", name)
        object.__setattr__(self, "registered_model_version", int(version_text))
        object.__setattr__(self, "model_uri", model_uri)


@dataclass(frozen=True)
class ApprovedEmbeddingSmokeBinding:
    """Fixed DEV evidence binding that a CAN_RUN caller cannot replace."""

    environment: str
    purpose: str
    model: EmbeddingModelBinding
    source_run_id: str
    artifact_sha256: str

    def __post_init__(self) -> None:
        """Prevent direct construction from bypassing the approved binding."""
        environment = _required_text(
            self.environment,
            "environment",
            "Product embedding smoke binding",
        )
        purpose = _required_text(
            self.purpose,
            "purpose",
            "Product embedding smoke binding",
        )
        _require_value(
            environment,
            "DEV",
            "environment",
            "Product embedding smoke binding",
        )
        _require_value(
            purpose,
            "compatibility_smoke_only",
            "purpose",
            "Product embedding smoke binding",
        )
        if not isinstance(self.model, EmbeddingModelBinding):
            raise ValueError(
                "Product embedding smoke binding model must be an "
                "EmbeddingModelBinding"
            )
        source_run_id = _required_text(
            self.source_run_id,
            "source_run_id",
            "Product embedding smoke binding",
        )
        if _RUN_ID_PATTERN.fullmatch(source_run_id) is None:
            raise ValueError(
                "Product embedding smoke binding source_run_id must be a "
                "32-character lowercase hexadecimal MLflow run ID"
            )
        artifact_sha256 = _required_text(
            self.artifact_sha256,
            "artifact_sha256",
            "Product embedding smoke binding",
        )
        if _SHA256_PATTERN.fullmatch(artifact_sha256) is None:
            raise ValueError(
                "Product embedding smoke binding artifact_sha256 must be a "
                "64-character lowercase hexadecimal SHA-256 digest"
            )
        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "purpose", purpose)
        object.__setattr__(self, "source_run_id", source_run_id)
        object.__setattr__(self, "artifact_sha256", artifact_sha256)

    @classmethod
    def from_dict(
        cls,
        raw: Mapping[str, Any],
    ) -> "ApprovedEmbeddingSmokeBinding":
        values = _required_mapping(raw, "Product embedding smoke binding")
        _require_exact_fields(
            values,
            _SMOKE_BINDING_FIELDS,
            "Product embedding smoke binding",
        )
        environment = _required_text(
            values["environment"],
            "environment",
            "Product embedding smoke binding",
        )
        purpose = _required_text(
            values["purpose"],
            "purpose",
            "Product embedding smoke binding",
        )
        _require_value(
            environment,
            "DEV",
            "environment",
            "Product embedding smoke binding",
        )
        _require_value(
            purpose,
            "compatibility_smoke_only",
            "purpose",
            "Product embedding smoke binding",
        )
        source_run_id = _required_text(
            values["source_run_id"],
            "source_run_id",
            "Product embedding smoke binding",
        )
        if _RUN_ID_PATTERN.fullmatch(source_run_id) is None:
            raise ValueError(
                "Product embedding smoke binding source_run_id must be a "
                "32-character lowercase hexadecimal MLflow run ID"
            )
        artifact_sha256 = _required_text(
            values["artifact_sha256"],
            "artifact_sha256",
            "Product embedding smoke binding",
        )
        if _SHA256_PATTERN.fullmatch(artifact_sha256) is None:
            raise ValueError(
                "Product embedding smoke binding artifact_sha256 must be a "
                "64-character lowercase hexadecimal SHA-256 digest"
            )
        model = EmbeddingModelBinding(
            registered_model_name=values["registered_model_name"],
            registered_model_version=values["registered_model_version"],
            model_uri=values["model_uri"],
        )
        return cls(
            environment=environment,
            purpose=purpose,
            model=model,
            source_run_id=source_run_id,
            artifact_sha256=artifact_sha256,
        )


@dataclass(frozen=True)
class ProductEmbeddingMaterializationBinding:
    """Exact model artifact approved for one DEV materialisation route."""

    environment: str
    purpose: str
    model: EmbeddingModelBinding
    source_registered_model_name: str
    source_registered_model_version: int
    source_run_id: str
    artifact_sha256: str
    model_staging_root: str
    inference_partitions: int
    inference_batch_size: int

    def __post_init__(self) -> None:
        """Reject ambiguous targets, mutable versions, and unsafe paths."""
        environment = _required_text(
            self.environment,
            "environment",
            "Product embedding materialization binding",
        )
        purpose = _required_text(
            self.purpose,
            "purpose",
            "Product embedding materialization binding",
        )
        _require_value(
            environment,
            "DEV",
            "environment",
            "Product embedding materialization binding",
        )
        if purpose not in {
            "personal_dev_validation",
            "shared_dev_materialization",
        }:
            raise ValueError(
                "Product embedding materialization binding purpose must be "
                "personal_dev_validation or shared_dev_materialization"
            )
        if not isinstance(self.model, EmbeddingModelBinding):
            raise ValueError(
                "Product embedding materialization binding model must "
                "be an EmbeddingModelBinding"
            )
        source_name = _required_text(
            self.source_registered_model_name,
            "source_registered_model_name",
            "Product embedding materialization binding",
        )
        raw_source_version = self.source_registered_model_version
        if isinstance(raw_source_version, bool):
            source_version_text = ""
        elif isinstance(raw_source_version, int):
            source_version_text = str(raw_source_version)
        elif isinstance(raw_source_version, str):
            source_version_text = raw_source_version.strip()
        else:
            source_version_text = ""
        if _MODEL_VERSION_PATTERN.fullmatch(source_version_text) is None:
            raise ValueError(
                "Product embedding materialization binding "
                "source_registered_model_version must be a positive numeric "
                "version"
            )
        source_version = int(source_version_text)
        if purpose == "shared_dev_materialization":
            if not self.model.registered_model_name.startswith(
                "marketingdata_dev.nextads_integration."
            ):
                raise ValueError(
                    "Shared product embedding materialization model must be "
                    "in marketingdata_dev.nextads_integration"
                )
        elif (
            self.model.registered_model_name != source_name
            or self.model.registered_model_version != source_version
        ):
            raise ValueError(
                "Personal DEV materialization must use the exact approved "
                "source model name and version"
            )

        source_run_id = _required_text(
            self.source_run_id,
            "source_run_id",
            "Product embedding materialization binding",
        )
        if _RUN_ID_PATTERN.fullmatch(source_run_id) is None:
            raise ValueError(
                "Product embedding materialization binding source_run_id must "
                "be a 32-character lowercase hexadecimal MLflow run ID"
            )

        artifact_sha256 = _required_text(
            self.artifact_sha256,
            "artifact_sha256",
            "Product embedding materialization binding",
        )
        if _SHA256_PATTERN.fullmatch(artifact_sha256) is None:
            raise ValueError(
                "Product embedding materialization binding artifact_sha256 "
                "must be a 64-character lowercase hexadecimal SHA-256 digest"
            )

        staging_root = _required_text(
            self.model_staging_root,
            "model_staging_root",
            "Product embedding materialization binding",
        ).rstrip("/")
        if not staging_root.startswith("/Volumes/") or ".." in Path(
            staging_root
        ).parts:
            raise ValueError(
                "Product embedding materialization binding "
                "model_staging_root must be an absolute Unity Catalog Volume "
                "path"
            )

        for field_name, maximum in (
            ("inference_partitions", 64),
            ("inference_batch_size", 1024),
        ):
            value = getattr(self, field_name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value <= 0
                or value > maximum
            ):
                raise ValueError(
                    "Product embedding materialization binding "
                    f"{field_name} must be between 1 and {maximum}"
                )

        object.__setattr__(self, "environment", environment)
        object.__setattr__(self, "purpose", purpose)
        object.__setattr__(
            self,
            "source_registered_model_name",
            source_name,
        )
        object.__setattr__(
            self,
            "source_registered_model_version",
            source_version,
        )
        object.__setattr__(self, "source_run_id", source_run_id)
        object.__setattr__(self, "artifact_sha256", artifact_sha256)
        object.__setattr__(self, "model_staging_root", staging_root)

    @classmethod
    def from_dict(
        cls,
        raw: Mapping[str, Any],
    ) -> "ProductEmbeddingMaterializationBinding":
        values = _required_mapping(
            raw,
            "Product embedding materialization binding",
        )
        _require_exact_fields(
            values,
            _MATERIALIZATION_BINDING_FIELDS,
            "Product embedding materialization binding",
        )
        model = EmbeddingModelBinding(
            registered_model_name=values["registered_model_name"],
            registered_model_version=values["registered_model_version"],
            model_uri=values["model_uri"],
        )
        return cls(
            environment=values["environment"],
            purpose=values["purpose"],
            model=model,
            source_registered_model_name=values[
                "source_registered_model_name"
            ],
            source_registered_model_version=values[
                "source_registered_model_version"
            ],
            source_run_id=values["source_run_id"],
            artifact_sha256=values["artifact_sha256"],
            model_staging_root=values["model_staging_root"],
            inference_partitions=values["inference_partitions"],
            inference_batch_size=values["inference_batch_size"],
        )


def load_product_embedding_definition(
    path: str | Path | None = None,
    *,
    requirements_path: str | Path | None = None,
) -> ProductEmbeddingDefinition:
    """Load the logical contract and verify its exact requirements pins."""
    contract_path = (
        Path(path)
        if path is not None
        else DEFAULT_PRODUCT_EMBEDDING_CONTRACT_PATH
    )
    raw_document = yaml.safe_load(contract_path.read_text())
    document = _required_mapping(raw_document, "Product embedding document")
    _require_exact_fields(
        document,
        {"product_embeddings"},
        "Product embedding document",
    )
    definition = ProductEmbeddingDefinition.from_dict(
        document["product_embeddings"]
    )

    resolved_requirements_path = (
        Path(requirements_path)
        if requirements_path is not None
        else PROJECT_ROOT / definition.runtime_profile.requirements_file
    )
    requirement_pins, pip_options = _read_exact_requirements(
        resolved_requirements_path
    )
    if requirement_pins != definition.runtime_profile.package_versions:
        raise ValueError(
            "Product embedding package_versions do not match the embedding "
            "requirements file"
        )
    if pip_options != definition.runtime_profile.pip_options:
        raise ValueError(
            "Product embedding pip_options do not match the embedding "
            "requirements file"
        )
    return definition


def load_approved_dev_smoke_binding(
    path: str | Path | None = None,
) -> ApprovedEmbeddingSmokeBinding:
    """Load the fixed personal DEV model used only for compatibility proof."""
    binding_path = (
        Path(path) if path is not None else DEFAULT_DEV_SMOKE_BINDING_PATH
    )
    raw_document = yaml.safe_load(binding_path.read_text())
    document = _required_mapping(
        raw_document,
        "Product embedding smoke document",
    )
    _require_exact_fields(
        document,
        {"product_embedding_smoke"},
        "Product embedding smoke document",
    )
    return ApprovedEmbeddingSmokeBinding.from_dict(
        document["product_embedding_smoke"]
    )


def load_product_embedding_materialization_binding(
    path: str | Path | None = None,
) -> ProductEmbeddingMaterializationBinding:
    """Load the fixed DEV promotion source and shared runtime target."""
    binding_path = (
        Path(path)
        if path is not None
        else DEFAULT_DEV_MATERIALIZATION_BINDING_PATH
    )
    raw_document = yaml.safe_load(binding_path.read_text())
    document = _required_mapping(
        raw_document,
        "Product embedding materialization document",
    )
    _require_exact_fields(
        document,
        {"product_embedding_materialization"},
        "Product embedding materialization document",
    )
    return ProductEmbeddingMaterializationBinding.from_dict(
        document["product_embedding_materialization"]
    )


def validate_materialization_binding_target(
    binding: ProductEmbeddingMaterializationBinding,
    *,
    catalog: str,
    schema: str,
) -> None:
    """Keep personal proof and shared DEV publication in separate schemas."""
    resolved_catalog = _required_text(
        catalog,
        "catalog",
        "Product embedding materialization target",
    ).lower()
    resolved_schema = _required_text(
        schema,
        "schema",
        "Product embedding materialization target",
    ).lower()
    if binding.purpose == "shared_dev_materialization":
        expected = ("marketingdata_dev", "nextads_feature_store")
    else:
        model_parts = binding.model.registered_model_name.split(".")
        expected = (model_parts[0].lower(), model_parts[1].lower())
    actual = (resolved_catalog, resolved_schema)
    if actual != expected:
        raise ValueError(
            "Product embedding materialization binding does not match the "
            f"target Feature Store schema: expected {expected[0]}."
            f"{expected[1]}, found {actual[0]}.{actual[1]}"
        )


__all__ = [
    "ApprovedEmbeddingSmokeBinding",
    "DEFAULT_DEV_SMOKE_BINDING_PATH",
    "DEFAULT_DEV_MATERIALIZATION_BINDING_PATH",
    "DEFAULT_PRODUCT_EMBEDDING_CONTRACT_PATH",
    "DEFAULT_PERSONAL_DEV_MATERIALIZATION_BINDING_PATH",
    "EmbeddingModelBinding",
    "EmbeddingRuntimeProfile",
    "ProductEmbeddingDefinition",
    "ProductEmbeddingMaterializationBinding",
    "load_approved_dev_smoke_binding",
    "load_product_embedding_definition",
    "load_product_embedding_materialization_binding",
    "validate_materialization_binding_target",
]

"""Promote one exact registered artifact without retraining it."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any, Callable

from next_ads.model_development.contracts import (
    MODEL_VERSION_TAG_ARTIFACT_DIGEST,
    MODEL_VERSION_TAG_BUILD_ID,
    MODEL_VERSION_TAG_SOURCE_MODEL_NAME,
    MODEL_VERSION_TAG_SOURCE_MODEL_VERSION,
    MODEL_VERSION_TAG_TRAINING_RECEIPT_ID,
    ModelBuild,
    ModelDefinition,
    TrainingSetReceipt,
)
from next_ads.model_development.runtime import model_build_id
from next_ads.model_development.spark_training import (
    MODEL_EVALUATION_METRICS,
    artifact_directory_digest,
)


_DIGEST = re.compile(r"[0-9a-f]{64}")
_EXACT_MODEL_URI = re.compile(r"models:/([^/@\s]+)/([1-9][0-9]*)")
REGISTERED_MODEL_COPY = "REGISTERED_MODEL_COPY"
SOURCE_ALIAS_REHEARSAL = "SOURCE_ALIAS_REHEARSAL"
PROMOTION_MODES = frozenset({REGISTERED_MODEL_COPY, SOURCE_ALIAS_REHEARSAL})
_LEGACY_MODEL_VERSION_TAGS = {
    MODEL_VERSION_TAG_ARTIFACT_DIGEST: "nextads.artifact_digest",
    MODEL_VERSION_TAG_BUILD_ID: "nextads.model_build_id",
    MODEL_VERSION_TAG_SOURCE_MODEL_NAME: "nextads.source_model_name",
    MODEL_VERSION_TAG_SOURCE_MODEL_VERSION: "nextads.source_model_version",
    MODEL_VERSION_TAG_TRAINING_RECEIPT_ID: "nextads.training_receipt_id",
}


def _model_version_tag(tags: dict[str, str], key: str) -> str | None:
    return tags.get(key) or tags.get(_LEGACY_MODEL_VERSION_TAGS[key])


def _set_model_version_tags(
    client: Any,
    *,
    model_name: str,
    version: int | str,
    tags: dict[str, str],
) -> None:
    for key, value in tags.items():
        if "." in key or "=" in key:
            raise ValueError(f"Unity Catalog model tag key is invalid: {key}")
        client.set_model_version_tag(
            name=model_name,
            version=version,
            key=key,
            value=value,
        )


def _exact_registered_model_uri(model_name: str, version: int | str) -> str:
    """Return one numeric registered-model URI, rejecting aliases and stages."""
    if not isinstance(model_name, str) or not model_name.strip():
        raise ValueError("registered model name must not be empty")
    if isinstance(version, bool) or not str(version).isdigit():
        raise ValueError("registered model version must be numeric")
    numeric_version = int(version)
    if numeric_version < 1:
        raise ValueError("registered model version must be positive")
    model_uri = f"models:/{model_name.strip()}/{numeric_version}"
    if _EXACT_MODEL_URI.fullmatch(model_uri) is None:
        raise ValueError("registered model URI must name one numeric version")
    return model_uri


def registered_model_artifact_digest(
    model_uri: str,
    *,
    artifact_downloader: Callable[..., str] | None = None,
) -> str:
    """Hash artifacts resolved through one exact registered-model URI.

    Resolving through ``models:/name/version`` works for both legacy run-backed
    MLflow models and newer registered artifacts without relying on a tracking
    run ID that may only exist in the source workspace.
    """
    if (
        not isinstance(model_uri, str)
        or _EXACT_MODEL_URI.fullmatch(model_uri) is None
    ):
        raise ValueError(
            "model_uri must name one numeric registered model version"
        )
    if artifact_downloader is None:
        from mlflow.artifacts import download_artifacts

        artifact_downloader = download_artifacts
    artifact_path = artifact_downloader(artifact_uri=model_uri)
    return artifact_directory_digest(artifact_path)


def recover_registered_model_build(
    client: Any,
    *,
    registered_model_name: str,
    definition: ModelDefinition,
    receipt: TrainingSetReceipt,
) -> ModelBuild | None:
    """Recover an exact version created before its READY receipt was saved."""
    expected_build_id = model_build_id(definition, receipt)
    expected_params = {
        "model_build_id": expected_build_id,
        "model_definition_checksum": definition.checksum,
        "runtime_profile": definition.runtime_profile,
        "training_receipt_id": receipt.receipt_id,
    }
    matches = []
    for summary in client.search_model_versions(
        f"name='{registered_model_name}'"
    ):
        version = client.get_model_version(
            registered_model_name,
            summary.version,
        )
        run_id = getattr(version, "run_id", None)
        if not run_id:
            continue
        run = client.get_run(run_id)
        params = getattr(run.data, "params", {}) or {}
        if all(params.get(key) == value for key, value in expected_params.items()):
            matches.append((version, run))
    if not matches:
        return None
    if len(matches) != 1:
        versions = sorted(int(match[0].version) for match in matches)
        raise ValueError(
            "Multiple registered model versions match one model build: "
            f"{versions}"
        )

    version, run = matches[0]
    if str(getattr(run.info, "status", "")).upper() != "FINISHED":
        raise ValueError("Recovered MLflow run is not FINISHED")
    version_status = str(getattr(version, "status", "READY")).upper()
    if not version_status.endswith("READY"):
        raise ValueError("Recovered registered model version is not READY")
    metrics = {
        str(name): float(value)
        for name, value in (getattr(run.data, "metrics", {}) or {}).items()
    }
    missing_metrics = sorted(set(MODEL_EVALUATION_METRICS) - set(metrics))
    if missing_metrics:
        raise ValueError(
            "Recovered MLflow run is missing metrics: "
            + ", ".join(missing_metrics)
        )
    numeric_version = int(version.version)
    model_uri = _exact_registered_model_uri(
        registered_model_name,
        numeric_version,
    )
    digest = registered_model_artifact_digest(model_uri)
    start_time = getattr(run.info, "start_time", None)
    end_time = getattr(run.info, "end_time", None)
    if start_time is None or end_time is None:
        raise ValueError("Recovered MLflow run has incomplete timestamps")
    _set_model_version_tags(
        client,
        model_name=registered_model_name,
        version=numeric_version,
        tags={
            MODEL_VERSION_TAG_ARTIFACT_DIGEST: digest,
            MODEL_VERSION_TAG_BUILD_ID: expected_build_id,
            MODEL_VERSION_TAG_TRAINING_RECEIPT_ID: receipt.receipt_id,
        },
    )
    client.set_registered_model_alias(
        name=registered_model_name,
        alias="dev_candidate",
        version=numeric_version,
    )
    return ModelBuild(
        model_build_id=expected_build_id,
        model_name=definition.model_name,
        training_receipt_id=receipt.receipt_id,
        model_definition_checksum=definition.checksum,
        runtime_profile=definition.runtime_profile,
        status="READY",
        created_at=datetime.fromtimestamp(start_time / 1000, timezone.utc),
        mlflow_run_id=run.info.run_id,
        registered_model_name=registered_model_name,
        registered_model_version=numeric_version,
        model_uri=model_uri,
        artifact_digest=digest,
        metrics=tuple(sorted(metrics.items())),
        completed_at=datetime.fromtimestamp(end_time / 1000, timezone.utc),
    )


def validate_registered_model_build(client: Any, build: ModelBuild) -> None:
    """Verify a READY receipt still resolves to the exact MLflow artifact."""
    if (
        build.status != "READY"
        or not build.registered_model_name
        or not build.registered_model_version
        or not build.artifact_digest
    ):
        raise ValueError("Model build has no exact registered artifact")
    version = client.get_model_version(
        build.registered_model_name,
        build.registered_model_version,
    )
    tags = version.tags or {}
    if (
        _model_version_tag(tags, MODEL_VERSION_TAG_BUILD_ID)
        != build.model_build_id
    ):
        raise ValueError("Registered model version has a different build ID")
    if (
        _model_version_tag(tags, MODEL_VERSION_TAG_ARTIFACT_DIGEST)
        != build.artifact_digest
    ):
        raise ValueError("Registered model version has a different digest tag")
    source_model_uri = _exact_registered_model_uri(
        build.registered_model_name,
        build.registered_model_version,
    )
    if build.model_uri != source_model_uri:
        raise ValueError("Model build URI is not its exact registered version")
    if (
        registered_model_artifact_digest(source_model_uri)
        != build.artifact_digest
    ):
        raise ValueError("Registered model artifact bytes no longer match")


@dataclass(frozen=True)
class ModelPromotionReceipt:
    """Evidence that a destination version contains the source artifact."""

    model_build_id: str
    source_model_uri: str
    destination_model_name: str
    destination_model_version: int
    destination_run_id: str | None
    artifact_digest: str
    alias: str
    promotion_mode: str
    promoted_at: datetime

    def __post_init__(self) -> None:
        """Require an exact destination version and verified digest."""
        for field_name in (
            "model_build_id",
            "source_model_uri",
            "destination_model_name",
            "alias",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must not be empty")
        if self.destination_run_id is not None and (
            not isinstance(self.destination_run_id, str)
            or not self.destination_run_id.strip()
        ):
            raise ValueError("destination_run_id must be null or non-empty")
        if self.destination_model_version < 1:
            raise ValueError("destination_model_version must be positive")
        if _DIGEST.fullmatch(self.artifact_digest) is None:
            raise ValueError("artifact_digest must be a SHA-256 digest")
        if self.promotion_mode not in PROMOTION_MODES:
            raise ValueError("Unsupported promotion mode")


def _existing_destination_version(
    client: Any,
    *,
    destination_model_name: str,
    model_build_id: str,
) -> Any | None:
    for summary in client.search_model_versions(
        f"name='{destination_model_name}'"
    ):
        version = client.get_model_version(
            destination_model_name,
            summary.version,
        )
        if _model_version_tag(
            version.tags or {},
            MODEL_VERSION_TAG_BUILD_ID,
        ) == model_build_id:
            return version
    return None


def promote_exact_registered_version(
    client: Any,
    *,
    source_model_name: str,
    source_model_version: int,
    destination_model_name: str,
    alias: str,
) -> tuple[ModelPromotionReceipt, bool]:
    """Copy one tagged source version idempotently and verify both digests."""
    if source_model_version < 1:
        raise ValueError("source_model_version must be positive")
    for field_name, value in {
        "source_model_name": source_model_name,
        "destination_model_name": destination_model_name,
        "alias": alias,
    }.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{field_name} must not be empty")
    source = client.get_model_version(
        source_model_name,
        source_model_version,
    )
    source_tags = source.tags or {}
    model_build_id = _model_version_tag(
        source_tags,
        MODEL_VERSION_TAG_BUILD_ID,
    )
    expected_digest = _model_version_tag(
        source_tags,
        MODEL_VERSION_TAG_ARTIFACT_DIGEST,
    )
    if not model_build_id:
        raise ValueError("Source model version has no NextAds build ID")
    if not expected_digest or _DIGEST.fullmatch(expected_digest) is None:
        raise ValueError("Source model version has no valid artifact digest")
    source_model_uri = _exact_registered_model_uri(
        source_model_name,
        source_model_version,
    )
    source_digest = registered_model_artifact_digest(source_model_uri)
    if source_digest != expected_digest:
        raise ValueError("Source artifact bytes do not match its digest tag")

    destination = _existing_destination_version(
        client,
        destination_model_name=destination_model_name,
        model_build_id=model_build_id,
    )
    reused = destination is not None
    if destination is None:
        copied = client.copy_model_version(
            src_model_uri=source_model_uri,
            dst_name=destination_model_name,
        )
        destination = client.get_model_version(
            destination_model_name,
            copied.version,
        )
    destination_model_uri = _exact_registered_model_uri(
        destination_model_name,
        destination.version,
    )
    destination_digest = registered_model_artifact_digest(
        destination_model_uri
    )
    if destination_digest != source_digest:
        raise ValueError("Destination artifact bytes differ from the source")
    _set_model_version_tags(
        client,
        model_name=destination_model_name,
        version=destination.version,
        tags={
            MODEL_VERSION_TAG_BUILD_ID: model_build_id,
            MODEL_VERSION_TAG_ARTIFACT_DIGEST: source_digest,
            MODEL_VERSION_TAG_SOURCE_MODEL_NAME: source_model_name,
            MODEL_VERSION_TAG_SOURCE_MODEL_VERSION: str(source_model_version),
        },
    )
    client.set_registered_model_alias(
        name=destination_model_name,
        alias=alias,
        version=int(destination.version),
    )
    return (
        ModelPromotionReceipt(
            model_build_id=model_build_id,
            source_model_uri=source_model_uri,
            destination_model_name=destination_model_name,
            destination_model_version=int(destination.version),
            destination_run_id=getattr(destination, "run_id", None) or None,
            artifact_digest=destination_digest,
            alias=alias,
            promotion_mode=REGISTERED_MODEL_COPY,
            promoted_at=datetime.now(timezone.utc),
        ),
        reused,
    )


def promote_exact_model_build(
    client: Any,
    build: ModelBuild,
    *,
    destination_model_name: str | None,
    alias: str,
    promotion_mode: str = REGISTERED_MODEL_COPY,
) -> tuple[ModelPromotionReceipt, bool]:
    """Promote exactly, or rehearse aliases when target access is unavailable."""
    if (
        build.status != "READY"
        or not build.model_uri
        or not build.artifact_digest
    ):
        raise ValueError("Promotion requires a READY exact model build")
    if promotion_mode not in PROMOTION_MODES:
        raise ValueError(f"Unsupported promotion mode: {promotion_mode}")
    if not alias.strip():
        raise ValueError("Promotion alias must not be empty")
    if not build.registered_model_name or not build.registered_model_version:
        raise ValueError(
            "Promotion requires one exact registered source version"
        )
    source_model_uri = _exact_registered_model_uri(
        build.registered_model_name,
        build.registered_model_version,
    )
    if build.model_uri != source_model_uri:
        raise ValueError("Model build URI is not its exact registered version")
    source_digest = registered_model_artifact_digest(source_model_uri)
    if source_digest != build.artifact_digest:
        raise ValueError(
            "Source artifact digest does not match the source model build"
        )
    if promotion_mode == SOURCE_ALIAS_REHEARSAL:
        if destination_model_name and (
            destination_model_name.strip() != build.registered_model_name
        ):
            raise ValueError(
                "Source alias rehearsal cannot name a different model"
            )
        destination_model_name = build.registered_model_name
        destination = client.get_model_version(
            destination_model_name,
            build.registered_model_version,
        )
        reused = True
    else:
        if not destination_model_name or not destination_model_name.strip():
            raise ValueError("Registered model copy needs a destination model")
        destination_model_name = destination_model_name.strip()
        existing = _existing_destination_version(
            client,
            destination_model_name=destination_model_name,
            model_build_id=build.model_build_id,
        )
        reused = existing is not None
        if existing is None:
            copied = client.copy_model_version(
                src_model_uri=build.model_uri,
                dst_name=destination_model_name,
            )
            destination = client.get_model_version(
                destination_model_name,
                copied.version,
            )
        else:
            destination = existing

    destination_model_uri = _exact_registered_model_uri(
        destination_model_name,
        destination.version,
    )
    destination_digest = registered_model_artifact_digest(
        destination_model_uri
    )
    if destination_digest != source_digest:
        raise ValueError(
            "Promoted artifact digest does not match the source model build"
        )
    if not reused or promotion_mode == SOURCE_ALIAS_REHEARSAL:
        _set_model_version_tags(
            client,
            model_name=destination_model_name,
            version=destination.version,
            tags={
                MODEL_VERSION_TAG_BUILD_ID: build.model_build_id,
                MODEL_VERSION_TAG_ARTIFACT_DIGEST: build.artifact_digest,
            },
        )
    client.set_registered_model_alias(
        name=destination_model_name,
        alias=alias,
        version=int(destination.version),
    )
    return (
        ModelPromotionReceipt(
            model_build_id=build.model_build_id,
            source_model_uri=build.model_uri,
            destination_model_name=destination_model_name,
            destination_model_version=int(destination.version),
            destination_run_id=getattr(destination, "run_id", None) or None,
            artifact_digest=destination_digest,
            alias=alias,
            promotion_mode=promotion_mode,
            promoted_at=datetime.now(timezone.utc),
        ),
        reused,
    )


__all__ = [
    "PROMOTION_MODES",
    "REGISTERED_MODEL_COPY",
    "SOURCE_ALIAS_REHEARSAL",
    "ModelPromotionReceipt",
    "promote_exact_model_build",
    "promote_exact_registered_version",
    "recover_registered_model_build",
    "registered_model_artifact_digest",
    "validate_registered_model_build",
]

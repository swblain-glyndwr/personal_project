"""Promote one exact registered artifact without retraining it."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any

from next_ads.model_development.contracts import ModelBuild
from next_ads.model_development.spark_training import (
    artifact_directory_digest,
)


_DIGEST = re.compile(r"[0-9a-f]{64}")
REGISTERED_MODEL_COPY = "REGISTERED_MODEL_COPY"
SOURCE_ALIAS_REHEARSAL = "SOURCE_ALIAS_REHEARSAL"
PROMOTION_MODES = frozenset({REGISTERED_MODEL_COPY, SOURCE_ALIAS_REHEARSAL})


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
    if tags.get("nextads.model_build_id") != build.model_build_id:
        raise ValueError("Registered model version has a different build ID")
    if tags.get("nextads.artifact_digest") != build.artifact_digest:
        raise ValueError("Registered model version has a different digest tag")
    run_id = version.run_id or build.mlflow_run_id
    if not run_id:
        raise ValueError("Registered model version has no source run")
    artifact_path = client.download_artifacts(run_id, "model")
    if artifact_directory_digest(artifact_path) != build.artifact_digest:
        raise ValueError("Registered model artifact bytes no longer match")


@dataclass(frozen=True)
class ModelPromotionReceipt:
    """Evidence that a destination version contains the source artifact."""

    model_build_id: str
    source_model_uri: str
    destination_model_name: str
    destination_model_version: int
    destination_run_id: str
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
            "destination_run_id",
            "alias",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must not be empty")
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
        if (version.tags or {}).get("nextads.model_build_id") == model_build_id:
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
    model_build_id = source_tags.get("nextads.model_build_id")
    expected_digest = source_tags.get("nextads.artifact_digest")
    if not model_build_id:
        raise ValueError("Source model version has no NextAds build ID")
    if not expected_digest or _DIGEST.fullmatch(expected_digest) is None:
        raise ValueError("Source model version has no valid artifact digest")
    if not source.run_id:
        raise ValueError("Source model version has no MLflow run")
    source_digest = artifact_directory_digest(
        client.download_artifacts(source.run_id, "model")
    )
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
            src_model_uri=(
                f"models:/{source_model_name}/{source_model_version}"
            ),
            dst_name=destination_model_name,
        )
        destination = client.get_model_version(
            destination_model_name,
            copied.version,
        )
    if not destination.run_id:
        raise ValueError("Destination model version has no MLflow run")
    destination_digest = artifact_directory_digest(
        client.download_artifacts(destination.run_id, "model")
    )
    if destination_digest != source_digest:
        raise ValueError("Destination artifact bytes differ from the source")
    for key, value in {
        "nextads.model_build_id": model_build_id,
        "nextads.artifact_digest": source_digest,
        "nextads.source_model_name": source_model_name,
        "nextads.source_model_version": str(source_model_version),
    }.items():
        client.set_model_version_tag(
            name=destination_model_name,
            version=destination.version,
            key=key,
            value=value,
        )
    client.set_registered_model_alias(
        name=destination_model_name,
        alias=alias,
        version=int(destination.version),
    )
    return (
        ModelPromotionReceipt(
            model_build_id=model_build_id,
            source_model_uri=(
                f"models:/{source_model_name}/{source_model_version}"
            ),
            destination_model_name=destination_model_name,
            destination_model_version=int(destination.version),
            destination_run_id=destination.run_id,
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
    if build.status != "READY" or not build.model_uri or not build.artifact_digest:
        raise ValueError("Promotion requires a READY exact model build")
    if promotion_mode not in PROMOTION_MODES:
        raise ValueError(f"Unsupported promotion mode: {promotion_mode}")
    if not alias.strip():
        raise ValueError("Promotion alias must not be empty")
    if promotion_mode == SOURCE_ALIAS_REHEARSAL:
        if (
            not build.registered_model_name
            or not build.registered_model_version
            or not build.mlflow_run_id
        ):
            raise ValueError(
                "Source alias rehearsal needs the exact registered source version"
            )
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

    artifact_path = client.download_artifacts(destination.run_id, "model")
    destination_digest = artifact_directory_digest(artifact_path)
    if destination_digest != build.artifact_digest:
        raise ValueError(
            "Promoted artifact digest does not match the source model build"
        )
    if not reused or promotion_mode == SOURCE_ALIAS_REHEARSAL:
        client.set_model_version_tag(
            name=destination_model_name,
            version=destination.version,
            key="nextads.model_build_id",
            value=build.model_build_id,
        )
        client.set_model_version_tag(
            name=destination_model_name,
            version=destination.version,
            key="nextads.artifact_digest",
            value=build.artifact_digest,
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
            destination_run_id=destination.run_id,
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
    "validate_registered_model_build",
]

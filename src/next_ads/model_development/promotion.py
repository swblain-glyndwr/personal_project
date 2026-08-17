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


def promote_exact_model_build(
    client: Any,
    build: ModelBuild,
    *,
    destination_model_name: str,
    alias: str,
) -> tuple[ModelPromotionReceipt, bool]:
    """Copy or reuse a destination version and verify its artifact digest."""
    if build.status != "READY" or not build.model_uri or not build.artifact_digest:
        raise ValueError("Promotion requires a READY exact model build")
    if not destination_model_name.strip() or not alias.strip():
        raise ValueError("Promotion destination and alias must not be empty")
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
    if not reused:
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
            promoted_at=datetime.now(timezone.utc),
        ),
        reused,
    )


__all__ = ["ModelPromotionReceipt", "promote_exact_model_build"]

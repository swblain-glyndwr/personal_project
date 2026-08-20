"""Consistent, searchable evidence for durable job outputs."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any


OUTPUT_DESTINATION_PREFIX = "NEXTADS_OUTPUT="
LOGGER = logging.getLogger("next_ads.outputs")


def log_output_location(
    destination: str,
    *,
    kind: str,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Log one PII-free durable destination after a successful write."""
    if not isinstance(destination, str) or not destination.strip():
        raise ValueError("Output destination must be non-blank text")
    if not isinstance(kind, str) or not kind.strip():
        raise ValueError("Output kind must be non-blank text")

    payload: dict[str, Any] = {
        "destination": destination.strip(),
        "kind": kind.strip(),
    }
    for key, value in sorted((details or {}).items()):
        if value is not None:
            payload[key] = value

    LOGGER.info(
        "%s%s",
        OUTPUT_DESTINATION_PREFIX,
        json.dumps(
            payload, default=str, separators=(",", ":"), sort_keys=True
        ),
    )
    return payload


__all__ = ["OUTPUT_DESTINATION_PREFIX", "log_output_location"]

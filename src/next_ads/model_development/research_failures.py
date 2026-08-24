"""PII-safe failure envelopes for durable model-research evidence."""

from __future__ import annotations

import hashlib
import json
import re


_SAFE_NAME = re.compile(r"[^A-Za-z0-9_.-]+")


def safe_failure_reason(error: BaseException | str, *, stage: str) -> str:
    """Return a bounded failure reference without persisting exception text.

    The original exception still reaches the task log through the caller's
    normal ``raise`` path. Durable Delta rows, MLflow tags and evidence files
    retain only a stable digest that can be matched to that log entry.
    """
    stage_name = _SAFE_NAME.sub("_", str(stage).strip()).strip("_.-")
    if not stage_name:
        raise ValueError("Failure stage must contain a safe name")
    error_type = (
        error.__class__.__name__
        if isinstance(error, BaseException)
        else "RecordedFailure"
    )
    safe_type = _SAFE_NAME.sub("_", error_type).strip("_.-") or "Exception"
    raw = (
        f"{error.__class__.__module__}.{error.__class__.__qualname__}: {error}"
        if isinstance(error, BaseException)
        else str(error)
    )
    payload = {
        "error_type": safe_type,
        "message": "Stage failed; inspect the task log using message_sha256.",
        "message_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        "stage": stage_name,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


__all__ = ["safe_failure_reason"]

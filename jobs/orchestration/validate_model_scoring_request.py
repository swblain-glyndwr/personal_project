"""Validate a declared model-scoring route before operational writes."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys
from typing import Any

import yaml


def resolve_project_root() -> Path:
    """Resolve the bundle root for file and Databricks workspace execution."""
    try:
        return Path(__file__).resolve().parents[2]
    except NameError:
        from dsutils.dbc import get_dbutils

        dbutils = get_dbutils()
        notebook_path = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        if not notebook_path.startswith("/Workspace"):
            notebook_path = "/Workspace" + notebook_path
        return Path(notebook_path).parents[2]


PROJECT_ROOT = resolve_project_root()
SRC_ROOT = PROJECT_ROOT / "src"
if not (SRC_ROOT / "next_ads").is_dir():
    raise RuntimeError(f"Canonical NextAds package not found under {SRC_ROOT}")
sys.path.insert(0, str(SRC_ROOT))
sys.path.insert(1, str(PROJECT_ROOT))


from next_ads.common.job_logging import configure_job_logging


DEFAULT_SCORING_CONFIG = (
    PROJECT_ROOT / "configs" / "scoring" / "scoring_settings.yaml"
)
SUPPORTED_MODELS = {
    "theme_affinity": {
        "provider_id": "theme_affinity",
        "implementation": "theme_affinity",
        "capability": "account_theme",
        "compatibility_publisher": "theme_affinity_legacy",
        "foundation_id": "account_theme_features",
    }
}
EVIDENCE_PREFIX = "MODEL_SCORING_REQUEST="
LOGGER = logging.getLogger(__name__)


def _mapping(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return value


def validate_model_scoring_request(
    model_name: str,
    *,
    config_path: str | Path = DEFAULT_SCORING_CONFIG,
) -> dict[str, str]:
    """Resolve one supported model name to its owned scoring declaration."""
    requested = str(model_name or "").strip()
    if not requested:
        raise ValueError("model_name is required")
    raw = yaml.safe_load(Path(config_path).read_text()) or {}
    default = _mapping(raw.get("default"), "default")
    scoring = _mapping(default.get("scoring"), "default.scoring")
    providers = _mapping(
        scoring.get("providers"),
        "default.scoring.providers",
    )
    provider = _mapping(
        providers.get(requested),
        f"scoring provider {requested}",
    )
    provider_id = str(provider.get("provider_id") or "").strip()
    implementation = str(provider.get("implementation") or "").strip()
    expected = SUPPORTED_MODELS.get(requested)
    if expected is None:
        raise ValueError(
            f"No operational scoring implementation for {requested}"
        )
    mismatched = sorted(
        field_name
        for field_name, expected_value in expected.items()
        if provider.get(field_name) != expected_value
    )
    if mismatched:
        raise ValueError(
            f"Scoring declaration for {requested} does not match "
            + ", ".join(mismatched)
        )
    return {
        "model_name": requested,
        "provider_id": provider_id,
        "implementation": implementation,
        "compatibility_publisher": str(
            provider["compatibility_publisher"]
        ),
        "foundation_id": str(provider["foundation_id"]),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_name", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    configure_job_logging("INFO")
    evidence = validate_model_scoring_request(args.model_name)

    from dsutils.dbc import get_dbutils

    task_values = get_dbutils().jobs.taskValues
    for key, value in evidence.items():
        task_values.set(key=key, value=value)
    LOGGER.info(
        "%s%s",
        EVIDENCE_PREFIX,
        json.dumps(evidence, sort_keys=True, separators=(",", ":")),
    )


if __name__ == "__main__":
    main()

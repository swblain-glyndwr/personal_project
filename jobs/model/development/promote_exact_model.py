"""Promote one exact generic model version without retraining it."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_ROOT))
sys.path.insert(1, str(PROJECT_ROOT))


from next_ads.ml.lifecycle import configure_mlflow
from next_ads.model_development import promote_exact_registered_version


LOGGER = logging.getLogger(__name__)
EVIDENCE_PREFIX = "MODEL_PROMOTION_EVIDENCE="


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_model_name", required=True)
    parser.add_argument("--source_model_version", default="")
    parser.add_argument("--source_alias", default="")
    parser.add_argument("--target_model_name", required=True)
    parser.add_argument("--target_alias", required=True)
    parser.add_argument("--allowed_source_model_prefix", required=True)
    parser.add_argument("--allowed_target_model_prefix", required=True)
    parser.add_argument("--source_environment", required=True)
    parser.add_argument("--target_environment", required=True)
    parser.add_argument("--client", required=True)
    parser.add_argument("--model_family", required=True)
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def _require_prefix(value: str, prefix: str, field_name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{field_name} must start with {prefix}")


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))
    _require_prefix(
        args.source_model_name,
        args.allowed_source_model_prefix,
        "source_model_name",
    )
    _require_prefix(
        args.target_model_name,
        args.allowed_target_model_prefix,
        "target_model_name",
    )
    if not args.source_model_version and not args.source_alias:
        raise ValueError("source_model_version or source_alias is required")

    import mlflow

    configure_mlflow(mlflow)
    client = mlflow.tracking.MlflowClient()
    if args.source_model_version:
        source_version = int(args.source_model_version)
    else:
        source_version = int(
            client.get_model_version_by_alias(
                args.source_model_name,
                args.source_alias,
            ).version
        )
    receipt, reused = promote_exact_registered_version(
        client,
        source_model_name=args.source_model_name,
        source_model_version=source_version,
        destination_model_name=args.target_model_name,
        alias=args.target_alias,
    )
    evidence = {
        **receipt.__dict__,
        "client": args.client,
        "model_family": args.model_family,
        "source_environment": args.source_environment,
        "target_environment": args.target_environment,
        "reused": reused,
    }
    LOGGER.info(
        "%s%s",
        EVIDENCE_PREFIX,
        json.dumps(evidence, default=str, sort_keys=True),
    )


if __name__ == "__main__":
    main()

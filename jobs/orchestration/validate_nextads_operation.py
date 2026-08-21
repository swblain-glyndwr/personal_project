"""Fail closed before the shared NextAds job selects an operation branch."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"
if not (SRC_ROOT / "next_ads").is_dir():
    raise RuntimeError(f"Canonical NextAds package not found under {SRC_ROOT}")
sys.path.insert(0, str(SRC_ROOT))
sys.path.insert(1, str(PROJECT_ROOT))


from next_ads.common.job_logging import configure_job_logging


CANDIDATE_BUILD = "CANDIDATE_BUILD"
PREPARE_SCORING_INPUTS = "PREPARE_SCORING_INPUTS"
OPERATIONS = (CANDIDATE_BUILD, PREPARE_SCORING_INPUTS)
LOGGER = logging.getLogger(__name__)


def validate_operation(value: str) -> str:
    """Return one exact supported operation or reject the request."""
    operation = str(value or "")
    if operation not in OPERATIONS:
        raise ValueError(
            "operation must be one of " + ", ".join(OPERATIONS)
        )
    return operation


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--operation", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    configure_job_logging("INFO")
    operation = validate_operation(args.operation)
    LOGGER.info("Validated NextAds job operation %s", operation)


if __name__ == "__main__":
    main()

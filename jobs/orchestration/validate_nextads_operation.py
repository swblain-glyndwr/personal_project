"""Fail closed before the shared NextAds job selects an operation branch."""

from __future__ import annotations

import argparse
import logging


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
    logging.basicConfig(level=logging.INFO)
    operation = validate_operation(args.operation)
    LOGGER.info("Validated NextAds job operation %s", operation)


if __name__ == "__main__":
    main()

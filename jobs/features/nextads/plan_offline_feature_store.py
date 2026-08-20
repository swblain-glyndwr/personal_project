"""Print the repository-defined offline Feature Store plan."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"
if not (SRC_ROOT / "next_ads").is_dir():
    raise RuntimeError(f"Canonical NextAds package not found under {SRC_ROOT}")
sys.path.insert(0, str(SRC_ROOT))


from next_ads.features.feature_store_registry import DEFAULT_REGISTRY_PATH
from next_ads.features.offline_feature_store_plan import (
    build_offline_feature_store_plan,
    render_offline_feature_store_plan,
)


def build_parser() -> argparse.ArgumentParser:
    """Create the read-only plan command parser."""
    parser = argparse.ArgumentParser(
        description=(
            "Resolve offline feature contracts and environment bindings "
            "without changing Databricks or repository state."
        )
    )
    parser.add_argument(
        "--environment",
        choices=("ALL", "DEV", "PREPROD", "PROD"),
        default="ALL",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        dest="output_format",
    )
    parser.add_argument(
        "--registry",
        type=Path,
        default=DEFAULT_REGISTRY_PATH,
    )
    parser.add_argument(
        "--release-id",
        default=None,
        help=(
            "Release identifier used to resolve isolated PREPROD table "
            "names. Without it, PREPROD locations remain templates."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Render the requested plan and return a process exit code."""
    args = build_parser().parse_args(argv)
    environments = None if args.environment == "ALL" else (args.environment,)
    plan = build_offline_feature_store_plan(
        registry_path=args.registry,
        environments=environments,
        release_id=args.release_id,
    )
    if args.output_format == "json":
        print(json.dumps(plan, indent=2, sort_keys=True))
    else:
        print(render_offline_feature_store_plan(plan))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

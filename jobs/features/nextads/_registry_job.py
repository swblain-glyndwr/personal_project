"""Shared helpers for Next Ads feature-store Databricks jobs."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
import logging
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"
if not (SRC_ROOT / "next_ads").is_dir():
    raise RuntimeError(f"Canonical NextAds package not found under {SRC_ROOT}")
sys.path.insert(0, str(SRC_ROOT))
sys.path.insert(1, str(PROJECT_ROOT))


from next_ads.features import FeatureStoreRegistry, load_feature_store_registry


LOGGER = logging.getLogger(__name__)


def configure_job_logging(log_level: str) -> None:
    """Configure job logging while keeping dependency internals quiet."""
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)


def parse_common_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference_date", default=None)
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--schema", default=None)
    parser.add_argument("--source_catalog", default="marketingdata_prod")
    parser.add_argument("--source_schema", default="warehouse")
    parser.add_argument("--theme_source_catalog", default=None)
    parser.add_argument("--theme_source_schema", default="ds_sandbox")
    parser.add_argument(
        "--theme_table_prefix",
        default="next_uk_nextads_account_theme_foundation",
    )
    parser.add_argument("--replace_reference_date", default="true")
    parser.add_argument("--job_env", default="dev")
    parser.add_argument("--client", default="next_uk")
    parser.add_argument("--theme_training_reference_date", default="skip")
    parser.add_argument(
        "--theme_training_table_prefix",
        default="next_uk_nextads_theme_affinity_training",
    )
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def _builder_table_names(
    builder: str,
    registry: FeatureStoreRegistry,
    *,
    include_scaffolds: bool = False,
) -> tuple[str, ...]:
    return tuple(
        feature.name
        for feature in registry.features_for_builder(
            builder,
            include_scaffolds=include_scaffolds,
        )
    )


def validate_builder_output_tables(
    builder: str,
    output_table_names: Iterable[str],
    registry: FeatureStoreRegistry | None = None,
) -> tuple[str, ...]:
    """Require a builder to produce exactly its implemented registry outputs."""
    if isinstance(output_table_names, str):
        raise ValueError(
            "Builder output table names must be an iterable of names"
        )

    actual = tuple(output_table_names)
    if any(
        not isinstance(table_name, str) or not table_name.strip()
        for table_name in actual
    ):
        raise ValueError("Builder output table names must be non-blank text")
    duplicate_names = sorted(
        {table_name for table_name in actual if actual.count(table_name) > 1}
    )
    if duplicate_names:
        raise ValueError(
            f"Feature-store builder {builder} produced duplicate output names: "
            + ", ".join(duplicate_names)
        )

    active_registry = registry or load_feature_store_registry()
    declared = active_registry.features_for_builder(
        builder,
        include_scaffolds=True,
    )
    if not declared:
        raise ValueError(f"Unknown feature-store builder: {builder}")
    expected = _builder_table_names(builder, active_registry)
    missing = sorted(set(expected) - set(actual))
    unexpected = sorted(set(actual) - set(expected))
    if missing or unexpected:
        details = []
        if missing:
            details.append("missing=" + ",".join(missing))
        if unexpected:
            details.append("unexpected=" + ",".join(unexpected))
        raise ValueError(
            f"Feature-store builder {builder} outputs do not match the registry: "
            + " ".join(details)
        )
    return actual


def log_owned_tables(
    builder: str,
    args: argparse.Namespace,
    *,
    include_scaffolds: bool = False,
) -> list[str]:
    configure_job_logging(args.log_level)
    registry = load_feature_store_registry()
    catalog = args.catalog or registry.default_catalog
    schema = args.schema or registry.default_schema
    owned_tables = [
        registry.resolved_table_path(
            table_name, catalog=catalog, schema=schema
        )
        for table_name in _builder_table_names(
            builder,
            registry,
            include_scaffolds=include_scaffolds,
        )
    ]

    LOGGER.info(
        "Feature-store job %s reference_date=%s target=%s.%s",
        builder,
        args.reference_date,
        catalog,
        schema,
    )
    for table_path in owned_tables:
        LOGGER.info("Registered output table: %s", table_path)
    return owned_tables


def metadata_only_main(builder: str) -> None:
    args = parse_common_args()
    owned_tables = log_owned_tables(builder, args, include_scaffolds=True)
    LOGGER.info(
        "%s completed metadata-only scaffold for %s tables",
        builder,
        len(owned_tables),
    )

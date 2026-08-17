"""Read-only DBR 15.4 smoke proof for the model-development route."""

from __future__ import annotations

from datetime import date, datetime, timezone
import json
import logging
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SRC_ROOT = PROJECT_ROOT / "src"
if not (SRC_ROOT / "next_ads").is_dir():
    raise RuntimeError(f"Canonical NextAds package not found under {SRC_ROOT}")
sys.path.insert(0, str(SRC_ROOT))
sys.path.insert(1, str(PROJECT_ROOT))


from dsutils.dbc import configure_spark
from next_ads.features.embedding_runtime import (
    installed_package_versions,
    resolve_runtime_version,
)
from next_ads.features.snapshot_reader import ReadyFeatureBinding
from next_ads.model_development import validate_snapshot_time_boundary


LOGGER = logging.getLogger(__name__)
MANIFEST_PREFIX = "MODEL_DEVELOPMENT_RUNTIME_SMOKE="
EXPECTED_PACKAGES = {
    "databricks-feature-engineering": "0.12.1",
    "dynaconf": "3.2.12",
    "mlflow": "3.11.1",
}


def validate_runtime_versions(
    runtime_version: str,
    package_versions: dict[str, str],
) -> None:
    """Require the exact runtime and libraries used by the DEV jobs."""
    if not runtime_version.startswith("15.4"):
        raise ValueError(
            "Model development must run on DBR 15.4; found "
            f"{runtime_version}"
        )
    mismatched = {
        package: (expected, package_versions.get(package))
        for package, expected in EXPECTED_PACKAGES.items()
        if package_versions.get(package) != expected
    }
    if mismatched:
        raise ValueError(
            "Model development package versions do not match: "
            + json.dumps(mismatched, sort_keys=True)
        )


def prove_future_binding_rejection() -> str:
    """Exercise the same leakage boundary used by training receipts."""
    binding = ReadyFeatureBinding(
        feature_snapshot_id="future-fixture",
        feature_snapshot_attempt_id="future-fixture-attempt",
        feature_build_id="future-fixture-build",
        feature_build_attempt_id="future-fixture-build-attempt",
        reference_date=date(2026, 8, 12),
        registry_checksum="a" * 64,
        git_commit="runtime-smoke",
        completed_at=datetime(2026, 8, 12, tzinfo=timezone.utc),
        feature_id="next_uk_nextads_fs_account_profile",
        backing_table="fixture.account_profile",
        delta_version=1,
        row_count=1,
        output_schema_checksum="b" * 64,
        backing_schema_checksum="b" * 64,
        value_checksum="c" * 64,
        write_receipt_id="fixture-receipt",
    )
    try:
        validate_snapshot_time_boundary(binding, date(2026, 8, 11))
    except ValueError as exc:
        if "is after observation end" not in str(exc):
            raise
        return str(exc)
    raise AssertionError("A future-dated Feature Store binding was accepted")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    spark = configure_spark()
    runtime_version = resolve_runtime_version(spark)
    package_versions = installed_package_versions(EXPECTED_PACKAGES)
    validate_runtime_versions(runtime_version, package_versions)

    import mlflow  # noqa: F401
    from databricks.feature_engineering import (  # noqa: F401
        FeatureEngineeringClient,
    )

    leakage_result = prove_future_binding_rejection()
    evidence = {
        "future_lookup_rejected": True,
        "future_lookup_message": leakage_result,
        "package_versions": package_versions,
        "runtime_version": runtime_version,
        "status": "PASS",
        "writes_performed": False,
    }
    LOGGER.info(
        "%s%s",
        MANIFEST_PREFIX,
        json.dumps(evidence, sort_keys=True, separators=(",", ":")),
    )


if __name__ == "__main__":
    main()

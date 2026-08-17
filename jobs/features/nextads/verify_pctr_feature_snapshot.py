"""Prove that Analytics pCTR consumers see only one READY snapshot."""

from __future__ import annotations

import argparse
import json
import logging

from _registry_job import configure_job_logging
from dsutils.dbc import configure_spark, get_dbutils
from next_ads.features.snapshot_reader import read_ready_feature


LOGGER = logging.getLogger(__name__)
FEATURE_IDS = (
    "next_uk_nextads_fs_account_advert_affinity_daily",
    "next_uk_nextads_fs_pctr_model_input",
    "next_uk_nextads_fs_session_context_daily",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--reference_date", required=True)
    parser.add_argument("--current_attempt_id", required=True)
    parser.add_argument("--expect_current_attempt_ready", required=True)
    parser.add_argument("--log_level", default="INFO")
    return parser.parse_args()


def verify_ready_snapshot(
    spark,
    *,
    catalog: str,
    schema: str,
    reference_date: str,
    current_attempt_id: str,
    expect_current_attempt_ready: bool,
) -> dict[str, object]:
    evidence = []
    for feature_id in FEATURE_IDS:
        _, binding = read_ready_feature(
            spark,
            feature_id,
            catalog=catalog,
            schema=schema,
            reference_date=reference_date,
        )
        evidence.append(
            {
                "feature_id": feature_id,
                "snapshot_id": binding.feature_snapshot_id,
                "snapshot_attempt_id": (
                    binding.feature_snapshot_attempt_id
                ),
                "delta_version": binding.delta_version,
                "row_count": binding.row_count,
                "schema_checksum": binding.output_schema_checksum,
                "value_checksum": binding.value_checksum,
                "write_receipt_id": binding.write_receipt_id,
            }
        )
    attempt_ids = {row["snapshot_attempt_id"] for row in evidence}
    snapshot_ids = {row["snapshot_id"] for row in evidence}
    if len(attempt_ids) != 1 or len(snapshot_ids) != 1:
        raise ValueError("The three pCTR features do not share one READY snapshot")
    ready_attempt_id = str(next(iter(attempt_ids)))
    if expect_current_attempt_ready and ready_attempt_id != current_attempt_id:
        raise ValueError("The current feature build did not become READY")
    if not expect_current_attempt_ready and ready_attempt_id == current_attempt_id:
        raise ValueError("The injected failure became visible as READY")
    return {
        "reference_date": reference_date,
        "current_attempt_id": current_attempt_id,
        "ready_attempt_id": ready_attempt_id,
        "current_attempt_ready": ready_attempt_id == current_attempt_id,
        "features": evidence,
    }


def main() -> None:
    args = parse_args()
    configure_job_logging(args.log_level)
    evidence = verify_ready_snapshot(
        configure_spark(),
        catalog=args.catalog,
        schema=args.schema,
        reference_date=args.reference_date,
        current_attempt_id=args.current_attempt_id,
        expect_current_attempt_ready=(
            args.expect_current_attempt_ready.strip().lower() == "true"
        ),
    )
    serialised = json.dumps(evidence, sort_keys=True, separators=(",", ":"))
    task_values = get_dbutils().jobs.taskValues
    task_values.set(key="feature_snapshot_evidence", value=serialised)
    task_values.set(key="ready_attempt_id", value=evidence["ready_attempt_id"])
    LOGGER.info("FEATURE_SNAPSHOT_EVIDENCE=%s", serialised)


if __name__ == "__main__":
    main()

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
import json
from typing import Any, Mapping

from pyspark.sql import DataFrame

from next_ads.common.delta_writes import quote_qualified_identifier

INPUT_CONTRACT_VERSION = "nextads_scoring_inputs/v2"


@dataclass(frozen=True)
class InputVersionBinding:
    table: str
    delta_version: int
    schema_version: str
    schema_checksum: str


def schema_checksum(frame: Any) -> str:
    signature = [
        (field.name, field.dataType.simpleString()) for field in frame.schema
    ]
    return hashlib.sha256(
        json.dumps(signature, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def build_input_snapshot_id(
    run_date: date,
    source_bindings: Mapping[str, InputVersionBinding],
    *,
    contract_version: str = INPUT_CONTRACT_VERSION,
    git_commit: str,
) -> str:
    """Build identity from canonical, immutable input and code bindings."""
    if not source_bindings:
        raise ValueError("At least one source binding is required")
    payload = {
        "contract_version": contract_version,
        "run_date": run_date.isoformat(),
        "git_commit": git_commit,
        "sources": {
            name: {
                "table": binding.table,
                "delta_version": binding.delta_version,
                "schema_version": binding.schema_version,
                "schema_checksum": binding.schema_checksum,
            }
            for name, binding in sorted(source_bindings.items())
        },
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    return f"scoring_inputs_{run_date:%Y%m%d}_{digest[:20]}"


def latest_delta_version(spark: Any, table: str) -> int:
    row = spark.sql(
        f"DESCRIBE HISTORY {quote_qualified_identifier(table)} LIMIT 1"
    ).first()
    if row is None:
        raise ValueError(f"No Delta history found for {table}")
    return int(row["version"])


def read_delta_version(spark: Any, table: str, version: int) -> DataFrame:
    if (
        isinstance(version, bool)
        or not isinstance(version, int)
        or version < 0
    ):
        raise ValueError("Delta version must be a non-negative integer")
    return spark.read.option("versionAsOf", version).table(table)

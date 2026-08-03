from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from next_ads.common.delta_writes import quote_qualified_identifier

INPUT_CONTRACT_VERSION = "nextads_scoring_inputs/v1"


@dataclass(frozen=True)
class ContentSummary:
    row_count: int
    distinct_key_count: int
    null_key_count: int
    duplicate_key_count: int
    content_checksum: str

    def require_valid(self, source_name: str) -> None:
        if self.row_count == 0:
            raise ValueError(f"{source_name} is empty")
        if self.null_key_count:
            raise ValueError(
                f"{source_name} contains {self.null_key_count} null keys"
            )
        if self.duplicate_key_count:
            raise ValueError(
                f"{source_name} contains "
                f"{self.duplicate_key_count} duplicate keys"
            )


def summarise_content(
    df: DataFrame,
    *,
    key_columns: Sequence[str],
    content_columns: Sequence[str] | None = None,
) -> ContentSummary:
    """Validate keys and checksum a frame with one commutative aggregation."""
    keys = list(key_columns)
    columns = list(content_columns or df.columns)
    if not keys or not columns:
        raise ValueError("Key and content columns must not be empty")
    if len(keys) != len(set(keys)) or len(columns) != len(set(columns)):
        raise ValueError("Key and content columns must be unique")
    missing = sorted((set(keys) | set(columns)).difference(df.columns))
    if missing:
        raise ValueError(f"Missing input columns: {', '.join(missing)}")

    any_null = F.exists(
        F.array(*[F.col(column).isNull() for column in keys]),
        lambda value: value,
    )
    canonical_row = F.to_json(
        F.struct(*[F.col(column) for column in columns]),
        options={"ignoreNullFields": "false"},
    )
    row_hash = F.xxhash64(canonical_row)
    result = (
        df.agg(
            F.count(F.lit(1)).alias("row_count"),
            F.countDistinct(
                F.struct(*[F.col(column) for column in keys])
            ).alias("distinct_key_count"),
            F.coalesce(
                F.sum(F.when(any_null, 1).otherwise(0)),
                F.lit(0),
            ).alias("null_key_count"),
            F.coalesce(
                F.sum(row_hash.cast("decimal(38,0)")),
                F.lit(Decimal(0)),
            ).alias("hash_sum"),
            F.coalesce(F.min(row_hash), F.lit(0)).alias("hash_min"),
            F.coalesce(F.max(row_hash), F.lit(0)).alias("hash_max"),
        )
        .first()
    )
    row_count = int(result["row_count"])
    distinct_key_count = int(result["distinct_key_count"])
    checksum_payload = "|".join(
        [
            str(row_count),
            str(result["hash_sum"]),
            str(result["hash_min"]),
            str(result["hash_max"]),
        ]
    )
    return ContentSummary(
        row_count=row_count,
        distinct_key_count=distinct_key_count,
        null_key_count=int(result["null_key_count"]),
        duplicate_key_count=row_count - distinct_key_count,
        content_checksum=hashlib.sha256(
            checksum_payload.encode("utf-8")
        ).hexdigest(),
    )


def build_input_snapshot_id(
    run_date: date,
    source_summaries: dict[str, ContentSummary],
    *,
    contract_version: str = INPUT_CONTRACT_VERSION,
) -> str:
    """Build a content identity that deliberately excludes Delta versions."""
    if not source_summaries:
        raise ValueError("At least one source summary is required")
    identity = [contract_version, run_date.isoformat()]
    identity.extend(
        f"{name}:{summary.content_checksum}"
        for name, summary in sorted(source_summaries.items())
    )
    digest = hashlib.sha256("|".join(identity).encode("utf-8")).hexdigest()
    return f"scoring_inputs_{run_date:%Y%m%d}_{digest[:20]}"


def latest_delta_version(spark: Any, table: str) -> int:
    row = spark.sql(
        f"DESCRIBE HISTORY {quote_qualified_identifier(table)} LIMIT 1"
    ).first()
    if row is None:
        raise ValueError(f"No Delta history found for {table}")
    return int(row["version"])


def read_delta_version(spark: Any, table: str, version: int) -> DataFrame:
    if isinstance(version, bool) or not isinstance(version, int) or version < 0:
        raise ValueError("Delta version must be a non-negative integer")
    return spark.read.option("versionAsOf", version).table(table)

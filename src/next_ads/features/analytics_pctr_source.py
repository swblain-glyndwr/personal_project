"""Typed bindings and exact receipts for the Analytics pCTR feature source."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

from next_ads.common.delta_writes import (
    quote_qualified_identifier,
    replace_scope_by_name,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
SUPPORTED_SCOPES = {"PERSONAL_DEV", "SHARED_DEV"}
DEFAULT_RECEIPT_TABLE_NAME = (
    "next_uk_nextads_analytics_pctr_feature_source_receipts"
)


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Analytics pCTR source {field_name} must be non-blank")
    return value.strip()


def parse_reference_date(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or not value.strip():
        raise ValueError("reference_date must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError("reference_date must be YYYY-MM-DD") from exc


def parse_optional_delta_version(value: object) -> int | None:
    """Parse an exact Delta version, with ``latest`` resolved once per run."""
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(
            "Delta version must be a non-negative integer or latest"
        )
    if isinstance(value, int):
        if value < 0:
            raise ValueError(
                "Delta version must be a non-negative integer or latest"
            )
        return value
    if isinstance(value, str):
        normalised = value.strip().lower()
        if normalised in {"", "latest"}:
            return None
        if normalised.isdigit():
            return int(normalised)
    raise ValueError("Delta version must be a non-negative integer or latest")


@dataclass(frozen=True)
class AnalyticsPctrSourceDefinition:
    """Repository-approved physical source for one DEV route."""

    scope: str
    catalog: str
    schema: str
    table_name: str
    delta_version: int | None
    fixed_reference_date: date | None
    expected_table_id: str | None = None
    expected_schema_sha256: str | None = None
    producing_run_id: str | None = None
    receipt_table_name: str = DEFAULT_RECEIPT_TABLE_NAME

    def __post_init__(self) -> None:
        """Validate scope and prevent personal/shared namespace crossover."""
        scope = self.scope.strip().upper()
        if scope not in SUPPORTED_SCOPES:
            raise ValueError(
                "Analytics pCTR source scope must be PERSONAL_DEV or SHARED_DEV"
            )
        object.__setattr__(self, "scope", scope)
        for field_name in (
            "catalog",
            "schema",
            "table_name",
            "receipt_table_name",
        ):
            object.__setattr__(
                self,
                field_name,
                _required_text(getattr(self, field_name), field_name),
            )
        if self.delta_version is not None:
            parse_optional_delta_version(self.delta_version)
        if self.expected_schema_sha256 is not None:
            digest = self.expected_schema_sha256.strip().lower()
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise ValueError(
                    "Analytics pCTR source expected_schema_sha256 must be a "
                    "64-character lowercase hexadecimal digest"
                )
            object.__setattr__(self, "expected_schema_sha256", digest)
        if self.scope == "PERSONAL_DEV" and self.schema in {
            "nextads_feature_store",
            "nextads_integration",
        }:
            raise ValueError(
                "A personal Analytics pCTR binding cannot use a shared DEV schema"
            )
        if self.scope == "SHARED_DEV" and (
            self.catalog != "marketingdata_dev"
            or self.schema != "nextads_integration"
        ):
            raise ValueError(
                "The shared DEV Analytics pCTR source must be in "
                "marketingdata_dev.nextads_integration"
            )

    @property
    def table_path(self) -> str:
        return ".".join((self.catalog, self.schema, self.table_name))

    @property
    def receipt_table_path(self) -> str:
        return ".".join(
            (self.catalog, self.schema, self.receipt_table_name)
        )

    def validate_target(self, *, catalog: str, schema: str) -> None:
        is_shared_dev = (
            catalog == "marketingdata_dev" and schema == "nextads_feature_store"
        )
        if is_shared_dev and self.scope != "SHARED_DEV":
            raise ValueError(
                "Shared DEV Feature Store writes require the shared Analytics "
                "pCTR source binding"
            )
        if not is_shared_dev and self.scope == "SHARED_DEV":
            raise ValueError(
                "Personal Feature Store writes require a personal Analytics "
                "pCTR source binding"
            )


@dataclass(frozen=True)
class DeltaSourceBinding:
    """Exact external Delta table receipt consumed by one feature build."""

    source_role: str
    table_path: str
    delta_version: int
    reference_date: date
    table_id: str | None = None
    schema_sha256: str | None = None
    reference_date_row_count: int | None = None
    producing_run_id: str | None = None

    def __post_init__(self) -> None:
        """Validate the exact Delta source receipt."""
        source_role = self.source_role.strip()
        table_path = self.table_path.strip()
        if not source_role:
            raise ValueError("Delta source role must be non-blank")
        if len(table_path.split(".")) != 3 or any(
            not part.strip() for part in table_path.split(".")
        ):
            raise ValueError(
                "Delta source table must be a three-part catalog.schema.table path"
            )
        if (
            isinstance(self.delta_version, bool)
            or not isinstance(self.delta_version, int)
            or self.delta_version < 0
        ):
            raise ValueError(
                "Delta source version must be a non-negative integer"
            )
        if not isinstance(self.reference_date, date):
            raise TypeError("Delta source reference_date must be a date")
        object.__setattr__(self, "source_role", source_role)
        object.__setattr__(self, "table_path", table_path)

    def as_dict(self) -> dict[str, object]:
        return {
            "source_role": self.source_role,
            "table_path": self.table_path,
            "table_id": self.table_id,
            "delta_version": self.delta_version,
            "reference_date": self.reference_date.isoformat(),
            "reference_date_row_count": self.reference_date_row_count,
            "schema_sha256": self.schema_sha256,
            "producing_run_id": self.producing_run_id,
        }


def load_analytics_pctr_source_definition(
    path: str | Path,
    *,
    catalog: str | None = None,
    schema: str | None = None,
) -> AnalyticsPctrSourceDefinition:
    binding_path = Path(path)
    if not binding_path.is_absolute():
        binding_path = PROJECT_ROOT / binding_path
    raw_document = yaml.safe_load(binding_path.read_text())
    raw = raw_document.get("analytics_pctr_source", {})
    fixed_reference_date_raw = raw.get("fixed_reference_date")
    substitutions = {
        "catalog": _required_text(catalog, "catalog") if catalog else None,
        "schema": _required_text(schema, "schema") if schema else None,
    }

    def resolve_location(field_name: str) -> str:
        value = _required_text(raw.get(field_name), field_name)
        if "{" not in value:
            return value
        missing = [
            name
            for name in ("catalog", "schema")
            if "{" + name + "}" in value and substitutions[name] is None
        ]
        if missing:
            raise ValueError(
                "Analytics pCTR source binding requires substitutions for "
                + ", ".join(missing)
            )
        try:
            return value.format(**substitutions)
        except (KeyError, ValueError) as exc:
            raise ValueError(
                f"Analytics pCTR source {field_name} has invalid placeholders"
            ) from exc

    return AnalyticsPctrSourceDefinition(
        scope=_required_text(raw.get("scope"), "scope"),
        catalog=resolve_location("catalog"),
        schema=resolve_location("schema"),
        table_name=_required_text(raw.get("table_name"), "table_name"),
        delta_version=parse_optional_delta_version(raw.get("delta_version")),
        fixed_reference_date=(
            parse_reference_date(fixed_reference_date_raw)
            if fixed_reference_date_raw not in {None, "", "requested"}
            else None
        ),
        expected_table_id=(
            _required_text(raw["expected_table_id"], "expected_table_id")
            if raw.get("expected_table_id") is not None
            else None
        ),
        expected_schema_sha256=raw.get("expected_schema_sha256"),
        producing_run_id=(
            str(raw["producing_run_id"])
            if raw.get("producing_run_id") is not None
            else None
        ),
        receipt_table_name=_required_text(
            raw.get("receipt_table_name", DEFAULT_RECEIPT_TABLE_NAME),
            "receipt_table_name",
        ),
    )


def latest_delta_version(spark: Any, table_path: str) -> int:
    row = spark.sql(
        f"DESCRIBE HISTORY {quote_qualified_identifier(table_path)} LIMIT 1"
    ).first()
    if row is None:
        raise ValueError(f"No Delta history found for {table_path}")
    return int(row["version"])


def _schema_sha256(frame: Any) -> str:
    schema_json = json.loads(frame.schema.json())
    canonical = json.dumps(
        schema_json,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _detail_value(row: Any, key: str) -> object:
    if hasattr(row, "asDict"):
        return row.asDict().get(key)
    if isinstance(row, dict):
        return row.get(key)
    return row[key]


def bind_analytics_pctr_source(
    spark: Any,
    *,
    definition: AnalyticsPctrSourceDefinition,
    reference_date: str | date,
) -> tuple[DeltaSourceBinding, Any]:
    """Resolve, validate and read one exact Analytics pCTR Delta version."""
    from pyspark.sql import functions as F

    resolved_reference_date = parse_reference_date(reference_date)
    if (
        definition.fixed_reference_date is not None
        and definition.fixed_reference_date != resolved_reference_date
    ):
        raise ValueError(
            "Analytics pCTR source binding is fixed to "
            f"{definition.fixed_reference_date.isoformat()}, not "
            f"{resolved_reference_date.isoformat()}"
        )
    resolved_version = (
        latest_delta_version(spark, definition.table_path)
        if definition.delta_version is None
        else definition.delta_version
    )
    detail = spark.sql(
        f"DESCRIBE DETAIL {quote_qualified_identifier(definition.table_path)}"
    ).first()
    if detail is None:
        raise ValueError(
            f"No Delta table detail found for {definition.table_path}"
        )
    table_id = str(_detail_value(detail, "id") or "").strip()
    if not table_id:
        raise ValueError(
            f"Delta table ID is unavailable for {definition.table_path}"
        )
    if (
        definition.expected_table_id is not None
        and table_id != definition.expected_table_id
    ):
        raise ValueError(
            "Analytics pCTR source table ID does not match its approved binding"
        )

    frame = spark.read.option("versionAsOf", resolved_version).table(
        definition.table_path
    )
    schema_sha256 = _schema_sha256(frame)
    if (
        definition.expected_schema_sha256 is not None
        and schema_sha256 != definition.expected_schema_sha256
    ):
        raise ValueError(
            "Analytics pCTR source schema does not match its approved binding"
        )
    column_lookup = {str(column).lower(): column for column in frame.columns}
    date_column = column_lookup.get("rundate") or column_lookup.get(
        "reference_date"
    )
    if date_column is None:
        raise ValueError(
            "Analytics pCTR source has no rundate or reference_date column"
        )
    reference_rows = frame.where(
        F.to_date(F.col(date_column)) == F.lit(resolved_reference_date)
    ).count()
    if reference_rows == 0:
        raise ValueError(
            "Analytics pCTR source has no rows for "
            f"{resolved_reference_date.isoformat()} at Delta version "
            f"{resolved_version}"
        )
    receipt = DeltaSourceBinding(
        source_role="analytics_pctr_features",
        table_path=definition.table_path,
        table_id=table_id,
        delta_version=resolved_version,
        reference_date=resolved_reference_date,
        reference_date_row_count=reference_rows,
        schema_sha256=schema_sha256,
        producing_run_id=definition.producing_run_id,
    )
    return receipt, frame


def serialise_source_binding(binding: DeltaSourceBinding) -> str:
    return json.dumps(binding.as_dict(), sort_keys=True, separators=(",", ":"))


def with_producing_run_id(
    binding: DeltaSourceBinding,
    producing_run_id: object,
) -> DeltaSourceBinding:
    run_id = _required_text(producing_run_id, "producing_run_id")
    return replace(binding, producing_run_id=run_id)


def create_analytics_pctr_receipt_table(
    spark: Any,
    definition: AnalyticsPctrSourceDefinition,
) -> str:
    table_path = definition.receipt_table_path
    spark.sql(
        f"""
CREATE TABLE IF NOT EXISTS {quote_qualified_identifier(table_path)} (
  receipt_correlation_id STRING NOT NULL,
  producing_run_id STRING NOT NULL,
  reference_date DATE NOT NULL,
  source_role STRING NOT NULL,
  source_table_path STRING NOT NULL,
  source_table_id STRING NOT NULL,
  source_delta_version BIGINT NOT NULL,
  source_schema_sha256 STRING NOT NULL,
  reference_date_row_count BIGINT NOT NULL,
  recorded_at TIMESTAMP NOT NULL
)
USING delta
""".strip()
    )
    return table_path


def persist_analytics_pctr_source_binding(
    spark: Any,
    *,
    definition: AnalyticsPctrSourceDefinition,
    binding: DeltaSourceBinding,
    receipt_correlation_id: object,
) -> str:
    """Persist one exact child-job receipt for the parent Feature Store run."""
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    correlation_id = _required_text(
        receipt_correlation_id,
        "receipt_correlation_id",
    )
    if not binding.producing_run_id:
        raise ValueError(
            "Analytics pCTR receipt requires the producing job run ID"
        )
    if not binding.table_id or not binding.schema_sha256:
        raise ValueError(
            "Analytics pCTR receipt requires table ID and schema checksum"
        )
    if binding.reference_date_row_count is None:
        raise ValueError(
            "Analytics pCTR receipt requires the reference-date row count"
        )
    table_path = create_analytics_pctr_receipt_table(spark, definition)
    schema = T.StructType(
        [
            T.StructField("receipt_correlation_id", T.StringType(), False),
            T.StructField("producing_run_id", T.StringType(), False),
            T.StructField("reference_date", T.DateType(), False),
            T.StructField("source_role", T.StringType(), False),
            T.StructField("source_table_path", T.StringType(), False),
            T.StructField("source_table_id", T.StringType(), False),
            T.StructField("source_delta_version", T.LongType(), False),
            T.StructField("source_schema_sha256", T.StringType(), False),
            T.StructField(
                "reference_date_row_count",
                T.LongType(),
                False,
            ),
        ]
    )
    row = spark.createDataFrame(
        [
            (
                correlation_id,
                binding.producing_run_id,
                binding.reference_date,
                binding.source_role,
                binding.table_path,
                binding.table_id,
                binding.delta_version,
                binding.schema_sha256,
                binding.reference_date_row_count,
            )
        ],
        schema=schema,
    ).withColumn("recorded_at", F.current_timestamp())
    replace_scope_by_name(
        row,
        table_path,
        {"receipt_correlation_id": correlation_id},
        spark=spark,
        build_id=correlation_id,
        attempt_id=binding.producing_run_id,
        commit_metadata={
            "operation": "analytics_pctr_source_receipt",
            "reference_date": binding.reference_date.isoformat(),
            "source_delta_version": binding.delta_version,
        },
    )
    return table_path


def load_analytics_pctr_source_binding(
    spark: Any,
    *,
    definition: AnalyticsPctrSourceDefinition,
    receipt_correlation_id: object,
    reference_date: str | date,
) -> DeltaSourceBinding:
    """Load the source receipt correlated to this Feature Store run."""
    from pyspark.sql import functions as F

    correlation_id = _required_text(
        receipt_correlation_id,
        "receipt_correlation_id",
    )
    resolved_date = parse_reference_date(reference_date)
    rows = (
        spark.table(definition.receipt_table_path)
        .where(F.col("receipt_correlation_id") == F.lit(correlation_id))
        .where(F.col("reference_date") == F.lit(resolved_date))
        .limit(2)
        .collect()
    )
    if len(rows) != 1:
        raise ValueError(
            "Expected exactly one Analytics pCTR receipt for Feature Store run "
            f"{correlation_id} and reference date "
            f"{resolved_date.isoformat()}, found "
            f"{len(rows)}"
        )
    row = rows[0]
    binding = DeltaSourceBinding(
        source_role=row["source_role"],
        table_path=row["source_table_path"],
        table_id=row["source_table_id"],
        delta_version=int(row["source_delta_version"]),
        reference_date=row["reference_date"],
        reference_date_row_count=int(row["reference_date_row_count"]),
        schema_sha256=row["source_schema_sha256"],
        producing_run_id=row["producing_run_id"],
    )
    if binding.table_path != definition.table_path:
        raise ValueError(
            "Analytics pCTR receipt does not match the approved source table"
        )
    return binding


def read_analytics_pctr_source_binding(
    spark: Any,
    *,
    definition: AnalyticsPctrSourceDefinition,
    binding: DeltaSourceBinding,
) -> Any:
    """Read and revalidate the exact Delta version recorded by the child job."""
    from pyspark.sql import functions as F

    if binding.table_path != definition.table_path:
        raise ValueError(
            "Analytics pCTR receipt does not match the approved source table"
        )
    detail = spark.sql(
        f"DESCRIBE DETAIL {quote_qualified_identifier(binding.table_path)}"
    ).first()
    table_id = str(_detail_value(detail, "id") or "").strip() if detail else ""
    if table_id != binding.table_id:
        raise ValueError(
            "Analytics pCTR source table identity changed after publication"
        )
    frame = spark.read.option("versionAsOf", binding.delta_version).table(
        binding.table_path
    )
    if _schema_sha256(frame) != binding.schema_sha256:
        raise ValueError(
            "Analytics pCTR source schema changed from the recorded receipt"
        )
    lookup = {str(column).lower(): column for column in frame.columns}
    date_column = lookup.get("rundate") or lookup.get("reference_date")
    if date_column is None:
        raise ValueError(
            "Analytics pCTR source has no rundate or reference_date column"
        )
    exact_rows = frame.where(
        F.to_date(F.col(date_column)) == F.lit(binding.reference_date)
    )
    row_count = exact_rows.count()
    if row_count != binding.reference_date_row_count:
        raise ValueError(
            "Analytics pCTR source row count changed from the recorded receipt"
        )
    return frame

"""Run feature-store quality checks."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from functools import reduce
from pathlib import Path
from typing import Any

from _registry_job import (
    configure_job_logging,
    log_owned_tables,
    parse_common_args,
    validate_builder_output_tables,
)
from dsutils.dbc import configure_spark
from next_ads.features import (
    load_feature_store_registry,
    normalize_schema_name,
)
from next_ads.features.materialization import (
    create_feature_engineering_client,
    feature_table_path,
    write_feature_table,
)
from next_ads.features.sql_contracts import extract_create_table_columns
from next_ads.features.theme_affinity import resolve_theme_reference_date
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


LOGGER = logging.getLogger(__name__)
QUALITY_TABLE_NAME = "next_uk_nextads_fs_feature_quality_events"
MANIFEST_LOG_PREFIX = "FEATURE_STORE_DEV_AUDIT_MANIFEST="
MAX_BUILD_EVIDENCE_AGE = timedelta(hours=6)
QUALITY_EVENT_INPUT_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), nullable=False),
        StructField("check_name", StringType(), nullable=False),
        StructField("run_timestamp", TimestampType(), nullable=False),
        StructField("reference_date", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("row_count", LongType(), nullable=True),
        StructField("distinct_key_count", LongType(), nullable=True),
        StructField("null_key_count", LongType(), nullable=True),
        StructField("duplicate_key_count", LongType(), nullable=True),
        StructField("freshness_timestamp", TimestampType(), nullable=True),
        StructField("metric_value", DoubleType(), nullable=True),
        StructField("details", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
    ]
)


def quality_audit_features(registry) -> tuple[object, ...]:
    """Return every implemented definition in deterministic registry order."""
    return registry.implemented_features


def _is_skipped_on_demand(reference_date: str | None) -> bool:
    return not reference_date or reference_date.strip().lower() in {
        "skip",
        "none",
    }


def feature_audit_scope(
    feature,
    reference_date: str,
    training_reference_date: str | None,
) -> dict[str, str | None]:
    """Resolve a contract's audit scope without touching Spark."""
    if feature.name == QUALITY_TABLE_NAME:
        return {"kind": "GENERATED_EVENTS", "date": reference_date}
    if feature.freshness == "on_demand":
        if _is_skipped_on_demand(training_reference_date):
            return {"kind": "SKIPPED", "date": None}
        return {
            "kind": "REQUESTED_PARTITION",
            "date": training_reference_date,
        }
    if feature.timestamp_key:
        return {"kind": "REQUESTED_PARTITION", "date": reference_date}
    return {"kind": "WHOLE_TABLE", "date": None}


def _canonical_data_type(data_type: str) -> str:
    return re.sub(r"\s+", "", data_type.upper().replace("NOT NULL", ""))


def _contract_schema_columns(
    contract_path: Path,
) -> tuple[tuple[str, str], ...]:
    return tuple(
        (name, _canonical_data_type(data_type))
        for name, data_type in extract_create_table_columns(
            contract_path.read_text()
        )
    )


def _schema_hash(columns: tuple[tuple[str, str], ...]) -> str:
    canonical_contract = json.dumps(columns, separators=(",", ":"))
    return hashlib.sha256(canonical_contract.encode("utf-8")).hexdigest()


def _actual_schema_columns(dataframe) -> tuple[tuple[str, str], ...]:
    return tuple(
        (
            field.name,
            _canonical_data_type(field.dataType.simpleString()),
        )
        for field in dataframe.schema.fields
    )


def _schema_evidence(
    dataframe,
    expected_columns: tuple[tuple[str, str], ...],
) -> tuple[str, str]:
    actual_columns = _actual_schema_columns(dataframe)
    return (
        _schema_hash(actual_columns),
        "MATCH" if actual_columns == expected_columns else "MISMATCH",
    )


def _view_contract_schema_columns(
    view_contract_path: Path,
    source_contract_path: Path,
) -> tuple[tuple[str, str], ...]:
    sql = view_contract_path.read_text()
    projection_match = re.search(
        r"\bSELECT\b(?P<projection>.*?)\bFROM\b",
        sql,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if projection_match is None:
        raise ValueError(
            f"Compatibility view contract has no SELECT projection: "
            f"{view_contract_path}"
        )
    column_names = tuple(
        column.strip().strip("`")
        for column in projection_match.group("projection").split(",")
        if column.strip()
    )
    source_types = dict(_contract_schema_columns(source_contract_path))
    missing_columns = sorted(set(column_names) - set(source_types))
    if missing_columns:
        raise ValueError(
            f"Compatibility view {view_contract_path.name} projects columns "
            "missing from its source contract: " + ", ".join(missing_columns)
        )
    return tuple((name, source_types[name]) for name in column_names)


def _status_for_count(count: int | None, status: str) -> str:
    if status == "SKIPPED":
        return "SKIPPED"
    if count is None:
        return "NOT_AVAILABLE"
    return "PASS" if count == 0 else "FAIL"


def _feature_manifest_entry(
    *,
    feature,
    table_path: str,
    scope: dict[str, str | None],
    row_count: int | None,
    distinct_key_count: int | None,
    null_key_count: int | None,
    duplicate_key_count: int | None,
    schema_hash: str,
    actual_schema_hash: str | None,
    schema_status: str,
    output_delta_version: int | None,
    freshness_timestamp: str | None,
    freshness_status: str,
    status: str,
    error: str | None = None,
    actual_primary_keys: tuple[str, ...] | None = None,
    actual_timestamp_keys: tuple[str, ...] | None = None,
    feature_metadata_status: str = "NOT_CHECKED",
    commit_contract: str | None = None,
    commit_table_name: str | None = None,
    commit_reference_date: str | None = None,
    commit_evidence_status: str = "NOT_CHECKED",
    output_version_scope: str = "LATEST_DELTA_COMMIT",
) -> dict[str, object]:
    return {
        "object_type": "FEATURE",
        "name": feature.name,
        "physical_path": table_path,
        "state": feature.state.value,
        "builder": feature.builder,
        "write_mode": feature.write_mode,
        "scope_kind": scope["kind"],
        "scope_date": scope["date"],
        "status": status,
        "row_count": row_count,
        "distinct_key_count": distinct_key_count,
        "null_key_count": null_key_count,
        "duplicate_key_count": duplicate_key_count,
        "null_key_status": _status_for_count(null_key_count, status),
        "duplicate_key_status": _status_for_count(duplicate_key_count, status),
        "contract_schema_hash": schema_hash,
        "actual_schema_hash": actual_schema_hash,
        "schema_status": schema_status,
        "output_delta_version": output_delta_version,
        "output_version_scope": output_version_scope,
        "freshness_timestamp": freshness_timestamp,
        "freshness_status": freshness_status,
        "freshness_evidence_kind": "NEXTADS_DELTA_COMMIT_METADATA",
        "actual_primary_keys": actual_primary_keys,
        "actual_timestamp_keys": actual_timestamp_keys,
        "feature_metadata_status": feature_metadata_status,
        "commit_contract": commit_contract,
        "commit_table_name": commit_table_name,
        "commit_reference_date": commit_reference_date,
        "commit_evidence_status": commit_evidence_status,
        "error": error,
    }


def _null_key_condition(primary_keys: tuple[str, ...]):
    return reduce(
        lambda left, right: left | right,
        (F.col(column).isNull() for column in primary_keys),
    )


def _quality_event(
    table_name: str,
    reference_date: str | None,
    run_timestamp: datetime,
    row_count: int | None,
    distinct_key_count: int | None,
    null_key_count: int | None,
    duplicate_key_count: int | None,
    freshness_timestamp: datetime | None,
    status: str,
    details: str,
) -> dict[str, object]:
    return {
        "table_name": table_name,
        "check_name": "primary_key_quality",
        "run_timestamp": run_timestamp,
        "reference_date": reference_date,
        "status": status.lower(),
        "row_count": row_count,
        "distinct_key_count": distinct_key_count,
        "null_key_count": null_key_count,
        "duplicate_key_count": duplicate_key_count,
        "freshness_timestamp": freshness_timestamp,
        "metric_value": float(row_count) if row_count is not None else None,
        "details": details,
        "created_at": run_timestamp,
    }


def _quality_counts(
    dataframe, primary_keys: tuple[str, ...]
) -> tuple[int, int, int, int]:
    null_key_condition = _null_key_condition(primary_keys)
    key_struct = F.struct(*(F.col(column) for column in primary_keys))
    counts = dataframe.agg(
        F.count("*").alias("row_count"),
        F.countDistinct(key_struct).alias("distinct_key_count"),
        F.sum(F.when(null_key_condition, F.lit(1)).otherwise(F.lit(0))).alias(
            "null_key_count"
        ),
        F.sum(F.when(~null_key_condition, F.lit(1)).otherwise(F.lit(0))).alias(
            "valid_key_row_count"
        ),
        F.countDistinct(
            F.when(~null_key_condition, key_struct)
        ).alias("valid_distinct_key_count"),
    ).collect()[0]
    valid_key_row_count = int(counts["valid_key_row_count"] or 0)
    valid_distinct_key_count = int(
        counts["valid_distinct_key_count"] or 0
    )
    return (
        int(counts["row_count"] or 0),
        int(counts["distinct_key_count"] or 0),
        int(counts["null_key_count"] or 0),
        max(valid_key_row_count - valid_distinct_key_count, 0),
    )


def _quality_status(
    row_count: int,
    null_key_count: int,
    duplicate_key_count: int,
    schema_status: str,
    freshness_status: str = "PASS",
    feature_metadata_status: str = "PASS",
    commit_evidence_status: str = "PASS",
) -> str:
    if (
        row_count > 0
        and null_key_count == 0
        and duplicate_key_count == 0
        and schema_status == "MATCH"
        and freshness_status == "PASS"
        and feature_metadata_status == "PASS"
        and commit_evidence_status == "PASS"
    ):
        return "PASS"
    return "FAIL"


def _normalise_delta_timestamp(value: object | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalise_key_metadata(value: object | None) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(str(key) for key in value)


def _feature_table_metadata_evidence(
    feature_engineering_client,
    table_path: str,
    feature,
) -> dict[str, object]:
    metadata = feature_engineering_client.get_table(name=table_path)
    actual_primary_keys = _normalise_key_metadata(
        getattr(metadata, "primary_keys", None)
    )
    timestamp_value = getattr(metadata, "timestamp_keys", None)
    if timestamp_value is None:
        timestamp_value = getattr(metadata, "timeseries_columns", None)
    if timestamp_value is None:
        timestamp_value = getattr(metadata, "timeseries_column", None)
    actual_timestamp_keys = _normalise_key_metadata(timestamp_value)
    expected_timestamp_keys = (
        (feature.timestamp_key,) if feature.timestamp_key else ()
    )
    expected_entity_keys = tuple(
        key for key in feature.primary_keys if key != feature.timestamp_key
    )
    status = (
        "PASS"
        if actual_primary_keys == expected_entity_keys
        and actual_timestamp_keys == expected_timestamp_keys
        else "FAIL"
    )
    return {
        "actual_primary_keys": actual_primary_keys,
        "actual_timestamp_keys": actual_timestamp_keys,
        "feature_metadata_status": status,
    }


def _parse_feature_commit_metadata(value: object | None) -> dict[str, object]:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _latest_delta_evidence(
    spark,
    table_path: str,
) -> tuple[int | None, datetime | None, dict[str, object]]:
    try:
        rows = spark.sql(f"DESCRIBE HISTORY {table_path} LIMIT 1").collect()
    except Exception as exc:  # Databricks can restrict history independently.
        LOGGER.warning(
            "Could not read Delta history for %s: %s",
            table_path,
            exc,
        )
        return None, None, {}
    if not rows:
        return None, None, {}
    row = rows[0]
    if hasattr(row, "asDict"):
        row = row.asDict()
    try:
        version = row["version"]
    except (KeyError, TypeError):
        version = None
    try:
        timestamp = row["timestamp"]
    except (KeyError, TypeError):
        timestamp = None
    try:
        user_metadata = row["userMetadata"]
    except (KeyError, TypeError):
        user_metadata = None
    return (
        int(version) if version is not None else None,
        _normalise_delta_timestamp(timestamp),
        _parse_feature_commit_metadata(user_metadata),
    )


def _latest_delta_version(spark, table_path: str) -> int | None:
    return _latest_delta_evidence(spark, table_path)[0]


def _freshness_evidence(
    delta_timestamp: datetime | None,
    run_timestamp: datetime,
    commit_metadata: dict[str, object] | None = None,
    expected_table_name: str | None = None,
    expected_reference_date: str | None = None,
) -> tuple[str | None, str, str]:
    if delta_timestamp is None:
        return None, "NOT_CHECKED", "NOT_CHECKED"
    timestamp = delta_timestamp.astimezone(timezone.utc)
    recency_status = (
        "PASS"
        if timestamp >= run_timestamp - MAX_BUILD_EVIDENCE_AGE
        else "FAIL"
    )
    metadata = commit_metadata or {}
    commit_evidence_status = (
        "PASS"
        if metadata.get("contract") == "nextads_feature_build/v1"
        and metadata.get("table_name") == expected_table_name
        and metadata.get("reference_date") == expected_reference_date
        else "FAIL"
    )
    status = (
        "PASS"
        if recency_status == "PASS" and commit_evidence_status == "PASS"
        else "FAIL"
    )
    return timestamp.isoformat(), status, commit_evidence_status


def _event_from_manifest_entry(
    entry: dict[str, object],
    run_timestamp: datetime,
    audit_reference_date: str,
) -> dict[str, object]:
    return _quality_event(
        table_name=str(entry["name"]),
        reference_date=audit_reference_date,
        run_timestamp=run_timestamp,
        row_count=entry["row_count"],
        distinct_key_count=entry["distinct_key_count"],
        null_key_count=entry["null_key_count"],
        duplicate_key_count=entry["duplicate_key_count"],
        freshness_timestamp=_normalise_delta_timestamp(
            entry["freshness_timestamp"]
        ),
        status=str(entry["status"]),
        details=json.dumps(entry, sort_keys=True, separators=(",", ":")),
    )


def _audit_feature(
    spark,
    feature_engineering_client,
    feature,
    registry,
    target_catalog: str,
    target_schema: str,
    reference_date: str,
    training_reference_date: str | None,
    run_timestamp: datetime,
) -> tuple[dict[str, object], dict[str, object]]:
    table_path = feature_table_path(
        feature.name,
        target_catalog,
        target_schema,
        registry,
    )
    scope = feature_audit_scope(
        feature,
        reference_date,
        training_reference_date,
    )
    expected_schema = _contract_schema_columns(
        registry.sql_contract_path(feature.name)
    )
    schema_hash = _schema_hash(expected_schema)
    try:
        metadata_evidence = _feature_table_metadata_evidence(
            feature_engineering_client,
            table_path,
            feature,
        )
    except Exception as exc:
        entry = _feature_manifest_entry(
            feature=feature,
            table_path=table_path,
            scope=scope,
            row_count=None,
            distinct_key_count=None,
            null_key_count=None,
            duplicate_key_count=None,
            schema_hash=schema_hash,
            actual_schema_hash=None,
            schema_status="NOT_CHECKED",
            output_delta_version=None,
            freshness_timestamp=None,
            freshness_status="NOT_CHECKED",
            status="FAIL",
            feature_metadata_status="FAIL",
            error=f"Feature metadata unavailable: {type(exc).__name__}: {exc}"[
                :1000
            ],
        )
        return (
            _event_from_manifest_entry(entry, run_timestamp, reference_date),
            entry,
        )

    if scope["kind"] == "SKIPPED":
        metadata_status = str(metadata_evidence["feature_metadata_status"])
        entry = _feature_manifest_entry(
            feature=feature,
            table_path=table_path,
            scope=scope,
            row_count=None,
            distinct_key_count=None,
            null_key_count=None,
            duplicate_key_count=None,
            schema_hash=schema_hash,
            actual_schema_hash=None,
            schema_status="SKIPPED",
            output_delta_version=None,
            freshness_timestamp=None,
            freshness_status="SKIPPED",
            status="SKIPPED" if metadata_status == "PASS" else "FAIL",
            **metadata_evidence,
            error=(
                None
                if metadata_status == "PASS"
                else "Live Feature Engineering keys do not match the registry"
            ),
        )
        return (
            _event_from_manifest_entry(
                entry,
                run_timestamp,
                reference_date,
            ),
            entry,
        )

    actual_schema_hash = None
    schema_status = "UNAVAILABLE"
    try:
        dataframe = spark.table(table_path)
        actual_schema_hash, schema_status = _schema_evidence(
            dataframe,
            expected_schema,
        )
        if scope["kind"] == "REQUESTED_PARTITION":
            dataframe = dataframe.where(
                F.col(feature.timestamp_key)
                == F.lit(scope["date"]).cast("date")
            )

        (
            row_count,
            distinct_key_count,
            null_key_count,
            duplicate_key_count,
        ) = _quality_counts(
            dataframe,
            feature.primary_keys,
        )
        (
            output_delta_version,
            delta_timestamp,
            commit_metadata,
        ) = _latest_delta_evidence(
            spark,
            table_path,
        )
        expected_commit_reference_date = str(scope["date"] or reference_date)
        (
            freshness_timestamp,
            freshness_status,
            commit_evidence_status,
        ) = _freshness_evidence(
            delta_timestamp,
            run_timestamp,
            commit_metadata,
            feature.name,
            expected_commit_reference_date,
        )
        status = _quality_status(
            row_count,
            null_key_count,
            duplicate_key_count,
            schema_status,
            freshness_status,
            str(metadata_evidence["feature_metadata_status"]),
            commit_evidence_status,
        )
        errors = []
        if schema_status == "MISMATCH":
            errors.append(
                "live schema does not match the ordered contract columns/types"
            )
        if freshness_status != "PASS":
            errors.append(
                "latest Delta commit is missing, stale or not tagged for the "
                "audited table/reference date"
            )
        if metadata_evidence["feature_metadata_status"] != "PASS":
            errors.append(
                "live Feature Engineering keys do not match the registry"
            )
        entry = _feature_manifest_entry(
            feature=feature,
            table_path=table_path,
            scope=scope,
            row_count=row_count,
            distinct_key_count=distinct_key_count,
            null_key_count=null_key_count,
            duplicate_key_count=duplicate_key_count,
            schema_hash=schema_hash,
            actual_schema_hash=actual_schema_hash,
            schema_status=schema_status,
            output_delta_version=output_delta_version,
            freshness_timestamp=freshness_timestamp,
            freshness_status=freshness_status,
            status=status,
            **metadata_evidence,
            commit_contract=str(commit_metadata.get("contract") or "") or None,
            commit_table_name=(
                str(commit_metadata.get("table_name") or "") or None
            ),
            commit_reference_date=(
                str(commit_metadata.get("reference_date") or "") or None
            ),
            commit_evidence_status=commit_evidence_status,
            error="; ".join(errors) or None,
        )
    except Exception as exc:
        entry = _feature_manifest_entry(
            feature=feature,
            table_path=table_path,
            scope=scope,
            row_count=None,
            distinct_key_count=None,
            null_key_count=None,
            duplicate_key_count=None,
            schema_hash=schema_hash,
            actual_schema_hash=actual_schema_hash,
            schema_status=schema_status,
            output_delta_version=None,
            freshness_timestamp=None,
            freshness_status="UNAVAILABLE",
            status="FAIL",
            **metadata_evidence,
            error=f"{type(exc).__name__}: {exc}"[:1000],
        )
    return (
        _event_from_manifest_entry(entry, run_timestamp, reference_date),
        entry,
    )


def validate_feature_audit_coverage(
    feature_entries: list[dict[str, object]],
    registry,
) -> tuple[str, ...]:
    """Require one manifest entry for every implemented definition."""
    expected = tuple(
        feature.name for feature in quality_audit_features(registry)
    )
    actual = tuple(str(entry["name"]) for entry in feature_entries)
    duplicates = sorted({name for name in actual if actual.count(name) > 1})
    missing = sorted(set(expected) - set(actual))
    unexpected = sorted(set(actual) - set(expected))
    if duplicates or missing or unexpected:
        details = []
        if duplicates:
            details.append("duplicates=" + ",".join(duplicates))
        if missing:
            details.append("missing=" + ",".join(missing))
        if unexpected:
            details.append("unexpected=" + ",".join(unexpected))
        raise ValueError(
            "Feature-store audit coverage does not match the registry: "
            + " ".join(details)
        )
    return expected


def _view_definition_status(
    spark,
    view_path: str,
    expected_source_path: str,
) -> str:
    rows = spark.sql(f"SHOW CREATE TABLE {view_path}").collect()
    if not rows:
        return "FAIL"
    row = rows[0]
    if hasattr(row, "asDict"):
        row = row.asDict()
    if isinstance(row, dict):
        statement = next(iter(row.values()), "")
    else:
        statement = row[0]
    normalized_statement = re.sub(
        r"\s+",
        " ",
        str(statement).replace("`", "").lower(),
    )
    normalized_source = expected_source_path.replace("`", "").lower()
    return "PASS" if normalized_source in normalized_statement else "FAIL"


def _audit_implemented_compatibility_view(
    spark,
    registry,
    view: dict[str, Any],
    source_feature,
    source_entry: dict[str, object],
    view_path: str,
) -> dict[str, object]:
    view_name = str(view["name"])
    expected_schema = _view_contract_schema_columns(
        registry.view_contract_path(view_name),
        registry.sql_contract_path(source_feature.name),
    )
    contract_schema_hash = _schema_hash(expected_schema)
    scope_kind = source_entry["scope_kind"]
    scope_date = source_entry["scope_date"]
    actual_schema_hash = None
    schema_status = "UNAVAILABLE"
    view_definition_status = "UNAVAILABLE"
    source_row_parity_status = "UNAVAILABLE"

    try:
        dataframe = spark.table(view_path)
        actual_schema_hash, schema_status = _schema_evidence(
            dataframe,
            expected_schema,
        )
        if (
            scope_date is not None
            and source_feature.timestamp_key
            and source_feature.timestamp_key in dataframe.columns
        ):
            dataframe = dataframe.where(
                F.col(source_feature.timestamp_key)
                == F.lit(scope_date).cast("date")
            )
        (
            row_count,
            distinct_key_count,
            null_key_count,
            duplicate_key_count,
        ) = _quality_counts(
            dataframe,
            source_feature.primary_keys,
        )
        view_definition_status = _view_definition_status(
            spark,
            view_path,
            str(source_entry["physical_path"]),
        )
        source_row_parity_status = (
            "PASS"
            if row_count == source_entry["row_count"]
            and distinct_key_count == source_entry["distinct_key_count"]
            and null_key_count == source_entry["null_key_count"]
            and duplicate_key_count == source_entry["duplicate_key_count"]
            else "FAIL"
        )
        ready = (
            source_entry["status"] == "PASS"
            and schema_status == "MATCH"
            and view_definition_status == "PASS"
            and source_row_parity_status == "PASS"
            and _quality_status(
                row_count,
                null_key_count,
                duplicate_key_count,
                schema_status,
            )
            == "PASS"
        )
        status = "READY" if ready else "FAIL"
        error = None
        if not ready:
            error = (
                "Physical compatibility view did not pass source, schema, "
                "definition, row/key parity, non-empty, null-key and "
                "duplicate-key checks"
            )
    except Exception as exc:
        status = "FAIL"
        row_count = None
        distinct_key_count = None
        null_key_count = None
        duplicate_key_count = None
        error = f"{type(exc).__name__}: {exc}"[:1000]

    return {
        "object_type": "COMPATIBILITY_VIEW",
        "name": view_name,
        "physical_path": view_path,
        "state": "COMPATIBILITY_VIEW",
        "source_state": source_feature.state.value,
        "source_feature": source_feature.name,
        "builder": source_feature.builder,
        "write_mode": source_feature.write_mode,
        "scope_kind": scope_kind,
        "scope_date": scope_date,
        "status": status,
        "row_count": row_count,
        "distinct_key_count": distinct_key_count,
        "null_key_count": null_key_count,
        "duplicate_key_count": duplicate_key_count,
        "null_key_status": _status_for_count(null_key_count, status),
        "duplicate_key_status": _status_for_count(duplicate_key_count, status),
        "contract_schema_hash": contract_schema_hash,
        "actual_schema_hash": actual_schema_hash,
        "schema_status": schema_status,
        "view_definition_status": view_definition_status,
        "source_row_parity_status": source_row_parity_status,
        "output_delta_version": None,
        "output_version_scope": "SOURCE_FEATURE_VERSION",
        "freshness_timestamp": source_entry["freshness_timestamp"],
        "freshness_status": source_entry["freshness_status"],
        "freshness_evidence_kind": source_entry["freshness_evidence_kind"],
        "actual_primary_keys": source_entry["actual_primary_keys"],
        "actual_timestamp_keys": source_entry["actual_timestamp_keys"],
        "feature_metadata_status": source_entry["feature_metadata_status"],
        "commit_contract": source_entry["commit_contract"],
        "commit_table_name": source_entry["commit_table_name"],
        "commit_reference_date": source_entry["commit_reference_date"],
        "commit_evidence_status": source_entry["commit_evidence_status"],
        "error": error,
    }


def compatibility_view_manifest_entries(
    spark,
    registry,
    feature_entries: list[dict[str, object]],
    target_catalog: str,
    target_schema: str,
) -> list[dict[str, object]]:
    """Audit implemented views and leave scaffold-backed views blocked."""
    feature_entries_by_name = {
        str(entry["name"]): entry for entry in feature_entries
    }
    namespace = f"{target_catalog}.{normalize_schema_name(target_schema)}"
    view_entries = []

    for view in registry.compatibility_views:
        source_feature = registry.table_spec(str(view["source_feature"]))
        source_entry = feature_entries_by_name.get(source_feature.name)
        view_name = str(view["name"])
        view_path = f"{namespace}.{view_name}"
        expected_schema = _view_contract_schema_columns(
            registry.view_contract_path(view_name),
            registry.sql_contract_path(source_feature.name),
        )
        if not source_feature.implemented:
            view_entries.append(
                {
                    "object_type": "COMPATIBILITY_VIEW",
                    "name": view_name,
                    "physical_path": view_path,
                    "state": "COMPATIBILITY_VIEW",
                    "source_state": source_feature.state.value,
                    "source_feature": source_feature.name,
                    "builder": source_feature.builder,
                    "write_mode": source_feature.write_mode,
                    "scope_kind": "BLOCKED",
                    "scope_date": None,
                    "status": "BLOCKED",
                    "row_count": None,
                    "distinct_key_count": None,
                    "null_key_count": None,
                    "duplicate_key_count": None,
                    "null_key_status": "BLOCKED",
                    "duplicate_key_status": "BLOCKED",
                    "contract_schema_hash": _schema_hash(expected_schema),
                    "actual_schema_hash": None,
                    "schema_status": "BLOCKED",
                    "view_definition_status": "BLOCKED",
                    "source_row_parity_status": "BLOCKED",
                    "output_delta_version": None,
                    "output_version_scope": "BLOCKED",
                    "freshness_timestamp": None,
                    "freshness_status": "BLOCKED",
                    "freshness_evidence_kind": (
                        "NEXTADS_DELTA_COMMIT_METADATA"
                    ),
                    "actual_primary_keys": None,
                    "actual_timestamp_keys": None,
                    "feature_metadata_status": "BLOCKED",
                    "commit_contract": None,
                    "commit_table_name": None,
                    "commit_reference_date": None,
                    "commit_evidence_status": "BLOCKED",
                    "error": "missing_contracts="
                    + ",".join(source_feature.missing_contracts),
                }
            )
            continue
        if source_entry is None:
            raise ValueError(
                f"Compatibility view {view_name} has no source audit entry "
                f"for {source_feature.name}"
            )
        view_entries.append(
            _audit_implemented_compatibility_view(
                spark,
                registry,
                view,
                source_feature,
                source_entry,
                view_path,
            )
        )
    return view_entries


def build_dev_audit_manifest(
    spark,
    registry,
    feature_entries: list[dict[str, object]],
    target_catalog: str,
    target_schema: str,
    reference_date: str,
    run_timestamp: datetime,
) -> dict[str, object]:
    """Build the deterministic shared-DEV current-contract audit manifest."""
    expected_order = validate_feature_audit_coverage(feature_entries, registry)
    entries_by_name = {str(entry["name"]): entry for entry in feature_entries}
    ordered_features = [entries_by_name[name] for name in expected_order]
    view_entries = compatibility_view_manifest_entries(
        spark,
        registry,
        ordered_features,
        target_catalog,
        target_schema,
    )
    scaffold_count = len(
        [
            feature
            for feature in registry.offline_features
            if not feature.implemented
        ]
    )
    failed_current_contracts = [
        str(entry["name"])
        for entry in [*ordered_features, *view_entries]
        if entry["status"] == "FAIL"
    ]
    overall_status = (
        "CURRENT_IMPLEMENTED_FAIL"
        if failed_current_contracts
        else "CURRENT_IMPLEMENTED_PASS"
    )
    skipped_current_contracts = [
        str(entry["name"])
        for entry in ordered_features
        if entry["status"] == "SKIPPED"
    ]
    current_implemented_complete = (
        not failed_current_contracts
        and not skipped_current_contracts
        and all(
            entry["status"] == "READY"
            for entry in view_entries
            if entry["source_state"] != "SCAFFOLD"
        )
    )
    dev_complete = (
        current_implemented_complete
        and scaffold_count == 0
        and all(entry["status"] == "READY" for entry in view_entries)
    )
    return {
        "manifest_version": "offline_feature_audit/v1",
        "generated_at": run_timestamp.isoformat(),
        "reference_date": reference_date,
        "implemented_count": len(ordered_features),
        "compatibility_view_count": len(view_entries),
        "scaffold_count": scaffold_count,
        "overall_status": overall_status,
        "failed_current_contracts": failed_current_contracts,
        "skipped_current_contracts": skipped_current_contracts,
        "current_implemented_complete": current_implemented_complete,
        "dev_complete": dev_complete,
        "contracts": [*ordered_features, *view_entries],
    }


def serialize_dev_audit_manifest(manifest: dict[str, object]) -> str:
    """Serialize with stable key ordering and no incidental whitespace."""
    return json.dumps(manifest, sort_keys=True, separators=(",", ":"))


def main() -> None:
    args = parse_common_args()
    configure_job_logging(args.log_level)
    log_owned_tables("quality_checks", args)

    spark = configure_spark()
    registry = load_feature_store_registry()
    feature_engineering_client = create_feature_engineering_client()
    validate_builder_output_tables(
        "quality_checks",
        (QUALITY_TABLE_NAME,),
        registry,
    )
    target_catalog = args.catalog or registry.default_catalog
    target_schema = args.schema or registry.default_schema
    source_catalog = args.theme_source_catalog or target_catalog
    reference_date = resolve_theme_reference_date(
        spark,
        source_catalog,
        args.theme_source_schema,
        args.theme_table_prefix,
        args.reference_date,
    )
    run_timestamp = datetime.now(timezone.utc)
    quality_events = []
    feature_entries = []
    quality_feature = registry.table_spec(QUALITY_TABLE_NAME)

    for feature in quality_audit_features(registry):
        if feature.name == QUALITY_TABLE_NAME:
            continue
        event, entry = _audit_feature(
            spark,
            feature_engineering_client,
            feature,
            registry,
            target_catalog,
            target_schema,
            reference_date,
            args.theme_training_reference_date,
            run_timestamp,
        )
        quality_events.append(event)
        feature_entries.append(entry)

    quality_table_path = feature_table_path(
        QUALITY_TABLE_NAME,
        target_catalog,
        target_schema,
        registry,
    )
    quality_event_count = len(quality_audit_features(registry))
    quality_scope = feature_audit_scope(
        quality_feature,
        reference_date,
        args.theme_training_reference_date,
    )
    quality_expected_schema = _contract_schema_columns(
        registry.sql_contract_path(QUALITY_TABLE_NAME)
    )
    quality_metadata_error = None
    try:
        quality_metadata_evidence = _feature_table_metadata_evidence(
            feature_engineering_client,
            quality_table_path,
            quality_feature,
        )
    except Exception as exc:
        quality_metadata_evidence = {
            "actual_primary_keys": None,
            "actual_timestamp_keys": None,
            "feature_metadata_status": "FAIL",
        }
        quality_metadata_error = (
            f"Feature metadata unavailable: {type(exc).__name__}: {exc}"[:1000]
        )
    quality_entry = _feature_manifest_entry(
        feature=quality_feature,
        table_path=quality_table_path,
        scope=quality_scope,
        row_count=quality_event_count,
        distinct_key_count=quality_event_count,
        null_key_count=0,
        duplicate_key_count=0,
        schema_hash=_schema_hash(quality_expected_schema),
        actual_schema_hash=None,
        schema_status="PENDING_WRITE",
        output_delta_version=None,
        freshness_timestamp=None,
        freshness_status="PENDING_WRITE",
        status=(
            "PENDING"
            if quality_metadata_evidence["feature_metadata_status"] == "PASS"
            else "FAIL"
        ),
        **quality_metadata_evidence,
        error=quality_metadata_error,
    )
    quality_events.append(
        _event_from_manifest_entry(
            quality_entry,
            run_timestamp,
            reference_date,
        )
    )
    feature_entries.append(quality_entry)

    write_error: Exception | None = None
    try:
        quality_df = spark.createDataFrame(
            quality_events,
            schema=QUALITY_EVENT_INPUT_SCHEMA,
        )
        written_table_path = write_feature_table(
            spark,
            QUALITY_TABLE_NAME,
            quality_df,
            catalog=target_catalog,
            schema=target_schema,
            reference_date=reference_date,
            replace_reference_date=False,
            feature_engineering_client=feature_engineering_client,
        )
        quality_entry["physical_path"] = written_table_path
        written_quality_df = spark.table(written_table_path)
        (
            quality_entry["actual_schema_hash"],
            quality_entry["schema_status"],
        ) = _schema_evidence(
            written_quality_df,
            quality_expected_schema,
        )
        (
            output_delta_version,
            delta_timestamp,
            commit_metadata,
        ) = _latest_delta_evidence(
            spark,
            written_table_path,
        )
        quality_entry["output_delta_version"] = output_delta_version
        (
            quality_entry["freshness_timestamp"],
            quality_entry["freshness_status"],
            quality_entry["commit_evidence_status"],
        ) = _freshness_evidence(
            delta_timestamp,
            run_timestamp,
            commit_metadata,
            QUALITY_TABLE_NAME,
            reference_date,
        )
        quality_entry["commit_contract"] = commit_metadata.get("contract")
        quality_entry["commit_table_name"] = commit_metadata.get("table_name")
        quality_entry["commit_reference_date"] = commit_metadata.get(
            "reference_date"
        )

        persisted_quality_entry = dict(quality_entry)
        persisted_quality_entry["status"] = "MANIFEST_ONLY"
        persisted_quality_entry["output_delta_version"] = None
        persisted_quality_entry["output_version_scope"] = (
            "EXACT_VERSION_IN_FINAL_MANIFEST"
        )
        persisted_quality_event = _event_from_manifest_entry(
            persisted_quality_entry,
            run_timestamp,
            reference_date,
        )
        write_feature_table(
            spark,
            QUALITY_TABLE_NAME,
            spark.createDataFrame(
                [persisted_quality_event],
                schema=QUALITY_EVENT_INPUT_SCHEMA,
            ),
            catalog=target_catalog,
            schema=target_schema,
            reference_date=reference_date,
            replace_reference_date=False,
            feature_engineering_client=feature_engineering_client,
        )

        (
            final_output_delta_version,
            final_delta_timestamp,
            final_commit_metadata,
        ) = _latest_delta_evidence(spark, written_table_path)
        quality_entry["output_delta_version"] = final_output_delta_version
        (
            quality_entry["freshness_timestamp"],
            quality_entry["freshness_status"],
            quality_entry["commit_evidence_status"],
        ) = _freshness_evidence(
            final_delta_timestamp,
            run_timestamp,
            final_commit_metadata,
            QUALITY_TABLE_NAME,
            reference_date,
        )
        quality_entry["commit_contract"] = final_commit_metadata.get(
            "contract"
        )
        quality_entry["commit_table_name"] = final_commit_metadata.get(
            "table_name"
        )
        quality_entry["commit_reference_date"] = final_commit_metadata.get(
            "reference_date"
        )
        quality_errors = []
        if quality_entry["schema_status"] != "MATCH":
            quality_errors.append(
                "live schema does not match the ordered contract columns/types"
            )
        if quality_entry["freshness_status"] != "PASS":
            quality_errors.append(
                "latest Delta commit is missing, stale or not tagged for the "
                "audited table/reference date"
            )
        if quality_entry["feature_metadata_status"] != "PASS":
            quality_errors.append(
                "live Feature Engineering keys do not match the registry"
            )
        quality_entry["status"] = "FAIL" if quality_errors else "PASS"
        quality_entry["error"] = "; ".join(quality_errors) or None
        LOGGER.info(
            "Wrote feature-store quality events; exact self evidence is in "
            "the final manifest: %s",
            written_table_path,
        )
    except Exception as exc:
        write_error = exc
        quality_entry["status"] = "FAIL"
        if quality_entry["schema_status"] == "PENDING_WRITE":
            quality_entry["schema_status"] = "UNAVAILABLE"
        if quality_entry["freshness_status"] == "PENDING_WRITE":
            quality_entry["freshness_status"] = "UNAVAILABLE"
        quality_entry["error"] = f"{type(exc).__name__}: {exc}"[:1000]

    manifest = build_dev_audit_manifest(
        spark,
        registry,
        feature_entries,
        target_catalog,
        target_schema,
        reference_date,
        run_timestamp,
    )
    LOGGER.info(
        "%s%s",
        MANIFEST_LOG_PREFIX,
        serialize_dev_audit_manifest(manifest),
    )

    if write_error is not None:
        raise write_error
    if manifest["overall_status"] != "CURRENT_IMPLEMENTED_PASS":
        raise ValueError(
            "Feature-store quality checks failed: "
            + ", ".join(manifest["failed_current_contracts"])
        )


if __name__ == "__main__":
    main()

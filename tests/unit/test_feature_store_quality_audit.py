import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FEATURE_JOB_ROOT = PROJECT_ROOT / "jobs" / "features" / "nextads"
sys.path.insert(0, str(FEATURE_JOB_ROOT))

from jobs.features.nextads import quality_checks  # noqa: E402
from next_ads.features import load_feature_store_registry  # noqa: E402


RUN_TIMESTAMP = datetime(2026, 8, 12, 21, 0, tzinfo=timezone.utc)
REFERENCE_DATE = "2026-08-12"


def _passing_feature_entries(registry):
    entries = []
    for feature in registry.implemented_features:
        scope = quality_checks.feature_audit_scope(
            feature,
            REFERENCE_DATE,
            "skip",
        )
        skipped = scope["kind"] == "SKIPPED"
        entries.append(
            quality_checks._feature_manifest_entry(
                feature=feature,
                table_path=(
                    "marketingdata_dev.nextads_feature_store." + feature.name
                ),
                scope=scope,
                row_count=None if skipped else 10,
                distinct_key_count=None if skipped else 10,
                null_key_count=None if skipped else 0,
                duplicate_key_count=None if skipped else 0,
                schema_hash="a" * 64,
                actual_schema_hash=None if skipped else "a" * 64,
                schema_status="SKIPPED" if skipped else "MATCH",
                output_delta_version=None if skipped else 7,
                freshness_timestamp=(
                    None if skipped else RUN_TIMESTAMP.isoformat()
                ),
                freshness_status="SKIPPED" if skipped else "PASS",
                status="SKIPPED" if skipped else "PASS",
            )
        )
    return entries


class _ViewFrame:
    columns = []


class _ViewSpark:
    def __init__(self, error=None):
        self.error = error
        self.table_calls = []
        self.sql_calls = []

    def table(self, table_path):
        self.table_calls.append(table_path)
        if self.error:
            raise self.error
        return _ViewFrame()

    def sql(self, query):
        self.sql_calls.append(query)
        if self.error:
            raise self.error
        source_table = (
            "next_uk_nextads_fs_pctr_model_input"
            if "next_uk_nextads_pctr_features_latest" in query
            else "next_uk_nextads_fs_theme_affinity_model_input"
        )
        return _HistoryResult(
            [
                {
                    "createtab_stmt": (
                        "CREATE VIEW example AS SELECT * FROM "
                        "marketingdata_dev.nextads_feature_store."
                        + source_table
                    )
                }
            ]
        )


def _stub_readable_view_checks(monkeypatch, *, row_count=10):
    monkeypatch.setattr(
        quality_checks,
        "_schema_evidence",
        lambda _dataframe, _expected: ("b" * 64, "MATCH"),
    )
    monkeypatch.setattr(
        quality_checks,
        "_quality_counts",
        lambda _dataframe, _keys: (row_count, row_count, 0, 0),
    )
    return _ViewSpark()


def test_quality_audit_inventory_matches_the_registered_implemented_contracts():
    registry = load_feature_store_registry()
    features = quality_checks.quality_audit_features(registry)

    assert len(features) == len(registry.implemented_features)
    assert {feature.name for feature in features} == {
        feature.name for feature in registry.implemented_features
    }
    assert not any(not feature.implemented for feature in features)


def test_quality_audit_scopes_cover_daily_latest_on_demand_and_generated_events():
    registry = load_feature_store_registry()

    daily = registry.table_spec("next_uk_nextads_fs_account_profile")
    latest = registry.table_spec("next_uk_nextads_fs_item_attributes_latest")
    training = registry.table_spec(
        "next_uk_nextads_fs_theme_affinity_training_input"
    )
    quality = registry.table_spec(quality_checks.QUALITY_TABLE_NAME)

    assert quality_checks.feature_audit_scope(
        daily, REFERENCE_DATE, "skip"
    ) == {"kind": "REQUESTED_PARTITION", "date": REFERENCE_DATE}
    assert quality_checks.feature_audit_scope(
        latest, REFERENCE_DATE, "skip"
    ) == {"kind": "WHOLE_TABLE", "date": None}
    assert quality_checks.feature_audit_scope(
        training, REFERENCE_DATE, "skip"
    ) == {"kind": "SKIPPED", "date": None}
    assert quality_checks.feature_audit_scope(
        training, REFERENCE_DATE, "2026-07-01"
    ) == {"kind": "REQUESTED_PARTITION", "date": "2026-07-01"}
    assert quality_checks.feature_audit_scope(
        quality, REFERENCE_DATE, "skip"
    ) == {"kind": "GENERATED_EVENTS", "date": REFERENCE_DATE}


def test_quality_event_matches_the_existing_sql_contract_fields():
    event = quality_checks._quality_event(
        table_name="example",
        reference_date=REFERENCE_DATE,
        run_timestamp=RUN_TIMESTAMP,
        row_count=10,
        distinct_key_count=10,
        null_key_count=0,
        duplicate_key_count=0,
        freshness_timestamp=RUN_TIMESTAMP,
        status="PASS",
        details="{}",
    )

    assert event["freshness_timestamp"] == RUN_TIMESTAMP
    assert event["created_at"] == RUN_TIMESTAMP
    assert "freshness_status" not in event
    assert set(event) == {
        "table_name",
        "check_name",
        "run_timestamp",
        "reference_date",
        "status",
        "row_count",
        "distinct_key_count",
        "null_key_count",
        "duplicate_key_count",
        "freshness_timestamp",
        "metric_value",
        "details",
        "created_at",
    }
    assert quality_checks.QUALITY_EVENT_INPUT_SCHEMA.fieldNames() == list(event)


def test_quality_event_uses_audit_date_for_whole_table_and_skipped_scopes():
    registry = load_feature_store_registry()
    entries = _passing_feature_entries(registry)
    whole_table_entry = next(
        entry for entry in entries if entry["scope_kind"] == "WHOLE_TABLE"
    )
    skipped_entry = next(
        entry for entry in entries if entry["scope_kind"] == "SKIPPED"
    )

    whole_table_event = quality_checks._event_from_manifest_entry(
        whole_table_entry,
        RUN_TIMESTAMP,
        REFERENCE_DATE,
    )
    skipped_event = quality_checks._event_from_manifest_entry(
        skipped_entry,
        RUN_TIMESTAMP,
        REFERENCE_DATE,
    )

    assert whole_table_entry["scope_date"] is None
    assert skipped_entry["scope_date"] is None
    assert whole_table_event["reference_date"] == REFERENCE_DATE
    assert skipped_event["reference_date"] == REFERENCE_DATE
    assert whole_table_event["freshness_timestamp"] == RUN_TIMESTAMP
    assert skipped_event["freshness_timestamp"] is None


def test_manifest_reports_registry_coverage_without_claiming_dev_complete(
    monkeypatch,
):
    registry = load_feature_store_registry()
    spark = _stub_readable_view_checks(monkeypatch)
    manifest = quality_checks.build_dev_audit_manifest(
        spark,
        registry,
        _passing_feature_entries(registry),
        "marketingdata_dev",
        "nextads_feature_store",
        REFERENCE_DATE,
        RUN_TIMESTAMP,
    )

    assert manifest["implemented_count"] == len(registry.implemented_features)
    assert manifest["compatibility_view_count"] == len(
        registry.compatibility_views
    )
    assert manifest["scaffold_count"] == (
        len(registry.offline_features) - len(registry.implemented_features)
    )
    assert manifest["overall_status"] == "CURRENT_IMPLEMENTED_PASS"
    assert manifest["skipped_current_contracts"] == [
        "next_uk_nextads_fs_shopping_bag_account_activity_90d",
        "next_uk_nextads_fs_theme_affinity_training_input"
    ]
    assert manifest["current_implemented_complete"] is False
    assert manifest["dev_complete"] is False
    assert len(manifest["contracts"]) == (
        len(registry.implemented_features) + len(registry.compatibility_views)
    )

    contracts = {entry["name"]: entry for entry in manifest["contracts"]}
    theme_view = contracts["next_uk_nextads_theme_affinity_features_latest"]
    pctr_view = contracts["next_uk_nextads_pctr_features_latest"]
    assert theme_view["status"] == "READY"
    assert theme_view["row_count"] == 10
    assert pctr_view["status"] == "READY"
    assert pctr_view["source_state"] == "COMPATIBILITY"
    assert pctr_view["row_count"] == 10

    required_fields = {
        "physical_path",
        "state",
        "builder",
        "scope_date",
        "row_count",
        "null_key_status",
        "duplicate_key_status",
        "contract_schema_hash",
        "actual_schema_hash",
        "schema_status",
        "output_delta_version",
        "freshness_timestamp",
        "freshness_status",
        "feature_metadata_status",
        "actual_primary_keys",
        "actual_timestamp_keys",
        "commit_evidence_status",
        "commit_reference_date",
        "output_version_scope",
    }
    assert all(
        required_fields <= set(entry) for entry in manifest["contracts"]
    )
    assert all(
        len(entry["contract_schema_hash"]) == 64
        for entry in manifest["contracts"]
    )


def test_manifest_serialization_is_stable_and_marks_current_failures(
    monkeypatch,
):
    registry = load_feature_store_registry()
    spark = _stub_readable_view_checks(monkeypatch)
    entries = _passing_feature_entries(registry)
    account_entry = next(
        entry
        for entry in entries
        if entry["name"] == "next_uk_nextads_fs_account_profile"
    )
    account_entry["status"] = "FAIL"
    account_entry["row_count"] = 0

    manifest = quality_checks.build_dev_audit_manifest(
        spark,
        registry,
        list(reversed(entries)),
        "marketingdata_dev",
        "nextads_feature_store",
        REFERENCE_DATE,
        RUN_TIMESTAMP,
    )
    first = quality_checks.serialize_dev_audit_manifest(manifest)
    second = quality_checks.serialize_dev_audit_manifest(manifest)

    assert first == second
    assert json.loads(first) == manifest
    assert manifest["overall_status"] == "CURRENT_IMPLEMENTED_FAIL"
    assert manifest["failed_current_contracts"] == [
        "next_uk_nextads_fs_account_profile"
    ]
    assert manifest["contracts"][0]["name"] == (
        "next_uk_nextads_fs_account_profile"
    )


def test_manifest_rejects_incomplete_implemented_coverage(monkeypatch):
    registry = load_feature_store_registry()
    spark = _stub_readable_view_checks(monkeypatch)
    entries = _passing_feature_entries(registry)[:-1]

    with pytest.raises(ValueError, match="missing=.*feature_quality_events"):
        quality_checks.build_dev_audit_manifest(
            spark,
            registry,
            entries,
            "marketingdata_dev",
            "nextads_feature_store",
            REFERENCE_DATE,
            RUN_TIMESTAMP,
        )


def test_implemented_view_is_physically_read_and_counted(monkeypatch):
    registry = load_feature_store_registry()
    spark = _stub_readable_view_checks(monkeypatch)

    entries = quality_checks.compatibility_view_manifest_entries(
        spark,
        registry,
        _passing_feature_entries(registry),
        "marketingdata_dev",
        "nextads_feature_store",
    )

    assert spark.table_calls == [
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_theme_affinity_features_latest",
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_pctr_features_latest",
    ]
    assert spark.sql_calls == [
        "SHOW CREATE TABLE marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_theme_affinity_features_latest",
        "SHOW CREATE TABLE marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_pctr_features_latest",
    ]
    assert entries[0]["status"] == "READY"
    assert entries[0]["row_count"] == 10
    assert entries[1]["status"] == "READY"


def test_unreadable_implemented_views_are_each_reported_as_failed(
    monkeypatch,
):
    registry = load_feature_store_registry()
    spark = _ViewSpark(error=RuntimeError("view missing"))

    entries = quality_checks.compatibility_view_manifest_entries(
        spark,
        registry,
        _passing_feature_entries(registry),
        "marketingdata_dev",
        "nextads_feature_store",
    )

    assert len(spark.table_calls) == 2
    assert not spark.sql_calls
    assert entries[0]["status"] == "FAIL"
    assert "view missing" in entries[0]["error"]
    assert entries[1]["status"] == "FAIL"
    assert "view missing" in entries[1]["error"]


def test_implemented_view_requires_source_row_and_key_parity(monkeypatch):
    registry = load_feature_store_registry()
    spark = _stub_readable_view_checks(monkeypatch, row_count=9)
    feature_entries = _passing_feature_entries(registry)

    entries = quality_checks.compatibility_view_manifest_entries(
        spark,
        registry,
        feature_entries,
        "marketingdata_dev",
        "nextads_feature_store",
    )

    assert entries[0]["status"] == "FAIL"
    assert entries[0]["source_row_parity_status"] == "FAIL"
    assert entries[0]["view_definition_status"] == "PASS"


class _FeatureTableMetadata:
    primary_keys = ["account_number"]
    timestamp_keys = ["reference_date"]


class _FeatureMetadataClient:
    def __init__(self, metadata=None):
        self.metadata = metadata or _FeatureTableMetadata()

    def get_table(self, *, name):
        self.name = name
        return self.metadata


def test_feature_metadata_evidence_matches_registry_keys_and_timestamp_key():
    registry = load_feature_store_registry()
    feature = registry.table_spec("next_uk_nextads_fs_account_profile")
    client = _FeatureMetadataClient()

    evidence = quality_checks._feature_table_metadata_evidence(
        client,
        "catalog.schema.table",
        feature,
    )

    assert client.name == "catalog.schema.table"
    assert evidence == {
        "actual_primary_keys": ("account_number",),
        "actual_timestamp_keys": ("reference_date",),
        "feature_metadata_status": "PASS",
    }


def test_feature_metadata_evidence_accepts_timestamp_in_primary_keys():
    registry = load_feature_store_registry()
    feature = registry.table_spec("next_uk_nextads_fs_account_profile")
    metadata = _FeatureTableMetadata()
    metadata.primary_keys = ["account_number", "reference_date"]
    client = _FeatureMetadataClient(metadata)

    evidence = quality_checks._feature_table_metadata_evidence(
        client,
        "catalog.schema.table",
        feature,
    )

    assert evidence == {
        "actual_primary_keys": ("account_number", "reference_date"),
        "actual_timestamp_keys": ("reference_date",),
        "feature_metadata_status": "PASS",
    }


def test_feature_metadata_evidence_accepts_timestamp_moved_to_end():
    registry = load_feature_store_registry()
    feature = registry.table_spec("next_uk_nextads_fs_labels_clicks")
    metadata = _FeatureTableMetadata()
    metadata.primary_keys = [
        "account_number",
        "advert_id",
        "location",
        "label_horizon_days",
        "session_date",
    ]
    metadata.timestamp_keys = ["session_date"]
    client = _FeatureMetadataClient(metadata)

    evidence = quality_checks._feature_table_metadata_evidence(
        client,
        "catalog.schema.table",
        feature,
    )

    assert evidence == {
        "actual_primary_keys": (
            "account_number",
            "advert_id",
            "location",
            "label_horizon_days",
            "session_date",
        ),
        "actual_timestamp_keys": ("session_date",),
        "feature_metadata_status": "PASS",
    }


def test_feature_metadata_evidence_rejects_reordered_entity_keys():
    registry = load_feature_store_registry()
    feature = registry.table_spec("next_uk_nextads_fs_labels_clicks")
    metadata = _FeatureTableMetadata()
    metadata.primary_keys = [
        "advert_id",
        "account_number",
        "location",
        "label_horizon_days",
        "session_date",
    ]
    metadata.timestamp_keys = ["session_date"]
    client = _FeatureMetadataClient(metadata)

    evidence = quality_checks._feature_table_metadata_evidence(
        client,
        "catalog.schema.table",
        feature,
    )

    assert evidence["feature_metadata_status"] == "FAIL"


def test_feature_metadata_evidence_rejects_incorrect_primary_keys():
    registry = load_feature_store_registry()
    feature = registry.table_spec("next_uk_nextads_fs_account_profile")
    metadata = _FeatureTableMetadata()
    metadata.primary_keys = ["account_number", "unexpected_key", "reference_date"]
    client = _FeatureMetadataClient(metadata)

    evidence = quality_checks._feature_table_metadata_evidence(
        client,
        "catalog.schema.table",
        feature,
    )

    assert evidence["feature_metadata_status"] == "FAIL"


class _DataType:
    def __init__(self, name):
        self.name = name

    def simpleString(self):  # noqa: N802 - mirrors Spark API
        return self.name


class _Field:
    def __init__(self, name, data_type):
        self.name = name
        self.dataType = _DataType(data_type)


class _SchemaFrame:
    def __init__(self, fields):
        self.schema = type("Schema", (), {"fields": fields})()


def test_ordered_schema_evidence_detects_type_and_order_drift():
    expected = (("account_number", "STRING"), ("reference_date", "DATE"))
    matching = _SchemaFrame(
        [_Field("account_number", "string"), _Field("reference_date", "date")]
    )
    drifted = _SchemaFrame(
        [_Field("reference_date", "date"), _Field("account_number", "bigint")]
    )

    matching_hash, matching_status = quality_checks._schema_evidence(
        matching,
        expected,
    )
    drifted_hash, drifted_status = quality_checks._schema_evidence(
        drifted,
        expected,
    )

    assert matching_hash == quality_checks._schema_hash(expected)
    assert matching_status == "MATCH"
    assert drifted_hash != matching_hash
    assert drifted_status == "MISMATCH"
    assert quality_checks._quality_status(10, 0, 0, drifted_status) == "FAIL"


class _HistoryResult:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class _HistorySpark:
    def __init__(self, rows=None, error=None):
        self.rows = rows or []
        self.error = error
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        if self.error:
            raise self.error
        return _HistoryResult(self.rows)


def test_delta_version_is_recorded_when_history_is_available():
    spark = _HistorySpark(rows=[{"version": 42}])

    assert (
        quality_checks._latest_delta_version(spark, "catalog.schema.table")
        == 42
    )
    assert spark.queries == ["DESCRIBE HISTORY catalog.schema.table LIMIT 1"]


def test_delta_evidence_records_version_and_normalised_timestamp():
    spark = _HistorySpark(
        rows=[
            {
                "version": 42,
                "timestamp": "2026-08-12T20:30:00Z",
                "userMetadata": json.dumps(
                    {
                        "contract": "nextads_feature_build/v1",
                        "reference_date": REFERENCE_DATE,
                        "table_name": "example",
                    }
                ),
            }
        ]
    )

    version, timestamp, metadata = quality_checks._latest_delta_evidence(
        spark,
        "catalog.schema.table",
    )

    assert version == 42
    assert timestamp == datetime(2026, 8, 12, 20, 30, tzinfo=timezone.utc)
    assert metadata == {
        "contract": "nextads_feature_build/v1",
        "reference_date": REFERENCE_DATE,
        "table_name": "example",
    }


def test_freshness_uses_latest_delta_commit_not_the_audit_clock():
    recent_commit = RUN_TIMESTAMP - timedelta(hours=1)
    stale_commit = RUN_TIMESTAMP - timedelta(hours=7)
    matching_metadata = {
        "contract": "nextads_feature_build/v1",
        "reference_date": REFERENCE_DATE,
        "table_name": "example",
    }

    assert quality_checks._freshness_evidence(
        recent_commit,
        RUN_TIMESTAMP,
        matching_metadata,
        "example",
        REFERENCE_DATE,
    ) == (recent_commit.isoformat(), "PASS", "PASS")
    assert quality_checks._freshness_evidence(
        stale_commit,
        RUN_TIMESTAMP,
        matching_metadata,
        "example",
        REFERENCE_DATE,
    ) == (stale_commit.isoformat(), "FAIL", "PASS")
    assert quality_checks._freshness_evidence(None, RUN_TIMESTAMP) == (
        None,
        "NOT_CHECKED",
        "NOT_CHECKED",
    )


def test_freshness_rejects_recent_commit_for_a_different_partition():
    recent_commit = RUN_TIMESTAMP - timedelta(hours=1)
    wrong_partition_metadata = {
        "contract": "nextads_feature_build/v1",
        "reference_date": "2026-08-11",
        "table_name": "example",
    }

    assert quality_checks._freshness_evidence(
        recent_commit,
        RUN_TIMESTAMP,
        wrong_partition_metadata,
        "example",
        REFERENCE_DATE,
    ) == (recent_commit.isoformat(), "FAIL", "FAIL")


def test_delta_version_is_optional_when_history_cannot_be_read(caplog):
    spark = _HistorySpark(error=RuntimeError("history unavailable"))

    assert (
        quality_checks._latest_delta_version(spark, "catalog.schema.table")
        is None
    )
    assert "Could not read Delta history" in caplog.text

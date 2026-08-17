from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from next_ads.common.delta_writes import DeltaWriteReceipt
from next_ads.features.feature_builds import (
    BUILDING,
    FAIL,
    FAILED,
    PASS,
    READY,
    VALIDATING,
    FeatureBuild,
    FeatureOutputBinding,
    FeatureSnapshotBinding,
    FeatureSourceBinding,
    mark_feature_build_failed,
    mark_feature_build_ready,
    mark_feature_snapshot_failed,
    mark_feature_snapshot_ready,
    prepare_feature_snapshot,
    resolve_ready_feature,
    select_latest_ready_snapshot,
)
from next_ads.features import feature_build_store
from next_ads.features.sql_contracts import extract_create_table_columns


PROJECT_ROOT = Path(__file__).resolve().parents[2]
REFERENCE_DATE = date(2026, 8, 1)
STARTED_AT = datetime(2026, 8, 1, 20, 0, tzinfo=timezone.utc)
COMMITTED_AT = STARTED_AT + timedelta(minutes=10)
VALIDATED_AT = STARTED_AT + timedelta(minutes=12)
COMPLETED_AT = STARTED_AT + timedelta(minutes=15)
REGISTRY_CHECKSUM = "a" * 64
SCHEMA_CHECKSUM = "b" * 64
VALUE_CHECKSUM = "c" * 64
BACKING_SCHEMA_CHECKSUM = "d" * 64
BUILD_ID = "feature-build-20260801"
BUILD_ATTEMPT_ID = "feature-build-20260801:attempt:0"
FEATURE_IDS = ("account_profile", "product_embeddings")


def _source(
    *,
    build_id: str = BUILD_ID,
    attempt_id: str = BUILD_ATTEMPT_ID,
    source_name: str = "customer_history",
) -> FeatureSourceBinding:
    return FeatureSourceBinding(
        feature_build_id=build_id,
        feature_build_attempt_id=attempt_id,
        reference_date=REFERENCE_DATE,
        source_name=source_name,
        source_table="marketingdata_prod.warehouse.customer_history",
        delta_version=41,
        schema_checksum=SCHEMA_CHECKSUM,
        row_count=100,
        captured_at=STARTED_AT,
    )


def _receipt(
    feature_id: str,
    *,
    build_id: str = BUILD_ID,
    attempt_id: str = BUILD_ATTEMPT_ID,
    delta_version: int = 42,
) -> DeltaWriteReceipt:
    return DeltaWriteReceipt(
        statement="INSERT INTO backing REPLACE WHERE attempt_id = '0'",
        attempts=2,
        receipt_id=f"receipt-{feature_id}-{attempt_id}",
        target_table=(
            "marketingdata_dev.nextads_feature_store_internal."
            f"{feature_id}_build_data"
        ),
        delta_version=delta_version,
        row_count=100,
        schema_checksum=BACKING_SCHEMA_CHECKSUM,
        build_id=build_id,
        attempt_id=attempt_id,
        git_commit="abc123",
        committed_at=COMMITTED_AT,
        write_duration_ms=1200,
    )


def _output(
    feature_id: str,
    *,
    build_id: str = BUILD_ID,
    attempt_id: str = BUILD_ATTEMPT_ID,
    delta_version: int = 42,
    validation_status: str = PASS,
    duplicate_key_count: int = 0,
) -> FeatureOutputBinding:
    return FeatureOutputBinding.from_delta_write_receipt(
        _receipt(
            feature_id,
            build_id=build_id,
            attempt_id=attempt_id,
            delta_version=delta_version,
        ),
        feature_build_id=build_id,
        feature_build_attempt_id=attempt_id,
        reference_date=REFERENCE_DATE,
        feature_id=feature_id,
        contract_schema_checksum=SCHEMA_CHECKSUM,
        output_schema_checksum=SCHEMA_CHECKSUM,
        value_checksum=VALUE_CHECKSUM,
        validated_at=VALIDATED_AT,
        null_key_count=0,
        duplicate_key_count=duplicate_key_count,
        freshness_status=PASS,
        row_drift_status=PASS,
        validation_status=validation_status,
    )


def _build(
    *,
    build_id: str = BUILD_ID,
    attempt_id: str = BUILD_ATTEMPT_ID,
    outputs: tuple[FeatureOutputBinding, ...] | None = None,
    status: str = VALIDATING,
) -> FeatureBuild:
    return FeatureBuild(
        feature_build_id=build_id,
        feature_build_attempt_id=attempt_id,
        reference_date=REFERENCE_DATE,
        registry_checksum=REGISTRY_CHECKSUM,
        git_commit="abc123",
        required_feature_ids=FEATURE_IDS,
        status=status,
        started_at=STARTED_AT,
        sources=(
            _source(build_id=build_id, attempt_id=attempt_id),
        ),
        outputs=(
            outputs
            if outputs is not None
            else tuple(
                _output(
                    feature_id,
                    build_id=build_id,
                    attempt_id=attempt_id,
                    delta_version=42 + index,
                )
                for index, feature_id in enumerate(FEATURE_IDS)
            )
        ),
        job_run_id=123,
        execution_count=0,
    )


def _ready_build(
    *,
    build_id: str = BUILD_ID,
    attempt_id: str = BUILD_ATTEMPT_ID,
    completed_at: datetime = COMPLETED_AT,
) -> FeatureBuild:
    return mark_feature_build_ready(
        _build(build_id=build_id, attempt_id=attempt_id),
        completed_at=completed_at,
    )


def _ready_snapshot(
    *,
    build_id: str = BUILD_ID,
    build_attempt_id: str = BUILD_ATTEMPT_ID,
    snapshot_id: str = "feature-snapshot-20260801",
    snapshot_attempt_id: str = "feature-snapshot-20260801:attempt:0",
    completed_at: datetime = COMPLETED_AT + timedelta(minutes=1),
):
    build = _ready_build(
        build_id=build_id,
        attempt_id=build_attempt_id,
        completed_at=completed_at - timedelta(minutes=1),
    )
    prepared = prepare_feature_snapshot(
        build,
        feature_snapshot_id=snapshot_id,
        feature_snapshot_attempt_id=snapshot_attempt_id,
        created_at=completed_at - timedelta(seconds=30),
    )
    return mark_feature_snapshot_ready(
        prepared,
        build,
        persisted_feature_ids=FEATURE_IDS,
        completed_at=completed_at,
    )


def test_delta_sources_require_an_exact_version_and_complete_lineage():
    source = _source()

    assert source.delta_version == 41
    with pytest.raises(ValueError, match="delta_version"):
        replace(source, delta_version=-1)
    with pytest.raises(ValueError, match="supplied together"):
        replace(source, source_feature_id="product_embeddings")


def test_upstream_feature_source_pins_the_exact_output_attempt():
    output = _output("product_embeddings")

    source = FeatureSourceBinding.from_feature_output(
        output,
        consumer_build_id="downstream-build",
        consumer_build_attempt_id="downstream-build:attempt:0",
        source_name="product_embeddings",
        captured_at=VALIDATED_AT,
    )

    assert source.source_table == output.backing_table
    assert source.delta_version == output.delta_version
    assert source.source_feature_build_attempt_id == BUILD_ATTEMPT_ID
    assert source.source_write_receipt_id == output.write_receipt_id


def test_output_binding_uses_the_common_exact_delta_receipt():
    output = _output("account_profile")

    assert output.delta_version == 42
    assert output.retry_count == 1
    assert output.write_receipt_id.startswith("receipt-account_profile")
    assert output.output_schema_checksum == SCHEMA_CHECKSUM
    assert output.backing_schema_checksum == BACKING_SCHEMA_CHECKSUM
    assert output.passed
    with pytest.raises(ValueError, match="build_id does not match"):
        FeatureOutputBinding.from_delta_write_receipt(
            _receipt("account_profile"),
            feature_build_id="another-build",
            feature_build_attempt_id=BUILD_ATTEMPT_ID,
            reference_date=REFERENCE_DATE,
            feature_id="account_profile",
            contract_schema_checksum=SCHEMA_CHECKSUM,
            output_schema_checksum=SCHEMA_CHECKSUM,
            value_checksum=VALUE_CHECKSUM,
            validated_at=VALIDATED_AT,
            null_key_count=0,
            duplicate_key_count=0,
            freshness_status=PASS,
            row_drift_status=PASS,
            validation_status=PASS,
        )


@pytest.mark.parametrize(
    "output",
    [
        _output("account_profile", validation_status=FAIL),
        _output("account_profile", duplicate_key_count=1),
        replace(
            _output("account_profile"),
            output_schema_checksum="e" * 64,
        ),
    ],
)
def test_ready_build_rejects_any_required_output_that_did_not_pass(output):
    build = _build(
        outputs=(output, _output("product_embeddings", delta_version=43))
    )

    with pytest.raises(ValueError, match="failed outputs"):
        mark_feature_build_ready(build, completed_at=COMPLETED_AT)


def test_ready_build_requires_every_declared_feature_output():
    build = _build(outputs=(_output("account_profile"),))

    with pytest.raises(ValueError, match="every required output"):
        mark_feature_build_ready(build, completed_at=COMPLETED_AT)


def test_failed_build_retains_partial_backing_output_without_becoming_ready():
    partial_output = _output("account_profile")
    build = _build(outputs=(partial_output,), status=BUILDING)

    failed = mark_feature_build_failed(
        build,
        failure_reason="Injected failure after account features",
        completed_at=COMPLETED_AT,
    )

    assert failed.status == FAILED
    assert failed.outputs == (partial_output,)
    assert failed.outputs[0].backing_table.endswith(
        "account_profile_build_data"
    )
    with pytest.raises(ValueError, match="Only a READY feature build"):
        prepare_feature_snapshot(
            failed,
            feature_snapshot_id="failed-snapshot",
            feature_snapshot_attempt_id="failed-snapshot:attempt:0",
            created_at=COMPLETED_AT,
        )


def test_snapshot_is_not_readable_until_every_binding_is_persisted():
    build = _ready_build()
    prepared = prepare_feature_snapshot(
        build,
        feature_snapshot_id="feature-snapshot-20260801",
        feature_snapshot_attempt_id="feature-snapshot-20260801:attempt:0",
        created_at=COMPLETED_AT,
    )

    assert prepared.status == BUILDING
    with pytest.raises(ValueError, match="only from a READY snapshot"):
        resolve_ready_feature(prepared, "account_profile")
    with pytest.raises(ValueError, match="must be persisted before READY"):
        mark_feature_snapshot_ready(
            prepared,
            build,
            persisted_feature_ids=("account_profile",),
            completed_at=COMPLETED_AT + timedelta(minutes=1),
        )

    ready = mark_feature_snapshot_ready(
        prepared,
        build,
        persisted_feature_ids=reversed(FEATURE_IDS),
        completed_at=COMPLETED_AT + timedelta(minutes=1),
    )
    resolved = resolve_ready_feature(ready, "product_embeddings")

    assert ready.status == READY
    assert resolved.delta_version == 43
    assert resolved.feature_build_attempt_id == BUILD_ATTEMPT_ID


def test_snapshot_binding_cannot_drift_from_the_ready_build_receipt():
    build = _ready_build()
    prepared = prepare_feature_snapshot(
        build,
        feature_snapshot_id="feature-snapshot-20260801",
        feature_snapshot_attempt_id="feature-snapshot-20260801:attempt:0",
        created_at=COMPLETED_AT,
    )
    changed = replace(
        prepared.bindings[0],
        delta_version=prepared.bindings[0].delta_version + 1,
    )
    drifted = replace(
        prepared,
        bindings=(changed, prepared.bindings[1]),
    )

    with pytest.raises(ValueError, match="do not match the READY outputs"):
        mark_feature_snapshot_ready(
            drifted,
            build,
            persisted_feature_ids=FEATURE_IDS,
            completed_at=COMPLETED_AT + timedelta(minutes=1),
        )


def test_failed_retry_leaves_the_previous_ready_snapshot_resolvable():
    previous = _ready_snapshot()
    retry_build = _ready_build(
        build_id="feature-build-20260801-retry",
        attempt_id="feature-build-20260801-retry:attempt:1",
        completed_at=COMPLETED_AT + timedelta(minutes=10),
    )
    retry = prepare_feature_snapshot(
        retry_build,
        feature_snapshot_id="feature-snapshot-20260801-retry",
        feature_snapshot_attempt_id="feature-snapshot-20260801-retry:attempt:1",
        created_at=COMPLETED_AT + timedelta(minutes=11),
    )
    failed_retry = mark_feature_snapshot_failed(
        retry,
        failure_reason="Injected failure before READY",
        completed_at=COMPLETED_AT + timedelta(minutes=12),
    )

    selected = select_latest_ready_snapshot((previous, failed_retry))

    assert selected is previous
    assert failed_retry.bindings
    assert resolve_ready_feature(selected, "account_profile").delta_version == 42


def test_a_later_complete_ready_snapshot_replaces_the_previous_selection():
    previous = _ready_snapshot()
    later = _ready_snapshot(
        build_id="feature-build-20260801-later",
        build_attempt_id="feature-build-20260801-later:attempt:1",
        snapshot_id="feature-snapshot-20260801-later",
        snapshot_attempt_id="feature-snapshot-20260801-later:attempt:1",
        completed_at=COMPLETED_AT + timedelta(minutes=20),
    )

    assert select_latest_ready_snapshot((later, previous)) is later
    assert (
        select_latest_ready_snapshot(
            (later, previous),
            reference_date=REFERENCE_DATE + timedelta(days=1),
        )
        is None
    )


def test_snapshot_bindings_reject_an_output_that_failed_quality():
    output = _output("account_profile", validation_status=FAIL)

    with pytest.raises(ValueError, match="cannot bind a failed"):
        FeatureSnapshotBinding.from_feature_output(
            output,
            feature_snapshot_id="snapshot",
            feature_snapshot_attempt_id="snapshot:attempt:0",
            bound_at=COMPLETED_AT,
        )


def test_operational_sql_contracts_store_exact_versions_and_ready_evidence():
    contracts = {
        "create_table_next_uk_nextads_feature_builds.sql": {
            "feature_build_id",
            "feature_build_attempt_id",
            "reference_date",
            "registry_checksum",
            "status",
            "failure_reason",
        },
        "create_table_next_uk_nextads_feature_build_sources.sql": {
            "feature_build_id",
            "feature_build_attempt_id",
            "source_name",
            "source_table",
            "delta_version",
            "schema_checksum",
        },
        "create_table_next_uk_nextads_feature_build_outputs.sql": {
            "feature_build_id",
            "feature_build_attempt_id",
            "feature_id",
            "backing_table",
            "delta_version",
            "output_schema_checksum",
            "backing_schema_checksum",
            "write_receipt_id",
            "validation_status",
        },
        "create_table_next_uk_nextads_feature_snapshots.sql": {
            "feature_snapshot_id",
            "feature_snapshot_attempt_id",
            "feature_build_attempt_id",
            "status",
            "binding_count",
            "failure_reason",
        },
        "create_table_next_uk_nextads_feature_snapshot_bindings.sql": {
            "feature_snapshot_id",
            "feature_snapshot_attempt_id",
            "feature_build_attempt_id",
            "feature_id",
            "backing_table",
            "delta_version",
            "output_schema_checksum",
            "backing_schema_checksum",
            "write_receipt_id",
        },
    }
    contract_root = PROJECT_ROOT / "sql" / "features" / "nextads"

    for file_name, expected_columns in contracts.items():
        sql = (contract_root / file_name).read_text()
        columns = dict(extract_create_table_columns(sql))
        assert expected_columns.issubset(columns)
        if "delta_version" in expected_columns:
            assert "BIGINT NOT NULL" in columns["delta_version"].upper()


def test_ready_build_is_persisted_after_its_sources_and_outputs(monkeypatch):
    calls = []
    monkeypatch.setattr(
        feature_build_store,
        "typed_table_frame",
        lambda _spark, table, rows: {"table": table, "rows": rows},
    )
    monkeypatch.setattr(
        feature_build_store,
        "replace_scope_by_name",
        lambda frame, table, scope, **kwargs: calls.append(
            (frame, table, scope, kwargs)
        ),
    )

    feature_build_store.persist_feature_build(
        object(),
        catalog="marketingdata_dev",
        schema="nextads_feature_store",
        build=_ready_build(),
    )

    assert [call[3]["commit_metadata"]["operation"] for call in calls] == [
        "feature_build_sources",
        "feature_build_outputs",
        "feature_build_status",
    ]
    assert calls[-1][0]["rows"][0]["status"] == READY


def test_ready_snapshot_header_is_persisted_after_exact_bindings(monkeypatch):
    calls = []
    monkeypatch.setattr(
        feature_build_store,
        "typed_table_frame",
        lambda _spark, table, rows: {"table": table, "rows": rows},
    )
    monkeypatch.setattr(
        feature_build_store,
        "replace_scope_by_name",
        lambda frame, table, scope, **kwargs: calls.append(
            (frame, table, scope, kwargs)
        ),
    )

    feature_build_store.persist_feature_snapshot(
        object(),
        catalog="marketingdata_dev",
        schema="nextads_feature_store",
        snapshot=_ready_snapshot(),
    )

    assert [call[3]["commit_metadata"]["operation"] for call in calls] == [
        "feature_snapshot_bindings",
        "feature_snapshot_status",
    ]
    assert calls[-1][0]["rows"][0]["status"] == READY

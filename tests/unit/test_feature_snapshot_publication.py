from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from next_ads.common.delta_writes import DeltaWriteReceipt
from next_ads.features.feature_builds import (
    BUILDING,
    FeatureBuild,
    FeatureSourceBinding,
    READY,
)
from next_ads.features import snapshot_publication as publication


REFERENCE_DATE = date(2026, 8, 1)
BUILD_ID = "123"
ATTEMPT_ID = "123"
FEATURES = ("feature_a", "feature_b")
CHECKSUM = "a" * 64


def _build():
    now = datetime(2026, 8, 1, tzinfo=timezone.utc)
    return FeatureBuild(
        feature_build_id=BUILD_ID,
        feature_build_attempt_id=ATTEMPT_ID,
        reference_date=REFERENCE_DATE,
        registry_checksum="b" * 64,
        git_commit="abc123",
        required_feature_ids=FEATURES,
        status=BUILDING,
        started_at=now,
        sources=(
            FeatureSourceBinding(
                feature_build_id=BUILD_ID,
                feature_build_attempt_id=ATTEMPT_ID,
                reference_date=REFERENCE_DATE,
                source_name="analytics_pctr",
                source_table="catalog.schema.source",
                delta_version=9,
                schema_checksum="c" * 64,
                captured_at=now,
            ),
        ),
    )


def _receipts():
    committed_at = datetime(2026, 8, 1, 1, tzinfo=timezone.utc)
    return {
        feature: DeltaWriteReceipt(
            statement="INSERT",
            attempts=1,
            receipt_id=f"receipt-{feature}",
            target_table=f"catalog.schema.{feature}",
            delta_version=index,
            row_count=10,
            schema_checksum=CHECKSUM,
            build_id=BUILD_ID,
            attempt_id=ATTEMPT_ID,
            git_commit="abc123",
            committed_at=committed_at,
            write_duration_ms=20,
        )
        for index, feature in enumerate(FEATURES, start=20)
    }


def _patch_validation(monkeypatch, *, previous_count=None):
    monkeypatch.setattr(
        publication,
        "align_to_feature_table_contract",
        lambda frame, *_args: frame,
    )
    monkeypatch.setattr(publication, "schema_checksum", lambda _frame: CHECKSUM)
    monkeypatch.setattr(
        publication,
        "feature_value_checksum",
        lambda _frame, **_kwargs: "d" * 64,
    )
    monkeypatch.setattr(
        publication,
        "validate_unique_non_null_keys",
        lambda _frame, _keys: SimpleNamespace(
            row_count=10,
            distinct_key_count=10,
            null_key_count=0,
        ),
    )
    monkeypatch.setattr(
        publication,
        "_previous_row_counts",
        lambda *_args, **_kwargs: (
            {}
            if previous_count is None
            else {feature: previous_count for feature in FEATURES}
        ),
    )


def test_ready_header_is_written_only_after_validated_exact_bindings(
    monkeypatch,
):
    _patch_validation(monkeypatch)
    persisted = []
    monkeypatch.setattr(
        publication,
        "persist_feature_build",
        lambda _spark, **kwargs: persisted.append(
            ("build", kwargs["build"].status)
        ),
    )
    monkeypatch.setattr(
        publication,
        "persist_feature_snapshot",
        lambda _spark, **kwargs: persisted.append(
            ("snapshot", kwargs["snapshot"].status)
        ),
    )
    registry = SimpleNamespace(
        table_spec=lambda _feature: SimpleNamespace(primary_keys=("id",))
    )

    ready_build, ready_snapshot = publication.publish_ready_feature_group(
        object(),
        catalog="catalog",
        schema="schema",
        group_id="analytics_pctr",
        build=_build(),
        frames={feature: object() for feature in FEATURES},
        receipts=_receipts(),
        registry=registry,
    )

    assert ready_build.status == READY
    assert ready_snapshot.status == READY
    assert [binding.delta_version for binding in ready_snapshot.bindings] == [
        20,
        21,
    ]
    assert persisted == [("build", READY), ("snapshot", READY)]


def test_same_date_row_drift_stops_snapshot_publication(monkeypatch):
    _patch_validation(monkeypatch, previous_count=9)
    monkeypatch.setattr(
        publication,
        "persist_feature_build",
        lambda *_args, **_kwargs: pytest.fail("build must not become READY"),
    )
    monkeypatch.setattr(
        publication,
        "persist_feature_snapshot",
        lambda *_args, **_kwargs: pytest.fail("snapshot must not be written"),
    )
    registry = SimpleNamespace(
        table_spec=lambda _feature: SimpleNamespace(primary_keys=("id",))
    )

    with pytest.raises(ValueError, match="failed outputs"):
        publication.publish_ready_feature_group(
            object(),
            catalog="catalog",
            schema="schema",
            group_id="analytics_pctr",
            build=_build(),
            frames={feature: object() for feature in FEATURES},
            receipts=_receipts(),
            registry=registry,
        )

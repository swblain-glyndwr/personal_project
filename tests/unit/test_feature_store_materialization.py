from datetime import datetime, timezone

import pytest

from next_ads.common.delta_writes import DeltaWriteReceipt
from next_ads.features import load_feature_store_registry
from next_ads.features import materialization


class _FakeCatalog:
    def tableExists(self, _table_path):  # noqa: N802 - Spark API spelling
        return True


class _FakeSpark:
    def __init__(self):
        self.catalog = _FakeCatalog()
        self.sql_calls = []

    def sql(self, query):
        self.sql_calls.append(query)


class _FakeFrame:
    columns = [
        "item_id",
        "embedding_model_name",
        "embedding_model_version",
    ]


def _stub_contract_alignment(monkeypatch):
    monkeypatch.setattr(
        materialization,
        "align_to_feature_table_contract",
        lambda dataframe, _table_name, _registry: dataframe,
    )
    monkeypatch.setattr(
        materialization,
        "validate_required_column_values",
        lambda _dataframe, _keys, _table_name: None,
    )


def _receipt(table_path):
    return DeltaWriteReceipt(
        statement=(
            "INSERT INTO target REPLACE WHERE "
            "reference_date = DATE '2026-08-12'"
        ),
        attempts=1,
        receipt_id="receipt-1",
        target_table=table_path,
        delta_version=12,
        row_count=10,
        schema_checksum="a" * 64,
        committed_at=datetime(2026, 8, 12, tzinfo=timezone.utc),
    )


def test_registry_declares_latest_snapshots_as_complete_overwrites():
    registry = load_feature_store_registry()

    overwrite_features = {
        feature.name
        for feature in registry.offline_features
        if feature.write_mode == "overwrite"
    }

    assert overwrite_features == {
        "next_uk_nextads_fs_item_attributes_latest",
        "next_uk_nextads_fs_product_embeddings_latest",
    }


def test_overwrite_snapshot_uses_one_tagged_delta_transaction(monkeypatch):
    _stub_contract_alignment(monkeypatch)
    spark = _FakeSpark()
    frame = _FakeFrame()
    calls = []

    def replace_table(df, table_path, **kwargs):
        calls.append((df, table_path, kwargs))
        return _receipt(table_path)

    monkeypatch.setattr(materialization, "replace_table_by_name", replace_table)

    path = materialization.write_feature_table(
        spark,
        "next_uk_nextads_fs_item_attributes_latest",
        frame,
        catalog="marketingdata_dev",
        schema="stephen_blain",
        reference_date="2026-08-12",
        mode="overwrite",
    )

    assert path == (
        "marketingdata_dev.stephen_blain."
        "next_uk_nextads_fs_item_attributes_latest"
    )
    assert not spark.sql_calls
    assert calls[0][0] is frame
    assert calls[0][1] == path
    assert calls[0][2]["commit_metadata"] == {
        "contract": "nextads_feature_build/v1",
        "reference_date": "2026-08-12",
        "table_name": "next_uk_nextads_fs_item_attributes_latest",
    }


def test_overwrite_does_not_import_feature_engineering_client(monkeypatch):
    _stub_contract_alignment(monkeypatch)
    spark = _FakeSpark()
    calls = []

    def fail_if_created():
        raise AssertionError("overwrite must not create the FE client")

    monkeypatch.setattr(
        materialization,
        "create_feature_engineering_client",
        fail_if_created,
    )
    monkeypatch.setattr(
        materialization,
        "replace_table_by_name",
        lambda df, table_path, **kwargs: (
            calls.append((df, table_path, kwargs)) or _receipt(table_path)
        ),
    )

    materialization.write_feature_table(
        spark,
        "next_uk_nextads_fs_product_embeddings_latest",
        _FakeFrame(),
        catalog="marketingdata_dev",
        schema="stephen_blain",
        reference_date="2026-08-12",
        mode="overwrite",
    )

    assert calls


def test_feature_write_replaces_one_date_in_one_delta_transaction(monkeypatch):
    _stub_contract_alignment(monkeypatch)
    spark = _FakeSpark()
    calls = []

    def replace_scope(df, table_path, scope, **kwargs):
        calls.append((df, table_path, scope, kwargs))
        return _receipt(table_path)

    monkeypatch.setattr(materialization, "replace_scope_by_name", replace_scope)

    result = materialization.write_feature_table(
        spark,
        "next_uk_nextads_fs_product_embeddings_latest",
        _FakeFrame(),
        catalog="marketingdata_dev",
        schema="stephen_blain",
        reference_date="2026-08-12",
        reference_date_column="embedding_model_version",
        mode="merge",
        build_id="build-1",
        attempt_id="attempt-1",
        git_commit="abc123",
        return_receipt=True,
    )

    assert not spark.sql_calls
    assert calls[0][2] == {"embedding_model_version": "2026-08-12"}
    assert calls[0][3]["build_id"] == "build-1"
    assert calls[0][3]["attempt_id"] == "attempt-1"
    assert result.table_path == calls[0][1]
    assert result.receipt.delta_version == 12


def test_feature_write_rejects_an_unknown_mode_before_writing(monkeypatch):
    _stub_contract_alignment(monkeypatch)

    with pytest.raises(ValueError, match="write mode must be"):
        materialization.write_feature_table(
            _FakeSpark(),
            "next_uk_nextads_fs_item_attributes_latest",
            _FakeFrame(),
            mode="append",
        )


def test_merge_requires_an_explicit_single_date_scope(monkeypatch):
    _stub_contract_alignment(monkeypatch)

    with pytest.raises(ValueError, match="explicit reference-date scope"):
        materialization.write_feature_table(
            _FakeSpark(),
            "next_uk_nextads_fs_product_embeddings_latest",
            _FakeFrame(),
            reference_date=None,
            mode="merge",
        )

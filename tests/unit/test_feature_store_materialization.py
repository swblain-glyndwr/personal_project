import json

import pytest

from next_ads.features import load_feature_store_registry
from next_ads.features import materialization


class _FakeConf:
    def __init__(self, initial=None):
        self.values = dict(initial or {})

    def get(self, key):
        if key not in self.values:
            raise KeyError(key)
        return self.values[key]

    def set(self, key, value):
        self.values[key] = value

    def unset(self, key):
        self.values.pop(key, None)


class _FakeCatalog:
    def tableExists(self, _table_path):  # noqa: N802 - Spark API spelling
        return True


class _FakeSpark:
    def __init__(self, initial_conf=None):
        self.catalog = _FakeCatalog()
        self.conf = _FakeConf(initial_conf)
        self.sql_calls = []

    def sql(self, query):
        self.sql_calls.append(query)


class _FakeWriter:
    def __init__(self, frame):
        self.frame = frame
        self.write_mode = None

    def mode(self, mode):
        self.write_mode = mode
        return self

    def insertInto(self, table_path):  # noqa: N802 - Spark API spelling
        self.frame.write_calls.append(
            {
                "name": table_path,
                "mode": self.write_mode,
                "commit_metadata": self.frame.spark.conf.get(
                    materialization.DELTA_COMMIT_METADATA_KEY
                ),
            }
        )


class _FakeFrame:
    columns = [
        "item_id",
        "embedding_model_name",
        "embedding_model_version",
    ]

    def __init__(self, spark):
        self.spark = spark
        self.write_calls = []
        self.write = _FakeWriter(self)


class _FakeClient:
    def __init__(self, spark):
        self.spark = spark
        self.write_calls = []

    def write_table(self, *, name, df, mode):
        self.write_calls.append(
            {
                "name": name,
                "df": df,
                "mode": mode,
                "commit_metadata": self.spark.conf.get(
                    materialization.DELTA_COMMIT_METADATA_KEY
                ),
            }
        )


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


def test_overwrite_snapshot_is_tagged_and_does_not_delete_a_partition(
    monkeypatch,
):
    _stub_contract_alignment(monkeypatch)
    spark = _FakeSpark()
    client = _FakeClient(spark)
    frame = _FakeFrame(spark)

    path = materialization.write_feature_table(
        spark,
        "next_uk_nextads_fs_item_attributes_latest",
        frame,
        catalog="marketingdata_dev",
        schema="stephen_blain",
        reference_date="2026-08-12",
        mode="overwrite",
        feature_engineering_client=client,
    )

    assert path == (
        "marketingdata_dev.stephen_blain."
        "next_uk_nextads_fs_item_attributes_latest"
    )
    assert not spark.sql_calls
    assert not client.write_calls
    assert frame.write_calls[0]["mode"] == "overwrite"
    assert json.loads(frame.write_calls[0]["commit_metadata"]) == {
        "contract": "nextads_feature_build/v1",
        "reference_date": "2026-08-12",
        "table_name": "next_uk_nextads_fs_item_attributes_latest",
    }
    assert materialization.DELTA_COMMIT_METADATA_KEY not in spark.conf.values


def test_overwrite_does_not_import_feature_engineering_client(monkeypatch):
    _stub_contract_alignment(monkeypatch)
    spark = _FakeSpark()
    frame = _FakeFrame(spark)

    def fail_if_created():
        raise AssertionError("overwrite must not create the FE client")

    monkeypatch.setattr(
        materialization,
        "create_feature_engineering_client",
        fail_if_created,
    )

    materialization.write_feature_table(
        spark,
        "next_uk_nextads_fs_product_embeddings_latest",
        frame,
        catalog="marketingdata_dev",
        schema="stephen_blain",
        reference_date="2026-08-12",
        mode="overwrite",
    )

    assert frame.write_calls


def test_feature_write_restores_existing_delta_commit_metadata(monkeypatch):
    _stub_contract_alignment(monkeypatch)
    key = materialization.DELTA_COMMIT_METADATA_KEY
    spark = _FakeSpark({key: "existing-metadata"})
    client = _FakeClient(spark)

    materialization.write_feature_table(
        spark,
        "next_uk_nextads_fs_item_attributes_latest",
        _FakeFrame(spark),
        catalog="marketingdata_dev",
        schema="stephen_blain",
        reference_date="2026-08-12",
        mode="overwrite",
        feature_engineering_client=client,
    )

    assert spark.conf.get(key) == "existing-metadata"


def test_feature_write_rejects_an_unknown_mode_before_writing(monkeypatch):
    _stub_contract_alignment(monkeypatch)
    spark = _FakeSpark()
    client = _FakeClient(spark)

    with pytest.raises(ValueError, match="write mode must be"):
        materialization.write_feature_table(
            spark,
            "next_uk_nextads_fs_item_attributes_latest",
            _FakeFrame(spark),
            mode="append",
            feature_engineering_client=client,
        )

    assert not client.write_calls

from argparse import Namespace
import importlib
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOB_DIRECTORY = PROJECT_ROOT / "jobs" / "features" / "nextads"
if str(JOB_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(JOB_DIRECTORY))

job = importlib.import_module(
    "jobs.features.nextads.build_advert_product_profile_daily"
)


SOURCE_CATALOG = "marketingdata_prod"
SOURCE_SCHEMA = "warehouse"
TARGET_CATALOG = "marketingdata_dev"
TARGET_SCHEMA = "nextads_feature_store"
REFERENCE_DATE = "2026-08-01"


def test_source_paths_use_only_the_live_bridge_and_target_embedding_tables():
    paths = job.resolve_advert_product_profile_source_paths(
        source_catalog=SOURCE_CATALOG,
        source_schema=SOURCE_SCHEMA,
        target_catalog=TARGET_CATALOG,
        target_schema=TARGET_SCHEMA,
    )

    assert paths.v2_sort_history == (
        "marketingdata_prod.warehouse.nextads_sort_order_v2"
    )
    assert paths.representative_items == (
        "marketingdata_prod.warehouse.next_uk_nextads_ad_items"
    )
    assert paths.v2_control == (
        "marketingdata_prod.warehouse.next_uk_nextads_control_sheet_v2"
    )
    assert paths.v1_control == (
        "marketingdata_prod.warehouse.next_uk_nextads_control_sheet"
    )
    assert paths.product_embeddings == (
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_fs_product_embeddings_latest"
    )
    assert "next_ads_sort_order_latest" not in repr(paths)


def test_source_reader_uses_a_typed_empty_legacy_sort_frame():
    class RecordingSpark:
        def __init__(self):
            self.table_names = []
            self.empty_frames = []

        def table(self, name):
            self.table_names.append(name)
            return f"frame:{name}"

        def createDataFrame(self, rows, schema):  # noqa: N802
            self.empty_frames.append((rows, schema))
            return "empty-legacy-sort-history"

    spark = RecordingSpark()

    frames = job.read_advert_product_profile_source_frames(
        spark,
        source_catalog=SOURCE_CATALOG,
        source_schema=SOURCE_SCHEMA,
        target_catalog=TARGET_CATALOG,
        target_schema=TARGET_SCHEMA,
    )

    assert spark.table_names == [
        "marketingdata_prod.warehouse.nextads_sort_order_v2",
        "marketingdata_prod.warehouse.next_uk_nextads_ad_items",
        "marketingdata_prod.warehouse.next_uk_nextads_control_sheet_v2",
        "marketingdata_prod.warehouse.next_uk_nextads_control_sheet",
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_fs_product_embeddings_latest",
    ]
    assert spark.empty_frames == [
        (
            [],
            "UniqueAdID STRING, items STRING, item_pos BIGINT, rundate DATE",
        )
    ]
    assert frames["legacy_sort_history"] == "empty-legacy-sort-history"
    assert set(frames) == {
        "v2_sort_history",
        "legacy_sort_history",
        "representative_items",
        "v2_control",
        "v1_control",
        "product_embeddings",
    }


def test_main_uses_shared_date_bridge_profile_and_registered_merge(
    monkeypatch,
):
    args = Namespace(
        catalog=TARGET_CATALOG,
        schema=TARGET_SCHEMA,
        source_catalog=SOURCE_CATALOG,
        source_schema=SOURCE_SCHEMA,
        reference_date=None,
        product_embedding_binding="configs/features/personal.yaml",
        replace_reference_date="true",
        log_level="INFO",
    )
    spark = object()
    client = object()
    embedding_binding = object()
    registry = SimpleNamespace(
        default_catalog="unused",
        default_schema="unused",
        table_spec=lambda name: SimpleNamespace(
            timestamp_key="feature_date",
            write_mode="merge",
        ),
    )
    source_frames = {
        "v2_sort_history": "v2-sort",
        "legacy_sort_history": "empty-legacy",
        "representative_items": "representative",
        "v2_control": "v2-control",
        "v1_control": "v1-control",
        "product_embeddings": "product-embeddings",
    }
    calls = {}

    monkeypatch.setattr(job, "parse_common_args", lambda: args)
    monkeypatch.setattr(job, "configure_job_logging", lambda level: None)
    monkeypatch.setattr(job, "log_owned_tables", lambda builder, args: None)
    monkeypatch.setattr(job, "configure_spark", lambda: spark)
    monkeypatch.setattr(job, "load_feature_store_registry", lambda: registry)
    monkeypatch.setattr(
        job,
        "load_product_embedding_materialization_binding",
        lambda path: calls.setdefault("binding_path", path)
        and embedding_binding,
    )
    monkeypatch.setattr(
        job,
        "validate_materialization_binding_target",
        lambda actual_binding, **kwargs: calls.setdefault(
            "binding_target", (actual_binding, kwargs)
        ),
    )
    monkeypatch.setattr(
        job,
        "resolve_reference_date_from_theme",
        lambda actual_spark, actual_args: REFERENCE_DATE,
    )
    monkeypatch.setattr(
        job,
        "create_feature_engineering_client",
        lambda: client,
    )

    def validate(builder, outputs, actual_registry):
        calls["ownership"] = (builder, outputs, actual_registry)

    monkeypatch.setattr(job, "validate_builder_output_tables", validate)

    def read_sources(actual_spark, **kwargs):
        calls["read"] = (actual_spark, kwargs)
        return source_frames

    monkeypatch.setattr(
        job,
        "read_advert_product_profile_source_frames",
        read_sources,
    )

    def build_bridge(**kwargs):
        calls["bridge"] = kwargs
        return "canonical-bridge"

    monkeypatch.setattr(job, "build_advert_item_bridge", build_bridge)

    def build_profiles(bridge, product_embeddings, *, approved_binding):
        calls["profiles"] = (
            bridge,
            product_embeddings,
            approved_binding,
        )
        return "profiles"

    monkeypatch.setattr(
        job,
        "build_advert_product_profile_frame",
        build_profiles,
    )
    monkeypatch.setattr(
        job,
        "require_non_empty_profile_output",
        lambda profiles: calls.setdefault("non_empty", profiles) or profiles,
    )

    def write_table(*positional, **kwargs):
        calls["write"] = (positional, kwargs)
        return "target-table"

    monkeypatch.setattr(job, "write_feature_table", write_table)

    job.main()

    assert calls["binding_path"] == "configs/features/personal.yaml"
    assert calls["binding_target"] == (
        embedding_binding,
        {"catalog": TARGET_CATALOG, "schema": TARGET_SCHEMA},
    )
    assert calls["ownership"] == (
        job.BUILDER,
        (job.OUTPUT_TABLE,),
        registry,
    )
    assert calls["read"] == (
        spark,
        {
            "source_catalog": SOURCE_CATALOG,
            "source_schema": SOURCE_SCHEMA,
            "target_catalog": TARGET_CATALOG,
            "target_schema": TARGET_SCHEMA,
        },
    )
    assert calls["bridge"] == {
        "v2_sort_history": "v2-sort",
        "legacy_sort_history": "empty-legacy",
        "representative_items": "representative",
        "v2_control": "v2-control",
        "v1_control": "v1-control",
        "feature_date": REFERENCE_DATE,
        "cutoff_date": "2026-07-31",
    }
    assert calls["profiles"] == (
        "canonical-bridge",
        "product-embeddings",
        embedding_binding,
    )
    assert calls["non_empty"] == "profiles"
    positional, keyword = calls["write"]
    assert positional == (spark, job.OUTPUT_TABLE, "profiles")
    assert keyword == {
        "catalog": TARGET_CATALOG,
        "schema": TARGET_SCHEMA,
        "reference_date": REFERENCE_DATE,
        "reference_date_column": "feature_date",
        "replace_reference_date": True,
        "mode": "merge",
        "registry": registry,
        "feature_engineering_client": client,
    }


def test_empty_profile_output_is_rejected_before_write():
    class EmptyProfiles:
        def localCheckpoint(self, *, eager):  # noqa: N802
            assert eager is True
            return self

        def limit(self, _count):
            return self

        def collect(self):
            return []

    with pytest.raises(ValueError, match="produced no rows"):
        job.require_non_empty_profile_output(EmptyProfiles())

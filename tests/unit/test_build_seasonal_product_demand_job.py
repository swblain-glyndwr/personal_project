from argparse import Namespace
import importlib
from pathlib import Path
from types import SimpleNamespace
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOB_DIRECTORY = PROJECT_ROOT / "jobs" / "features" / "nextads"
if str(JOB_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(JOB_DIRECTORY))

job = importlib.import_module(
    "jobs.features.nextads.build_seasonal_product_demand_daily"
)


SOURCE_CATALOG = "marketingdata_prod"
SOURCE_SCHEMA = "warehouse"
TARGET_CATALOG = "marketingdata_dev"
TARGET_SCHEMA = "nextads_feature_store"
REFERENCE_DATE = "2026-08-17"


def test_source_paths_use_account_events_bridge_and_target_embeddings():
    paths = job.resolve_seasonal_product_demand_source_paths(
        source_catalog=SOURCE_CATALOG,
        source_schema=SOURCE_SCHEMA,
        target_catalog=TARGET_CATALOG,
        target_schema=TARGET_SCHEMA,
    )

    assert paths.account_views == (
        "marketingdata_prod.warehouse."
        "bq_views_sessions_next_uk_with_accounts"
    )
    assert paths.account_purchases == (
        "marketingdata_prod.warehouse.baskets_uk_3y"
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


def test_source_reader_supplies_a_typed_empty_legacy_sort_frame():
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

    frames = job.read_seasonal_product_demand_source_frames(
        spark,
        source_catalog=SOURCE_CATALOG,
        source_schema=SOURCE_SCHEMA,
        target_catalog=TARGET_CATALOG,
        target_schema=TARGET_SCHEMA,
    )

    assert spark.table_names == [
        "marketingdata_prod.warehouse."
        "bq_views_sessions_next_uk_with_accounts",
        "marketingdata_prod.warehouse.baskets_uk_3y",
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


def test_main_builds_bridge_and_registered_seasonal_output(monkeypatch):
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
    pinned_spark = SimpleNamespace(source_bindings=("source-binding",))
    registry = SimpleNamespace(
        default_catalog="unused",
        default_schema="unused",
        table_spec=lambda name: SimpleNamespace(
            timestamp_key="feature_date",
            write_mode="merge",
        ),
    )
    source_frames = {
        "account_views": "views",
        "account_purchases": "purchases",
        "v2_sort_history": "v2-sort",
        "legacy_sort_history": "empty-legacy",
        "representative_items": "representative",
        "v2_control": "v2-control",
        "v1_control": "v1-control",
        "product_embeddings": "product-embeddings",
    }
    calls = {}
    embedding_binding = object()

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
        "feature_group_identity",
        lambda *_args: {
            "feature_build_id": "build:seasonal",
            "feature_build_attempt_id": "attempt:seasonal",
            "git_commit": "revision",
        },
    )
    monkeypatch.setattr(job, "PinnedSourceSession", lambda *_args, **_kwargs: pinned_spark)

    def validate(builder, outputs, actual_registry):
        calls["ownership"] = (builder, outputs, actual_registry)

    monkeypatch.setattr(job, "validate_builder_output_tables", validate)

    def read_sources(actual_spark, **kwargs):
        calls["read"] = (actual_spark, kwargs)
        return source_frames

    monkeypatch.setattr(
        job,
        "read_seasonal_product_demand_source_frames",
        read_sources,
    )

    def build_bridge(**kwargs):
        calls["bridge"] = kwargs
        return "canonical-bridge"

    monkeypatch.setattr(job, "build_advert_item_bridge", build_bridge)

    def build_demand(**kwargs):
        calls["demand"] = kwargs
        return "seasonal-demand"

    monkeypatch.setattr(
        job,
        "build_seasonal_product_demand_frame",
        build_demand,
    )

    def write_group(*positional, **kwargs):
        calls["write"] = (positional, kwargs)
        return object(), SimpleNamespace(
            feature_snapshot_id="snapshot",
            feature_snapshot_attempt_id="attempt:seasonal",
        )

    monkeypatch.setattr(job, "write_and_publish_feature_group", write_group)

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
        pinned_spark,
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
        "cutoff_date": "2026-08-16",
    }
    assert calls["demand"] == {
        "account_views": "views",
        "account_purchases": "purchases",
        "advert_item_bridge": "canonical-bridge",
        "product_embeddings": "product-embeddings",
        "approved_binding": embedding_binding,
        "reference_date": REFERENCE_DATE,
    }
    positional, keyword = calls["write"]
    assert positional == (spark,)
    assert keyword == {
        "catalog": TARGET_CATALOG,
        "schema": TARGET_SCHEMA,
        "group_id": job.BUILDER,
        "reference_date": job.parse_reference_date(REFERENCE_DATE),
        "frames": {job.OUTPUT_TABLE: "seasonal-demand"},
        "sources": ("source-binding",),
        "replace_reference_date": True,
        "registry": registry,
        "feature_build_id": "build:seasonal",
        "feature_build_attempt_id": "attempt:seasonal",
        "git_commit": "revision",
    }

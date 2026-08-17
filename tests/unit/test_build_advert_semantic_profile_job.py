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
    "jobs.features.nextads.build_advert_semantic_profile_daily"
)


SOURCE_CATALOG = "marketingdata_prod"
SOURCE_SCHEMA = "warehouse"
TARGET_CATALOG = "marketingdata_dev"
TARGET_SCHEMA = "nextads_feature_store"
REFERENCE_DATE = "2026-08-17"


def test_source_paths_use_registered_features_and_repository_sources():
    paths = job.resolve_advert_semantic_source_paths(
        source_catalog=SOURCE_CATALOG,
        source_schema=SOURCE_SCHEMA,
        target_catalog=TARGET_CATALOG,
        target_schema=TARGET_SCHEMA,
    )

    assert paths.advert_core == (
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_fs_advert_core_daily"
    )
    assert paths.advert_attributes == (
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_fs_advert_attribute_profile_daily"
    )
    assert paths.product_embeddings == (
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_fs_product_embeddings_latest"
    )
    assert paths.existing_profiles == (
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_fs_advert_semantic_profile_daily"
    )
    assert paths.control_sheet_latest == (
        "marketingdata_prod.warehouse.next_uk_nextads_control_sheet_latest"
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


def test_source_reader_keeps_legacy_sort_history_explicitly_empty():
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

    frames = job.read_advert_semantic_source_frames(
        spark,
        source_catalog=SOURCE_CATALOG,
        source_schema=SOURCE_SCHEMA,
        target_catalog=TARGET_CATALOG,
        target_schema=TARGET_SCHEMA,
    )

    assert spark.table_names == [
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_fs_advert_core_daily",
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_fs_advert_attribute_profile_daily",
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_fs_product_embeddings_latest",
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_fs_advert_semantic_profile_daily",
        "marketingdata_prod.warehouse.next_uk_nextads_control_sheet_latest",
        "marketingdata_prod.warehouse.nextads_sort_order_v2",
        "marketingdata_prod.warehouse.next_uk_nextads_ad_items",
        "marketingdata_prod.warehouse.next_uk_nextads_control_sheet_v2",
        "marketingdata_prod.warehouse.next_uk_nextads_control_sheet",
    ]
    assert spark.empty_frames == [
        (
            [],
            "UniqueAdID STRING, items STRING, item_pos BIGINT, rundate DATE",
        )
    ]
    assert frames["legacy_sort_history"] == "empty-legacy-sort-history"


def test_main_runs_repo_text_through_exact_model_and_registered_write(
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
    registry = SimpleNamespace(
        default_catalog="unused",
        default_schema="unused",
        table_spec=lambda name: SimpleNamespace(
            timestamp_key="feature_date",
            write_mode="merge",
        ),
    )
    definition = object()
    binding = object()
    client = object()
    source_frames = {
        "advert_core": "advert-core",
        "advert_attributes": "advert-attributes",
        "product_embeddings": "product-embeddings",
        "existing_profiles": "existing-profiles",
        "control_sheet_latest": "control-sheet-latest",
        "v2_sort_history": "v2-sort",
        "legacy_sort_history": "empty-legacy",
        "representative_items": "representative-items",
        "v2_control": "v2-control",
        "v1_control": "v1-control",
    }
    calls = {}

    monkeypatch.setitem(sys.modules, "mlflow", SimpleNamespace())
    monkeypatch.setattr(job, "parse_common_args", lambda: args)
    monkeypatch.setattr(job, "configure_job_logging", lambda level: None)
    monkeypatch.setattr(job, "log_owned_tables", lambda builder, args: None)
    monkeypatch.setattr(job, "configure_spark", lambda: spark)
    monkeypatch.setattr(job, "load_feature_store_registry", lambda: registry)
    monkeypatch.setattr(
        job,
        "resolve_reference_date_from_theme",
        lambda actual_spark, actual_args: REFERENCE_DATE,
    )
    monkeypatch.setattr(
        job,
        "load_product_embedding_definition",
        lambda: definition,
    )
    monkeypatch.setattr(
        job,
        "load_product_embedding_materialization_binding",
        lambda path: calls.setdefault("binding_path", path) and binding,
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
        "validate_builder_output_tables",
        lambda builder, outputs, actual_registry: calls.update(
            ownership=(builder, outputs, actual_registry)
        ),
    )
    monkeypatch.setattr(
        job,
        "validate_materialization_runtime",
        lambda actual_spark, actual_definition: {"runtime": "15.4"},
    )
    monkeypatch.setattr(
        job,
        "prepare_validated_model_for_executors",
        lambda mlflow, actual_binding, actual_definition: (
            Path("model"),
            {"model": "exact"},
        ),
    )
    monkeypatch.setattr(
        job,
        "read_advert_semantic_source_frames",
        lambda actual_spark, **kwargs: source_frames,
    )

    def build_bridge(**kwargs):
        calls["bridge"] = kwargs
        return "bridge"

    monkeypatch.setattr(job, "build_advert_item_bridge", build_bridge)
    monkeypatch.setattr(
        job,
        "build_advert_image_flags",
        lambda control, reference_date: calls.update(
            images=(control, reference_date)
        )
        or "image-flags",
    )
    monkeypatch.setattr(
        job,
        "select_exact_product_text",
        lambda products, actual_binding: calls.update(
            product_text=(products, actual_binding)
        )
        or "product-text",
    )
    monkeypatch.setattr(
        job,
        "_reference_date_partition",
        lambda frame, reference_date: f"{frame}:{reference_date}",
    )

    def build_text(*positional):
        calls["text"] = positional
        return "semantic-text"

    monkeypatch.setattr(job, "build_advert_semantic_text_source", build_text)

    def build_vectors(text, existing, **kwargs):
        calls["vectors"] = (text, existing, kwargs)
        return "vectors", SimpleNamespace(
            source_row_count=2,
            reused_row_count=1,
            generated_row_count=1,
            output_row_count=2,
        )

    monkeypatch.setattr(
        job,
        "build_advert_semantic_vector_frame",
        build_vectors,
    )
    monkeypatch.setattr(
        job,
        "build_advert_semantic_profile_frame",
        lambda text, vectors: calls.update(profile=(text, vectors))
        or "profiles",
    )
    monkeypatch.setattr(
        job,
        "create_feature_engineering_client",
        lambda: client,
    )

    def write_table(*positional, **kwargs):
        calls["write"] = (positional, kwargs)
        return "target-table"

    monkeypatch.setattr(job, "write_feature_table", write_table)

    job.main()

    assert calls["binding_path"] == "configs/features/personal.yaml"
    assert calls["binding_target"] == (
        binding,
        {"catalog": TARGET_CATALOG, "schema": TARGET_SCHEMA},
    )
    assert calls["ownership"] == (
        job.BUILDER,
        (job.OUTPUT_TABLE,),
        registry,
    )
    assert calls["bridge"] == {
        "v2_sort_history": "v2-sort",
        "legacy_sort_history": "empty-legacy",
        "representative_items": "representative-items",
        "v2_control": "v2-control",
        "v1_control": "v1-control",
        "feature_date": REFERENCE_DATE,
        "cutoff_date": "2026-08-16",
    }
    assert calls["images"] == (
        "control-sheet-latest",
        REFERENCE_DATE,
    )
    assert calls["product_text"] == ("product-embeddings", binding)
    assert calls["text"] == (
        f"advert-core:{REFERENCE_DATE}",
        f"advert-attributes:{REFERENCE_DATE}",
        "bridge",
        "product-text",
        "image-flags",
    )
    assert calls["vectors"] == (
        "semantic-text",
        "existing-profiles",
        {"binding": binding, "model_path": Path("model")},
    )
    assert calls["profile"] == ("semantic-text", "vectors")
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

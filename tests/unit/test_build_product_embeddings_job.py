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
    "jobs.features.nextads.build_product_embeddings_latest"
)


def test_main_validates_runtime_and_model_before_replacing_complete_snapshot(
    monkeypatch,
):
    args = Namespace(
        catalog="marketingdata_dev",
        schema="nextads_feature_store",
        source_catalog="marketingdata_prod",
        source_schema="warehouse",
        reference_date="2026-08-01",
        product_embedding_binding="configs/features/personal.yaml",
        log_level="INFO",
    )
    source_path = "marketingdata_prod.warehouse.product_catalog_history"
    target_path = (
        "marketingdata_dev.nextads_feature_store."
        "next_uk_nextads_fs_product_embeddings_latest"
    )

    class Spark:
        def __init__(self):
            self.reads = []

        def table(self, table_path):
            self.reads.append(table_path)
            return f"frame:{table_path}"

    spark = Spark()
    registry = SimpleNamespace(
        default_catalog="unused",
        default_schema="unused",
        resolved_table_path=lambda *_args, **_kwargs: target_path,
        table_spec=lambda _name: SimpleNamespace(write_mode="overwrite"),
    )
    definition = object()
    binding = object()
    calls = {}
    pinned_spark = SimpleNamespace(
        table=spark.table,
        source_bindings=("source-binding",),
    )

    monkeypatch.setattr(job, "parse_common_args", lambda: args)
    monkeypatch.setattr(job, "configure_job_logging", lambda _level: None)
    monkeypatch.setattr(job, "log_owned_tables", lambda *_args: None)
    monkeypatch.setattr(job, "configure_spark", lambda: spark)
    monkeypatch.setattr(job, "load_feature_store_registry", lambda: registry)
    monkeypatch.setattr(
        job,
        "resolve_reference_date_from_theme",
        lambda *_args: "2026-08-01",
    )
    monkeypatch.setattr(
        job,
        "feature_group_identity",
        lambda *_args: {
            "feature_build_id": "build:embeddings",
            "feature_build_attempt_id": "attempt:embeddings",
            "git_commit": "revision",
        },
    )
    monkeypatch.setattr(job, "PinnedSourceSession", lambda *_args, **_kwargs: pinned_spark)
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
        lambda *values: calls.setdefault("ownership", values),
    )
    monkeypatch.setattr(
        job,
        "validate_materialization_runtime",
        lambda actual_spark, actual_definition: calls.setdefault(
            "runtime",
            (actual_spark, actual_definition),
        )
        and {"runtime": "PASS"},
    )
    monkeypatch.setattr(
        job,
        "prepare_validated_model_for_executors",
        lambda _mlflow, actual_binding, actual_definition: (
            Path("approved-model"),
            {"model": "PASS"},
        ),
    )
    monkeypatch.setattr(
        job,
        "build_current_product_text_source",
        lambda frame, **kwargs: calls.setdefault(
            "source",
            (frame, kwargs),
        )
        and "product-text",
    )
    monkeypatch.setattr(
        job,
        "build_product_embeddings_frame",
        lambda source, existing, **kwargs: (
            calls.setdefault("build", (source, existing, kwargs))
            and "complete-snapshot",
            SimpleNamespace(
                source_row_count=2,
                reused_row_count=1,
                generated_row_count=1,
                output_row_count=2,
            ),
        ),
    )
    monkeypatch.setattr(
        job,
        "write_and_publish_feature_group",
        lambda *positional, **kwargs: (
            calls.setdefault("write", (positional, kwargs)) and object(),
            SimpleNamespace(
                feature_snapshot_id="snapshot",
                feature_snapshot_attempt_id="attempt:embeddings",
            ),
        ),
    )
    monkeypatch.setitem(sys.modules, "mlflow", object())

    job.main()

    assert calls["binding_path"] == "configs/features/personal.yaml"
    assert calls["binding_target"] == (
        binding,
        {
            "catalog": "marketingdata_dev",
            "schema": "nextads_feature_store",
        },
    )
    assert spark.reads == [source_path, target_path]
    assert calls["runtime"] == (spark, definition)
    assert calls["source"] == (
        f"frame:{source_path}",
        {"reference_date": "2026-08-01"},
    )
    assert calls["build"] == (
        "product-text",
        f"frame:{target_path}",
        {"binding": binding, "model_path": Path("approved-model")},
    )
    positional, keyword = calls["write"]
    assert positional == (spark,)
    assert keyword["frames"] == {job.OUTPUT_TABLE: "complete-snapshot"}
    assert keyword["sources"] == ("source-binding",)
    assert keyword["reference_date"] == job.parse_reference_date("2026-08-01")

from argparse import Namespace
from datetime import date
import importlib
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

from next_ads.features.analytics_pctr_source import (
    AnalyticsPctrSourceDefinition,
)
from next_ads.features.pctr_affinity import DeltaSourceBinding


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOB_DIRECTORY = PROJECT_ROOT / "jobs" / "features" / "nextads"
if str(JOB_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(JOB_DIRECTORY))

job = importlib.import_module(
    "jobs.features.nextads.build_pctr_affinity_features"
)


REFERENCE_DATE = "2026-08-01"
SOURCE_TABLE = (
    "marketingdata_dev.nextads_integration."
    "next_uk_nextAds_analytics_pctr_features"
)


def _args(**overrides):
    values = {
        "reference_date": "predict",
        "catalog": "marketingdata_dev",
        "schema": "nextads_feature_store",
        "source_catalog": "marketingdata_prod",
        "source_schema": "warehouse",
        "theme_source_catalog": "marketingdata_prod",
        "theme_source_schema": "warehouse",
        "theme_table_prefix": "next_uk_nextads_account_theme_foundation",
        "analytics_pctr_source_binding": (
            "configs/features/analytics_pctr_source_dev.yaml"
        ),
        "analytics_pctr_source_catalog": "marketingdata_dev",
        "analytics_pctr_source_schema": "nextads_integration",
        "analytics_pctr_receipt_correlation_id": "987654321",
        "country_mapping_schema": "search",
        "replace_reference_date": "true",
        "log_level": "INFO",
    }
    values.update(overrides)
    return Namespace(**values)


def test_source_paths_cover_web_app_customer_and_page_sources():
    paths = job.resolve_source_paths(_args())

    assert paths.web_sessions == (
        "marketingdata_prod.warehouse.bq_sessions_next_uk"
    )
    assert paths.app_sessions == (
        "marketingdata_prod.warehouse.bq_sessions_next_uk_app"
    )
    assert paths.rpid_accounts == (
        "marketingdata_prod.warehouse.rpid_with_accounts"
    )
    assert paths.customer_accounts == (
        "marketingdata_prod.warehouse.svoccust"
    )
    assert paths.web_pages == ("marketingdata_prod.warehouse.bq_pages_next_uk")
    assert paths.country_mapping == (
        "marketingdata_prod.search.nov_country_mapping"
    )

def test_session_reader_reads_only_the_declared_bq_sources():
    paths = job.resolve_source_paths(_args())

    class Spark:
        def __init__(self):
            self.tables = []

        def table(self, table_path):
            self.tables.append(table_path)
            return f"frame:{table_path}"

    spark = Spark()
    frames = job.read_session_source_frames(spark, paths)

    assert spark.tables == [
        paths.web_sessions,
        paths.app_sessions,
        paths.rpid_accounts,
        paths.customer_accounts,
        paths.web_pages,
        paths.country_mapping,
    ]
    assert set(frames) == {
        "web_sessions",
        "app_sessions",
        "account_mappings",
        "customer_accounts",
        "page_events",
        "country_mapping",
    }


def test_session_publication_pins_every_delta_source(monkeypatch):
    paths = job.resolve_source_paths(_args())

    class Reader:
        def __init__(self):
            self.version = None
            self.reads = []

        def option(self, name, value):
            assert name == "versionAsOf"
            self.version = value
            return self

        def table(self, table_path):
            self.reads.append((table_path, self.version))
            return f"frame:{table_path}:{self.version}"

    spark = SimpleNamespace(
        read=Reader(),
        catalog=SimpleNamespace(
            getTable=lambda _table_path: SimpleNamespace(tableType="MANAGED"),
            tableExists=lambda _table_path: False,
        ),
    )
    monkeypatch.setattr(job, "latest_delta_version", lambda *_args: 17)
    monkeypatch.setattr(job, "schema_checksum", lambda _frame: "a" * 64)

    frames, bindings = job.read_pinned_session_sources(
        spark,
        paths,
        feature_build_id="100",
        feature_build_attempt_id="101",
        reference_date=date.fromisoformat(REFERENCE_DATE),
        captured_at=job.datetime.now(job.timezone.utc),
        target_catalog="marketingdata_dev",
        target_schema="Stephen_Blain",
    )

    assert len(frames) == 6
    assert len(bindings) == 6
    assert all(binding.delta_version == 17 for binding in bindings)
    assert {binding.source_name for binding in bindings} == set(frames)
    assert all(version == 17 for _, version in spark.read.reads)


def test_session_publication_snapshots_a_view_before_pinning(monkeypatch):
    paths = job.resolve_source_paths(_args())
    snapshotted = []

    class Reader:
        def option(self, _name, _value):
            return self

        def table(self, table_path):
            return f"frame:{table_path}"

    spark = SimpleNamespace(
        read=Reader(),
        catalog=SimpleNamespace(
            getTable=lambda table_path: SimpleNamespace(
                tableType=("VIEW" if table_path == paths.rpid_accounts else "MANAGED")
            ),
            tableExists=lambda _table_path: False,
        ),
    )
    monkeypatch.setattr(job, "latest_delta_version", lambda *_args: 17)
    monkeypatch.setattr(job, "schema_checksum", lambda _frame: "a" * 64)
    monkeypatch.setattr(
        job,
        "snapshot_view_source",
        lambda _spark, **kwargs: (
            snapshotted.append(kwargs)
            or "marketingdata_dev.Stephen_Blain.account_mapping_snapshot"
        ),
    )

    _frames, bindings = job.read_pinned_session_sources(
        spark,
        paths,
        feature_build_id="100",
        feature_build_attempt_id="101",
        reference_date=date.fromisoformat(REFERENCE_DATE),
        captured_at=job.datetime.now(job.timezone.utc),
        target_catalog="marketingdata_dev",
        target_schema="Stephen_Blain",
    )

    assert snapshotted == [
        {
            "source_name": "account_mappings",
            "source_view": paths.rpid_accounts,
            "feature_build_attempt_id": "101",
            "target_catalog": "marketingdata_dev",
            "target_schema": "Stephen_Blain",
        }
    ]
    account_binding = next(
        binding for binding in bindings if binding.source_name == "account_mappings"
    )
    assert account_binding.source_table.endswith("account_mapping_snapshot")


def test_view_snapshot_is_build_scoped_and_records_its_source():
    calls = []

    class Writer:
        def format(self, value):
            calls.append(("format", value))
            return self

        def mode(self, value):
            calls.append(("mode", value))
            return self

        def saveAsTable(self, value):  # noqa: N802 - mirrors Spark writer API
            calls.append(("save", value))

    spark = SimpleNamespace(
        table=lambda table_path: SimpleNamespace(write=Writer()),
        sql=lambda statement: calls.append(("sql", statement)),
        catalog=SimpleNamespace(tableExists=lambda _table_path: False),
    )

    target = job.snapshot_view_source(
        spark,
        source_name="account_mappings",
        source_view="marketingdata_prod.warehouse.rpid_with_accounts",
        feature_build_attempt_id="101",
        target_catalog="marketingdata_dev",
        target_schema="Stephen_Blain",
    )

    assert target.startswith(
        "marketingdata_dev.Stephen_Blain."
        "next_uk_nextads_fs_source_account_mappings_"
    )
    assert ("mode", "errorifexists") in calls
    assert any(
        kind == "sql" and "nextads.source_view" in value
        for kind, value in calls
    )


def test_view_snapshot_retry_reuses_the_same_build_scoped_table():
    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=lambda _table_path: True),
        table=lambda _table_path: pytest.fail("retry must not rewrite source"),
        sql=lambda _statement: pytest.fail("retry must not alter source"),
    )

    first = job.snapshot_view_source(
        spark,
        source_name="account_mappings",
        source_view="marketingdata_prod.warehouse.rpid_with_accounts",
        feature_build_attempt_id="101",
        target_catalog="marketingdata_dev",
        target_schema="Stephen_Blain",
    )
    retry = job.snapshot_view_source(
        spark,
        source_name="account_mappings",
        source_view="marketingdata_prod.warehouse.rpid_with_accounts",
        feature_build_attempt_id="101",
        target_catalog="marketingdata_dev",
        target_schema="Stephen_Blain",
    )

    assert retry == first


@pytest.mark.parametrize(
    ("catalog", "schema"),
    [
        ("marketingdata_prod", "Stephen_Blain"),
        ("marketingdata_dev", "nextads_feature_store"),
        ("marketingdata_dev", "nextads_integration"),
        ("marketingdata_dev", "ds_sandbox"),
    ],
)
def test_failure_injection_is_blocked_outside_personal_dev(catalog, schema):
    with pytest.raises(ValueError, match="personal DEV schema"):
        job.validate_failure_injection(
            job.FAILURE_INJECTION_AFTER_FIRST_WRITE,
            catalog=catalog,
            schema=schema,
        )


def test_failure_injection_is_allowed_in_personal_dev():
    assert job.validate_failure_injection(
        job.FAILURE_INJECTION_AFTER_FIRST_WRITE,
        catalog="marketingdata_dev",
        schema="Stephen_Blain",
    ) == job.FAILURE_INJECTION_AFTER_FIRST_WRITE



def test_main_pins_analytics_output_and_writes_both_registered_tables(
    monkeypatch,
):
    args = _args()
    spark = object()
    client = object()
    registry = SimpleNamespace(
        default_catalog="unused",
        default_schema="unused",
        table_spec=lambda table_name: SimpleNamespace(
            timestamp_key=(
                "session_date"
                if table_name == job.SESSION_TABLE
                else "reference_date"
            ),
            write_mode="merge",
        ),
    )
    binding = DeltaSourceBinding(
        source_role="analytics_pctr_features",
        table_path=SOURCE_TABLE,
        delta_version=31,
        reference_date=date.fromisoformat(REFERENCE_DATE),
    )
    definition = AnalyticsPctrSourceDefinition(
        scope="SHARED_DEV",
        catalog="marketingdata_dev",
        schema="nextads_integration",
        table_name="next_uk_nextads_analytics_pctr_features",
        delta_version=None,
        fixed_reference_date=None,
    )
    session_frames = {
        "web_sessions": "web-sessions",
        "app_sessions": "app-sessions",
        "account_mappings": "accounts",
        "customer_accounts": "customers",
        "page_events": "pages",
        "country_mapping": "countries",
    }
    calls = {"writes": []}

    monkeypatch.setattr(job, "parse_args", lambda: args)
    monkeypatch.setattr(job, "configure_job_logging", lambda _level: None)
    monkeypatch.setattr(job, "log_owned_tables", lambda *_a, **_k: None)
    monkeypatch.setattr(job, "configure_spark", lambda: spark)
    monkeypatch.setattr(job, "load_feature_store_registry", lambda: registry)
    monkeypatch.setattr(
        job,
        "load_analytics_pctr_source_definition",
        lambda *_a, **_k: definition,
    )
    monkeypatch.setattr(
        job,
        "resolve_reference_date_from_theme",
        lambda actual_spark, actual_args: REFERENCE_DATE,
    )

    def load_binding(actual_spark, **kwargs):
        calls["binding"] = (actual_spark, kwargs)
        return binding

    monkeypatch.setattr(
        job,
        "load_analytics_pctr_source_binding",
        load_binding,
    )

    def read_binding(actual_spark, **kwargs):
        calls["reader"] = (actual_spark, kwargs)
        return "analytics-output"

    monkeypatch.setattr(
        job,
        "read_analytics_pctr_source_binding",
        read_binding,
    )
    monkeypatch.setattr(
        job,
        "read_session_source_frames",
        lambda actual_spark, paths: session_frames,
    )
    monkeypatch.setattr(
        job,
        "build_account_advert_affinity_frame",
        lambda frame, run_date: "affinity-output",
    )
    monkeypatch.setattr(
        job,
        "build_analytics_pctr_model_input_frame",
        lambda frame, run_date: "pctr-model-input",
    )

    def build_sessions(*frames):
        calls["session_builder"] = frames
        return "session-output"

    monkeypatch.setattr(job, "build_session_context_frame", build_sessions)

    def validate(builder, outputs, actual_registry):
        calls["ownership"] = (builder, tuple(outputs), actual_registry)

    monkeypatch.setattr(job, "validate_builder_output_tables", validate)
    monkeypatch.setattr(
        job,
        "create_feature_engineering_client",
        lambda: client,
    )

    def write_table(*positional, **kwargs):
        calls["writes"].append((positional, kwargs))
        return f"target:{positional[1]}"

    monkeypatch.setattr(job, "write_feature_table", write_table)

    job.main()

    assert calls["binding"] == (
        spark,
        {
            "definition": definition,
            "receipt_correlation_id": "987654321",
            "reference_date": REFERENCE_DATE,
        },
    )
    assert calls["reader"] == (
        spark,
        {"definition": definition, "binding": binding},
    )
    assert calls["session_builder"] == (
        "web-sessions",
        "app-sessions",
        "accounts",
        "customers",
        "pages",
        "countries",
        REFERENCE_DATE,
    )
    assert calls["ownership"] == (
        job.BUILDER,
        (
            job.AFFINITY_TABLE,
            job.PCTR_MODEL_INPUT_TABLE,
            job.SESSION_TABLE,
        ),
        registry,
    )
    assert [write[0][1:3] for write in calls["writes"]] == [
        (job.AFFINITY_TABLE, "affinity-output"),
        (job.PCTR_MODEL_INPUT_TABLE, "pctr-model-input"),
        (job.SESSION_TABLE, "session-output"),
    ]
    assert calls["writes"][0][1]["reference_date_column"] == ("reference_date")
    assert calls["writes"][1][1]["reference_date_column"] == "reference_date"
    assert calls["writes"][2][1]["reference_date_column"] == "session_date"
    assert all(
        write[1]["feature_engineering_client"] is client
        for write in calls["writes"]
    )

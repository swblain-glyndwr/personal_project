from argparse import Namespace
from datetime import date
import importlib
from pathlib import Path
from types import SimpleNamespace
import sys

from next_ads.features.analytics_pctr_source import (
    AnalyticsPctrSourceDefinition,
    DeltaSourceBinding,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOB_DIRECTORY = PROJECT_ROOT / "jobs" / "features" / "nextads"
if str(JOB_DIRECTORY) not in sys.path:
    sys.path.insert(0, str(JOB_DIRECTORY))

job = importlib.import_module(
    "jobs.features.nextads.validate_analytics_pctr_source"
)


def test_validator_persists_and_exposes_the_actual_source_job_run(monkeypatch):
    spark = object()
    definition = AnalyticsPctrSourceDefinition(
        scope="SHARED_DEV",
        catalog="marketingdata_dev",
        schema="nextads_integration",
        table_name="next_uk_nextads_analytics_pctr_features",
        delta_version=None,
        fixed_reference_date=None,
    )
    resolved = DeltaSourceBinding(
        source_role="analytics_pctr_features",
        table_path=definition.table_path,
        table_id="table-id-1",
        delta_version=31,
        reference_date=date(2026, 8, 1),
        reference_date_row_count=10,
        schema_sha256="a" * 64,
    )
    values = {}

    class TaskValues:
        @staticmethod
        def set(key, value):
            values[key] = value

    monkeypatch.setattr(
        job,
        "parse_args",
        lambda: Namespace(
            reference_date="2026-08-01",
            source_binding="binding.yaml",
            source_catalog="marketingdata_dev",
            source_schema="nextads_integration",
            producing_job_run_id="123456789",
            receipt_correlation_id="987654321",
            log_level="INFO",
        ),
    )
    monkeypatch.setattr(job, "configure_job_logging", lambda _level: None)
    monkeypatch.setattr(job, "configure_spark", lambda: spark)
    monkeypatch.setattr(job, "get_dbutils", lambda: SimpleNamespace(
        jobs=SimpleNamespace(taskValues=TaskValues())
    ))
    monkeypatch.setattr(
        job,
        "load_analytics_pctr_source_definition",
        lambda *_args, **_kwargs: definition,
    )
    monkeypatch.setattr(
        job,
        "bind_analytics_pctr_source",
        lambda *_args, **_kwargs: (resolved, object()),
    )

    persisted = {}

    def persist(
        actual_spark,
        *,
        definition,
        binding,
        receipt_correlation_id,
    ):
        persisted["call"] = (
            actual_spark,
            definition,
            binding,
            receipt_correlation_id,
        )
        return definition.receipt_table_path

    monkeypatch.setattr(
        job,
        "persist_analytics_pctr_source_binding",
        persist,
    )

    job.main()

    receipt = persisted["call"][2]
    assert persisted["call"][:2] == (spark, definition)
    assert persisted["call"][3] == "987654321"
    assert receipt.producing_run_id == "123456789"
    assert values["source_delta_version"] == 31
    assert values["source_table_id"] == "table-id-1"
    assert values["source_schema_sha256"] == "a" * 64
    assert values["source_reference_date_row_count"] == 10
    assert values["source_receipt_table"] == definition.receipt_table_path

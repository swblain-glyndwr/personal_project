import threading
import time
from types import SimpleNamespace

import pytest

import next_ads.ranking.theme_affinity.publish_outputs as publish_outputs_module
from next_ads.ranking.theme_affinity.publish_outputs import (
    DEFAULT_PUBLISH_TABLE_SUFFIXES,
    parse_table_suffixes,
    publish_theme_affinity_outputs,
)


class FakeDataFrame:
    def __init__(self, spark, table_name, row_count=1):
        self.spark = spark
        self.table_name = table_name
        self.row_count = row_count
        self.columns = ["id"]

    def where(self, _condition):
        return self

    def limit(self, _rows):
        return self

    def count(self):
        return self.row_count


class FakeSpark:
    def __init__(self, existing_tables, empty_tables=()):
        self.existing_tables = set(existing_tables)
        self.empty_tables = set(empty_tables)
        self.table_reads = []
        self.writes = []
        self.sql_statements = []
        self.catalog = SimpleNamespace(
            tableExists=lambda table: table in self.existing_tables
        )

    def table(self, table_name):
        self.table_reads.append(table_name)
        if table_name not in self.existing_tables:
            raise RuntimeError(f"missing table: {table_name}")
        return FakeDataFrame(
            self,
            table_name,
            row_count=0 if table_name in self.empty_tables else 1,
        )

    def sql(self, statement):
        self.sql_statements.append(statement)


@pytest.fixture(autouse=True)
def atomic_table_replacement(monkeypatch):
    def replace(df, table, scope, columns, *, spark):
        spark.writes.append(
            {
                "source_table": df.table_name,
                "target_table": table,
                "scope": scope,
                "columns": columns,
                "operation": "replace_scope_by_name",
            }
        )

    monkeypatch.setattr(
        publish_outputs_module,
        "replace_scope_by_name",
        replace,
    )


def test_parse_table_suffixes_uses_default_contract():
    assert parse_table_suffixes("") == DEFAULT_PUBLISH_TABLE_SUFFIXES
    assert parse_table_suffixes(None) == DEFAULT_PUBLISH_TABLE_SUFFIXES


def test_parse_table_suffixes_ignores_empty_values():
    assert parse_table_suffixes(
        "advanced_features, customer_features,,popularity_metrics"
    ) == (
        "advanced_features",
        "customer_features",
        "popularity_metrics",
    )


def test_parse_table_suffixes_rejects_foundation_owned_outputs():
    with pytest.raises(ValueError, match="Foundation-owned outputs"):
        parse_table_suffixes("advanced_features,ranked")


def test_publish_outputs_noops_when_namespaces_match():
    spark = FakeSpark(existing_tables=[])

    published = publish_theme_affinity_outputs(
        spark,
        source_namespace="marketingdata_prod.warehouse",
        target_namespace="marketingdata_prod.warehouse",
        table_prefix="next_uk_nextads_theme_affinity_predict",
        run_date="2026-08-04",
    )

    assert published == []
    assert spark.table_reads == []
    assert spark.writes == []
    assert spark.sql_statements == []


def test_publish_outputs_writes_when_namespace_matches_but_target_prefix_differs():
    namespace = "marketingdata_prod.ds_sandbox"
    source_prefix = "next_uk_nextads_theme_affinity_predict"
    target_prefix = "next_uk_nextads_theme_affinity_predict_publish_test"
    spark = FakeSpark(
        existing_tables={
            f"{namespace}.{source_prefix}_ranked",
        }
    )

    published = publish_theme_affinity_outputs(
        spark,
        source_namespace=namespace,
        target_namespace=namespace,
        table_prefix=source_prefix,
        target_table_prefix=target_prefix,
        table_suffixes=("ranked",),
        run_date="2026-08-04",
    )

    assert published == [f"{namespace}.{target_prefix}_ranked"]
    assert spark.writes == [
        {
            "source_table": f"{namespace}.{source_prefix}_ranked",
            "target_table": f"{namespace}.{target_prefix}_ranked",
            "scope": {"reference_date": "2026-08-04"},
            "columns": ["id"],
            "operation": "replace_scope_by_name",
        }
    ]
    assert spark.sql_statements == [
        "CREATE TABLE IF NOT EXISTS "
        f"`marketingdata_prod`.`ds_sandbox`.`{target_prefix}_ranked` "
        "LIKE "
        f"`marketingdata_prod`.`ds_sandbox`.`{source_prefix}_ranked`"
    ]


def test_publish_outputs_writes_delta_tables_for_configured_suffixes():
    source_namespace = "marketingdata_prod.ds_sandbox"
    target_namespace = "marketingdata_prod.warehouse"
    table_prefix = "next_uk_nextads_theme_affinity_predict"
    suffixes = ("ranked", "complete")
    spark = FakeSpark(
        existing_tables={
            f"{source_namespace}.{table_prefix}_ranked",
            f"{source_namespace}.{table_prefix}_complete",
        }
    )

    published = publish_theme_affinity_outputs(
        spark,
        source_namespace=source_namespace,
        target_namespace=target_namespace,
        table_prefix=table_prefix,
        table_suffixes=suffixes,
        run_date="2026-08-04",
    )

    assert published == [
        f"{target_namespace}.{table_prefix}_ranked",
        f"{target_namespace}.{table_prefix}_complete",
    ]
    assert sorted(spark.writes, key=lambda write: write["target_table"]) == [
        {
            "source_table": f"{source_namespace}.{table_prefix}_complete",
            "target_table": f"{target_namespace}.{table_prefix}_complete",
            "scope": {"reference_date": "2026-08-04"},
            "columns": ["id"],
            "operation": "replace_scope_by_name",
        },
        {
            "source_table": f"{source_namespace}.{table_prefix}_ranked",
            "target_table": f"{target_namespace}.{table_prefix}_ranked",
            "scope": {"reference_date": "2026-08-04"},
            "columns": ["id"],
            "operation": "replace_scope_by_name",
        },
    ]
    assert sorted(spark.sql_statements) == sorted(
        [
            "CREATE TABLE IF NOT EXISTS "
            f"`marketingdata_prod`.`warehouse`.`{table_prefix}_ranked` "
            "LIKE "
            f"`marketingdata_prod`.`ds_sandbox`.`{table_prefix}_ranked`",
            "CREATE TABLE IF NOT EXISTS "
            f"`marketingdata_prod`.`warehouse`.`{table_prefix}_complete` "
            "LIKE "
            f"`marketingdata_prod`.`ds_sandbox`.`{table_prefix}_complete`",
        ]
    )


def test_publish_outputs_runs_concurrently_with_bounded_workers_and_order(
    monkeypatch,
):
    source_namespace = "marketingdata_prod.ds_sandbox"
    target_namespace = "marketingdata_prod.warehouse"
    table_prefix = "next_uk_nextads_theme_affinity_predict"
    suffixes = ("ranked", "complete", "customer_features")
    spark = FakeSpark(
        existing_tables={
            f"{source_namespace}.{table_prefix}_{suffix}"
            for suffix in suffixes
        }
    )
    lock = threading.Lock()
    workers_started = threading.Event()
    active_workers = 0
    peak_workers = 0

    def replace(df, table, scope, columns, *, spark):
        nonlocal active_workers, peak_workers
        with lock:
            active_workers += 1
            peak_workers = max(peak_workers, active_workers)
            if active_workers == 2:
                workers_started.set()
        assert workers_started.wait(timeout=2)
        time.sleep(0.01)
        with lock:
            active_workers -= 1
        spark.writes.append(table)

    monkeypatch.setattr(publish_outputs_module, "MAX_PUBLISH_WORKERS", 2)
    monkeypatch.setattr(
        publish_outputs_module, "replace_scope_by_name", replace
    )

    published = publish_theme_affinity_outputs(
        spark,
        source_namespace=source_namespace,
        target_namespace=target_namespace,
        table_prefix=table_prefix,
        table_suffixes=suffixes,
        run_date="2026-08-04",
    )

    assert peak_workers == 2
    assert published == [
        f"{target_namespace}.{table_prefix}_{suffix}" for suffix in suffixes
    ]


def test_publish_outputs_propagates_worker_failure(monkeypatch):
    source_namespace = "marketingdata_prod.ds_sandbox"
    target_namespace = "marketingdata_prod.warehouse"
    table_prefix = "next_uk_nextads_theme_affinity_predict"
    suffixes = ("ranked", "complete")
    spark = FakeSpark(
        existing_tables={
            f"{source_namespace}.{table_prefix}_{suffix}"
            for suffix in suffixes
        }
    )

    def replace(df, table, scope, columns, *, spark):
        if table.endswith("_complete"):
            raise RuntimeError("copy failed")
        spark.writes.append(table)

    monkeypatch.setattr(
        publish_outputs_module, "replace_scope_by_name", replace
    )

    with pytest.raises(RuntimeError, match="copy failed"):
        publish_theme_affinity_outputs(
            spark,
            source_namespace=source_namespace,
            target_namespace=target_namespace,
            table_prefix=table_prefix,
            table_suffixes=suffixes,
            run_date="2026-08-04",
        )


def test_publish_outputs_does_not_recreate_an_existing_target():
    source_namespace = "marketingdata_prod.ds_sandbox"
    target_namespace = "marketingdata_prod.warehouse"
    table_prefix = "next_uk_nextads_theme_affinity_predict"
    source_table = f"{source_namespace}.{table_prefix}_ranked"
    target_table = f"{target_namespace}.{table_prefix}_ranked"
    spark = FakeSpark(existing_tables={source_table, target_table})

    publish_theme_affinity_outputs(
        spark,
        source_namespace=source_namespace,
        target_namespace=target_namespace,
        table_prefix=table_prefix,
        table_suffixes=("ranked",),
        run_date="2026-08-04",
    )

    assert spark.sql_statements == []
    assert spark.writes[0]["target_table"] == target_table


def test_publish_outputs_fails_clearly_for_missing_source_table():
    spark = FakeSpark(existing_tables=[])

    with pytest.raises(ValueError, match="source table not found"):
        publish_theme_affinity_outputs(
            spark,
            source_namespace="marketingdata_prod.ds_sandbox",
            target_namespace="marketingdata_prod.warehouse",
            table_prefix="next_uk_nextads_theme_affinity_predict",
            table_suffixes=("ranked",),
            run_date="2026-08-04",
        )


def test_publish_outputs_does_not_replace_target_from_empty_run_date():
    source_table = (
        "marketingdata_prod.ds_sandbox."
        "next_uk_nextads_theme_affinity_predict_customer_features"
    )
    spark = FakeSpark(
        existing_tables={source_table},
        empty_tables={source_table},
    )

    with pytest.raises(ValueError, match="empty for 2026-08-04"):
        publish_theme_affinity_outputs(
            spark,
            source_namespace="marketingdata_prod.ds_sandbox",
            target_namespace="marketingdata_prod.warehouse",
            table_prefix="next_uk_nextads_theme_affinity_predict",
            table_suffixes=("customer_features",),
            run_date="2026-08-04",
        )

    assert spark.writes == []

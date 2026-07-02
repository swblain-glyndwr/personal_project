import pytest

from next_ads.ranking.theme_affinity.publish_outputs import (
    DEFAULT_PUBLISH_TABLE_SUFFIXES,
    parse_table_suffixes,
    publish_theme_affinity_outputs,
)


class FakeWriter:
    def __init__(self, spark, source_table):
        self.spark = spark
        self.source_table = source_table
        self.format_value = None
        self.mode_value = None
        self.options = {}

    def format(self, value):
        self.format_value = value
        return self

    def mode(self, value):
        self.mode_value = value
        return self

    def option(self, key, value):
        self.options[key] = value
        return self

    def saveAsTable(self, table_name):  # noqa: N802 - mirrors Spark API
        self.spark.writes.append(
            {
                "source_table": self.source_table,
                "target_table": table_name,
                "format": self.format_value,
                "mode": self.mode_value,
                "options": self.options,
            }
        )


class FakeDataFrame:
    def __init__(self, spark, table_name):
        self.spark = spark
        self.table_name = table_name

    @property
    def write(self):
        return FakeWriter(self.spark, self.table_name)


class FakeSpark:
    def __init__(self, existing_tables):
        self.existing_tables = set(existing_tables)
        self.table_reads = []
        self.writes = []

    def table(self, table_name):
        self.table_reads.append(table_name)
        if table_name not in self.existing_tables:
            raise RuntimeError(f"missing table: {table_name}")
        return FakeDataFrame(self, table_name)


def test_parse_table_suffixes_uses_default_contract():
    assert parse_table_suffixes("") == DEFAULT_PUBLISH_TABLE_SUFFIXES
    assert parse_table_suffixes(None) == DEFAULT_PUBLISH_TABLE_SUFFIXES


def test_parse_table_suffixes_ignores_empty_values():
    assert parse_table_suffixes("ranked, complete,,popularity_metrics") == (
        "ranked",
        "complete",
        "popularity_metrics",
    )


def test_publish_outputs_noops_when_namespaces_match():
    spark = FakeSpark(existing_tables=[])

    published = publish_theme_affinity_outputs(
        spark,
        source_namespace="marketingdata_prod.warehouse",
        target_namespace="marketingdata_prod.warehouse",
        table_prefix="next_uk_nextads_theme_affinity_predict",
    )

    assert published == []
    assert spark.table_reads == []
    assert spark.writes == []


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
    )

    assert published == [f"{namespace}.{target_prefix}_ranked"]
    assert spark.writes == [
        {
            "source_table": f"{namespace}.{source_prefix}_ranked",
            "target_table": f"{namespace}.{target_prefix}_ranked",
            "format": "delta",
            "mode": "overwrite",
            "options": {"overwriteSchema": "true"},
        }
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
    )

    assert published == [
        f"{target_namespace}.{table_prefix}_ranked",
        f"{target_namespace}.{table_prefix}_complete",
    ]
    assert spark.writes == [
        {
            "source_table": f"{source_namespace}.{table_prefix}_ranked",
            "target_table": f"{target_namespace}.{table_prefix}_ranked",
            "format": "delta",
            "mode": "overwrite",
            "options": {"overwriteSchema": "true"},
        },
        {
            "source_table": f"{source_namespace}.{table_prefix}_complete",
            "target_table": f"{target_namespace}.{table_prefix}_complete",
            "format": "delta",
            "mode": "overwrite",
            "options": {"overwriteSchema": "true"},
        },
    ]


def test_publish_outputs_fails_clearly_for_missing_source_table():
    spark = FakeSpark(existing_tables=[])

    with pytest.raises(ValueError, match="source table not found"):
        publish_theme_affinity_outputs(
            spark,
            source_namespace="marketingdata_prod.ds_sandbox",
            target_namespace="marketingdata_prod.warehouse",
            table_prefix="next_uk_nextads_theme_affinity_predict",
            table_suffixes=("ranked",),
        )

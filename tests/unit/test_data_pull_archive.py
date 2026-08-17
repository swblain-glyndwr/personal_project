from datetime import date
from types import SimpleNamespace

import pytest

import jobs.nextads_data.archive_sort_order_data as data_pull


class FakeDataFrame:
    def __init__(self, name, columns=None):
        self.name = name
        self.columns = list(columns or [])
        self.dropped = []

    def drop(self, column):
        self.dropped.append(column)
        return self


class FakeSpark:
    def __init__(self, tables):
        self.tables = tables
        self.reads = []

    def table(self, table):
        self.reads.append(table)
        return self.tables[table]


class FakeLogger:
    def info(self, *_args, **_kwargs):
        pass


def test_write_history_table_uses_supplied_primary_key(monkeypatch):
    calls = []
    df = FakeDataFrame("cms", ["CMSPageID", "cms_data", "rundate"])
    prepared = FakeDataFrame(
        "prepared",
        ["CMSPageID", "cms_data", "rundate"],
    )
    run_date = date(2026, 7, 29)
    spark = object()

    monkeypatch.setattr(
        data_pull,
        "with_run_date",
        lambda df_output, value: (
            prepared
            if df_output is df and value == run_date
            else None
        ),
    )

    def fake_replace(
        spark_arg,
        df_output,
        *,
        table,
        scope,
        key_columns,
        columns,
    ):
        calls.append(
            {
                "spark": spark_arg,
                "df": df_output,
                "table": table,
                "scope": scope,
                "key_columns": key_columns,
                "columns": columns,
            }
        )

    monkeypatch.setattr(
        data_pull,
        "replace_validated_scope",
        fake_replace,
    )

    data_pull.write_history_table(
        df,
        "catalog.schema.cms_history",
        FakeLogger(),
        pk_cols=["CMSPageID"],
        spark=spark,
        run_date=run_date,
    )

    assert calls == [
        {
            "spark": spark,
            "df": prepared,
            "table": "catalog.schema.cms_history",
            "scope": {"rundate": run_date},
            "key_columns": ["CMSPageID"],
            "columns": ["CMSPageID", "cms_data", "rundate"],
        }
    ]
    assert df.dropped == ["run_date", "rundate"]


def test_main_archives_sort_order_and_cms_with_their_own_keys(monkeypatch):
    calls = []
    sort_order_latest = FakeDataFrame("sort_order_latest")
    cms_content_latest = FakeDataFrame("cms_content_latest")
    config = SimpleNamespace(
        tables_write=SimpleNamespace(
            sort_order_v2_latest="catalog.schema.nextads_sort_order_v2_latest",
            sort_order_v2="catalog.schema.nextads_sort_order_v2",
            cms_content_latest="catalog.schema.next_uk_nextads_cms_content_latest",
            cms_content="catalog.schema.next_uk_nextads_cms_content",
        )
    )
    spark = FakeSpark(
        {
            config.tables_write.sort_order_v2_latest: sort_order_latest,
            config.tables_write.cms_content_latest: cms_content_latest,
        }
    )

    monkeypatch.setattr(
        data_pull,
        "setup_run_context",
        lambda *_args: (FakeLogger(), spark, "next_uk", config),
    )
    monkeypatch.setattr(
        data_pull,
        "write_history_table",
        lambda df, table, logger, pk_cols, **kwargs: calls.append(
            (df.name, table, pk_cols, kwargs)
        ),
    )

    run_date = date(2026, 7, 29)
    data_pull.main("dev", "next_uk", "INFO", run_date)

    assert calls == [
        (
            "sort_order_latest",
            "catalog.schema.nextads_sort_order_v2",
            ["UniqueAdID", "item_pos"],
            {"spark": spark, "run_date": run_date},
        ),
        (
            "cms_content_latest",
            "catalog.schema.next_uk_nextads_cms_content",
            ["CMSPageID"],
            {"spark": spark, "run_date": run_date},
        ),
    ]


@pytest.mark.parametrize(
    "invalid_run_date",
    [None, "", " ", "2026/07/29", "2026-7-29", "29-07-2026"],
)
def test_archive_run_date_rejects_non_iso_values(invalid_run_date):
    with pytest.raises(
        ValueError,
        match="--run_date must use ISO format YYYY-MM-DD",
    ):
        data_pull.resolve_run_date(invalid_run_date)


def test_archive_run_date_accepts_exact_iso_date():
    expected = date(2026, 7, 29)

    assert data_pull.resolve_run_date("2026-07-29") == expected
    assert data_pull.resolve_run_date(expected) == expected

from types import SimpleNamespace

import jobs.nextads_data.archive_sort_order_data as data_pull


class FakeDataFrame:
    def __init__(self, name):
        self.name = name
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
    df = FakeDataFrame("cms")

    def fake_delete_from_and_load(df_output, table, pk_cols, del_where):
        calls.append(
            {
                "df": df_output,
                "table": table,
                "pk_cols": pk_cols,
                "del_where": del_where,
            }
        )

    monkeypatch.setattr(
        data_pull, "delete_from_and_load", fake_delete_from_and_load
    )

    data_pull.write_history_table(
        df,
        "catalog.schema.cms_history",
        FakeLogger(),
        pk_cols=["CMSPageID"],
    )

    assert calls == [
        {
            "df": df,
            "table": "catalog.schema.cms_history",
            "pk_cols": ["CMSPageID"],
            "del_where": {"rundate": "current_date()"},
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
        lambda df, table, logger, pk_cols: calls.append(
            (df.name, table, pk_cols)
        ),
    )

    data_pull.main("dev", "next_uk", "INFO")

    assert calls == [
        (
            "sort_order_latest",
            "catalog.schema.nextads_sort_order_v2",
            ["UniqueAdID", "item_pos"],
        ),
        (
            "cms_content_latest",
            "catalog.schema.next_uk_nextads_cms_content",
            ["CMSPageID"],
        ),
    ]

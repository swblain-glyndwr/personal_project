from types import SimpleNamespace

from jobs.nextads_delivery import build_v2_payload


def test_payload_history_is_date_scoped_and_latest_is_replaced(monkeypatch):
    calls = []
    df_output = object()

    def fake_delete_from_and_load(df, table, pk_cols, del_where):
        calls.append(
            {
                "operation": "delete_from_and_load",
                "df": df,
                "table": table,
                "pk_cols": pk_cols,
                "del_where": del_where,
            }
        )

    def fake_truncate_and_load(df, table, pk_cols):
        calls.append(
            {
                "operation": "truncate_and_load",
                "df": df,
                "table": table,
                "pk_cols": pk_cols,
            }
        )

    monkeypatch.setattr(
        build_v2_payload,
        "delete_from_and_load",
        fake_delete_from_and_load,
    )
    monkeypatch.setattr(
        build_v2_payload,
        "truncate_and_load",
        fake_truncate_and_load,
    )

    build_v2_payload.write_payload_tables(
        df_output,
        "catalog.schema.nextads_payload",
        "catalog.schema.nextads_payload_latest",
        SimpleNamespace(info=lambda _message: None),
    )

    assert calls == [
        {
            "operation": "delete_from_and_load",
            "df": df_output,
            "table": "catalog.schema.nextads_payload",
            "pk_cols": ["roamingprofileid"],
            "del_where": {"rundate": "current_date()"},
        },
        {
            "operation": "truncate_and_load",
            "df": df_output,
            "table": "catalog.schema.nextads_payload_latest",
            "pk_cols": ["roamingprofileid"],
        },
    ]

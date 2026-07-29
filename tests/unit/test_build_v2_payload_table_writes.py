from datetime import date
from types import SimpleNamespace

from jobs.nextads_delivery import build_v2_payload


def test_payload_history_is_date_scoped_and_latest_is_replaced(monkeypatch):
    calls = []
    df_output = object()
    spark = object()
    run_date = date(2026, 7, 28)

    def fake_publish_history_and_latest(
        spark_arg,
        df,
        *,
        history_table,
        latest_table,
        key_columns,
        run_date,
    ):
        calls.append(
            {
                "operation": "publish_history_and_latest",
                "spark": spark_arg,
                "df": df,
                "history_table": history_table,
                "latest_table": latest_table,
                "key_columns": key_columns,
                "run_date": run_date,
            }
        )

    monkeypatch.setattr(
        build_v2_payload,
        "publish_history_and_latest",
        fake_publish_history_and_latest,
    )

    build_v2_payload.write_payload_tables(
        df_output,
        "catalog.schema.nextads_payload",
        "catalog.schema.nextads_payload_latest",
        SimpleNamespace(info=lambda _message: None),
        spark=spark,
        run_date=run_date,
    )

    assert calls == [
        {
            "operation": "publish_history_and_latest",
            "spark": spark,
            "df": df_output,
            "history_table": "catalog.schema.nextads_payload",
            "latest_table": "catalog.schema.nextads_payload_latest",
            "key_columns": ["roamingprofileid"],
            "run_date": run_date,
        },
    ]

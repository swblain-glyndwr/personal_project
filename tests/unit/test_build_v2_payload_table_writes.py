from datetime import date
from types import SimpleNamespace

import pytest

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


def test_payload_main_uses_explicit_logical_run_date(monkeypatch):
    run_date = date(2026, 7, 29)
    latest_payload = SimpleNamespace(
        count=lambda: 1,
        show=lambda _rows, truncate: None,
    )

    class Spark:
        def table(self, table):
            assert table == "catalog.schema.nextads_payload_latest"
            return latest_payload

    spark = Spark()
    config = SimpleNamespace(
        tables_write=SimpleNamespace(
            nextads_payload="catalog.schema.nextads_payload",
            nextads_payload_latest="catalog.schema.nextads_payload_latest",
        )
    )
    logger = SimpleNamespace(info=lambda *_args: None)
    input_frames = (object(), object(), object(), object(), object())
    output = object()
    write_calls = []

    monkeypatch.setattr(
        build_v2_payload,
        "setup_run_context",
        lambda *_args: (logger, spark, "next_uk", config),
    )
    monkeypatch.setattr(
        build_v2_payload,
        "capture_run_date",
        lambda _spark: pytest.fail(
            "explicit job run_date must not read the Spark clock"
        ),
    )
    monkeypatch.setattr(
        build_v2_payload,
        "get_payload_experiment_settings",
        lambda _client: {},
    )
    monkeypatch.setattr(
        build_v2_payload,
        "get_input_dataframes",
        lambda _config, _spark: input_frames,
    )
    monkeypatch.setattr(
        build_v2_payload,
        "combine_tables",
        lambda *args: object(),
    )
    monkeypatch.setattr(
        build_v2_payload,
        "make_payload",
        lambda _combined: object(),
    )
    monkeypatch.setattr(
        build_v2_payload,
        "get_rpid_mapping",
        lambda _source: object(),
    )
    monkeypatch.setattr(
        build_v2_payload,
        "set_rpid",
        lambda _payload, _mapping: output,
    )

    def fake_write_payload_tables(
        df_output,
        payload_table,
        payload_latest_table,
        _logger,
        *,
        spark,
        run_date,
    ):
        write_calls.append(
            (
                df_output,
                payload_table,
                payload_latest_table,
                spark,
                run_date,
            )
        )

    monkeypatch.setattr(
        build_v2_payload,
        "write_payload_tables",
        fake_write_payload_tables,
    )

    build_v2_payload.main(
        "prod",
        "next_uk",
        "",
        False,
        "2026-07-29",
    )

    assert write_calls == [
        (
            output,
            "catalog.schema.nextads_payload",
            "catalog.schema.nextads_payload_latest",
            spark,
            run_date,
        )
    ]


@pytest.mark.parametrize("run_date", [None, "", "   "])
def test_payload_run_date_uses_spark_clock_only_as_fallback(
    monkeypatch,
    run_date,
):
    expected = date(2026, 7, 29)
    calls = []

    def fake_capture_run_date(spark):
        calls.append(spark)
        return expected

    monkeypatch.setattr(
        build_v2_payload,
        "capture_run_date",
        fake_capture_run_date,
    )
    spark = object()

    assert build_v2_payload.resolve_run_date(spark, run_date) == expected
    assert calls == [spark]


@pytest.mark.parametrize(
    "run_date",
    ["20260729", "29-07-2026", "2026-02-30", 20260729],
)
def test_payload_run_date_rejects_non_iso_values(run_date):
    with pytest.raises(
        ValueError,
        match="--run_date must use ISO format YYYY-MM-DD",
    ):
        build_v2_payload.resolve_run_date(object(), run_date)

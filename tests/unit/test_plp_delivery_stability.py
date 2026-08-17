from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from next_ads.delivery import google_sheets


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class FakeFrame:
    def withColumn(self, *_args):  # noqa: N802 - mirrors Spark API
        return self

    def select(self, *_args):
        return self


def test_plp_config_client_is_distinct_from_delivery_realm(monkeypatch):
    calls = []
    config = object()
    delivery_config = object()

    monkeypatch.setattr(
        google_sheets.config_manager,
        "load_config",
        lambda job_env, *, client: (
            calls.append(("load", job_env, client)) or config
        ),
    )
    monkeypatch.setattr(
        google_sheets,
        "resolve_plp_gs_delivery_config",
        lambda *, config, client, territory: (
            calls.append(("route", config, client, territory))
            or delivery_config
        ),
    )

    result = google_sheets.load_plp_gs_runtime_config(
        job_env="prod",
        config_client="next_uk",
        delivery_client="next",
        territory="GB",
    )

    assert result == (config, delivery_config)
    assert calls == [
        ("load", "prod", "next_uk"),
        ("route", config, "next", "GB"),
    ]


def test_plp_publication_writes_history_before_latest(monkeypatch):
    calls = []
    frame = FakeFrame()
    spark = object()
    validation = SimpleNamespace(row_count=4)

    monkeypatch.setattr(
        google_sheets,
        "validate_unique_non_null_keys",
        lambda df, keys: (
            calls.append(("validate", df, keys)) or validation
        ),
    )
    monkeypatch.setattr(
        google_sheets,
        "replace_scope_by_name",
        lambda *args, **kwargs: calls.append(
            ("history", args, kwargs)
        ),
    )
    monkeypatch.setattr(
        google_sheets,
        "replace_table_by_name",
        lambda *args, **kwargs: calls.append(
            ("latest", args, kwargs)
        ),
    )

    result = google_sheets.publish_plp_tables(
        frame,
        history_table="plp_history",
        latest_table="plp_latest",
        run_date=date(2026, 7, 29),
        realm="Next",
        territory="GB",
        spark_session=spark,
    )

    assert result is validation
    assert [call[0] for call in calls] == [
        "validate",
        "history",
        "latest",
    ]
    assert calls[0][1:] == (
        frame,
        google_sheets.PLP_GS_OUTPUT_KEY_COLUMNS,
    )
    assert calls[1][1][1] == "plp_history"
    assert calls[1][1][2] == {
        "rundate": date(2026, 7, 29),
        "realm": "Next",
        "territory": "GB",
    }
    assert calls[1][1][3] == google_sheets.PLP_GS_HISTORY_COLUMNS
    assert calls[1][2] == {"spark": spark}
    assert calls[2][1] == (
        frame,
        "plp_latest",
        google_sheets.PLP_GS_OUTPUT_COLUMNS,
    )
    assert calls[2][2] == {"spark": spark}


def test_plp_history_failure_leaves_latest_untouched(monkeypatch):
    latest_calls = []

    monkeypatch.setattr(
        google_sheets,
        "validate_unique_non_null_keys",
        lambda _df, _keys: SimpleNamespace(row_count=1),
    )

    def fail_history(*_args, **_kwargs):
        raise RuntimeError("history unavailable")

    monkeypatch.setattr(
        google_sheets,
        "replace_scope_by_name",
        fail_history,
    )
    monkeypatch.setattr(
        google_sheets,
        "replace_table_by_name",
        lambda *args, **kwargs: latest_calls.append((args, kwargs)),
    )

    with pytest.raises(RuntimeError, match="history unavailable"):
        google_sheets.publish_plp_tables(
            FakeFrame(),
            history_table="plp_history",
            latest_table="plp_latest",
            run_date=date(2026, 7, 29),
            realm="Next",
            territory="GB",
            spark_session=object(),
        )

    assert latest_calls == []


def test_plp_route_has_no_arbitrary_dedup_or_inline_housekeeping():
    source = (
        PROJECT_ROOT / "src/next_ads/delivery/google_sheets.py"
    ).read_text()

    assert ".dropDuplicates(" not in source
    assert ".drop_duplicates(" not in source
    assert "optimize_delta_table" not in source
    assert "f_limit_history" not in source
    assert "limit_history" not in source
    assert "MERGE INTO " not in source.upper()
    assert "DELETE FROM " not in source.upper()
    assert "OPTIMIZE " not in source.upper()
    assert "VACUUM " not in source.upper()
    assert "saveAsTable(" not in source
    assert ".show(" not in source
    assert "replace_table_by_name(" in source
    assert "replace_scope_by_name(" in source
    assert "validate_unique_non_null_keys(" in source


@pytest.mark.parametrize(
    "invalid_run_date",
    [None, "", " ", "2026/07/29", "2026-7-29", "29-07-2026"],
)
def test_plp_run_date_rejects_non_iso_values(invalid_run_date):
    with pytest.raises(
        ValueError,
        match="--run_date must use ISO format YYYY-MM-DD",
    ):
        google_sheets.resolve_run_date(invalid_run_date)


def test_plp_run_date_accepts_exact_iso_date():
    expected = date(2026, 7, 29)

    assert google_sheets.resolve_run_date("2026-07-29") == expected
    assert google_sheets.resolve_run_date(expected) == expected

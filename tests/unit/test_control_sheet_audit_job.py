from datetime import date
from types import SimpleNamespace

import pytest

from jobs.nextads_control import audit_control_sheet as audit_job


def _config():
    return SimpleNamespace(
        tables_write=SimpleNamespace(
            control_sheet_raw_latest="catalog.schema.raw_latest_v1",
            control_sheet_raw="catalog.schema.raw_history_v1",
            control_sheet_latest="catalog.schema.processed_latest_v1",
            control_sheet="catalog.schema.processed_history_v1",
            control_sheet_raw_latest_v2="catalog.schema.raw_latest_v2",
            control_sheet_raw_v2="catalog.schema.raw_history_v2",
            control_sheet_latest_v2="catalog.schema.processed_latest_v2",
            control_sheet_v2="catalog.schema.processed_history_v2",
            cms_content_latest="catalog.schema.cms_latest",
        ),
        locations={
            "HomePage": {},
            "InheritedPage": {"inherit_ads_from": "HomePage"},
        },
        page_types={
            "HomePage": {},
            "ShoppingBagPage": {},
        },
        control_sheet=SimpleNamespace(date_format="dd/MM/yyyy"),
        control_sheet_v2=SimpleNamespace(date_format="dd/MM/yyyy"),
        webhooks=SimpleNamespace(input_warnings="input-warning-url"),
    )


class _Spark:
    def __init__(self):
        self.tables = []

    def table(self, table_name):
        self.tables.append(table_name)
        return table_name


class _Logger:
    def __init__(self):
        self.warning_calls = []
        self.exception_calls = []

    def info(self, *_args):
        return None

    def warning(self, *args):
        self.warning_calls.append(args)

    def exception(self, *args):
        self.exception_calls.append(args)


class _Report:
    has_warnings = True
    warning_count = 3

    @staticmethod
    def render():
        return "full warning report"

    @staticmethod
    def compact_message(max_chars):
        assert max_chars == 3500
        return "compact warning report"


def test_parse_run_date_requires_strict_iso_date():
    assert audit_job.parse_run_date("2026-07-29") == date(2026, 7, 29)

    for value in ("29/07/2026", "2026-7-29", ""):
        with pytest.raises(ValueError, match="ISO format"):
            audit_job.parse_run_date(value)


def test_route_specs_use_their_own_control_scope_contracts():
    config = _config()
    run_date = date(2026, 7, 29)

    v1 = audit_job.build_audit_spec(config, "v1", run_date)
    assert v1.placement_columns == ("HomePage",)
    assert v1.expected_scopes == ("HomePage", "InheritedPage")
    assert v1.scope_column == "Location"

    v2 = audit_job.build_audit_spec(config, "v2", run_date)
    assert v2.placement_columns == ("HomePage", "ShoppingBagPage")
    assert v2.expected_scopes == ("HomePage", "ShoppingBagPage")
    assert v2.scope_column == "PageType"


def test_warn_only_prod_audit_reads_snapshots_and_posts_one_message(monkeypatch):
    config = _config()
    spark = _Spark()
    logger = _Logger()
    calls = []
    posts = []

    monkeypatch.setattr(
        audit_job,
        "load_previous_partition",
        lambda history, run_date: (f"previous:{history}", run_date),
    )

    def fake_audit_control_sheet(**kwargs):
        calls.append(kwargs)
        return _Report()

    monkeypatch.setattr(
        audit_job,
        "audit_control_sheet",
        fake_audit_control_sheet,
    )
    monkeypatch.setattr(
        audit_job,
        "post_to_webhook",
        lambda url, message: posts.append((url, message)),
    )

    report = audit_job.run_audit(
        spark=spark,
        config=config,
        route="v2",
        run_date=date(2026, 7, 29),
        warn_only=True,
        job_env="prod",
        logger=logger,
    )

    assert report is not None
    assert spark.tables == [
        "catalog.schema.raw_latest_v2",
        "catalog.schema.raw_history_v2",
        "catalog.schema.processed_latest_v2",
        "catalog.schema.processed_history_v2",
        "catalog.schema.cms_latest",
    ]
    assert calls == [
        {
            "raw_current": "catalog.schema.raw_latest_v2",
            "processed_current": "catalog.schema.processed_latest_v2",
            "cms_latest": "catalog.schema.cms_latest",
            "spec": audit_job.build_audit_spec(
                config,
                "v2",
                date(2026, 7, 29),
            ),
            "previous_raw": "previous:catalog.schema.raw_history_v2",
            "previous_processed": (
                "previous:catalog.schema.processed_history_v2"
            ),
        }
    ]
    assert logger.warning_calls == [("\n%s", "full warning report")]
    assert posts == [
        ("input-warning-url", "compact warning report")
    ]


def test_warn_only_audit_propagates_technical_runtime_error(monkeypatch):
    config = _config()
    logger = _Logger()
    posts = []

    def fail_audit(**_kwargs):
        raise RuntimeError("test audit failure")

    monkeypatch.setattr(audit_job, "run_audit", fail_audit)
    monkeypatch.setattr(
        audit_job,
        "post_warning_safely",
        lambda logger, url, message: posts.append((url, message)),
    )

    with pytest.raises(RuntimeError, match="test audit failure"):
        audit_job.run_warning_only_audit(
            spark=_Spark(),
            config=config,
            route="v1",
            run_date=date(2026, 7, 29),
            warn_only=True,
            job_env="prod",
            logger=logger,
        )

    assert logger.exception_calls == []
    assert posts == []


def test_warn_only_main_setup_error_fails_the_candidate_route(
    monkeypatch,
):
    logger = _Logger()

    def fail_config(*_args, **_kwargs):
        raise RuntimeError("test config failure")

    monkeypatch.setattr(
        audit_job,
        "configure_logging",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(audit_job, "get_logger", lambda _name: logger)
    monkeypatch.setattr(
        audit_job.config_manager,
        "load_config",
        fail_config,
    )

    with pytest.raises(RuntimeError, match="test config failure"):
        audit_job.main(
            JOB_ENV="dev",
            CLIENT="next_uk",
            ROUTE="v1",
            RUN_DATE="2026-07-29",
            LOG_LEVEL="",
            WARN_ONLY=True,
        )

    assert logger.exception_calls == []

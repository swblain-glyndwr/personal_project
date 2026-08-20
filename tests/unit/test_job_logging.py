from __future__ import annotations

import logging
from pathlib import Path

import pytest

from next_ads.common.job_logging import configure_job_logging


REPO_ROOT = Path(__file__).resolve().parents[2]
MIGRATED_ENTRYPOINTS = {
    "jobs/model/development/adopt_analytics_pctr.py",
    "jobs/model/development/promote_exact_model.py",
    "jobs/model/development/run_declared_model.py",
    "jobs/model/development/run_declared_model_operation.py",
    "jobs/model/development/run_shopping_bag_ongoing_evaluation.py",
    "jobs/model/development/smoke_model_development_runtime.py",
    "jobs/model/research/run_automl_discovery.py",
    "jobs/model/research/run_declared_research.py",
    "jobs/model/research/select_research_candidate.py",
    "jobs/model/research/smoke_model_research_runtime.py",
    "jobs/orchestration/validate_model_scoring_request.py",
    "jobs/orchestration/validate_nextads_operation.py",
    "jobs/table_operations/table_maintenance.py",
    "src/next_ads/delivery/cosmos.py",
}
SAFE_DIRECT_CONFIGURATION = {
    "jobs/features/nextads/_registry_job.py",
    "jobs/table_operations/create_feature_store_tables.py",
    "jobs/table_operations/table_operations.py",
    "src/next_ads/common/job_logging.py",
}


def test_configure_job_logging_keeps_application_info_and_quiets_py4j(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root_logger = logging.getLogger()
    application_logger = logging.getLogger("next_ads.test.application")
    py4j_logger = logging.getLogger("py4j")
    clientserver_logger = logging.getLogger("py4j.clientserver")
    original_levels = {
        root_logger: root_logger.level,
        application_logger: application_logger.level,
        py4j_logger: py4j_logger.level,
        clientserver_logger: clientserver_logger.level,
    }
    configured: dict[str, object] = {}

    def fake_basic_config(**kwargs: object) -> None:
        configured.update(kwargs)
        root_logger.setLevel(int(kwargs["level"]))

    monkeypatch.setattr(logging, "basicConfig", fake_basic_config)
    try:
        application_logger.setLevel(logging.NOTSET)
        py4j_logger.setLevel(logging.NOTSET)
        clientserver_logger.setLevel(logging.NOTSET)

        configure_job_logging(
            "info",
            log_format="%(levelname)s %(message)s",
            force=True,
        )

        assert application_logger.getEffectiveLevel() == logging.INFO
        assert py4j_logger.getEffectiveLevel() == logging.WARNING
        assert clientserver_logger.getEffectiveLevel() == logging.WARNING
        assert configured == {
            "force": True,
            "format": "%(levelname)s %(message)s",
            "level": logging.INFO,
        }
    finally:
        for logger, level in original_levels.items():
            logger.setLevel(level)


def test_configure_job_logging_rejects_unknown_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def fake_basic_config(**_kwargs: object) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(logging, "basicConfig", fake_basic_config)

    with pytest.raises(ValueError, match="Unsupported log level"):
        configure_job_logging("verbose")

    assert called is False


def test_raw_basic_config_is_limited_to_shared_or_already_safe_helpers() -> (
    None
):
    raw_configuration = {
        path.relative_to(REPO_ROOT).as_posix()
        for root in (REPO_ROOT / "jobs", REPO_ROOT / "src")
        for path in root.rglob("*.py")
        if "logging.basicConfig"
        in path.read_text(encoding="utf-8", errors="replace")
    }

    assert raw_configuration <= SAFE_DIRECT_CONFIGURATION
    assert "src/next_ads/common/job_logging.py" in raw_configuration


def test_migrated_entrypoints_use_shared_job_logging() -> None:
    for relative_path in MIGRATED_ENTRYPOINTS:
        source = (REPO_ROOT / relative_path).read_text(
            encoding="utf-8", errors="replace"
        )
        assert "from next_ads.common.job_logging import" in source
        assert "configure_job_logging(" in source

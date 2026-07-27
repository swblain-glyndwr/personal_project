import sys
from pathlib import Path

import pytest

from jobs.table_operations import setup_dev_tables

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_dev_setup_rejects_non_dev_environment():
    with pytest.raises(ValueError, match="job_env=dev"):
        setup_dev_tables.run_dev_setup(
            mode="create_only",
            job_env="preprod",
            client="next_uk",
            log_level="INFO",
        )


def test_create_only_creates_missing_tables_without_seeding(monkeypatch):
    create_calls = []
    seed_calls = []

    monkeypatch.setattr(setup_dev_tables, "bootstrap_project_imports", lambda: None)
    monkeypatch.setattr(
        "jobs.table_operations.table_operations.create_missing_tables",
        lambda **kwargs: create_calls.append(kwargs),
    )
    monkeypatch.setattr(
        setup_dev_tables,
        "seed_latest_tables",
        lambda **kwargs: seed_calls.append(kwargs),
    )

    setup_dev_tables.run_dev_setup(
        mode="create_only",
        job_env="dev",
        client="next_uk",
        log_level="INFO",
    )

    assert create_calls == [
        {
            "job_env": "dev",
            "client": "next_uk",
            "log_level": "INFO",
            "confirm_mutating": True,
            "dry_run": False,
        }
    ]
    assert seed_calls == []


def test_seed_latest_creates_missing_tables_then_seeds(monkeypatch):
    calls = []

    monkeypatch.setattr(setup_dev_tables, "bootstrap_project_imports", lambda: None)
    monkeypatch.setattr(
        "jobs.table_operations.table_operations.create_missing_tables",
        lambda **kwargs: calls.append(("create", kwargs)),
    )
    monkeypatch.setattr(
        setup_dev_tables,
        "seed_latest_tables",
        lambda **kwargs: calls.append(("seed", kwargs)),
    )

    setup_dev_tables.run_dev_setup(
        mode="seed_latest",
        job_env="dev",
        client="next_uk",
        log_level="INFO",
    )

    assert calls == [
        (
            "create",
            {
                "job_env": "dev",
                "client": "next_uk",
                "log_level": "INFO",
                "confirm_mutating": True,
                "dry_run": False,
            },
        ),
        ("seed", {"client": "next_uk", "log_level": "INFO"}),
    ]


def test_dev_setup_does_not_run_candidate_pipeline_tasks(monkeypatch):
    monkeypatch.delitem(sys.modules, "scripts.parse_attributes", raising=False)
    monkeypatch.delitem(sys.modules, "scripts.parse_theme_mapping", raising=False)
    monkeypatch.delitem(sys.modules, "scripts.build_markov_chain", raising=False)
    monkeypatch.setattr(setup_dev_tables, "bootstrap_project_imports", lambda: None)
    monkeypatch.setattr(
        "jobs.table_operations.table_operations.create_missing_tables",
        lambda **kwargs: None,
    )

    setup_dev_tables.run_dev_setup(
        mode="create_only",
        job_env="dev",
        client="next_uk",
        log_level="INFO",
    )

    assert "scripts.parse_attributes" not in sys.modules
    assert "scripts.parse_theme_mapping" not in sys.modules
    assert "scripts.build_markov_chain" not in sys.modules


def test_legacy_sample_flag_maps_to_seed_latest():
    args = setup_dev_tables.parse_args(["--sample"])

    assert args.mode == "seed_latest"


def test_mode_argument_maps_to_requested_setup_mode():
    args = setup_dev_tables.parse_args(["--mode", "seed_latest"])

    assert args.mode == "seed_latest"


def test_legacy_standard_flag_maps_to_create_only():
    args = setup_dev_tables.parse_args(["--standard"])

    assert args.mode == "create_only"


def test_legacy_setup_and_candidate_wrappers_are_removed():
    obsolete_wrappers = [
        PROJECT_ROOT / "scripts" / "table_operations" / "setup_dev_tables.py",
        PROJECT_ROOT / "scripts" / "build_markov_chain.py",
        PROJECT_ROOT / "jobs" / "nextads_main" / "build_markov_chain.py",
    ]

    for wrapper in obsolete_wrappers:
        assert not wrapper.exists()

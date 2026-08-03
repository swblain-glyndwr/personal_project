from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from jobs.table_operations.create_tables import (
    extract_create_table_columns,
    extract_table_paths,
)
from next_ads.common.config_manager import load_config
from next_ads.common.paths import resolve_sql_contract_path
from next_ads.ranking.scoring_manifest import (
    ALL,
    EVALUATE,
    FAILED_BEFORE_PUBLISH,
    FALLBACK_PREVIOUS,
    READY,
    READY_FOR_NEXTADS,
    SERVING,
    ScoreProviderBuild,
    ScoreProviderSignal,
    ScoringInputSnapshot,
    ScoringInputSource,
    ScoringPortfolio,
    ScoringPortfolioEntry,
    validate_score_provider_builds,
    validate_scoring_config,
    validate_scoring_input_snapshots,
    validate_scoring_portfolios,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUN_DATE = date(2026, 7, 30)
COMPLETED_AT = datetime(2026, 7, 30, 16, 0, tzinfo=timezone.utc)
TABLE_REFS = (
    "scoring_input_snapshots",
    "scoring_input_snapshot_sources",
    "scoring_input_item_themes",
    "scoring_input_theme_mapping_raw",
    "scoring_foundation_builds",
    "scoring_foundation_outputs",
    "scoring_foundation_run_contexts",
    "score_provider_run_contexts",
    "score_provider_builds",
    "score_provider_signals",
    "scoring_portfolios",
    "scoring_portfolio_entries",
)


def _build(
    provider_build_id: str = "theme_affinity_20260730",
    provider_id: str = "theme_affinity",
    *,
    attempt: int = 0,
    status: str = READY_FOR_NEXTADS,
) -> ScoreProviderBuild:
    ready = status == READY_FOR_NEXTADS
    return ScoreProviderBuild(
        provider_build_id=provider_build_id,
        provider_build_attempt_id=f"{provider_build_id}:attempt:{attempt}",
        input_snapshot_id="scoring_inputs_20260730",
        run_date=RUN_DATE,
        capability="account_theme",
        use_case="theme_ranking",
        provider_id=provider_id,
        provider_version=f"{provider_id}/v1",
        contract_version="account_entity_scores/v1",
        model_name=provider_id,
        model_version="1",
        model_uri=f"models:/{provider_id}/1",
        pipeline_update_id="update-123",
        output_snapshot_id=(f"{provider_build_id}_output" if ready else None),
        output_table=(
            "catalog.schema.score_provider_signals" if ready else None
        ),
        output_delta_version=42 if ready else None,
        row_count=100 if ready else 0,
        account_count=10 if ready else 0,
        entity_count=20 if ready else 0,
        null_key_count=0,
        duplicate_key_count=0,
        invalid_score_count=0,
        output_checksum="abc123" if ready else None,
        warning_count=0,
        status=status,
        task_run_id=123 + attempt,
        execution_count=attempt,
        completed_at=COMPLETED_AT + timedelta(minutes=attempt),
    )


def _entry(
    entry_id: str,
    provider_build_id: str,
    role: str,
    mode: str,
    priority: int,
    slot: str | None = None,
) -> ScoringPortfolioEntry:
    return ScoringPortfolioEntry(
        portfolio_entry_id=entry_id,
        provider_build_id=provider_build_id,
        policy_role=role,
        execution_mode=mode,
        priority=priority,
        serving_slot=slot,
    )


def _portfolio(
    portfolio_id: str,
    route: str,
    *,
    attempt: int = 0,
    location: str = ALL,
    page_type: str = ALL,
    audience: str = ALL,
    customer_cell: str = ALL,
    status: str = READY_FOR_NEXTADS,
) -> ScoringPortfolio:
    prefix = f"{route}_{attempt}"
    entries = (
        _entry(
            f"{prefix}_best",
            "theme_affinity_20260730",
            "CHAMPION",
            SERVING,
            1,
            "best",
        ),
        _entry(
            f"{prefix}_best_challenger",
            "theme_affinity_20260730",
            "CHALLENGER",
            SERVING,
            1,
            "best_challenger",
        ),
        _entry(
            f"{prefix}_markov_shadow",
            "markov_20260730",
            "SHADOW",
            EVALUATE,
            1,
        ),
    )
    return ScoringPortfolio(
        portfolio_id=portfolio_id,
        portfolio_attempt_id=f"{portfolio_id}:attempt:{attempt}",
        run_date=RUN_DATE,
        capability="account_theme",
        use_case="theme_ranking",
        route=route,
        policy_id=f"{route}_default",
        policy_priority=100,
        location=location,
        page_type=page_type,
        audience=audience,
        customer_cell=customer_cell,
        contract_version="account_entity_scores/v1",
        status=status,
        warning_count=0,
        task_run_id=123 + attempt,
        execution_count=attempt,
        completed_at=COMPLETED_AT + timedelta(minutes=attempt),
        entries=entries,
    )


def _source(
    *,
    attempt: int = 0,
    required: bool = True,
    row_count: int = 10,
    null_key_count: int = 0,
    duplicate_key_count: int = 0,
) -> ScoringInputSource:
    return ScoringInputSource(
        input_snapshot_id="scoring_inputs_20260730",
        input_snapshot_attempt_id=f"scoring_inputs:attempt:{attempt}",
        run_date=RUN_DATE,
        source_name="theme_mapping",
        source_role="ITEM_THEME_MAPPING",
        source_table="catalog.schema.item_themes",
        delta_version=12,
        schema_version="item_themes/v1",
        is_required=required,
        row_count=row_count,
        distinct_key_count=max(
            0,
            row_count - duplicate_key_count,
        ),
        null_key_count=null_key_count,
        duplicate_key_count=duplicate_key_count,
        content_checksum="checksum",
        task_run_id=200 + attempt,
        execution_count=attempt,
        captured_at=COMPLETED_AT,
    )


def _snapshot(
    *,
    attempt: int = 0,
    source: ScoringInputSource | None = None,
) -> ScoringInputSnapshot:
    source = source or _source(attempt=attempt)
    return ScoringInputSnapshot(
        input_snapshot_id=source.input_snapshot_id,
        input_snapshot_attempt_id=source.input_snapshot_attempt_id,
        run_date=RUN_DATE,
        input_schema_version="scoring_inputs/v1",
        status=READY,
        warning_count=0,
        task_run_id=300 + attempt,
        execution_count=attempt,
        completed_at=COMPLETED_AT + timedelta(minutes=attempt),
        sources=(source,),
    )


def test_provider_build_is_role_and_model_neutral():
    theme_affinity = _build()
    markov = _build("markov_20260730", "markov")

    assert theme_affinity.capability == markov.capability
    assert not hasattr(theme_affinity, "model_role")
    assert not hasattr(theme_affinity, "theme_affinity_build_id")
    assert not hasattr(theme_affinity, "theme_input_snapshot_id")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_canonical_provider_signal_rejects_non_finite_scores(value):
    with pytest.raises(ValueError, match="must be finite"):
        ScoreProviderSignal(
            provider_build_id="build",
            account_number="account",
            entity_type="theme",
            entity_id="menswear",
            provider_id="theme_affinity",
            run_date=RUN_DATE,
            raw_score=value,
            score=0.5,
            provider_rank=1,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("row_count", 0),
        ("account_count", 0),
        ("entity_count", 0),
        ("null_key_count", 1),
        ("duplicate_key_count", 1),
        ("invalid_score_count", 1),
    ],
)
def test_ready_provider_build_requires_safe_non_empty_output(field, value):
    with pytest.raises(ValueError):
        replace(_build(), **{field: value})

    failed = _build(status=FAILED_BEFORE_PUBLISH)
    assert failed.output_checksum is None
    assert failed.row_count == 0


def test_portfolio_preserves_live_slots_and_allows_evaluated_challengers():
    portfolio = _portfolio("portfolio_v1", "v1")
    assert portfolio.entries[0].provider_build_id == (
        portfolio.entries[1].provider_build_id
    )

    extra = _entry(
        "v1_future_challenger",
        "future_provider_20260730",
        "CHALLENGER",
        EVALUATE,
        2,
    )
    expanded = replace(
        portfolio,
        entries=(*portfolio.entries, extra),
    )
    assert (
        len(
            [
                entry
                for entry in expanded.entries
                if entry.policy_role == "CHALLENGER"
            ]
        )
        == 2
    )

    with pytest.raises(ValueError, match="Unsupported execution mode"):
        _entry("bad", "provider", "CHALLENGER", "LIVE", 1)


def test_portfolios_are_selected_by_typed_scope():
    hp = _portfolio(
        "portfolio_hp",
        "v1",
        location="PH3",
        page_type="HomePage",
        audience="older",
        customer_cell="Champion",
    )
    sb = _portfolio(
        "portfolio_sb",
        "v1",
        location="SB1",
        page_type="ShoppingBagPage",
        audience="older",
        customer_cell="Champion",
    )
    assert validate_scoring_portfolios((hp, sb)) == (hp, sb)

    duplicate_scope = replace(
        hp,
        portfolio_id="another_hp",
        portfolio_attempt_id="another_hp:attempt:0",
    )
    with pytest.raises(ValueError, match="route/use-case/scope"):
        validate_scoring_portfolios((hp, duplicate_scope))


def test_fallback_portfolio_records_accepted_source():
    current = _portfolio("portfolio_v1", "v1")
    with pytest.raises(ValueError, match="accepted source"):
        replace(current, status=FALLBACK_PREVIOUS)

    fallback = replace(
        current,
        status=FALLBACK_PREVIOUS,
        fallback_source_portfolio_id="portfolio_v1_20260729",
        fallback_source_run_date=RUN_DATE - timedelta(days=1),
        fallback_source_completed_at=COMPLETED_AT - timedelta(hours=23),
    )
    assert fallback.fallback_source_portfolio_id.endswith("20260729")


def test_manifest_attempt_selection_matches_task_repair_ordering():
    failed = _build(status=FAILED_BEFORE_PUBLISH)
    repaired = _build(attempt=1)
    assert validate_score_provider_builds((failed, repaired)) == (repaired,)

    later_completion = replace(
        repaired,
        provider_build_attempt_id="theme_affinity:later-completion",
        completed_at=repaired.completed_at + timedelta(minutes=1),
    )
    assert validate_score_provider_builds(
        (failed, repaired, later_completion)
    ) == (later_completion,)

    contradictory = replace(
        repaired,
        provider_build_attempt_id="theme_affinity:contradictory",
    )
    with pytest.raises(ValueError, match="Contradictory"):
        validate_score_provider_builds((repaired, contradictory))


def test_input_attempt_accepts_parallel_source_task_provenance():
    snapshot = _snapshot()
    assert snapshot.sources[0].task_run_id != snapshot.task_run_id

    repaired = _snapshot(attempt=1)
    assert validate_scoring_input_snapshots((snapshot, repaired)) == (
        repaired,
    )


@pytest.mark.parametrize(
    "source",
    [
        _source(row_count=0),
        _source(null_key_count=1),
        _source(duplicate_key_count=1),
    ],
)
def test_ready_input_rejects_invalid_required_sources(source):
    with pytest.raises(ValueError, match="structurally invalid"):
        _snapshot(source=source)

    optional = replace(source, is_required=False)
    assert _snapshot(source=optional).sources == (optional,)


def test_scoring_tables_have_repo_owned_table_operation_contracts(
    monkeypatch,
):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "test_user")
    config = load_config("dev", client="next_uk")
    configured = extract_table_paths(config.tables_write.to_dict())

    for table_ref in TABLE_REFS:
        contract = resolve_sql_contract_path(table_ref)
        assert contract.is_file()
        assert extract_create_table_columns(contract.read_text())
        assert configured[table_ref].startswith(
            "marketingdata_dev.test_user.next_uk_nextads_"
        )


def test_scoring_table_constraints_are_dbr_15_4_compatible():
    for table_ref in TABLE_REFS:
        sql = resolve_sql_contract_path(table_ref).read_text().upper()
        assert "CHECK (" not in sql
        assert "UNIQUE (" not in sql
        if "CONSTRAINT " in sql:
            assert "PRIMARY KEY (" in sql

    signals_sql = resolve_sql_contract_path(
        "score_provider_signals"
    ).read_text()
    assert "partitioned by (RunDate, ProviderID)" in signals_sql
    builds_sql = resolve_sql_contract_path("score_provider_builds").read_text()
    assert "ThemeAffinityBuildID" not in builds_sql
    assert "ProviderBuildAttemptID string not null" in builds_sql
    assert "ScoringFoundationBuildID string" in builds_sql
    assert "ScoringFoundationBuildAttemptID string" in builds_sql
    assert [
        column
        for column, _ in extract_create_table_columns(builds_sql)
    ][-2:] == [
        "ScoringFoundationBuildID",
        "ScoringFoundationBuildAttemptID",
    ]

    contexts_sql = resolve_sql_contract_path(
        "score_provider_run_contexts"
    ).read_text()
    assert [
        column
        for column, _ in extract_create_table_columns(contexts_sql)
    ][-2:] == [
        "ScoringFoundationBuildID",
        "ScoringFoundationBuildAttemptID",
    ]

    foundation_builds_sql = resolve_sql_contract_path(
        "scoring_foundation_builds"
    ).read_text()
    assert "InputSnapshotAttemptID string not null" in foundation_builds_sql
    assert "InputBindingsJSON string not null" in foundation_builds_sql
    assert "PipelineID string" in foundation_builds_sql
    assert "PipelineUpdateID string" in foundation_builds_sql
    assert "PipelineTaskRunID bigint" in foundation_builds_sql
    assert "PipelineUpdateType string" in foundation_builds_sql

    foundation_outputs_sql = resolve_sql_contract_path(
        "scoring_foundation_outputs"
    ).read_text()
    assert "SourceTable string not null" in foundation_outputs_sql
    assert "SourceDeltaVersion bigint not null" in foundation_outputs_sql
    assert "SourceSchemaChecksum string not null" in foundation_outputs_sql
    assert "OutputSchemaChecksum string not null" in foundation_outputs_sql
    assert "InvalidValueCount bigint not null" in foundation_outputs_sql

    foundation_contexts_sql = resolve_sql_contract_path(
        "scoring_foundation_run_contexts"
    ).read_text()
    assert "InputSnapshotAttemptID string not null" in foundation_contexts_sql


@pytest.mark.parametrize(
    ("job_env", "namespace"),
    [
        ("preprod", "marketingdata_prod.ds_sandbox"),
        ("prod", "marketingdata_prod.warehouse"),
    ],
)
def test_scoring_tables_resolve_for_release_environments(
    monkeypatch,
    job_env,
    namespace,
):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    config = load_config(job_env, client="next_uk")
    for table_ref in TABLE_REFS:
        assert config.tables_write.get(table_ref).startswith(
            f"{namespace}.next_uk_nextads_"
        )


def test_scoring_config_preserves_current_serving_and_shadow_policy(
    monkeypatch,
):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "test_user")
    config = load_config("dev", client="next_uk")
    scoring = config.scoring.to_dict()
    validate_scoring_config(scoring)

    providers = scoring["providers"]
    assert providers["theme_affinity"]["capability"] == "account_theme"
    assert providers["markov"]["capability"] == "account_theme"
    assert scoring["canonical"]["provider_signals_table"].endswith(
        "next_uk_nextads_score_provider_signals"
    )
    routes = scoring["client_portfolios"]["next_uk"]["theme_ranking"]["routes"]
    for route in routes.values():
        entries = route["policies"][0]["entries"]
        live = {
            entry["serving_slot"]: entry["provider_id"]
            for entry in entries
            if entry["execution_mode"] == SERVING
        }
        evaluated = [
            entry["provider_id"]
            for entry in entries
            if entry["execution_mode"] == EVALUATE
        ]
        assert live == {
            "best": "theme_affinity",
            "best_challenger": "theme_affinity",
        }
        assert evaluated == ["markov"]


def test_config_accepts_native_provider_and_typed_scope_policy(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "test_user")
    scoring = deepcopy(load_config("dev", client="next_uk").scoring.to_dict())
    scoring["providers"]["ad_ctr"] = {
        "provider_id": "ad_ctr",
        "provider_version": "ad_ctr/v1",
        "implementation": "ad_ctr",
        "capability": "account_ad",
        "foundation_id": None,
        "adapter": "canonical_provider_job",
        "entity_type": "ad",
        "score_direction": "higher_is_better",
        "max_entities_per_account": 20,
    }
    v2 = scoring["client_portfolios"]["next_uk"]["theme_ranking"]["routes"][
        "v2"
    ]
    page_policy = deepcopy(v2["policies"][0])
    page_policy["policy_id"] = "v2_homepage"
    page_policy["priority"] = 10
    page_policy["selector"]["page_types"] = ["HomePage"]
    page_policy["selector"]["audiences"] = ["older", "younger"]
    v2["policies"].append(page_policy)

    validate_scoring_config(scoring)

    scoring["providers"]["ad_ctr"]["entity_type"] = "theme"
    with pytest.raises(ValueError, match="must match its capability"):
        validate_scoring_config(scoring)


def test_config_manager_loads_repo_owned_scoring_settings():
    from next_ads.common import config_manager

    assert "configs/scoring/scoring_settings.yaml" in (
        config_manager._settings_files()
    )


def test_legacy_client_config_exposes_scoring_tables():
    config = yaml.safe_load(
        (PROJECT_ROOT / "configs/clients/next_uk.yaml").read_text()
    )
    write_tables = config["default"]["tables"]["write"]
    for table_ref in TABLE_REFS:
        assert table_ref in write_tables

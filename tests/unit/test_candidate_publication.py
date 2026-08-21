from copy import deepcopy
from datetime import date, datetime, timezone
from types import SimpleNamespace
import time

import pytest

from next_ads.candidates import runtime
from next_ads.candidates.publication import (
    CandidateBuild,
    CandidateBuildPublisher,
    ServingPortfolioEntry,
    build_candidate_context,
    candidate_policy_checksum,
    group_serving_entries,
    load_serving_portfolio_entries,
    select_candidate_build,
    validate_assignment_rank_limit,
)
from next_ads.common.delta_writes import DeltaWriteReceipt
from next_ads.ranking.theme_score_retrieval import build_ad_group_mappings


RUN_DATE = date(2026, 8, 7)
COMPLETED_AT = datetime(2026, 8, 7, 18, 0, tzinfo=timezone.utc)


def _entry(
    entry_id="v1_best",
    slot="best",
    provider="theme_affinity",
):
    return ServingPortfolioEntry(
        portfolio_id="portfolio-v1",
        portfolio_attempt_id="portfolio-v1:attempt:0",
        portfolio_entry_id=entry_id,
        provider_build_id=f"{provider}-build",
        provider_build_attempt_id=f"{provider}-build:attempt:0",
        provider_output_table="catalog.schema.provider_signals",
        provider_output_delta_version=42,
        provider_source_run_date=RUN_DATE,
        input_snapshot_id="inputs-current",
        serving_slot=slot,
        experiment_id="delivery",
        variant_id=slot,
    )


def _context(entries, *, execution_count=0):
    return build_candidate_context(
        run_date=RUN_DATE,
        route="v1",
        output_grain="location",
        entries=entries,
        candidate_foundation_snapshot_id="foundation-current",
        control_table="catalog.schema.control",
        control_delta_version=17,
        candidate_policy_checksum_value="policy-checksum",
        task_run_id=123 + execution_count,
        execution_count=execution_count,
        git_commit="abc123",
    )


def test_candidate_build_identity_is_stable_across_task_repair():
    entries = (_entry(),)
    original = _context(entries)
    repaired = _context(entries, execution_count=1)

    assert repaired.candidate_build_id == original.candidate_build_id
    assert (
        repaired.candidate_build_attempt_id
        != original.candidate_build_attempt_id
    )
    assert repaired.control_delta_version == 17
    assert repaired.portfolio_attempt_id == "portfolio-v1:attempt:0"


def test_same_provider_serving_slots_share_one_compute_group():
    entries = (
        _entry(),
        _entry("v1_challenger", "best_challenger"),
    )

    groups = group_serving_entries(entries)

    assert len(groups) == 1
    assert {entry.serving_slot for entry in groups[0]} == {
        "best",
        "best_challenger",
    }


def test_different_synthetic_provider_uses_the_same_generic_runtime(
    monkeypatch,
):
    entries = (
        _entry(),
        _entry("v1_synthetic", "best_challenger", "synthetic"),
    )
    calls = []
    published = []

    class Publisher:
        def __init__(self, *_args, **_kwargs):
            return None

        def publish_provider(self, provider_entries, *_frames):
            published.append(
                tuple(entry.provider_build_id for entry in provider_entries)
            )

        def finalize(self, _entries, **_kwargs):
            return "accepted"

    monkeypatch.setattr(
        runtime,
        "load_serving_portfolio_entries",
        lambda *_args, **_kwargs: entries,
    )
    monkeypatch.setattr(runtime, "latest_delta_version", lambda *_args: 17)
    monkeypatch.setattr(runtime, "CandidateBuildPublisher", Publisher)

    def run_mapping(**kwargs):
        calls.append(
            (
                kwargs["provider_build_id"],
                kwargs["publish_compatibility"],
            )
        )
        kwargs["candidate_publisher"]("ranked", "groups", "members")

    monkeypatch.setattr(runtime, "run_theme_score_mapping", run_mapping)
    config = SimpleNamespace(
        tables_write=SimpleNamespace(
            scoring_portfolio_entries="portfolio_entries",
            scoring_input_item_themes="item_themes",
            candidate_builds="candidate_builds",
            candidate_scores="candidate_scores",
            candidate_ad_sets="candidate_ad_sets",
        )
    )
    cfg = {
        "locations": {},
        "results_prm": {"min_c_sessions": 10},
        "incrementality": {
            "auto_trading_switch": False,
            "incremental_lookback": 7,
        },
        "greedy_themes": {},
    }

    result = runtime.run_portfolio_candidate_build(
        spark="spark",
        config=config,
        cfg=cfg,
        client="next_uk",
        job_env="dev",
        run_date=RUN_DATE,
        route="v1",
        output_grain="location",
        portfolio_id="portfolio-v1",
        portfolio_attempt_id="portfolio-v1:attempt:0",
        current_input_snapshot_id="inputs-current",
        candidate_foundation_snapshot_id="foundation-current",
        foundation_inputs="foundation",
        control_table="control",
        output_preranked_table="compatibility",
        task_run_id=123,
        execution_count=0,
        git_commit="abc123",
        compatibility_top_count=100,
        apply_ad_feedback=True,
        ad_feedback_weight=0.05,
        write_score_components=True,
        logger="logger",
    )

    assert result == "accepted"
    assert sorted(calls) == [
        ("synthetic-build", False),
        ("theme_affinity-build", False),
    ]
    assert sorted(published) == [
        ("synthetic-build",),
        ("theme_affinity-build",),
    ]


def test_candidate_policy_checksum_changes_with_existing_behaviour():
    cfg = {
        "results_prm": {"min_c_sessions": 100},
        "incrementality": {
            "auto_trading_switch": True,
            "incremental_lookback": 7,
        },
        "greedy_themes": {"mens": 0.2},
    }
    original = candidate_policy_checksum(
        cfg,
        output_grain="location",
        apply_ad_feedback=True,
        ad_feedback_weight=0.05,
    )
    changed = deepcopy(cfg)
    changed["incrementality"]["incremental_lookback"] = 14

    assert original == candidate_policy_checksum(
        cfg,
        output_grain="location",
        apply_ad_feedback=True,
        ad_feedback_weight=0.05,
    )
    assert original != candidate_policy_checksum(
        changed,
        output_grain="location",
        apply_ad_feedback=True,
        ad_feedback_weight=0.05,
    )


def test_assignment_preflight_rejects_rank_above_twenty():
    validate_assignment_rank_limit(
        {"FY20": {"best_kwargs": {"return_ranks": [20]}}}
    )

    with pytest.raises(ValueError, match="outside 1-20"):
        validate_assignment_rank_limit(
            {"FY21": {"best_kwargs": {"return_ranks": [21]}}}
        )

    with pytest.raises(ValueError, match="outside 1-20"):
        validate_assignment_rank_limit(
            {"invalid": {"best_kwargs": {"return_ranks": ["20"]}}}
        )


def test_portfolio_loader_excludes_evaluation_only_entries(spark):
    table = "candidate_portfolio_loader_test"
    spark.createDataFrame(
        [
            (
                "portfolio-v1",
                "portfolio-v1:attempt:0",
                "serving-entry",
                "SERVING",
                "best",
            ),
            (
                "portfolio-v1",
                "portfolio-v1:attempt:0",
                "markov-shadow",
                "EVALUATE",
                "shadow",
            ),
        ],
        "PortfolioID string, PortfolioAttemptID string, "
        "PortfolioEntryID string, ExecutionMode string, ServingSlot string",
    ).selectExpr(
        "*",
        "concat(PortfolioEntryID, '-build') as ProviderBuildID",
        "concat(PortfolioEntryID, '-attempt') as ProviderBuildAttemptID",
        "'catalog.schema.provider' as ProviderOutputTable",
        "cast(42 as long) as ProviderOutputDeltaVersion",
        "cast('2026-08-07' as date) as ProviderSourceRunDate",
        "'inputs-current' as InputSnapshotID",
        "'delivery' as ExperimentID",
        "ServingSlot as VariantID",
    ).createOrReplaceTempView(table)

    entries = load_serving_portfolio_entries(
        spark,
        entries_table=table,
        portfolio_id="portfolio-v1",
        portfolio_attempt_id="portfolio-v1:attempt:0",
    )

    assert [entry.portfolio_entry_id for entry in entries] == ["serving-entry"]


def test_partial_candidate_rows_without_ready_header_are_not_selectable():
    with pytest.raises(ValueError, match="No accepted candidate build"):
        select_candidate_build((), run_date=RUN_DATE, route="v1")

    failed = CandidateBuild(
        candidate_build_id="build",
        candidate_build_attempt_id="build:attempt:0",
        run_date=RUN_DATE,
        route="v1",
        portfolio_id="portfolio",
        portfolio_attempt_id="portfolio:attempt:0",
        candidate_foundation_snapshot_id="foundation",
        status="FAILED_BEFORE_PUBLISH",
        completed_at=COMPLETED_AT,
        task_run_id=123,
        execution_count=0,
    )
    with pytest.raises(ValueError, match="No accepted candidate build"):
        select_candidate_build((failed,), run_date=RUN_DATE, route="v1")


def test_manifest_failure_repairs_from_existing_candidate_receipts(
    monkeypatch,
):
    entry = _entry()
    context = _context((entry,))
    publisher = object.__new__(CandidateBuildPublisher)
    publisher.spark = "spark"
    publisher.context = context
    publisher.builds_table = "candidate_builds"
    publisher.scores_table = "candidate_scores"
    publisher.ad_sets_table = "candidate_ad_sets"
    publisher.group_column = "Location"
    publisher._ad_sets = "ad-set-frame"
    publisher._score_frames = ["score-frame"]
    publisher._published_entry_ids = {entry.portfolio_entry_id}
    publisher._started_at = time.monotonic()

    receipts = {
        "candidate_ad_sets": DeltaWriteReceipt(
            statement="",
            attempts=1,
            receipt_id="ad-set-receipt",
            target_table="candidate_ad_sets",
            delta_version=41,
            row_count=20,
            schema_checksum="ad-set-schema",
        ),
        "candidate_scores": DeltaWriteReceipt(
            statement="",
            attempts=1,
            receipt_id="score-receipt",
            target_table="candidate_scores",
            delta_version=42,
            row_count=100,
            schema_checksum="score-schema",
        ),
    }
    receipt_lookups = []
    manifest_writes = []
    output_locations = []

    def find_receipt(_spark, *, target_table, **_kwargs):
        receipt_lookups.append(target_table)
        return receipts[target_table]

    monkeypatch.setattr(
        "next_ads.candidates.publication.find_delta_write_receipt",
        find_receipt,
    )
    monkeypatch.setattr(
        "next_ads.candidates.publication.typed_table_frame",
        lambda _spark, _table, rows: SimpleNamespace(columns=list(rows[0])),
    )
    monkeypatch.setattr(
        "next_ads.candidates.publication.replace_scope_by_name",
        lambda _frame, table, *_args, **_kwargs: manifest_writes.append(table),
    )
    monkeypatch.setattr(
        "next_ads.candidates.publication.log_output_location",
        lambda destination, **_kwargs: output_locations.append(destination),
    )

    with pytest.raises(RuntimeError, match="injected manifest failure"):
        publisher.finalize(
            (entry,),
            completed_at=COMPLETED_AT,
            before_ready=lambda _build: (_ for _ in ()).throw(
                RuntimeError("injected manifest failure")
            ),
        )
    assert manifest_writes == []

    repaired = publisher.finalize((entry,), completed_at=COMPLETED_AT)

    assert repaired.status == "READY_FOR_NEXTADS"
    assert manifest_writes == ["candidate_builds"]
    assert receipt_lookups == [
        "candidate_ad_sets",
        "candidate_scores",
        "candidate_ad_sets",
        "candidate_scores",
    ]
    assert output_locations == [
        "candidate_ad_sets",
        "candidate_scores",
        "candidate_ad_sets",
        "candidate_scores",
    ]


def test_candidate_rows_and_ad_sets_publish_before_ready_header(
    spark,
    monkeypatch,
):
    builds_table = "candidate_builds_publication_test"
    scores_table = "candidate_scores_publication_test"
    ad_sets_table = "candidate_ad_sets_publication_test"
    spark.createDataFrame(
        [],
        "CandidateBuildID string, CandidateBuildAttemptID string, "
        "RunDate date, Route string, OutputGrain string, PortfolioID string, "
        "PortfolioAttemptID string, CandidateFoundationSnapshotID string, "
        "ControlTable string, ControlDeltaVersion long, "
        "CandidateContractVersion string, CandidatePolicyVersion string, "
        "CandidatePolicyChecksum string, ProviderBindingsJSON string, "
        "Status string, EntryCount int, OutputBindingsJSON string, "
        "GitCommit string, RuntimeMs long, TaskRunID long, "
        "ExecutionCount int, CompletedAt timestamp",
    ).createOrReplaceTempView(builds_table)
    spark.createDataFrame(
        [],
        "CandidateBuildID string, CandidateBuildAttemptID string, "
        "RunDate date, Route string, PortfolioEntryID string, "
        "ServingSlot string, ExperimentID string, VariantID string, "
        "ProviderBuildID string, ProviderBuildAttemptID string, "
        "AccountNumber string, AdSetID string, UniqueAdID string, "
        "Score double, TriggerScore double, Rank int, CandidateID string",
    ).createOrReplaceTempView(scores_table)
    spark.createDataFrame(
        [],
        "CandidateBuildID string, CandidateBuildAttemptID string, "
        "RunDate date, Route string, AdSetID string, ScopeType string, "
        "ScopeValue string, UniqueAdID string",
    ).createOrReplaceTempView(ad_sets_table)
    operations = []

    def replace(frame, table, *_args, **_kwargs):
        materialized = spark.createDataFrame(frame.collect(), frame.schema)
        materialized.createOrReplaceTempView(table)
        operations.append(table)
        return DeltaWriteReceipt(
            statement=f"replace {table}",
            attempts=1,
            receipt_id=f"receipt-{table}",
            target_table=table,
            delta_version=len(operations),
            row_count=materialized.count(),
            schema_checksum=f"schema-{table}",
            build_id=_context((entry,)).candidate_build_id,
            attempt_id=_context((entry,)).candidate_build_attempt_id,
            git_commit="abc123",
        )

    monkeypatch.setattr(
        "next_ads.candidates.publication.replace_scope_by_name",
        replace,
    )
    monkeypatch.setattr(
        "next_ads.candidates.publication.find_delta_write_receipt",
        lambda *_args, **_kwargs: None,
    )
    entry = _entry()
    publisher = CandidateBuildPublisher(
        spark,
        _context((entry,)),
        builds_table=builds_table,
        scores_table=scores_table,
        ad_sets_table=ad_sets_table,
        group_column="Location",
    )
    ranked = spark.createDataFrame(
        [
            (
                "1",
                f"ad-{rank}",
                "adset-a",
                1.0 - rank / 100,
                0.8,
                rank,
            )
            for rank in range(1, 22)
        ],
        [
            "AccountNumber",
            "UniqueAdID",
            "AdSetID",
            "Score",
            "TriggerScore",
            "Rank",
        ],
    )
    ad_set_to_group = spark.createDataFrame(
        [("adset-a", "PL1")], ["AdSetID", "Location"]
    )
    ad_to_ad_set = spark.createDataFrame(
        [(f"ad-{rank}", "adset-a") for rank in range(1, 22)],
        ["UniqueAdID", "AdSetID"],
    )

    publisher.publish_provider((entry,), ranked, ad_set_to_group, ad_to_ad_set)
    result = publisher.finalize((entry,), completed_at=COMPLETED_AT)

    assert result.status == "READY_FOR_NEXTADS"
    assert operations == [ad_sets_table, scores_table, builds_table]
    assert spark.table(scores_table).count() == 20
    assert spark.table(scores_table).agg({"Rank": "max"}).first()[0] == 20
    bindings = spark.table(builds_table).first()["OutputBindingsJSON"]
    assert '"row_count":20' in bindings


def test_content_stable_ad_set_ids_match_at_one_four_and_eight_partitions(
    spark,
):
    rows = [
        ("ad-a", "PL1", 0),
        ("ad-b", "PL1", 0),
        ("ad-b", "PL2", 0),
        ("ad-a", "PL2", 0),
        ("ad-c", "PL3", 0),
    ]
    logger = SimpleNamespace(info=lambda *_args: None)

    def mapping(partitions):
        control = spark.createDataFrame(
            rows,
            ["UniqueAdID", "Location", "AudienceOnly"],
        ).repartition(partitions)
        _, groups, members = build_ad_group_mappings(
            spark,
            "unused",
            logger,
            control_ads_df=control,
        )
        return (
            sorted(tuple(row) for row in groups.collect()),
            sorted(tuple(row) for row in members.collect()),
        )

    one = mapping(1)
    assert mapping(4) == one
    assert mapping(8) == one
    assert all(ad_set_id.startswith("adset_") for ad_set_id, _ in one[0])

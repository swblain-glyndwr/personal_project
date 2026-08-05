import json
from pathlib import Path

import pytest

from jobs.nextads_assignment.bulk_build import (
    build_scope_invocations,
    select_phase_entries,
)
from jobs.nextads_assignment.publish_build import ScopeManifestEntry
from next_ads.decisioning.candidate_inputs import (
    clear_candidate_input_cache,
    load_accepted_candidate_inputs,
)


def _bindings(*, duplicate_challenger=False):
    rows = [
        {
            "portfolio_entry_id": "entry-best",
            "serving_slot": "best",
        },
        {
            "portfolio_entry_id": "entry-challenger",
            "serving_slot": "best_challenger",
        },
    ]
    if duplicate_challenger:
        rows.append(
            {
                "portfolio_entry_id": "entry-challenger-2",
                "serving_slot": "best_challenger",
            }
        )
    return json.dumps(rows)


def _candidate_tables(spark, suffix, *, status="READY_FOR_NEXTADS", bindings=None):
    builds = f"candidate_builds_{suffix}"
    scores = f"candidate_scores_{suffix}"
    ad_sets = f"candidate_ad_sets_{suffix}"
    spark.createDataFrame(
        [
            (
                "candidate-build",
                "candidate-attempt",
                "v1",
                "location",
                "portfolio",
                "portfolio-attempt",
                "candidate-foundation",
                bindings or _bindings(),
                status,
            )
        ],
        "CandidateBuildID string, CandidateBuildAttemptID string, Route string, "
        "OutputGrain string, PortfolioID string, PortfolioAttemptID string, "
        "CandidateFoundationSnapshotID string, ProviderBindingsJSON string, "
        "Status string",
    ).createOrReplaceTempView(builds)
    spark.createDataFrame(
        [
            (
                "candidate-build",
                "candidate-attempt",
                "v1",
                "entry-best",
                "account-1",
                "adset-a",
                "ad-best",
                0.9,
                0.8,
                1,
            ),
            (
                "candidate-build",
                "candidate-attempt",
                "v1",
                "entry-challenger",
                "account-1",
                "adset-a",
                "ad-challenger",
                0.7,
                0.6,
                1,
            ),
        ],
        "CandidateBuildID string, CandidateBuildAttemptID string, Route string, "
        "PortfolioEntryID string, AccountNumber string, AdSetID string, "
        "UniqueAdID string, Score double, TriggerScore double, Rank int",
    ).createOrReplaceTempView(scores)
    spark.createDataFrame(
        [
            (
                "candidate-build",
                "candidate-attempt",
                "v1",
                "adset-a",
                "location",
                "PL1",
                "ad-best",
            ),
            (
                "candidate-build",
                "candidate-attempt",
                "v1",
                "adset-a",
                "location",
                "PL1",
                "ad-challenger",
            ),
        ],
        "CandidateBuildID string, CandidateBuildAttemptID string, Route string, "
        "AdSetID string, ScopeType string, ScopeValue string, UniqueAdID string",
    ).createOrReplaceTempView(ad_sets)
    return builds, scores, ad_sets


def test_exact_candidate_attempt_resolves_public_slots_separately(spark):
    tables = _candidate_tables(spark, "slot_resolution")
    try:
        accepted = load_accepted_candidate_inputs(
            spark,
            builds_table=tables[0],
            scores_table=tables[1],
            ad_sets_table=tables[2],
            candidate_build_attempt_id="candidate-attempt",
            route="v1",
        )

        best = accepted.candidates_for_scope("best", "PL1").first()
        challenger = accepted.candidates_for_scope(
            "best_challenger", "PL1"
        ).first()

        assert best.UniqueAdID == "ad-best"
        assert challenger.UniqueAdID == "ad-challenger"
        assert accepted.provenance.candidate_build_id == "candidate-build"
        assert accepted.provenance.portfolio_attempt_id == "portfolio-attempt"
        assert accepted.provenance.candidate_foundation_snapshot_id == (
            "candidate-foundation"
        )
    finally:
        clear_candidate_input_cache()


@pytest.mark.parametrize(
    ("status", "bindings", "message"),
    [
        ("FAILED", _bindings(), "exactly one READY"),
        (
            "READY_FOR_NEXTADS",
            _bindings(duplicate_challenger=True),
            "exactly one best_challenger",
        ),
    ],
)
def test_candidate_inputs_fail_before_assignment_for_invalid_manifest(
    spark,
    status,
    bindings,
    message,
):
    tables = _candidate_tables(
        spark,
        f"invalid_{status.lower()}",
        status=status,
        bindings=bindings,
    )
    try:
        with pytest.raises(ValueError, match=message):
            load_accepted_candidate_inputs(
                spark,
                builds_table=tables[0],
                scores_table=tables[1],
                ad_sets_table=tables[2],
                candidate_build_attempt_id="candidate-attempt",
                route="v1",
            )
    finally:
        clear_candidate_input_cache()


def test_bulk_scope_invocations_preserve_phase_order_and_inheritance():
    manifest = (
        ScopeManifestEntry("PL1", "primary"),
        ScopeManifestEntry("SB1", "primary"),
        ScopeManifestEntry("SB2", "secondary", "SB1"),
    )
    common = ("--run_date", "2026-08-07")

    primary = build_scope_invocations(
        project_root=Path("C:/repo"),
        route="v1",
        phase="primary",
        manifest=manifest,
        common_arguments=common,
    )
    secondary = build_scope_invocations(
        project_root=Path("C:/repo"),
        route="v1",
        phase="secondary",
        manifest=manifest,
        common_arguments=common,
    )

    assert [invocation.scope for invocation in primary] == ["PL1", "SB1"]
    assert primary[0].arguments == (*common, "--location", "PL1")
    assert secondary[0].arguments == (
        *common,
        "--location",
        "SB2",
        "--inherit_basic_from",
        "SB1",
    )
    assert primary[0].script.as_posix().endswith(
        "jobs/nextads_assignment/build_page.py"
    )


def test_v2_bulk_phase_runs_all_five_scopes_in_manifest_order():
    manifest = tuple(
        ScopeManifestEntry(scope)
        for scope in (
            "HomePage",
            "ShoppingBagPage",
            "CheckoutPage",
            "ProductListingPage",
            "ForYouPage",
        )
    )

    selected = select_phase_entries(manifest, route="v2", phase="all")

    assert selected == manifest
    assert len(selected) == 5

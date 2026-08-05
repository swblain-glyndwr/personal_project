from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

PAGE_AND_DELIVERY_FILES = (
    "jobs/nextads_assignment/build_page.py",
    "jobs/nextads_assignment/bulk_build.py",
    "jobs/nextads_assignment/publish_build.py",
    "jobs/nextads_v2/build_page.py",
    "jobs/nextads_delivery/build_v2_payload.py",
    "jobs/nextads_delivery/masid_handoff_check.py",
    "jobs/nextads_delivery/plp_gs.py",
    "src/next_ads/decisioning/assignment_publication.py",
    "src/next_ads/decisioning/candidate_inputs.py",
    "src/next_ads/delivery/google_sheets.py",
    "src/next_ads/delivery/masid_handoff.py",
)

CRITICAL_ROUTE_FILES = (
    "jobs/nextads_candidates/build_theme_ad_candidates.py",
    "jobs/nextads_candidates/build_page_type_candidates_v2.py",
    "src/next_ads/candidates/publication.py",
    *PAGE_AND_DELIVERY_FILES,
)

MUTABLE_MODEL_OR_CANDIDATE_TABLES = (
    "preranked_ads_from_themes_latest",
    "preranked_ads_from_themes_v2_latest",
    "theme_affinity_model_latest",
    "score_provider_signals_latest",
    "candidate_scores_latest",
    "candidate_ad_sets_latest",
)

UNSAFE_CRITICAL_ROUTE_PATTERNS = (
    "F.rand(",
    ".sampleBy(",
    ".dropDuplicates(",
    ".drop_duplicates(",
    "delete_from_and_load(",
    "truncate_and_load(",
    "TRUNCATE TABLE",
    "DELETE FROM",
)


def _read(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")


def test_page_and_delivery_jobs_do_not_read_mutable_model_or_candidate_latest():
    offenders = {
        relative_path: table
        for relative_path in PAGE_AND_DELIVERY_FILES
        for table in MUTABLE_MODEL_OR_CANDIDATE_TABLES
        if table in _read(relative_path)
    }

    assert offenders == {}


def test_active_modular_route_has_no_unsafe_random_dedup_or_writer():
    offenders = {
        relative_path: pattern
        for relative_path in CRITICAL_ROUTE_FILES
        for pattern in UNSAFE_CRITICAL_ROUTE_PATTERNS
        if pattern in _read(relative_path)
    }

    assert offenders == {}


def test_modular_route_acceptance_contract_remains_explicit():
    contract = _read(
        "docs/CICD/nextads_modular_route_dev_acceptance.md"
    )
    required_statements = (
        "interchangeable provider, portfolio, candidate and assignment",
        "Markov remains a shadow provider",
        "Multiple simultaneous challenger traffic allocation is not active",
        "one Delta transaction per table",
        "target-ordered `REPLACE WHERE`",
        "05:00 Europe/London",
        "No public-preview Lakeflow metadata is required",
        "Feature compatibility remains separate from the nightly route",
    )

    for statement in required_statements:
        assert statement in contract

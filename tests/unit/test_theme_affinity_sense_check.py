from next_ads.ranking.theme_affinity.sense_check import (
    DATA_TABLE_SUFFIXES,
    INTERMEDIATE_TABLE_SUFFIXES,
    MODEL_OUTPUT_TABLE_SUFFIXES,
    SUMMARY_SCHEMA,
    _normalise_check_scope,
    _table_status,
)


def test_theme_affinity_sense_check_covers_intermediate_tables():
    assert "product_catalog" in INTERMEDIATE_TABLE_SUFFIXES
    assert "master" in INTERMEDIATE_TABLE_SUFFIXES
    assert "complete" in INTERMEDIATE_TABLE_SUFFIXES
    assert "ranked" in INTERMEDIATE_TABLE_SUFFIXES
    assert "half" in INTERMEDIATE_TABLE_SUFFIXES


def test_theme_affinity_sense_check_splits_data_and_model_outputs():
    assert "ranked" in DATA_TABLE_SUFFIXES
    assert "half" not in DATA_TABLE_SUFFIXES
    assert MODEL_OUTPUT_TABLE_SUFFIXES == ["half"]


def test_theme_affinity_sense_check_validates_scope():
    assert _normalise_check_scope("") == "all"
    assert _normalise_check_scope("DATA") == "data"


def test_theme_affinity_sense_check_fails_empty_candidate_against_populated_baseline():
    status, notes = _table_status(
        candidate_rows=0,
        baseline_rows=141580509,
        row_ratio=0.0,
        missing_columns=[],
        extra_columns=[],
    )

    assert status == "FAIL"
    assert "Candidate has no rows" in notes


def test_theme_affinity_sense_check_warns_on_large_row_count_drift():
    status, notes = _table_status(
        candidate_rows=90,
        baseline_rows=100,
        row_ratio=0.9,
        missing_columns=[],
        extra_columns=[],
    )

    assert status == "WARN"
    assert "row count differs" in notes


def test_theme_affinity_sense_check_uses_explicit_summary_schema():
    assert "candidate_table STRING" in SUMMARY_SCHEMA
    assert "baseline_table STRING" in SUMMARY_SCHEMA
    assert "row_ratio DOUBLE" in SUMMARY_SCHEMA
    assert "match_rate DOUBLE" in SUMMARY_SCHEMA

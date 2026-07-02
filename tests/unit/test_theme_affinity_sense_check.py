from next_ads.ranking.theme_affinity.sense_check import (
    DATA_TABLE_SUFFIXES,
    INTERMEDIATE_TABLE_SUFFIXES,
    MODEL_OUTPUT_TABLE_SUFFIXES,
    SenseCheckConfig,
    SUMMARY_SCHEMA,
    _normalise_check_scope,
    _table_status,
    run_sense_checks,
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


class _FakeWrite:
    def mode(self, _value):
        return self

    def option(self, _key, _value):
        return self

    def saveAsTable(self, _table):  # noqa: N802 - mirrors Spark API
        return None


class _FakeSummaryDataFrame:
    write = _FakeWrite()


class _FakeSpark:
    def __init__(self):
        self.summary_rows = None

    def createDataFrame(self, rows, schema):  # noqa: N802 - mirrors Spark API
        self.summary_rows = rows
        assert schema == SUMMARY_SCHEMA
        return _FakeSummaryDataFrame()


class _Config:
    ranking_model_table_prefix = "next_uk_nextads_theme_affinity_predict"


class _Runtime:
    namespace = "marketingdata_prod.warehouse"
    config = _Config()


def test_data_sense_checks_can_use_candidate_intermediate_namespace(monkeypatch):
    def fake_table_sense_row(
        spark,
        checked_at,
        check_name,
        candidate_table,
        baseline_table,
    ):
        return {
            "checked_at": checked_at,
            "check_name": check_name,
            "candidate_table": candidate_table,
            "baseline_table": baseline_table,
            "candidate_filter": "",
            "baseline_filter": "",
            "candidate_rows": None,
            "baseline_rows": None,
            "candidate_distinct_accounts": None,
            "baseline_distinct_accounts": None,
            "joined_rows": None,
            "row_ratio": None,
            "match_rate": None,
            "avg_abs_score_delta": None,
            "max_abs_score_delta": None,
            "missing_columns": "",
            "extra_columns": "",
            "status": "OK",
            "notes": "",
        }

    monkeypatch.setattr(
        "next_ads.ranking.theme_affinity.sense_check._table_sense_row",
        fake_table_sense_row,
    )
    spark = _FakeSpark()

    run_sense_checks(
        spark,
        _Runtime(),
        SenseCheckConfig(
            baseline_intermediate_namespace="marketingdata_prod.ds_sandbox",
            baseline_intermediate_prefix="next_uk_nextAds_predict_prod",
            baseline_final_table=(
                "marketingdata_prod.ds_sandbox."
                "next_uk_next_ads_hackathon_model_full"
            ),
            summary_table="marketingdata_prod.warehouse.summary",
            check_scope="data",
            candidate_intermediate_namespace="marketingdata_prod.ds_sandbox",
        ),
    )

    assert spark.summary_rows
    assert all(
        row["candidate_table"].startswith(
            "marketingdata_prod.ds_sandbox."
            "next_uk_nextads_theme_affinity_predict_"
        )
        for row in spark.summary_rows
    )

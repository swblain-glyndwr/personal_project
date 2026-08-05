from datetime import date
from pathlib import Path

import pytest
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
)

from next_ads.ranking import theme_score_retrieval
from next_ads.ranking.theme_score_ranking import calculate_score_range


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUN_DATE = date(2026, 8, 3)


class _Logger:
    def __init__(self):
        self.warnings = []

    def info(self, *_args):
        return None

    def warning(self, *args):
        self.warnings.append(args)


def _provider_signals(spark):
    return spark.createDataFrame(
        [
            ("build-a", "1", "theme", "boys", RUN_DATE, 0.2, 0.3),
            ("build-b", "1", "theme", "girls", RUN_DATE, 0.8, 0.9),
            (
                "build-a",
                "1",
                "theme",
                "future",
                date(2026, 8, 4),
                0.6,
                0.7,
            ),
            ("build-a", "1", "ad", "ad-1", RUN_DATE, 0.4, 0.5),
        ],
        [
            "ProviderBuildID",
            "AccountNumber",
            "EntityType",
            "EntityID",
            "RunDate",
            "RawScore",
            "Score",
        ],
    )


def test_candidate_reads_only_the_selected_provider_build_and_delta_version(
    spark,
    monkeypatch,
):
    calls = []

    def read_version(_spark, table, version):
        calls.append((table, version))
        return _provider_signals(spark)

    monkeypatch.setattr(
        theme_score_retrieval,
        "read_delta_version",
        read_version,
    )
    customers = spark.createDataFrame([("1",)], ["AccountNumber"])

    result = theme_score_retrieval.load_provider_theme_scores(
        spark,
        provider_signals_table="catalog.schema.provider_signals",
        provider_signals_delta_version=17,
        provider_build_id="build-a",
        provider_source_run_date=RUN_DATE,
        customer_base_df=customers,
    ).collect()

    assert calls == [("catalog.schema.provider_signals", 17)]
    assert [row.asDict() for row in result] == [
        {
            "AccountNumber": "1",
            "NextTheme": "boys",
            "ProbBase": 0.2,
            "ProbAgg": 0.2,
            "ProbAggRebased": 0.3,
        }
    ]


def test_candidate_rejects_missing_exact_provider_build(spark, monkeypatch):
    monkeypatch.setattr(
        theme_score_retrieval,
        "read_delta_version",
        lambda *_args: _provider_signals(spark),
    )
    customers = spark.createDataFrame([("1",)], ["AccountNumber"])

    with pytest.raises(ValueError, match="contains no theme signals"):
        theme_score_retrieval.load_provider_theme_scores(
            spark,
            provider_signals_table="catalog.schema.provider_signals",
            provider_signals_delta_version=17,
            provider_build_id="missing",
            provider_source_run_date=RUN_DATE,
            customer_base_df=customers,
        )


def test_candidate_rejects_provider_build_outside_customer_base(
    spark,
    monkeypatch,
):
    monkeypatch.setattr(
        theme_score_retrieval,
        "read_delta_version",
        lambda *_args: _provider_signals(spark),
    )
    customers = spark.createDataFrame([("2",)], ["AccountNumber"])

    with pytest.raises(ValueError, match="accepted customer base"):
        theme_score_retrieval.load_provider_theme_scores(
            spark,
            provider_signals_table="catalog.schema.provider_signals",
            provider_signals_delta_version=17,
            provider_build_id="build-a",
            provider_source_run_date=RUN_DATE,
            customer_base_df=customers,
        )


def test_candidate_quarantines_fallback_themes_changed_since_provider_input(
    spark,
    monkeypatch,
):
    monkeypatch.setattr(
        theme_score_retrieval,
        "read_delta_version",
        lambda *_args: _provider_signals(spark),
    )
    customers = spark.createDataFrame([("1",)], ["AccountNumber"])
    allowed_themes = spark.createDataFrame([("other",)], ["NextTheme"])

    with pytest.raises(ValueError, match="contains no theme signals"):
        theme_score_retrieval.load_provider_theme_scores(
            spark,
            provider_signals_table="catalog.schema.provider_signals",
            provider_signals_delta_version=17,
            provider_build_id="build-a",
            provider_source_run_date=RUN_DATE,
            customer_base_df=customers,
            allowed_themes_df=allowed_themes,
        )


def test_constant_provider_scores_use_a_neutral_range(spark):
    scores = spark.createDataFrame(
        [(0.5,), (0.5,)],
        ["ProbAggRebased"],
    )
    logger = _Logger()

    assert calculate_score_range(scores, logger) == (0.5, 1.0)
    assert logger.warnings


def test_empty_or_all_null_provider_scores_fail_clearly(spark):
    schema = StructType(
        [StructField("ProbAggRebased", DoubleType(), nullable=True)]
    )
    scores = spark.createDataFrame([(None,)], schema)

    with pytest.raises(ValueError, match="empty or all null"):
        calculate_score_range(scores, _Logger())


def test_active_candidate_entrypoints_do_not_read_mutable_provider_latest():
    mapping = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_score_mapping.py"
    ).read_text()
    v1 = (
        PROJECT_ROOT / "jobs/nextads_candidates/build_theme_ad_candidates.py"
    ).read_text()
    v2 = (
        PROJECT_ROOT
        / "jobs/nextads_candidates/build_page_type_candidates_v2.py"
    ).read_text()

    assert "theme_affinity_assignment_sources" not in mapping
    assert "load_provider_theme_scores(" in mapping
    for entrypoint in (v1, v2):
        assert 'get_arg("--provider_build_id")' not in entrypoint
        assert 'get_arg("--portfolio_id")' in entrypoint
        assert 'get_arg("--portfolio_attempt_id")' in entrypoint
        assert 'get_arg("--current_input_snapshot_id")' in entrypoint
        assert "run_portfolio_candidate_build(" in entrypoint
    assert "control_sheet_delta_version" in mapping

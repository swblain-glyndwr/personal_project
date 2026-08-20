from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from next_ads.ranking.theme_score_generation import (
    merge_and_rank_theme_scores,
    select_global_top_themes,
    select_latest_view_themes,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def local_spark():
    try:
        return (
            SparkSession.builder.master("local[1]")
            .appName("next-ads-theme-score-generation-tests")
            .getOrCreate()
        )
    except (RuntimeError, ValueError) as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")


def _rows(df, columns):
    return sorted(tuple(row[column] for column in columns) for row in df.collect())


def test_latest_view_theme_has_total_order(local_spark):
    frame = local_spark.createDataFrame(
        [
            ("a", "Zulu", "2026-07-01"),
            ("a", "Alpha", "2026-07-01"),
            ("a", "Alpha", "2026-07-01"),
            ("b", "Beta", "2026-06-30"),
        ],
        ["account_number", "theme", "date"],
    )

    result = select_latest_view_themes(frame)

    assert _rows(result, ["account_number", "theme"]) == [
        ("a", "Alpha"),
        ("b", "Beta"),
    ]


def test_global_top_themes_breaks_sales_ties_by_theme(local_spark):
    sales = local_spark.createDataFrame(
        [(f"theme_{index:02d}", 10) for index in range(26)],
        ["theme", "sales_count"],
    )

    result = select_global_top_themes(sales, limit=25)

    assert [row.next_theme for row in result.collect()] == [
        f"theme_{index:02d}" for index in range(25)
    ]


def test_score_fallback_and_rank_are_repartition_invariant(local_spark):
    scores = local_spark.createDataFrame(
        [
            ("a", "theme_000", 0.8, 0.2, 0.6),
            *[
                ("a", f"theme_{index:03d}", 0.5, 0.2, 0.3)
                for index in range(1, 102)
            ],
        ],
        [
            "account_number",
            "next_theme",
            "prob_agg",
            "prob_base",
            "prob_agg_rebased",
        ],
    )
    fallbacks = local_spark.createDataFrame(
        [
            ("theme_000", 0.0, 0.0, -999.0),
            ("fallback", 0.0, 0.0, -999.0),
        ],
        ["next_theme", "prob_agg", "prob_base", "prob_agg_rebased"],
    )

    outputs = []
    for partitions in (1, 4, 8):
        result = merge_and_rank_theme_scores(
            scores.repartition(partitions),
            fallbacks.repartition(partitions),
            limit=100,
        )
        rows = _rows(result, ["account_number", "next_theme"])
        assert len(rows) == len(set(rows)) == 100
        assert ("a", "theme_000") in rows
        assert ("a", "fallback") not in rows
        outputs.append(rows)

    assert outputs[0] == outputs[1] == outputs[2]


def test_job_uses_retry_safe_generation_helpers():
    source = (
        PROJECT_ROOT / "jobs/nextads_candidates/build_theme_scores.py"
    ).read_text()
    helper_source = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_score_generation.py"
    ).read_text()

    assert "merge_and_rank_theme_scores(" in source
    assert "select_global_top_themes(" in source
    assert "select_latest_view_themes(" in source
    assert 'F.hash("account_number", "next_theme")' not in source
    assert 'how="left_anti"' in helper_source
    assert 'F.col("next_theme").asc()' in helper_source

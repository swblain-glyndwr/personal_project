from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _read_source(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text()


def test_theme_mapping_lineage_has_no_unordered_spark_aggregates():
    retrieval_source = _read_source(
        "src/next_ads/ranking/theme_score_retrieval.py"
    )

    assert "F.collect_set(" not in retrieval_source
    assert "F.collect_list(" not in retrieval_source
    assert "source_ad2group.collect()" in retrieval_source
    assert "spark.createDataFrame(ad_group_rows" in retrieval_source


def test_remaining_ranking_windows_use_deterministic_total_orders():
    eligibility_source = _read_source(
        "src/next_ads/ranking/theme_score_eligibility.py"
    )
    ranking_source = _read_source(
        "src/next_ads/ranking/theme_score_ranking.py"
    )
    mapping_source = _read_source(
        "src/next_ads/ranking/theme_score_mapping.py"
    )

    assert "F.monotonically_increasing_id(" not in eligibility_source
    assert 'F.first("ProbBase")' not in eligibility_source
    assert 'F.max("ProbBase")' in eligibility_source
    assert 'F.col("AccountNumber").asc()' in eligibility_source
    assert 'F.col("AdSeen").asc_nulls_last()' in ranking_source
    assert "break lineage" not in mapping_source
    assert "Caching deterministic final results" in mapping_source

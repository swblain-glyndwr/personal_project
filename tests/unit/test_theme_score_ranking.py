from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_theme_ad_ranking_uses_deterministic_total_order():
    source = (
        PROJECT_ROOT / "src/next_ads/ranking/theme_score_ranking.py"
    ).read_text()
    start = source.index("def rank_top_ads_per_adset(")
    end = source.index("\ndef map_ranked_ads_to_locations(")
    ranking_source = source[start:end]

    assert "F.rand(" not in ranking_source
    assert "top_ads_per_location: int | None = None" in ranking_source
    assert ranking_source.count("F.xxhash64(") == 2
    assert ranking_source.count("F.row_number().over(") == 2
    assert "F.rank().over(" not in ranking_source
    assert ranking_source.count('F.col("UniqueAdID").asc()') == 2
    assert ".dropDuplicates(" not in ranking_source
    assert ".drop_duplicates(" not in ranking_source

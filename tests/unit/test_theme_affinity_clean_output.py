import pytest

from pathlib import Path

from next_ads.ranking.theme_affinity.clean_output import _ranked_theme_mapping


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class FakeDataFrame:
    def __init__(self, count):
        self._count = count

    def limit(self, _rows):
        return self

    def count(self):
        return self._count


class FakeSpark:
    def __init__(self, count):
        self.count = count
        self.query = None

    def sql(self, query):
        self.query = query
        return FakeDataFrame(self.count)


def test_ranked_theme_mapping_requires_populated_table():
    spark = FakeSpark(count=0)

    with pytest.raises(ValueError, match="has no theme_rank = 1 rows"):
        _ranked_theme_mapping(
            spark,
            "marketingdata_dev.test_user.next_uk_nextads_item_themes_latest",
        )


def test_ranked_theme_mapping_reads_rank_one_themes():
    spark = FakeSpark(count=1)

    mapping = _ranked_theme_mapping(
        spark,
        "marketingdata_dev.test_user.next_uk_nextads_item_themes_latest",
    )

    assert mapping.count() == 1
    assert "WHERE theme_rank = 1" in spark.query


def test_model_reranking_uses_theme_as_final_tiebreaker():
    source = (
        PROJECT_ROOT
        / "src/next_ads/ranking/theme_affinity/clean_output.py"
    ).read_text()
    start = source.index("def _rerank_model_output(")
    end = source.index("\ndef clean_model_output(")
    reranking_source = source[start:end]

    assert reranking_source.count('F.col("theme").asc()') == 2
    assert reranking_source.count(".desc_nulls_last()") == 2

import pytest

from next_ads.ranking.theme_affinity.clean_output import _ranked_theme_mapping


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

from uuid import uuid4

import pytest

from next_ads.ranking.theme_score_retrieval import build_ad_group_mappings


pytestmark = pytest.mark.databricks


class _Logger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)


def _mapping_rows(dataframe, *columns):
    return [
        tuple(row[column] for column in columns)
        for row in dataframe.orderBy(*columns).collect()
    ]


def _build_from_rows(spark, rows, partitions):
    view_name = f"control_sheet_{uuid4().hex}"
    control_sheet = spark.createDataFrame(
        rows,
        ["UniqueAdID", "Location", "AudienceOnly"],
    ).repartition(partitions)
    control_sheet.createOrReplaceTempView(view_name)

    try:
        return build_ad_group_mappings(
            spark,
            view_name,
            _Logger(),
        )
    finally:
        spark.catalog.dropTempView(view_name)


def test_ad_group_mappings_are_stable_across_repartitioning(spark):
    rows = [
        ("ad-a", "PL1", 0),
        ("ad-b", "PL1", 0),
        ("ad-b", "PL2", 0),
        ("ad-a", "PL2", 0),
        ("ad-c", "PL3", 0),
        ("ad-c", "PL3", 0),
        ("ad-ineligible", "PL1", 1),
    ]

    one_partition = _build_from_rows(spark, rows, partitions=1)
    four_partitions = _build_from_rows(spark, rows, partitions=4)

    def normalise(mapping):
        ad_to_group, adset_to_group, ad_to_adset = mapping
        return (
            _mapping_rows(ad_to_group, "Location", "UniqueAdID"),
            _mapping_rows(adset_to_group, "AdSetID", "Location"),
            _mapping_rows(ad_to_adset, "AdSetID", "UniqueAdID"),
        )

    expected = (
        [
            ("PL1", "ad-a"),
            ("PL1", "ad-b"),
            ("PL2", "ad-a"),
            ("PL2", "ad-b"),
            ("PL3", "ad-c"),
        ],
        [(1, "PL1"), (1, "PL2"), (2, "PL3")],
        [(1, "ad-a"), (1, "ad-b"), (2, "ad-c")],
    )

    assert normalise(one_partition) == expected
    assert normalise(four_partitions) == expected

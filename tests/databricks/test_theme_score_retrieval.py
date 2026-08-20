from uuid import uuid4

import pytest

from next_ads.ranking.theme_score_retrieval import (
    _content_stable_ad_set_id,
    build_ad_group_mappings,
)


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
    eight_partitions = _build_from_rows(spark, rows, partitions=8)

    def normalise(mapping):
        ad_to_group, adset_to_group, ad_to_adset = mapping
        return (
            _mapping_rows(ad_to_group, "Location", "UniqueAdID"),
            _mapping_rows(adset_to_group, "AdSetID", "Location"),
            _mapping_rows(ad_to_adset, "AdSetID", "UniqueAdID"),
        )

    shared_ad_set = _content_stable_ad_set_id(("ad-a", "ad-b"))
    single_ad_set = _content_stable_ad_set_id(("ad-c",))
    expected = (
        [
            ("PL1", "ad-a"),
            ("PL1", "ad-b"),
            ("PL2", "ad-a"),
            ("PL2", "ad-b"),
            ("PL3", "ad-c"),
        ],
        [
            (shared_ad_set, "PL1"),
            (shared_ad_set, "PL2"),
            (single_ad_set, "PL3"),
        ],
        [
            (shared_ad_set, "ad-a"),
            (shared_ad_set, "ad-b"),
            (single_ad_set, "ad-c"),
        ],
    )

    assert normalise(one_partition) == expected
    assert normalise(four_partitions) == expected
    assert normalise(eight_partitions) == expected

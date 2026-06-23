import pytest
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual

from dsutils.etl import build_spark_schema
import next_ads.Assignment as assignment
from next_ads.Assignment import (
    assign_preranked_ads_v2,
)


@pytest.fixture
def local_spark(monkeypatch):
    try:
        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("next-ads-assignment-v2-tests")
            .getOrCreate()
        )
    except RuntimeError as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")
    monkeypatch.setattr(assignment, "get_spark", lambda: spark)
    yield spark


def test_assign_preranked_ads_v2_returns_trigger_score(local_spark):
    spark = local_spark

    preranked_schema = build_spark_schema([
        ["AccountNumber", "string", "not null"],
        ["UniqueAdID", "string", "not null"],
        ["PageType", "string", "not null"],
        ["Rank", "int", "not null"],
        ["TriggerScore", "float", "null"],
    ])
    preranked = spark.createDataFrame(
        [
            ["acc1", "ad1", "ShoppingBag", 1, 0.8],
            ["acc1", "ad2", "ShoppingBag", 2, 0.4],
            ["acc1", "ad3", "HomePage", 1, 0.9],
            ["acc2", "ad1", "ShoppingBag", 1, 0.7],
        ],
        preranked_schema,
    )
    preranked.createOrReplaceTempView("preranked_ads_v2_test")

    ads_schema = build_spark_schema([
        ["UniqueAdID", "string", "not null"],
    ])
    df_ads = spark.createDataFrame([["ad1"], ["ad2"]], ads_schema)

    cust_schema = build_spark_schema([
        ["AccountNumber", "string", "not null"],
    ])
    df_cust = spark.createDataFrame([["acc1"]], cust_schema)

    result = assign_preranked_ads_v2(
        df_ads=df_ads,
        preranked_ads_table="preranked_ads_v2_test",
        page_type="ShoppingBag",
        df_cust=df_cust,
        n_ads=2,
    )

    expected_schema = build_spark_schema([
        ["AccountNumber", "string", "not null"],
        ["UniqueAdID", "string", "not null"],
        ["Rank", "int", "not null"],
        ["TriggerScore", "float", "null"],
    ])
    expected = spark.createDataFrame(
        [
            ["acc1", "ad1", 1, 0.8],
            ["acc1", "ad2", 2, 0.4],
        ],
        expected_schema,
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False)


def test_assign_preranked_ads_v2_filters_by_page_type(local_spark):
    spark = local_spark

    preranked_schema = build_spark_schema([
        ["AccountNumber", "string", "not null"],
        ["UniqueAdID", "string", "not null"],
        ["PageType", "string", "not null"],
        ["Rank", "int", "not null"],
        ["TriggerScore", "float", "null"],
    ])
    preranked = spark.createDataFrame(
        [
            ["acc1", "ad1", "ShoppingBag", 1, 0.8],
            ["acc1", "ad2", "HomePage", 1, 0.4],
        ],
        preranked_schema,
    )
    preranked.createOrReplaceTempView("preranked_ads_v2_override_test")

    ads_schema = build_spark_schema([
        ["UniqueAdID", "string", "not null"],
    ])
    df_ads = spark.createDataFrame([["ad1"], ["ad2"]], ads_schema)

    result = assign_preranked_ads_v2(
        df_ads=df_ads,
        preranked_ads_table="preranked_ads_v2_override_test",
        page_type="ShoppingBag",
        n_ads=1,
    )

    expected_schema = build_spark_schema([
        ["AccountNumber", "string", "not null"],
        ["UniqueAdID", "string", "not null"],
        ["Rank", "int", "not null"],
        ["TriggerScore", "float", "null"],
    ])
    expected = spark.createDataFrame(
        [["acc1", "ad1", 1, 0.8]],
        expected_schema,
    )

    assertDataFrameEqual(result, expected, checkRowOrder=False)

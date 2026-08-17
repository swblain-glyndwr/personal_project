import inspect
import re

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.nextads_delivery import build_v2_payload


@pytest.fixture
def local_spark():
    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("next-ads-payload-determinism-tests")
            .getOrCreate()
        )
    except RuntimeError as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")
    yield spark


def _payload_rows():
    return [
        ("account-1", "ShoppingBag", False, 2, "experiment", False, 2, 0.3, "b"),
        ("account-1", "HomePage", False, 2, "experiment", False, 1, 0.9, "a"),
        ("account-1", "ProductList", False, 2, "experiment", False, 1, 0.7, "c"),
        ("account-1", "ShoppingBag", False, 2, "experiment", False, 1, 0.9, "a"),
        ("account-1", "HomePage", False, 2, "experiment", False, 2, 0.3, "b"),
        (
            "account-1",
            "OrderComplete",
            False,
            2,
            "experiment",
            False,
            1,
            0.5,
            "d",
        ),
        ("account-2", "HomePage", True, 2, "control", False, 1, 0.4, "e"),
        ("account-2", "ShoppingBag", True, 2, "control", False, 1, 0.4, "e"),
    ]


def _payload_json_by_account(spark, rows, partitions):
    columns = [
        "AccountNumber",
        "pageType",
        "control",
        "adFatigueImpressionThreshold",
        "experimentId",
        "enableAdFatigueRotation",
        "Rank",
        "Max_TriggerScore",
        "fragmentId",
    ]
    combined = spark.createDataFrame(rows, columns).repartition(partitions)
    return {
        row["account_number"]: row["payload_json"]
        for row in (
            build_v2_payload.make_payload(combined)
            .select(
                "account_number",
                F.to_json("next_ads").alias("payload_json"),
            )
            .collect()
        )
    }


def test_payload_collections_are_explicitly_sorted():
    source = inspect.getsource(build_v2_payload.make_payload)

    assert re.search(
        r"sort_array\(\s*F\.collect_list\(\"pageType\"\),\s*asc=True",
        source,
    )
    assert re.search(
        r"sort_array\(\s*F\.collect_list\(\s*F\.struct\("
        r"\s*F\.col\(\"pageTypes\"\)",
        source,
    )


def test_payload_json_is_stable_across_input_order_and_partitions(local_spark):
    rows = _payload_rows()
    baseline = _payload_json_by_account(local_spark, rows, partitions=1)

    assert _payload_json_by_account(
        local_spark,
        list(reversed(rows)),
        partitions=2,
    ) == baseline
    assert _payload_json_by_account(
        local_spark,
        rows[::2] + rows[1::2],
        partitions=4,
    ) == baseline

    fragments = (
        build_v2_payload.make_payload(
            local_spark.createDataFrame(
                rows,
                [
                    "AccountNumber",
                    "pageType",
                    "control",
                    "adFatigueImpressionThreshold",
                    "experimentId",
                    "enableAdFatigueRotation",
                    "Rank",
                    "Max_TriggerScore",
                    "fragmentId",
                ],
            ).repartition(3)
        )
        .where(F.col("account_number") == "account-1")
        .select("next_ads.ads.fragments")
        .first()[0]
    )
    page_type_groups = [list(fragment["pageTypes"]) for fragment in fragments]

    assert page_type_groups == [
        ["HomePage", "ShoppingBag"],
        ["OrderComplete"],
        ["ProductList"],
    ]

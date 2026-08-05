import inspect
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

import next_ads.decisioning.assignment as assignment
from next_ads.decisioning.assignment import (
    assign_nextgenads,
    assign_nextgenads_v2,
    assign_preranked_ads,
    assign_random_ads,
    assign_random_ads_v2,
    assign_random_ads_with_exclusions,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def local_spark(monkeypatch):
    try:
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("next-ads-assignment-determinism-tests")
            .getOrCreate()
        )
    except RuntimeError as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")

    monkeypatch.setattr(assignment, "get_spark", lambda: spark)
    yield spark


def _assignment_rows(dataframe):
    return sorted(
        tuple(row[column] for column in dataframe.columns)
        for row in dataframe.collect()
    )


def test_assignment_and_customer_cell_sources_use_stable_namespaces():
    assignment_source = (
        PROJECT_ROOT / "src/next_ads/decisioning/assignment.py"
    ).read_text()
    customer_cells_source = (
        PROJECT_ROOT / "jobs/nextads_cells/assign_customer_cells.py"
    ).read_text()

    assert "F.rand(" not in assignment_source
    assert "F.rand(" not in customer_cells_source
    assert ".sample(" not in customer_cells_source

    for namespace in (
        "assignment-v2-ad-slot",
        "assignment-v2-customer-offset",
        "assignment-v1-basic",
        "assignment-v1-basic-exclusion",
        "assignment-best-targeting-tie",
        "assignment-best-recommender-tie",
        "assignment-v1-preranked-tie",
    ):
        assert f'namespace="{namespace}"' in assignment_source

    assert assignment_source.count(
        '.withColumn("AdRankTB", F.row_number().over(w_ad_tb))'
    ) == 2
    assert (
        '.withColumn("RankTB", F.row_number().over(w_ad_tb))'
        in assignment_source
    )
    assert 'namespace="customer-cells-fallow"' in customer_cells_source
    assert (
        'namespace="customer-cells-dev-sample"'
        in customer_cells_source
    )
    assert (
        'namespace=f"customer-cells-{fixed_cell}"'
        in customer_cells_source
    )


def test_nextgen_multi_ad_slotting_deduplicates_before_window_and_filters_before_projection():
    v1_source = inspect.getsource(assign_nextgenads)
    v2_source = inspect.getsource(assign_nextgenads_v2)

    assert v1_source.index(".distinct()") < v1_source.index('"creative_slot"')
    assert v2_source.index(".distinct()") < v2_source.index("'creative_slot'")
    assert v1_source.index("F.pmod") < v1_source.index(
        '.select("AccountNumber", "UniqueAdID")'
    )
    assert v2_source.index("F.pmod") < v2_source.index(
        ".select('AccountNumber', 'UniqueAdID', 'Rank', 'TriggerScore')"
    )


def test_assign_random_ads_v2_is_stable_across_repartitioning(local_spark):
    ads = local_spark.createDataFrame(
        [
            ("a1", "A"),
            ("a2", "A"),
            ("a3", "A"),
            ("b1", "B"),
            ("b2", "B"),
        ],
        ["UniqueAdID", "Group"],
    )
    customers = local_spark.createDataFrame(
        [
            *((f"account-a-{index}", "A") for index in range(8)),
            *((f"account-b-{index}", "B") for index in range(6)),
        ],
        ["AccountNumber", "Group"],
    )

    one_partition = assign_random_ads_v2(
        ads.repartition(1),
        customers.repartition(1),
        grp_col="Group",
        n_ads=2,
    )
    four_partitions = assign_random_ads_v2(
        ads.repartition(4),
        customers.repartition(4),
        grp_col="Group",
        n_ads=2,
    )

    one_partition_rows = _assignment_rows(one_partition)
    assert one_partition_rows == _assignment_rows(four_partitions)
    assert len(one_partition_rows) == 28
    ranks_by_account = {}
    for account, _, rank in one_partition_rows:
        ranks_by_account.setdefault(account, set()).add(rank)
    assert all(
        ranks == {1, 2}
        for ranks in ranks_by_account.values()
    )


def test_nextgen_multi_ad_assignment_is_unique_and_partition_stable(
    local_spark,
):
    accounts = [f"account-{index}" for index in range(12)]
    assignment_rows = [
        (account, 213, rank, float(10 - rank))
        for account in accounts
        for rank in (1, 2)
    ]
    local_spark.createDataFrame(
        assignment_rows,
        [
            "AccountNumber",
            "assigned_cluster_id",
            "assignment_rank",
            "target_score",
        ],
    ).createOrReplaceTempView("nextgen_multi_ad_assignments_test")

    ads = local_spark.createDataFrame(
        [
            ("ad-a", "213"),
            ("ad-a", "213"),
            ("ad-b", "213"),
        ],
        ["UniqueAdID", "ClusterID"],
    )
    customers = local_spark.createDataFrame(
        [(account,) for account in accounts],
        ["AccountNumber"],
    )

    v1_one_partition = assign_nextgenads(
        ads.repartition(1),
        "nextgen_multi_ad_assignments_test",
        customers.repartition(1),
        return_ranks=[1],
    )
    v1_four_partitions = assign_nextgenads(
        ads.repartition(4),
        "nextgen_multi_ad_assignments_test",
        customers.repartition(4),
        return_ranks=[1],
    )
    v1_rows = _assignment_rows(v1_one_partition)
    assert v1_rows == _assignment_rows(v1_four_partitions)
    assert len(v1_rows) == len(accounts)
    assert {row[0] for row in v1_rows} == set(accounts)
    assert {row[1] for row in v1_rows} == {"ad-a", "ad-b"}

    v2_one_partition = assign_nextgenads_v2(
        ads.repartition(1),
        "nextgen_multi_ad_assignments_test",
        customers.repartition(1),
        n_ads=2,
    )
    v2_four_partitions = assign_nextgenads_v2(
        ads.repartition(4),
        "nextgen_multi_ad_assignments_test",
        customers.repartition(4),
        n_ads=2,
    )
    v2_rows = _assignment_rows(v2_one_partition)
    assert v2_rows == _assignment_rows(v2_four_partitions)
    assert len(v2_rows) == len(accounts) * 2
    assert {
        (row[0], row[2]) for row in v2_rows
    } == {
        (account, rank) for account in accounts for rank in (1, 2)
    }
    assert {row[1] for row in v2_rows} == {"ad-a", "ad-b"}


def test_assign_random_ads_is_stable_and_balanced(local_spark):
    ads = local_spark.createDataFrame(
        [
            ("a1", "A"),
            ("a2", "A"),
            ("a3", "A"),
            ("b1", "B"),
            ("b2", "B"),
        ],
        ["UniqueAdID", "Group"],
    )
    customers = local_spark.createDataFrame(
        [
            *((f"account-a-{index}", "A") for index in range(12)),
            *((f"account-b-{index}", "B") for index in range(8)),
        ],
        ["AccountNumber", "Group"],
    )

    one_partition = assign_random_ads(
        ads.repartition(1),
        customers.repartition(1),
        grp_col="Group",
    )
    four_partitions = assign_random_ads(
        ads.repartition(4),
        customers.repartition(4),
        grp_col="Group",
    )

    one_partition_rows = _assignment_rows(one_partition)
    assert one_partition_rows == _assignment_rows(four_partitions)
    assert len(one_partition_rows) == 20

    exposure_counts = {
        row["UniqueAdID"]: row["count"]
        for row in one_partition.groupBy("UniqueAdID").count().collect()
    }
    assert exposure_counts == {
        "a1": 4,
        "a2": 4,
        "a3": 4,
        "b1": 4,
        "b2": 4,
    }


def test_assign_random_ads_with_exclusions_is_stable_and_eligible(
    local_spark,
):
    ads = local_spark.createDataFrame(
        [
            ("a1", "A"),
            ("a2", "A"),
            ("a3", "A"),
        ],
        ["UniqueAdID", "Group"],
    )
    customers = local_spark.createDataFrame(
        [
            ("account-1", "A", "a1"),
            ("account-2", "A", "a2"),
            ("account-3", "A", None),
        ],
        ["AccountNumber", "Group", "ExcludedAdID"],
    )

    one_partition = assign_random_ads_with_exclusions(
        ads.repartition(1),
        customers.repartition(1),
        grp_col="Group",
    )
    four_partitions = assign_random_ads_with_exclusions(
        ads.repartition(4),
        customers.repartition(4),
        grp_col="Group",
    )

    one_partition_rows = _assignment_rows(one_partition)
    assert one_partition_rows == _assignment_rows(four_partitions)
    assert len(one_partition_rows) == 3

    exclusions = {
        row["AccountNumber"]: row["ExcludedAdID"]
        for row in customers.collect()
    }
    assert all(
        exclusions[account] is None or ad_id != exclusions[account]
        for account, ad_id in one_partition_rows
    )


def test_assign_preranked_ads_breaks_exact_ties_stably(local_spark):
    ads = local_spark.createDataFrame(
        [("a1",), ("a2",), ("a3",)],
        ["UniqueAdID"],
    )
    preranked = local_spark.createDataFrame(
        [
            ("account-1", "a1", 0.8, 1),
            ("account-1", "a2", 0.8, 1),
            ("account-2", "a2", 0.7, 1),
            ("account-2", "a3", 0.7, 1),
        ],
        ["AccountNumber", "UniqueAdID", "Score", "Rank"],
    )

    preranked.repartition(1).createOrReplaceTempView(
        "preranked_assignment_ties"
    )
    one_partition = assign_preranked_ads(
        df_ads=ads.repartition(1),
        preranked_ads_table="preranked_assignment_ties",
    )
    one_partition_rows = _assignment_rows(one_partition)

    preranked.repartition(4).createOrReplaceTempView(
        "preranked_assignment_ties"
    )
    four_partitions = assign_preranked_ads(
        df_ads=ads.repartition(4),
        preranked_ads_table="preranked_assignment_ties",
    )

    assert one_partition_rows == _assignment_rows(four_partitions)
    assert len(one_partition_rows) == 2

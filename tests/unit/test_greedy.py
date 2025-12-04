from pyspark.testing import assertDataFrameEqual
import pytest
from next_ads.Assignment import greedy_assignment
from dsutils.dbc import configure_spark
from dsutils.etl import build_spark_schema


@pytest.fixture
def df_test():
    """
    Standard test dataset with 3 items (A, B, C) and 10 users each.
    Rankings are set up so item A gets users 1-10, then B gets users 1-10,
    then C gets users 1-10.
    """
    data = [
        ['A', 'user1', 1],
        ['A', 'user2', 2],
        ['A', 'user3', 3],
        ['A', 'user4', 4],
        ['A', 'user5', 5],
        ['A', 'user6', 6],
        ['A', 'user7', 7],
        ['A', 'user8', 8],
        ['A', 'user9', 9],
        ['A', 'user10', 10],
        ['B', 'user1', 11],
        ['B', 'user2', 12],
        ['B', 'user3', 13],
        ['B', 'user4', 14],
        ['B', 'user5', 15],
        ['B', 'user6', 16],
        ['B', 'user7', 17],
        ['B', 'user8', 18],
        ['B', 'user9', 19],
        ['B', 'user10', 20],
        ['C', 'user1', 21],
        ['C', 'user2', 22],
        ['C', 'user3', 23],
        ['C', 'user4', 24],
        ['C', 'user5', 25],
        ['C', 'user6', 26],
        ['C', 'user7', 27],
        ['C', 'user8', 28],
        ['C', 'user9', 29],
        ['C', 'user10', 30]
    ]
    schema = build_spark_schema([
        ["item", "string", "null"],
        ["user", "string", "null"],
        ["rank", "int", "null"]
    ])
    spark = configure_spark()
    return spark.createDataFrame(data, schema)


def test_greedy_assignment_all_items_equal_quotas(df_test):
    """
    Test where all items have equal quotas of 2.
    Expected: A gets user1, user2; B gets user3, user4; C gets user5, user6
    """
    item_quotas = {'A': 2, 'B': 2, 'C': 2}

    result = greedy_assignment(df_test, item_quotas)

    schema_exp = build_spark_schema([
        ["item", "string", "null"],
        ["user", "string", "null"]
    ])

    data_exp = [
        ['A', 'user1'],
        ['A', 'user2'],
        ['B', 'user3'],
        ['B', 'user4'],
        ['C', 'user5'],
        ['C', 'user6']
    ]

    spark = configure_spark()
    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected, checkRowOrder=False)


def test_greedy_assignment_partial_items_with_quotas(df_test):
    """
    Test where only some items (A and C) have quotas.
    Expected: A gets user1, user2; C gets user3, user4 (B is skipped)
    """
    item_quotas = {'A': 2, 'C': 2}

    result = greedy_assignment(df_test, item_quotas)

    schema_exp = build_spark_schema([
        ["item", "string", "null"],
        ["user", "string", "null"]
    ])

    data_exp = [
        ['A', 'user1'],
        ['A', 'user2'],
        ['C', 'user3'],
        ['C', 'user4']
    ]

    spark = configure_spark()
    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected, checkRowOrder=False)


def test_greedy_assignment_single_item_different_quota(df_test):
    """
    Test where only one item (B) has a quota of 4.
    Expected: B gets user1, user2, user3, user4
    """
    item_quotas = {'B': 4}

    result = greedy_assignment(df_test, item_quotas)

    schema_exp = build_spark_schema([
        ["item", "string", "null"],
        ["user", "string", "null"]
    ])

    data_exp = [
        ['B', 'user1'],
        ['B', 'user2'],
        ['B', 'user3'],
        ['B', 'user4']
    ]

    spark = configure_spark()
    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected, checkRowOrder=False)


def test_greedy_assignment_custom_column_names():
    """
    Test that custom column names work correctly.
    """
    data = [
        ['product1', 'customer1', 1],
        ['product1', 'customer2', 2],
        ['product2', 'customer1', 3],
        ['product2', 'customer3', 4]
    ]
    schema = build_spark_schema([
        ["product", "string", "null"],
        ["customer", "string", "null"],
        ["priority", "int", "null"]
    ])

    spark = configure_spark()
    df = spark.createDataFrame(data, schema)

    item_quotas = {'product1': 1, 'product2': 1}

    result = greedy_assignment(
        df,
        item_quotas,
        item_col='product',
        user_col='customer',
        rank_col='priority',
        logging_interval=1
    )

    schema_exp = build_spark_schema([
        ["product", "string", "null"],
        ["customer", "string", "null"]
    ])

    data_exp = [
        ['product1', 'customer1'],
        ['product2', 'customer3']
    ]

    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected, checkRowOrder=False)


def test_greedy_assignment_no_duplicate_users():
    """
    Test that users are never assigned to multiple items.
    Even if they appear in rankings for multiple items, they should only
    be assigned once.
    """
    data = [
        ['A', 'user1', 1],
        ['B', 'user1', 2],
        ['C', 'user1', 3],
        ['A', 'user2', 4],
        ['B', 'user2', 5],
        ['C', 'user2', 6]
    ]
    schema = build_spark_schema([
        ["item", "string", "null"],
        ["user", "string", "null"],
        ["rank", "int", "null"]
    ])

    spark = configure_spark()
    df = spark.createDataFrame(data, schema)

    item_quotas = {'A': 1, 'B': 1, 'C': 1}

    result = greedy_assignment(
        df,
        item_quotas,
        logging_interval=1
    )

    # user1 should go to A (rank 1), user2 should go to B (rank 4)
    # C should get nothing because both users are already assigned
    schema_exp = build_spark_schema([
        ["item", "string", "null"],
        ["user", "string", "null"]
    ])

    data_exp = [
        ['A', 'user1'],
        ['B', 'user2']
    ]

    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected, checkRowOrder=False)


def test_greedy_assignment_empty_quotas(df_test):
    """
    Test with empty quotas dictionary.
    Expected: Empty result since no items have quotas.
    """
    item_quotas = {}

    result = greedy_assignment(df_test, item_quotas)

    schema_exp = build_spark_schema([
        ["item", "string", "null"],
        ["user", "string", "null"]
    ])

    spark = configure_spark()
    expected = spark.createDataFrame([], schema_exp)

    assertDataFrameEqual(result, expected, checkRowOrder=False)


def test_greedy_assignment_quota_exceeds_users():
    """
    Test where quota is larger than available users for an item.
    Expected: All available users should be assigned.
    """
    data = [
        ['A', 'user1', 1],
        ['A', 'user2', 2],
        ['A', 'user3', 3]
    ]
    schema = build_spark_schema([
        ["item", "string", "null"],
        ["user", "string", "null"],
        ["rank", "int", "null"]
    ])

    spark = configure_spark()
    df = spark.createDataFrame(data, schema)

    item_quotas = {'A': 10}  # Quota exceeds available users

    result = greedy_assignment(
        df,
        item_quotas,
        logging_interval=1
    )

    schema_exp = build_spark_schema([
        ["item", "string", "null"],
        ["user", "string", "null"]
    ])

    data_exp = [
        ['A', 'user1'],
        ['A', 'user2'],
        ['A', 'user3']
    ]

    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected, checkRowOrder=False)


def test_greedy_assignment_zero_quota(df_test):
    """
    Test with zero quota for items.
    Expected: No assignments.
    """
    item_quotas = {'A': 0, 'B': 0, 'C': 0}

    result = greedy_assignment(df_test, item_quotas)

    schema_exp = build_spark_schema([
        ["item", "string", "null"],
        ["user", "string", "null"]
    ])

    spark = configure_spark()
    expected = spark.createDataFrame([], schema_exp)

    assertDataFrameEqual(result, expected, checkRowOrder=False)


def test_greedy_assignment_mixed_quotas(df_test):
    """
    Test with mixed quotas where items have different capacities.
    Expected: Items filled according to their quotas in rank order.
    """
    item_quotas = {'A': 1, 'B': 3, 'C': 2}

    result = greedy_assignment(df_test, item_quotas)

    schema_exp = build_spark_schema([
        ["item", "string", "null"],
        ["user", "string", "null"]
    ])

    # A gets user1 (rank 1)
    # B gets user2, user3, user4 (ranks 12,13,14 are next after user1 assigned)
    # C gets user5, user6 (next available)
    data_exp = [
        ['A', 'user1'],
        ['B', 'user2'],
        ['B', 'user3'],
        ['B', 'user4'],
        ['C', 'user5'],
        ['C', 'user6']
    ]

    spark = configure_spark()
    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected, checkRowOrder=False)


def test_greedy_assignment_respects_rank_order():
    """
    Test that assignments strictly follow rank order.
    """
    data = [
        ['A', 'user3', 1],
        ['A', 'user2', 2],
        ['A', 'user1', 3],
        ['B', 'user3', 4],
        ['B', 'user2', 5],
        ['B', 'user1', 6]
    ]
    schema = build_spark_schema([
        ["item", "string", "null"],
        ["user", "string", "null"],
        ["rank", "int", "null"]
    ])
    spark = configure_spark()
    df = spark.createDataFrame(data, schema)

    item_quotas = {'A': 2, 'B': 1}

    result = greedy_assignment(
        df,
        item_quotas,
        logging_interval=1
    )

    schema_exp = build_spark_schema([
        ["item", "string", "null"],
        ["user", "string", "null"]
    ])

    # Rank 1: A gets user3
    # Rank 2: A gets user2
    # Rank 4: B gets user1 (user3 already assigned)
    data_exp = [
        ['A', 'user3'],
        ['A', 'user2'],
        ['B', 'user1']
    ]

    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected, checkRowOrder=False)

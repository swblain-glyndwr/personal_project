import pytest
from scripts.load_control_sheet_v2 import check_primary_key
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger


@pytest.fixture(scope="session")
def spark():
    spark = configure_spark()
    return spark


@pytest.fixture
def logger():
    configure_logging(log_level="DEBUG")
    logger = get_logger(__name__)
    return logger


@pytest.fixture
def sample_data_no_duplicates(spark):
    data = [
        ("ad1", "loc1", "div1", "masid1"),
        ("ad2", "loc1", "div1", "masid2"),
        ("ad3", "loc2", "div1", "masid3"),
    ]
    columns = ["UniqueAdID", "Location", "AlgoDivision", "MASIDToken"]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def sample_data_with_duplicates(spark):
    data = [
        ("ad1", "loc1", "div1", "masid1"),
        # Duplicate masid1 in same AlgoDivision and Location
        ("ad2", "loc1", "div1", "masid1"),
        ("ad3", "loc2", "div1", "masid2"),
        ("ad4", "loc1", "div1", "masid3"),
        ("ad5", "loc1", "div1", "masid1"),  # another duplicate masid1
    ]
    columns = ["UniqueAdID", "Location", "AlgoDivision", "MASIDToken"]
    return spark.createDataFrame(data, columns)


def test_check_primary_key_no_duplicates(sample_data_no_duplicates, logger):
    df = sample_data_no_duplicates
    result_df = check_primary_key(df, logger, "dev", "dummy_url")
    assert result_df.count() == df.count()
    assert result_df.collect() == df.collect()


def test_check_primary_key_with_duplicates(
    sample_data_with_duplicates, logger
):
    df = sample_data_with_duplicates
    result_df = check_primary_key(df, logger, "dev", "dummy_url")

    # The logic keeps the "last" ad alphabetically ('ad5') and removes the others ('ad1', 'ad2')
    # So, we expect 3 rows to remain: ad5, ad3, ad4
    assert result_df.count() == 3

    remaining_ids = [row.UniqueAdID for row in result_df.collect()]
    assert "ad5" in remaining_ids
    assert "ad3" in remaining_ids
    assert "ad4" in remaining_ids
    assert "ad1" not in remaining_ids
    assert "ad2" not in remaining_ids


def test_check_primary_key_pk_assertion(spark, logger):
    data = [
        ("ad1", "loc1", "div1", "masid1"),
        ("ad1", "loc1", "div1", "masid2"),  # Duplicate UniqueAdID and Location
    ]
    columns = ["UniqueAdID", "Location", "AlgoDivision", "MASIDToken"]
    df = spark.createDataFrame(data, columns)

    with pytest.raises(Exception):
        check_primary_key(df, logger, "dev", "dummy_url")

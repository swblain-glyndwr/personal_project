from pyspark.testing import assertDataFrameEqual
import pytest
from next_ads.control.attributes import (
    collect_attribute_set as new_collect_attribute_set,
)
from next_ads.control.attributes import parse_ad_attributes as new_parse_ad_attributes
from next_ads.Attributes import collect_attribute_set
from next_ads.Attributes import parse_ad_attributes
from dsutils.dbc import configure_spark
from dsutils.etl import build_spark_schema


def test_old_and_new_attribute_import_paths_match():
    assert parse_ad_attributes is new_parse_ad_attributes
    assert collect_attribute_set is new_collect_attribute_set


def test_parse_ad_attributes_basic():
    """Test basic functionality with default parameters."""
    schema = build_spark_schema([
            ["UniqueAdID", "string", "null"],
            ["TargetingAttributes", "string", "null"]])

    data = [
        ("ad1", "gender:male, age:30"),
        ("ad2", "location:NYC, interest:sports")
    ]

    spark = configure_spark()
    df = spark.createDataFrame(data, schema)

    result = parse_ad_attributes(df)

    schema_exp = build_spark_schema([
            ["UniqueAdID", "string", "null"],
            ["attribute", "string", "null"],
            ["value", "string", "null"]])

    data_exp = [("ad1", "gender", "male"),
                ("ad1", "age", "30"),
                ("ad2", "location", "NYC"),
                ("ad2", "interest", "sports")]

    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected)


def test_parse_ad_attributes_custom_columns():
    """Test with custom column names."""
    schema = build_spark_schema([
            ["CustomAdID", "string", "null"],
            ["CustomAttributes", "string", "null"]])

    data = [("custom_ad1", "type:banner, size:300x250")]

    spark = configure_spark()
    df = spark.createDataFrame(data, schema)

    result = parse_ad_attributes(
        df,
        ad_id_col="CustomAdID",
        attribute_col="CustomAttributes"
    )

    schema_exp = build_spark_schema([
            ["CustomAdID", "string", "null"],
            ["attribute", "string", "null"],
            ["value", "string", "null"]])

    data_exp = [("custom_ad1", "type", "banner"),
                ("custom_ad1", "size", "300x250")]

    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected)


def test_parse_ad_attributes_custom_delimiters():
    """Test with custom delimiters."""
    schema = build_spark_schema([
            ["UniqueAdID", "string", "null"],
            ["TargetingAttributes", "string", "null"]])

    data = [("ad1", "gender=male|age=30")]

    spark = configure_spark()
    df = spark.createDataFrame(data, schema)

    result = parse_ad_attributes(
        df,
        split_delimiter="|",
        key_value_delimiter="="
    )

    schema_exp = build_spark_schema([
            ["UniqueAdID", "string", "null"],
            ["attribute", "string", "null"],
            ["value", "string", "null"]])

    data_exp = [("ad1", "gender", "male"),
                ("ad1", "age", "30")]

    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected)


def test_parse_ad_attributes_same_delimeters():
    """Test where `split_delimeter` and `key_value_delimeter`
    are the same.
    """
    schema = build_spark_schema([
            ["UniqueAdID", "string", "null"],
            ["TargetingAttributes", "string", "null"]])

    data = [("ad1", "")]

    spark = configure_spark()
    df = spark.createDataFrame(data, schema)

    with pytest.raises(ValueError):
        parse_ad_attributes(
            df,
            split_delimiter="=",
            key_value_delimiter="="
        )


def test_parse_ad_attributes_empty_string():
    """Test with empty attribute string (should be filtered out by default)"""
    schema = build_spark_schema([
            ["UniqueAdID", "string", "null"],
            ["TargetingAttributes", "string", "null"]])

    data = [("ad1", "")]

    spark = configure_spark()
    df = spark.createDataFrame(data, schema)

    result = parse_ad_attributes(df)
    result_data = result.collect()

    assert len(result_data) == 0


def test_parse_ad_attributes_null_values():
    """Test with null attribute values."""
    schema = build_spark_schema([
            ["UniqueAdID", "string", "null"],
            ["TargetingAttributes", "string", "null"]])

    data = [
        ("ad1", None),
        ("ad2", "gender:male, age:30")
    ]

    spark = configure_spark()
    df = spark.createDataFrame(data, schema)

    result = parse_ad_attributes(df)

    schema_exp = build_spark_schema([
        ["UniqueAdID", "string", "null"],
        ["attribute", "string", "null"],
        ["value", "string", "null"]])

    data_exp = [("ad2", "gender", "male"),
                ("ad2", "age", "30")]

    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected)


def test_parse_ad_attributes_malformed_pairs():
    """Test with malformed key-value pairs."""
    schema = build_spark_schema([
            ["UniqueAdID", "string", "null"],
            ["TargetingAttributes", "string", "null"]])

    data = [
        ("ad1", "gender:male, invalid_pair, age:30"),
        ("ad2", "location:, :empty_key")
    ]

    spark = configure_spark()
    df = spark.createDataFrame(data, schema)

    result = parse_ad_attributes(df)

    schema_exp = build_spark_schema([
        ["UniqueAdID", "string", "null"],
        ["attribute", "string", "null"],
        ["value", "string", "null"]])

    data_exp = [("ad1", "gender", "male"),
                ("ad1", "age", "30")]

    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected)


def test_parse_ad_attributes_single_attribute():
    """Test with single attribute-value pair."""
    schema = build_spark_schema([
            ["UniqueAdID", "string", "null"],
            ["TargetingAttributes", "string", "null"]])

    data = [("ad1", "gender:female")]

    spark = configure_spark()
    df = spark.createDataFrame(data, schema)

    result = parse_ad_attributes(df)

    schema_exp = build_spark_schema([
        ["UniqueAdID", "string", "null"],
        ["attribute", "string", "null"],
        ["value", "string", "null"]])

    data_exp = [("ad1", "gender", "female")]

    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected)


def test_parse_ad_attributes_whitespace_handling():
    """Test handling of whitespace in attributes and values.
    Expected behavior is to trim surrounding whitespace of
    both `attribute` and `value`.
    """
    schema = build_spark_schema([
            ["UniqueAdID", "string", "null"],
            ["TargetingAttributes", "string", "null"]])

    data = [("ad1", " gender : male , age : 30 ")]

    spark = configure_spark()
    df = spark.createDataFrame(data, schema)

    result = parse_ad_attributes(df)

    schema_exp = build_spark_schema([
        ["UniqueAdID", "string", "null"],
        ["attribute", "string", "null"],
        ["value", "string", "null"]])

    data_exp = [("ad1", "gender", "male"),
                ("ad1", "age", "30")]

    expected = spark.createDataFrame(data_exp, schema_exp)

    assertDataFrameEqual(result, expected)

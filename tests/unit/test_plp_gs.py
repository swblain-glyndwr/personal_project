import pytest
from unittest.mock import patch
from pyspark.sql.types import StructType, StructField, StringType


# @pytest.fixture
# def mock_control_sheet_config():
#     """Mock control sheet configuration."""
#     return {
#         "CONTROL_SHEET_URL": "https://docs.google.com/spreadsheets/d/test123",
#         "CONTROL_SHEET_TAB": "Control Sheet",
#         "PLP_PLACEMENTS_TAB": "Placements",
#         "ADDITIONAL_PLP_PLACEMENTS_TAB": "Additional Placements",
#     }


@pytest.fixture
def mock_control_sheet_data(spark):
    """Mock control sheet data."""
    schema = StructType([
        StructField("UniqueAdID", StringType(), True),
        StructField("CMSPageID", StringType(), True),
        StructField("Realm", StringType(), True),
        StructField("Territory", StringType(), True),
        StructField("MASIDToken", StringType(), True),
        StructField("PLX", StringType(), True),
        StructField("PL1", StringType(), True),
        StructField("PL2", StringType(), True),
        StructField("PL3", StringType(), True),
        StructField("URL", StringType(), True),
        StructField("Status", StringType(), True),
    ])

    data = [
        (
            "AD001",
            "c364",
            "Next",
            "GB",
            "HPAA",
            "TRUE",
            "TRUE",
            "FALSE",
            "FALSE",
            "/page1",
            "active",
        ),
        (
            "AD002",
            "c1778_v2",
            "Next",
            "GB",
            "BBQT",
            "FALSE",
            "FALSE",
            "TRUE",
            "TRUE",
            "/page2",
            "active",
        ),
        (
            "AD003",
            "c511",
            "Next",
            "GB",
            "BXAA",
            "FALSE",
            "TRUE",
            "FALSE",
            "TRUE",
            "/products",
            "active",
        ),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_plp_placements_data(spark):
    """Mock PLP placements data."""
    schema = StructType([
        StructField("Location", StringType(), True),
        StructField("Page", StringType(), True),
        StructField("Screen", StringType(), True),
        StructField("PageGroup", StringType(), True),
    ])

    data = [
        ("PL1", "/page1", "Home", "GroupA"),
        ("PL2", "/page2", "PLP", "GroupB"),
        ("PL3", "/page3", "Cart", "GroupC"),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_plx_placements_data(spark):
    """Mock PLX additional placements data."""
    schema = StructType([
        StructField("Location", StringType(), True),
        StructField("Page", StringType(), True),
        StructField("Screen", StringType(), True),
    ])

    data = [
        ("PLX", "/plx-page1", "PLP"),
        ("PLX", "/plx-page2", "PLP"),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_control_sheet_table_df(spark):
    """Create mock control sheet DataFrame with correct schema."""
    control_sheet_table_schema = StructType([
        StructField("Realm", StringType(), True),
        StructField("Territory", StringType(), True),
        StructField("UniqueAdID", StringType(), True),
        StructField("CMSPageID", StringType(), True),
        StructField("PotNumber", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("MASIDToken", StringType(), True),
        StructField("Screen", StringType(), True),
        StructField("Page", StringType(), True),
    ])

    data = [
        (
            "next",
            "GB",
            "AD001",
            "CMS001",
            "POT001",
            "PL1",
            "TOKEN001",
            "Screen1",
            "/page1",
        ),
        (
            "next",
            "GB",
            "AD001",
            "CMS001",
            "POT001",
            "PL2",
            "TOKEN001",
            "Screen1",
            "/page2",
        ),
        (
            "next",
            "GB",
            "AD002",
            "CMS002",
            "POT002",
            "PL3",
            "TOKEN002",
            "Screen2",
            "/page1",
        ),
        (
            "next",
            "GB",
            "AD002",
            "CMS002",
            "POT002",
            "PLX",
            "TOKEN002",
            "Screen2",
            "/page3",
        ),
    ]

    return spark.createDataFrame(data, control_sheet_table_schema)


def test_process_control_sheet_basic(
    spark,
    config_dev,
    mock_control_sheet_data,
    mock_plp_placements_data,
    mock_plx_placements_data
):
    """Test basic processing of control sheet data."""

    with patch("scripts.plp_gs.spark") as mock_spark:
        # Setup mock spark.table() to return our test data
        def spark_table_side_effect(table_name):
            if "control_sheet_raw_latest" in table_name:
                return mock_control_sheet_data
            elif "control_sheet_plp_raw_latest" in table_name:
                return mock_plp_placements_data
            elif "multipage_locations_latest" in table_name:
                return mock_plx_placements_data
            else:
                raise ValueError(f"Unknown table: {table_name}")

        mock_spark.table.side_effect = spark_table_side_effect
        # Keep the real SQL operations
        mock_spark.sql = spark.sql

        # Import after patching
        from scripts.plp_gs import process_control_sheet

        # Run the function
        result_df = process_control_sheet(config=config_dev)

        # Assertions
        assert result_df is not None
        assert result_df.count() > 0

        # Verify spark.table was called 3 times
        assert mock_spark.table.call_count == 3

        # Define expected schema
        expected_schema = {
            "Action": StringType,
            "realm": StringType,
            "territory": StringType,
            "url": StringType,
            "masIdSlotsAndCMSContent": StringType,
        }
        expected_columns = list(expected_schema.keys())
        actual_columns = result_df.columns

        assert set(expected_columns).issubset(set(actual_columns)), (
            f"Missing columns. Expected: {expected_columns}, "
            f"Got: {actual_columns}"
        )

        # Check data types
        schema_dict = {
            field.name: field.dataType for field in result_df.schema.fields
        }
        for col in expected_columns:
            assert isinstance(schema_dict[col], StringType), (
                f"Column {col} should be StringType, got "
                f"{type(schema_dict[col])}"
            )

        # Define expected schema
        expected_schema = {
            "Action": StringType,
            "realm": StringType,
            "territory": StringType,
            "url": StringType,
            "masIdSlotsAndCMSContent": StringType,
        }

        # Check each field
        for field in result_df.schema.fields:
            if field.name in expected_schema:
                expected_type = expected_schema[field.name]
                assert isinstance(field.dataType, expected_type), (
                    f"Field {field.name} has wrong type: "
                    f"{type(field.dataType)}"
                )

        # Check if all expected fields are in result
        schema_fields = [field.name for field in result_df.schema.fields]
        for field_name in expected_schema:
            assert field_name in schema_fields, (
                "Expected output field name is not in result."
            )


def test_process_control_sheet_filters_active_plp(
    spark,
    config_dev,
    mock_control_sheet_data,
    mock_plp_placements_data,
    mock_plx_placements_data
):
    """Test basic processing of control sheet data."""

    with patch("scripts.plp_gs.spark") as mock_spark:
        # Setup mock spark.table() to return our test data
        def spark_table_side_effect(table_name):
            if "control_sheet_raw_latest" in table_name:
                return mock_control_sheet_data
            elif "control_sheet_plp_raw_latest" in table_name:
                return mock_plp_placements_data
            elif "multipage_locations_latest" in table_name:
                return mock_plx_placements_data
            else:
                raise ValueError(f"Unknown table: {table_name}")

        mock_spark.table.side_effect = spark_table_side_effect
        # Keep the real SQL operations
        mock_spark.sql = spark.sql

        # Import after patching
        from scripts.plp_gs import process_control_sheet

        result_df = process_control_sheet(
            config=config_dev
        )

        # All rows should have action='upsert' (since we filter for TRUE PLPs)
        actions = [
            row.Action
            for row in result_df.select("Action").distinct().collect()
        ]
        assert all(action == "upsert" for action in actions), (
            "All actions should be 'upsert' for active PLPs"
        )

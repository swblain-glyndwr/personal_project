import pytest
from unittest.mock import patch
from pyspark.sql.types import StructType, StructField, StringType


@pytest.fixture
def mock_control_sheet_config():
    """Mock control sheet configuration."""
    return {
        "CONTROL_SHEET_URL": "https://docs.google.com/spreadsheets/d/test123",
        "CONTROL_SHEET_TAB": "Control Sheet",
        "PLP_PLACEMENTS_TAB": "Placements",
        "ADDITIONAL_PLP_PLACEMENTS_TAB": "Additional Placements",
    }


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
            "fatface",
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
        StructField("Page", StringType(), True),
        StructField("Sales", StringType(), True),
    ])

    data = [
        ("/plx-page1", "100"),
        ("/plx-page2", "200"),
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
    mock_control_sheet_config,
    mock_control_sheet_data,
    mock_plp_placements_data
):
    """Test basic processing of control sheet data."""

    with patch(
        "next_ads.utils.gs_helpers.read_from_google_sheets_to_dataframe"
    ) as mock_read:
        # Mock the Google Sheets reads
        mock_read.side_effect = [
            mock_control_sheet_data,
            mock_plp_placements_data,
            IndexError("No PLX tab"),
        ]

        # Import after patching to use mocked version
        from scripts.plp_gs import process_control_sheet

        # Run the function with config parameter
        result_df = process_control_sheet(
            control_sheet_config=mock_control_sheet_config
        )

        # Assertions
        assert result_df is not None
        assert result_df.count() > 0

        # Verify Google Sheets was called with correct parameters
        assert mock_read.call_count == 3
        mock_read.assert_any_call(
            sheet_url=mock_control_sheet_config["CONTROL_SHEET_URL"],
            worksheet_name=mock_control_sheet_config["CONTROL_SHEET_TAB"]
        )
        mock_read.assert_any_call(
            sheet_url=mock_control_sheet_config["CONTROL_SHEET_URL"],
            worksheet_name=mock_control_sheet_config["PLP_PLACEMENTS_TAB"]
        )

        # Check output schema
        expected_columns = [
            "Action",
            "realm",
            "territory",
            "url",
            "masIdSlotsAndCMSContent",
        ]
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


def test_process_control_sheet_output_schema(
    spark,
    mock_control_sheet_config,
    mock_control_sheet_data,
    mock_plp_placements_data
):
    """Test that output schema matches expected format."""

    with patch(
        "next_ads.utils.gs_helpers.read_from_google_sheets_to_dataframe"
    ) as mock_read:
        mock_read.side_effect = [
            mock_control_sheet_data,
            mock_plp_placements_data,
            IndexError("No PLX tab"),
        ]

        from scripts.plp_gs import process_control_sheet

        result_df = process_control_sheet(
            control_sheet_config=mock_control_sheet_config
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
    mock_control_sheet_config,
    mock_control_sheet_data,
    mock_plp_placements_data
):
    """Test that only TRUE PLP placements are included."""

    with patch(
        "next_ads.utils.gs_helpers.read_from_google_sheets_to_dataframe"
    ) as mock_read:
        mock_read.side_effect = [
            mock_control_sheet_data,
            mock_plp_placements_data,
            IndexError("No PLX tab"),
        ]

        from scripts.plp_gs import process_control_sheet

        result_df = process_control_sheet(
            control_sheet_config=mock_control_sheet_config
        )

        # All rows should have action='upsert' (since we filter for TRUE PLPs)
        actions = [
            row.Action
            for row in result_df.select("Action").distinct().collect()
        ]
        assert all(action == "upsert" for action in actions), (
            "All actions should be 'upsert' for active PLPs"
        )


def test_process_control_sheet_with_plx(
    spark,
    mock_control_sheet_config,
    mock_control_sheet_data,
    mock_plp_placements_data
):
    """Test processing with PLX additional placements."""

    # Create mock PLX data
    plx_schema = StructType([
        StructField("URL", StringType(), True),
        StructField("Sales", StringType(), True),
    ])

    plx_data = [
        ("https://example.com/plx-page1", "100"),
        ("https://example.com/plx-page2", "200"),
    ]

    mock_plx_data = spark.createDataFrame(plx_data, plx_schema)

    with patch(
        "next_ads.utils.gs_helpers.read_from_google_sheets_to_dataframe"
    ) as mock_read:
        mock_read.side_effect = [
            mock_control_sheet_data,
            mock_plp_placements_data,
            mock_plx_data,
        ]

        from scripts.plp_gs import process_control_sheet

        result_df = process_control_sheet(
            control_sheet_config=mock_control_sheet_config
        )

        assert result_df is not None
        assert result_df.count() > 0

        # Check that all calls were made
        assert mock_read.call_count == 3


@patch("scripts.plp_gs.spark.table")
def test_output_schema(
    mock_spark_table,
    mock_control_sheet_table_df,
    config_dev
):
    """Test that output_df has correct schema after transformation."""
    from scripts.plp_gs import process_control_sheet_from_table

    mock_spark_table.return_value = mock_control_sheet_table_df

    # Execute
    result = process_control_sheet_from_table(
        control_sheet_table_name=(
            config_dev.task_plp_gs_per_client.control_sheet_table_name
        )
    )

    # Assert schema
    assert result is not None
    assert len(result.columns) == 5
    assert "Action" in result.columns
    assert "realm" in result.columns
    assert "territory" in result.columns
    assert "url" in result.columns
    assert "masIdSlotsAndCMSContent" in result.columns

    # Verify schema types
    schema_dict = {field.name: field.dataType for field in result.schema}
    assert schema_dict["Action"].typeName() == "string"
    assert schema_dict["realm"].typeName() == "string"
    assert schema_dict["territory"].typeName() == "string"
    assert schema_dict["url"].typeName() == "string"
    assert schema_dict["masIdSlotsAndCMSContent"].typeName() == "string"

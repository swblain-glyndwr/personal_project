import pytest
from typing import Generator

from pyspark.sql import DataFrame
from scripts import plp_gs


class TestProcessControlSheetFromTableIntegration:
    """Integration tests with real Spark (if available)."""

    @pytest.fixture(scope="class")
    def processed_result(self, spark, config_prod) -> Generator[DataFrame, None, None]:
        """
        Process control sheet once for entire test class.
        
        Args:
            spark: Spark session
            config_prod: Production config
            
        Returns:
            Generator[DataFrame, None, None]: Processed control sheet result
        """
        result = plp_gs.process_control_sheet(config_prod)
        
        # Cache the result to avoid re-computation
        result.cache()
        result.count()  # Force evaluation
        
        yield result
        
        # Cleanup
        result.unpersist()

    @pytest.mark.controlsheettable_integration
    def test_integration_with_real_table(self, processed_result):
        """Test with real production table."""
        result = processed_result

        assert result is not None

        # Verify output columns
        expected_cols = [
            "Action",
            "realm",
            "territory",
            "url",
            "masIdSlotsAndCMSContent",
        ]
        for col in expected_cols:
            assert col in result.columns

    @pytest.mark.controlsheettable_integration
    def test_integration_output_schema(self, processed_result):
        """Test output schema matches expected format."""
        result = processed_result

        # Check data types
        schema = result.schema
        assert schema["Action"].dataType.typeName() == "string"
        assert (
            schema["masIdSlotsAndCMSContent"].dataType.typeName() == "string"
        )

    @pytest.mark.controlsheettable_integration
    def test_integration_masidcmsid_format(self, processed_result):
        """Test that MASIDCMSid is properly formatted."""

        result = processed_result
        result_pdf = result.toPandas()

        # Check that masIdSlotsAndCMSContent contains pipe-separated values
        for masid_str in result_pdf["masIdSlotsAndCMSContent"]:
            parts = masid_str.split("|")
            assert len(parts) > 0

            # Each part should be in format PLx_TOKEN-CMSid
            for part in parts:
                assert "_" in part
                assert "-" in part

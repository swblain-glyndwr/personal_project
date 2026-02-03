import pytest

from scripts import plp_gs


class TestProcessControlSheetFromTableIntegration:
    """Integration tests with real Spark (if available)."""

    @pytest.mark.controlsheettable_integration
    def test_integration_with_real_table(self, spark, config_prod):
        """Test with real production table."""
        table_name = (
            config_prod.task_plp_gs_per_client.control_sheet_table_name
        )

        result = plp_gs.process_control_sheet_from_table(table_name)

        assert result is not None
        assert result.count() > 0

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
    def test_integration_output_schema(self, spark, config_prod):
        """Test output schema matches expected format."""
        table_name = (
            config_prod.task_plp_gs_per_client.control_sheet_table_name
        )

        result = plp_gs.process_control_sheet_from_table(table_name)

        # Check data types
        schema = result.schema
        assert schema["Action"].dataType.typeName() == "string"
        assert (
            schema["masIdSlotsAndCMSContent"].dataType.typeName() == "string"
        )

    @pytest.mark.controlsheettable_integration
    def test_integration_masidcmsid_format(self, spark, config_prod):
        """Test that MASIDCMSid is properly formatted."""
        table_name = (
            config_prod.task_plp_gs_per_client.control_sheet_table_name
        )

        result = plp_gs.process_control_sheet_from_table(table_name)
        result_pdf = result.toPandas()

        # Check that masIdSlotsAndCMSContent contains pipe-separated values
        for masid_str in result_pdf["masIdSlotsAndCMSContent"]:
            parts = masid_str.split("|")
            assert len(parts) > 0

            # Each part should be in format PLx_TOKEN-CMSid
            for part in parts:
                assert "_" in part
                assert "-" in part

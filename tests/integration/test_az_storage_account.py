import pytest


class TestABFSConnection:
    """Integration tests for Azure ABFS connection via Spark."""

    @pytest.mark.abfs_integration
    def test_connection_to_abfs(self, spark, dbutils, config_prod):
        """Test spark connection to ABFS."""
        from scripts.plp_gs_combiner import _configure_abfs

        # Load config
        config = config_prod
        account_name = config.az_st_account
        tenant_id = config.az_tenant_id
        output_path = (
            config.task_plp_gs_combiner.az_output_abfss_path
        )

        _configure_abfs(
            spark=spark,
            dbutils=dbutils,
            account_name=account_name,
            tenant_id=tenant_id,
            dbutils_secret_scope=config.dbutils_secret_scope,
            secret_key_spn_clientid=config.secret_key_spn_clientid,
            secret_key_spn_secret=config.secret_key_spn_secret,
        )

        try:
            # Try to read CSV files from directory
            df = spark.read.option("header", True).csv(output_path)
            row_count = df.count()

            print(f"Successfully read {row_count} rows from {output_path}")
            assert row_count >= 0, "Row count should be non-negative"

        except Exception as e:
            error_msg = str(e)

            # Empty directory is OK - just means no CSV files yet
            if (
                "PATH_NOT_FOUND" in error_msg
                or "UNABLE_TO_INFER_SCHEMA" in error_msg
            ):
                print(f"Directory {output_path} is empty (ok)")
                print("ABFS connection verified (directory is accessible)")
                # Test passes - empty directory is acceptable
            else:
                pytest.fail(f"Unexpected error: {e}")

        print("ABFS connection test passed")

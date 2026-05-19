import pytest


class TestLoadConfigIntegration:
    """Integration tests (if config files exist)."""

    @pytest.mark.config_integration
    def test_load_config_dev(self, config_dev):
        """Test loading real configuration files for dev."""
        config = config_dev

        # Basic assertions that config is loaded
        assert config is not None
        assert config.schema_read is not None
        assert config.catalog_read is not None
        assert config.catalog_write is not None
        assert config.schema_write is not None

        assert config.az_st_account is not None
        assert config.az_st_account_url is not None
        assert config.dbutils_secret_scope is not None

    @pytest.mark.config_integration
    def test_load_config_prod(self, config_prod):
        """Test loading real configuration files for prod."""
        config = config_prod

        assert config is not None
        assert config.schema_read is not None
        assert config.catalog_read is not None
        assert config.catalog_write is not None
        assert config.schema_write is not None

        assert config.az_st_account is not None
        assert config.az_st_account_url is not None
        assert config.dbutils_secret_scope is not None

    @pytest.mark.config_integration
    def test_load_config_global_solution(self, config_prod):
        """Test loading real configuration files for prod."""
        config = config_prod

        assert config is not None
        assert config.task_plp_gs_per_client is not None
        assert config.task_plp_gs_combiner is not None

        assert config.tables_write.nextads_plp_gs is not None
        assert config.tables_write.nextads_plp_gs_latest is not None

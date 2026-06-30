import os
import pytest
from unittest.mock import patch
from next_ads.common import config_manager as new_config_manager
from next_ads.common import paths as common_paths
from next_ads.common.config_manager import load_config as new_load_config
from next_ads.utils.config_manager import load_config


@pytest.fixture
def clean_env():
    """Fixture to clean up environment variables before and after each test."""
    # Store original env vars
    original_user_schema = os.environ.get("USER_SCHEMA")
    original_job_env = os.environ.get("JOB_ENV")

    yield

    # Restore original env vars after test
    if original_user_schema is not None:
        os.environ["USER_SCHEMA"] = original_user_schema
    else:
        os.environ.pop("USER_SCHEMA", None)

    if original_job_env is not None:
        os.environ["JOB_ENV"] = original_job_env
    else:
        os.environ.pop("JOB_ENV", None)


@pytest.fixture
def mock_dotenv(monkeypatch):
    """Mock load_dotenv to prevent loading actual .env.local file during tests."""
    with patch("next_ads.common.config_manager.load_dotenv"):
        yield


def test_old_and_new_config_import_paths_match():
    assert load_config is new_load_config


def test_config_paths_prefer_current_config_folder():
    settings_files = new_config_manager._settings_files()

    assert "configs/runtime/settings.yaml" in settings_files
    assert "configs/adsv2/load_control_sheet_v2_settings.yaml" in settings_files
    assert "configs/runtime/tables_settings.yaml" in settings_files
    assert "adsv2/load_control_sheet_v2_settings.yaml" not in settings_files
    assert "adsv2/tables_settings.yaml" not in settings_files


def test_config_paths_fall_back_to_legacy_config_folder(monkeypatch, tmp_path):
    monkeypatch.setattr(new_config_manager, "PROJECT_ROOT", tmp_path)

    settings_files = new_config_manager._settings_files()

    assert "config/settings.yaml" in settings_files
    assert "config/tables_settings.yaml" in settings_files
    assert "config/load_control_sheet_v2_settings.yaml" in settings_files


def test_client_config_path_prefers_configs_clients_folder():
    path = common_paths.resolve_client_config_path("next_uk")

    assert path.as_posix().endswith("configs/clients/next_uk.json")


def test_sql_contract_resolver_finds_grouped_sql_files():
    assert common_paths.resolve_sql_contract_path(
        "control_sheet_raw"
    ).as_posix().endswith(
        "sql/control/create_table_control_sheet_raw.sql"
    )
    assert common_paths.resolve_sql_contract_path(
        "results_topline"
    ).as_posix().endswith(
        "sql/reporting/create_table_results_topline.sql"
    )
    assert common_paths.resolve_sql_contract_path(
        "viewed_bought_latest"
    ).as_posix().endswith(
        "sql/realtime/create_table_viewed_bought_latest.sql"
    )


def test_configured_write_tables_have_sql_contracts(clean_env, mock_dotenv):
    os.environ["JOB_ENV"] = "dev"
    config = load_config("dev")
    table_refs = _extract_table_refs(config.tables_write.to_dict())

    assert table_refs
    missing_contracts = [
        table_ref
        for table_ref in table_refs
        if not common_paths.resolve_sql_contract_path(table_ref).exists()
    ]

    assert missing_contracts == []


def _extract_table_refs(value, parent_key=""):
    if isinstance(value, dict):
        refs = []
        for key, child in value.items():
            full_key = f"{parent_key}.{key}" if parent_key else key
            refs.extend(_extract_table_refs(child, full_key))
        return refs
    if isinstance(value, str):
        return [parent_key]
    return []


class TestLoadConfigSchemaWrite:
    """Test schema_write configuration based on USER_SCHEMA environment variable."""

    def test_user_schema_env_var_preserved_when_set(
        self, clean_env, mock_dotenv
    ):
        """Test that existing USER_SCHEMA environment variable is preserved."""
        # Arrange
        test_user = "user_preset"
        os.environ["USER_SCHEMA"] = test_user

        # Act
        load_config("dev")

        # Assert
        assert os.environ["USER_SCHEMA"] == test_user, (
            f"Expected USER_SCHEMA to be preserved as '{test_user}', got {os.environ['USER_SCHEMA']}"
        )

    def test_schema_write_with_user_schema_env_var_dev(
        self, clean_env, mock_dotenv
    ):
        """Test that schema_write is set correctly when USER_SCHEMA env var is provided in dev environment."""
        # Arrange
        os.environ.pop("USER_SCHEMA", None)  # Ensure it's not set
        os.environ["USER_SCHEMA"] = "user_schema_a"
        os.environ["JOB_ENV"] = "dev"

        # Act
        config = load_config("dev")

        # Assert
        assert config.schema_write == "user_schema_a", (
            f"Expected schema_write to be 'user_schema_a', got {config.schema_write}"
        )

    def test_schema_write_without_user_schema_env_var_dev(
        self, clean_env, mock_dotenv
    ):
        """Test that schema_write defaults to 'ds_sandbox' when USER_SCHEMA env var is not set in dev environment."""
        # Arrange
        os.environ.pop("USER_SCHEMA", None)  # Ensure it's not set
        os.environ["JOB_ENV"] = "dev"

        # Act
        config = load_config("dev")

        # Assert
        assert config.schema_write == "ds_sandbox", (
            f"Expected schema_write to be 'ds_sandbox', got {config.schema_write}"
        )

    def test_schema_write_with_user_schema_env_var_prod(
        self, clean_env, mock_dotenv
    ):
        """Test that schema_write is 'warehouse' in prod environment regardless of USER_SCHEMA."""
        # Arrange
        os.environ["USER_SCHEMA"] = "user_schema_a"
        os.environ["JOB_ENV"] = "prod"

        # Act
        config = load_config("prod")

        # Assert
        assert config.schema_write == "warehouse", (
            f"Expected schema_write to be 'warehouse' in prod, got {config.schema_write}"
        )

    def test_schema_write_without_user_schema_env_var_prod(
        self, clean_env, mock_dotenv
    ):
        """Test that schema_write is 'warehouse' in prod environment even when USER_SCHEMA is not set."""
        # Arrange
        os.environ.pop("USER_SCHEMA", None)  # Ensure it's not set
        os.environ["JOB_ENV"] = "prod"

        # Act
        config = load_config("prod")

        # Assert
        assert config.schema_write == "warehouse", (
            f"Expected schema_write to be 'warehouse' in prod, got {config.schema_write}"
        )

    def test_schema_write_preprod(self, clean_env, mock_dotenv):
        """Test that schema_write is correct in preprod environment."""
        # Arrange
        os.environ["USER_SCHEMA"] = "test_user"
        os.environ["JOB_ENV"] = "preprod"

        # Act
        config = load_config("preprod")

        # Assert
        assert config.schema_write == "ds_sandbox", (
            f"Expected schema_write to be 'ds_sandbox' in preprod, got {config.schema_write}"
        )

    def test_catalog_write_dev(self, clean_env, mock_dotenv):
        """Test that catalog_write is marketingdata_dev in dev environment."""
        # Arrange
        os.environ["JOB_ENV"] = "dev"

        # Act
        config = load_config("dev")

        # Assert
        assert config.catalog_write == "marketingdata_dev", (
            f"Expected catalog_write to be 'marketingdata_dev', got {config.catalog_write}"
        )

    def test_catalog_write_prod(self, clean_env, mock_dotenv):
        """Test that catalog_write is marketingdata_prod in prod environment."""
        # Arrange
        os.environ["JOB_ENV"] = "prod"

        # Act
        config = load_config("prod")

        # Assert
        assert config.catalog_write == "marketingdata_prod", (
            f"Expected catalog_write to be 'marketingdata_prod', got {config.catalog_write}"
        )

    def test_catalog_read_always_prod(self, clean_env, mock_dotenv):
        """Test that catalog_read is always marketingdata_prod across all environments."""
        # Arrange
        environments = ["dev", "preprod", "prod"]

        # Act & Assert
        for env in environments:
            config = load_config(env)
            assert config.catalog_read == "marketingdata_prod", (
                f"Expected catalog_read to be 'marketingdata_prod' in {env}, got {config.catalog_read}"
            )

    def test_schema_read_is_warehouse(self, clean_env, mock_dotenv):
        """Test that schema_read is always 'warehouse' across all environments."""
        # Arrange
        environments = ["dev", "preprod", "prod"]

        # Act & Assert
        for env in environments:
            config = load_config(env)
            assert config.schema_read == "warehouse", (
                f"Expected schema_read to be 'warehouse' in {env}, got {config.schema_read}"
            )

    def test_full_table_path_dev_with_user_schema(
        self, clean_env, mock_dotenv
    ):
        """Test full table path resolution in dev with user schema."""
        # Arrange
        os.environ["USER_SCHEMA"] = "user_schema_a"
        os.environ["JOB_ENV"] = "dev"

        # Act
        config = load_config("dev")
        full_path = f"{config.catalog_write}.{config.schema_write}.{config.client}_nextads_ad_items"

        # Assert
        expected_path = (
            "marketingdata_dev.user_schema_a.next_uk_nextads_ad_items"
        )
        assert full_path == expected_path, (
            f"Expected '{expected_path}', got '{full_path}'"
        )

    def test_full_table_path_prod(self, clean_env, mock_dotenv):
        """Test full table path resolution in prod."""
        # Arrange
        os.environ["JOB_ENV"] = "prod"

        # Act
        config = load_config("prod")
        full_path = f"{config.catalog_write}.{config.schema_write}.{config.client}_nextads_ad_items"

        # Assert
        expected_path = "marketingdata_prod.warehouse.next_uk_nextads_ad_items"
        assert full_path == expected_path, (
            f"Expected '{expected_path}', got '{full_path}'"
        )

import os
import pytest
from dsutils.dbc import configure_spark, get_dbutils
from next_ads.utils.config_manager import load_config


@pytest.fixture(scope="session", autouse=True)
def disable_env_local_in_tests():
    """
    Disable .env.local loading during tests to ensure deterministic config.
    
    Temporarily remove .env.local from the filesystem or set env vars
    to prevent Dynaconf from picking up local overrides.
    """
    # Set DYNACONF_SKIP_ENV to prevent Dynaconf from loading environment files
    os.environ["DYNACONF_SKIP_ENV"] = "true"
    yield
    # Cleanup: remove the override after tests
    if "DYNACONF_SKIP_ENV" in os.environ:
        del os.environ["DYNACONF_SKIP_ENV"]


@pytest.fixture(scope="session")
def spark():
    """
    Create a Spark session for testing.
    
    Scope is 'session' so Spark is created once for all tests.
    """
    spark = configure_spark()
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def dbutils():
    """
    Create a DBUtils instance for testing.
    
    Scope is 'session' so DBUtils is created once for all tests.
    """
    dbutils = get_dbutils()
    yield dbutils


@pytest.fixture(scope="session")
def config_dev():
    """
    Load dev config with .env.local disabled.
    
    Scope is 'session' so config is loaded once for all tests.
    """
    return load_config("dev")

@pytest.fixture(scope="session")
def config_preprod():
    """
    Load preprod config with .env.local disabled.
    
    Scope is 'session' so config is loaded once for all tests.
    """
    return load_config("preprod")

@pytest.fixture(scope="session")
def config_prod():
    """
    Load prod config with .env.local disabled.
    
    Scope is 'session' so config is loaded once for all tests.
    """
    return load_config("prod")
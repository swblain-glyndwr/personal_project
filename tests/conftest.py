import pytest
from dsutils.dbc import configure_spark, get_dbutils
from next_ads.utils.config_manager import load_config


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
    Create a Spark session for testing.
    
    Scope is 'session' so Spark is created once for all tests.
    """
    return load_config("dev")

@pytest.fixture(scope="session")
def config_preprod():
    """
    Create a Spark session for testing.
    
    Scope is 'session' so Spark is created once for all tests.
    """
    return load_config("preprod")

@pytest.fixture(scope="session")
def config_prod():
    """
    Create a Spark session for testing.
    
    Scope is 'session' so Spark is created once for all tests.
    """
    return load_config("prod")
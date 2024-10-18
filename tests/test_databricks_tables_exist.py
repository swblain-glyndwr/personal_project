import json
import pytest
from pyspark.sql import SparkSession


# Read table references
with open("../next_ads/config/resources.json") as f:
    rsc = json.load(f)

# TODO: Refactor resources.json so line below isn't necessary
TABLES_READ = rsc["tables"]["read"]
TABLES_WRITE = rsc["tables"]["read"]


@pytest.fixture
def spark() -> SparkSession:
    """
    Create a SparkSession (the entry point to Spark functionality) on
    # the cluster in the remote Databricks workspace. Unit tests do not
    # have access to this SparkSession by default.
    """
    return SparkSession.builder.getOrCreate()


def test_tables_exist(spark):
    for table in TABLES_READ:
        assert spark.catalog.tableExists(TABLES_READ[table])


# Test negative case
# def test_tables_exist2(spark):
#     for table in TABLES:
#         assert not spark.catalog.tableExists(TABLES[table])

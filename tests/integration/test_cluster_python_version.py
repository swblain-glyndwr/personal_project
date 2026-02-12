from pyspark.sql.types import StringType


def test_python_version(spark):
    import sys
    from pyspark.sql.functions import udf

    # Check driver Python
    print(f"Driver Python: {sys.version}")
    print(f"Driver executable: {sys.executable}")

    # Use UDF to get worker Python version (Spark Connect compatible)
    @udf(returnType=StringType())
    def get_worker_python_version():
        import sys
        return f"{sys.version_info.major}.{sys.version_info.minor}"
    
    # Create DataFrame with UDF
    worker_version = (
        spark.range(1)
        .select(get_worker_python_version().alias("worker_version"))
        .collect()[0]["worker_version"]
    )
    
    # Get driver version
    driver_major_minor = f"{sys.version_info.major}.{sys.version_info.minor}"
    print(f"Driver Python: {driver_major_minor}")
    print(f"Worker Python: {worker_version}")
    
    # Assert Databricks 15.4 uses Python 3.11
    assert worker_version == "3.11", (
        f"Databricks 15.4 LTS should use Python 3.11, "
        f"got {worker_version}. Check cluster configuration."
    )
    
    # Assert driver and worker match
    assert driver_major_minor == worker_version, (
        f"Driver ({driver_major_minor}) and Worker ({worker_version}) "
        f"Python versions must match"
    )
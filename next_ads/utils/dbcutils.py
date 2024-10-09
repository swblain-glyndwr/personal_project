import os


def get_spark():
    """
    Get `spark` (for the active `SparkSession`). Enables code compatibility
    across Databricks browser and DatabricksConnect session
    (e.g. via VS Code).

    Usage:
        spark.table(...)
            - Works via Databricks browser, but not via Databricks Connect
        get_spark().table(...)
            - Works via Databricks browser and Databricks connect
    """
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession()


def get_dbutils():
    """
    Gets `dbutils` for the active `SparkSession`; enables code compatibility
    across Databricks browser and DatabricksConnect session
    (e.g. via VS Code).

    Usage:
        dbutils.secrets.get(...)
            - Works via Databricks browser, but not via Databricks Connect
        get_dbutils().secrets.get(...)
            - Works via Databricks browser and Databricks Connect
    """
    if "VSCODE_PID" in os.environ.keys():
        from pyspark.dbutils import DBUtils
        return DBUtils(get_spark())
    else:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]


def get_display(df):
    """
    Gets `display` for the active `SparkSession`; enables code compatibility
    across Databricks browser and DatabricksConnect session
    (e.g. via VS Code).

    Usage:
        df.display()
            - Works via Databricks browser, but not via Databricks Connect
        get_display(df)
            - Works via Databricks browser and Databricks Connect
    """
    if "VSCODE_PID" in os.environ.keys():
        from IPython.display import display
        return display(df)
    else:
        return df.display()

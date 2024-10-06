import os


def get_spark():
    """
    Function gets Spark session.
    Enables compatibility with DatabricksConnect sessions.
    Usage:
    get_spark().table("schema.tablename")
    is equivalent to
    spark.table("schema.tablename")
    but enables common syntax across Databricks browser IDE
    and other IDE (e.g. VS Code).
    """
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession()


def get_dbutils():
    """
    Function gets dbutils for the active spark session.
    Enables compatibility with DatabricksConnect sessions.
    Usage:
    get_dbutils.secrets.get(...)
    is equivalent to
    dbutils.secrets.get(...)
    but enables common syntax across Databricks browser IDE
    and other IDE (e.g. VS Code).
    """
    if "VSCODE_PID" in os.environ.keys():
        from pyspark.dbutils import DBUtils
        return DBUtils(get_spark())
    else:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]


def get_display(df):
    """
    Function gets dbutils for the active spark session.
    Enables compatibility with DatabricksConnect sessions.
    Usage:
    get_display(df)
    is equivalent to
    display(df) or df.display()
    but enables common syntax across Databricks browser IDE
    and other IDE (e.g. VS Code).
    """
    if "VSCODE_PID" in os.environ.keys():
        from IPython.display import display
        return display(df)
    else:
        return df.display()

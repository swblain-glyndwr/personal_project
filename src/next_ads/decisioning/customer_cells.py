import pyspark.sql.functions as F


def ensure_audience_column(df, *, default_value: str = "false"):
    """Ensure customer cells contain a non-null string Audience split column."""
    if "Audience" not in df.columns:
        return df.withColumn("Audience", F.lit(default_value))

    return df.withColumn(
        "Audience",
        F.coalesce(F.col("Audience").cast("string"), F.lit(default_value)),
    )

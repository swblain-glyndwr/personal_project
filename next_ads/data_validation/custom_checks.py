from pandera.extensions import register_check_method
from pyspark.sql import Column
from pyspark.sql.functions import col, lit, regexp_like
from dsutils.logtools import get_logger


logger = get_logger(__name__)


# Register custom Spark string checks
@register_check_method
def str_matches_spark(pyspark_obj, *, pattern: str, **kwargs) -> Column:
    """
    Check that string column matches regex pattern in PySpark.

    Returns True if ALL values match, False if ANY fail.
    """
    # Get the DataFrame and column name from the pyspark_obj
    df = pyspark_obj.dataframe
    col_name = pyspark_obj.column_name

    condition = regexp_like(col(col_name), lit(pattern))
    failed_df = df.filter(~condition)
    failed_count = failed_df.count()

    if failed_count > 0:
        logger.error(
            f"{col_name} pattern validation failed for {failed_count} rows"
        )
        logger.error(f"Pattern: {pattern}")
        logger.error(
            f"\nOffending values in '{col_name}' (pattern: {pattern}):"
        )
        failed_df.select(col_name).show(n=10, truncate=False)

    return failed_count == 0


@register_check_method
def isin_spark(pyspark_obj, *, allowed_values: list, **kwargs) -> Column:
    """
    Check that column values are in allowed set in PySpark.

    Returns True if ALL values are in the set, False otherwise.
    """
    df = pyspark_obj.dataframe
    col_name = pyspark_obj.column_name

    # Create a condition for values in the allowed list
    condition = col(col_name).isin(allowed_values)

    # Count rows that are NOT in the list
    failed_df = df.filter(~condition)
    failed_count = failed_df.count()

    if failed_count > 0:
        logger.error(
            f"{col_name} value validation failed for {failed_count} rows"
        )
        logger.error(f"Allowed values: {allowed_values}")
        logger.error(
            f"\nOffending values in '{col_name}' (not in allowed list):"
        )
        failed_df.select(col_name).distinct().show(n=10, truncate=False)

    return failed_count == 0

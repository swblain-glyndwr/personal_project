from pandera.extensions import register_check_method
from pyspark.sql import Column
from pyspark.sql.functions import col, lit, regexp_like, count
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


@register_check_method
def unique_spark(pyspark_obj, *, check: bool, **kwargs) -> Column:
    """
    Checks that column values are unique (no duplicates) in PySpark.

    Returns True if ALL values are unique, False if ANY duplicates found.
    Logs all duplicate values for debugging.
    """
    if check:
        df = pyspark_obj.dataframe
        col_name = pyspark_obj.column_name

        # Count occurrences of each value
        duplicate_df = (
            df.groupBy(col_name)
            .agg(count("*").alias("occurrence_count"))
            .filter(col("occurrence_count") > 1)
            .orderBy(col("occurrence_count").desc())
        )

        duplicate_count = duplicate_df.count()

        if duplicate_count > 0:
            total_duplicate_rows = duplicate_df.select(
                col("occurrence_count")
            ).collect()
            total_rows_with_duplicates = sum(
                row["occurrence_count"] for row in total_duplicate_rows
            )

            logger.error(
                f"{col_name} uniqueness validation failed: "
                f"found {duplicate_count} duplicate value(s) in {total_rows_with_duplicates} rows"
            )
            logger.error(f"\nDuplicate values in '{col_name}':")
            duplicate_df.show(n=20, truncate=False)

            return False

        logger.info(
            f"{col_name} uniqueness check passed - all values are unique"
        )
        return True
    else:
        return True

from pyspark.sql import Column, Window
from pyspark.sql import functions as F


def subtract_mean(
        column: Column,
        partition_by: str = "") -> Column:
    """
    Subtracts column mean from column by optional partition.

    Arguments:
        column -- PySpark `Column` to process
        window -- PySpark `Window` over which to standardise

    Returns:
        PySpark `Column` with scaled values
    """
    if partition_by:
        w = Window.partitionBy(F.col(partition_by))
    else:
        w = Window.partitionBy(F.lit(1))

    new_column = (column - F.mean(column).over(w))

    return new_column


def z_score(
        column: Column,
        partition_by: str = "") -> Column:
    """
    Z-Score of column by optional partition.
    Z = (x-mean(x))/std(x)

    Arguments:
        column -- PySpark `Column` to process
        window -- PySpark `Window` over which to standardise

    Returns:
        PySpark `Column` with scaled values
    """
    if partition_by:
        w = Window.partitionBy(F.col(partition_by))
    else:
        w = Window.partitionBy(F.lit(1))

    new_column = (subtract_mean(column, partition_by)/F.std(column).over(w))

    return new_column

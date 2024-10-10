from pyspark.sql.types import StructField, StructType
from pyspark.sql import DataFrame
import sys
from utils.dbcutils import get_spark


def build_spark_field(
        name: str,
        dtype: str,
        nullable_str: str
        ) -> StructField:
    """
    Builds Spark StructField from agnostic input.

    Arguments:
        name {str} -- Name of field
        dtype {str} -- Type of column (only 'string' currently supported)
        nullable_str {str} -- Nullable status of field

    """
    spark_types = {
        "string": "StringType"
    }

    spark_type = getattr(
        sys.modules["pyspark.sql.types"],
        spark_types[dtype]
        )

    nullable_bool = False if nullable_str != "nullable" else True

    return StructField(name, spark_type(), nullable_bool)


def build_spark_schema(schema: list) -> StructType:
    """
    Builds Spark StructType object - requires `build_spark_fields`.

    Arguments:
        schema {list} -- List of list of strings
        e.g. `[["ID","string","nullable"],["Name","string","nullable"]]`

    """
    fields = [build_spark_field(*c) for c in schema]

    return StructType(fields)


def delete_from_and_load(
        df: DataFrame,
        table: str,
        del_where: dict = dict()) -> None:
    """
    Deletes from table where `rundate == current_date()` then
    inserts df into table with `rundate = current_date()`

    Arguments:
        df {DataFrame} -- PySpark dataframe to load
        table {str} -- Table to load into
        del_where {dict} -- col,val pairs to delete before insert
            e.g. {"rundate": "current_date()", "Location": "'HN1'"}
            appends "and Location = 'HN1' and rundate = current_date()"
            to the delete clause
    """
    df.createOrReplaceTempView("df_load")

    query_del = (
        f"""
        delete from {table}
        where 1 = 1
        """
        )

    if del_where:
        for k in del_where.keys():
            query_del = query_del + f"and {k} = {del_where[k]}"

    get_spark().sql(query_del)

    get_spark().sql(
        f"""
        insert into {table}
        select *, current_date() as rundate
        from df_load
        """
        )

    return None


def truncate_and_load(df: DataFrame, table: str) -> None:
    """
    Truncates table and then
    inserts df into table with `rundate = current_date()`

    Arguments:
        df {DataFrame} - PySpark dataframe to load
        table {str} - Table to load into
    """
    df.createOrReplaceTempView("df_load")

    get_spark().sql(
        f'''
        truncate table {table}
        ''')

    get_spark().sql(
        f'''
        insert into {table}
        select *, current_date() as rundate
        from df_load
        '''
        )

    return None

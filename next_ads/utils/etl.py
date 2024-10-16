from pyspark.sql.types import StructField, StructType
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import sys
from next_ads.utils.dbc import get_spark


def build_spark_field(
        name: str,
        dtype: str,
        null_str: str = "null"
        ) -> StructField:
    """
    Builds Spark StructField from agnostic input.

    Arguments:
        name {str} -- Name of field
        dtype {str} -- Type of column
        (currently supported: `"string"`, `"int"`, `"float"`)
        null_str {str} -- Optional - Nullable status of field
        (`"not null"` yields `nullable=False`; `nullable=True` by default)
    """
    spark_types = {
        "string": "StringType",
        "int": "IntegerType",
        "float": "FloatType"
    }

    spark_type = getattr(
        sys.modules["pyspark.sql.types"],
        spark_types[dtype]
        )

    nullable_bool = True
    if null_str.lower() == "not null":
        nullable_bool = False

    return StructField(name, spark_type(), nullable_bool)


def build_spark_schema(schema: list) -> StructType:
    """
    Builds Spark StructType object - requires `build_spark_fields`.

    Arguments:
        schema {list} -- List of list of strings
        e.g. `[["ID","string","not null"],["Name","string","null"]]`

    """
    fields = [build_spark_field(*c) for c in schema]

    return StructType(fields)


def assert_pk(df: DataFrame, pk_cols: list):
    """
    Assert Primary Key constraint on dataframe.
    Primary Key constraints only enforced from
    Databricks Runtime 15.2 and Databricks SQL 2024.30.
    Prior to this, PK constraints for information only.

    Arguments:
        df -- Dataframe to check
        pk_cols -- List of Primary Key columns

    Raises:
        AssertionError -- If PK constraint is violated (dups, null)
    """
    df_pk = df.select(*pk_cols)
    n = df_pk.count()
    nd = df_pk.drop_duplicates().count()
    assert n == nd, f"Duplicates found in Primary Key {pk_cols}"
    for c in pk_cols:
        null_count = df_pk.where(F.col(c).isNull()).count()
        assert null_count == 0, f"Null values found in PK col: {c}"


def delete_from_and_load(
        df: DataFrame,
        table: str,
        pk_cols: list = [],
        del_where: dict = dict()) -> None:
    """
    Deletes from table where `rundate == current_date()` then
    inserts df into table with `rundate = current_date()`

    Arguments:
        df {DataFrame} -- PySpark dataframe to load
        table {str} -- Table to load into
        pk_cols {list} -- Optional - Primary Key cols for assert_pk()
        del_where {dict} -- col,val pairs to delete before insert
            e.g. {"rundate": "current_date()", "Location": "'HN1'"}
            appends "and Location = 'HN1' and rundate = current_date()"
            to the delete clause
    """
    if pk_cols:
        assert_pk(df, pk_cols)

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


def truncate_and_load(
        df: DataFrame,
        table: str,
        pk_cols: list = []) -> None:
    """
    Truncates table and then
    inserts df into table with `rundate = current_date()`

    Arguments:
        df {DataFrame} - PySpark dataframe to load
        table {str} - Table to load into
        pk_cols {list} -- Optional - Primary Key cols for assert_pk()
    """
    if pk_cols:
        assert_pk(df, pk_cols)

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


def create_or_replace(
        df: DataFrame,
        table: str,
        pk_cols: list = []) -> None:
    """
    Drops table (if exists), then creates table and loads as
    select * from df (with `rundate = current_date()`) appended

    Arguments:
        df {DataFrame} - PySpark dataframe to load
        table {str} - Table to load into
        pk_cols {list} -- Optional - Primary Key cols for assert_pk()
    """
    if pk_cols:
        assert_pk(df, pk_cols)

    df.createOrReplaceTempView("df_load")

    get_spark().sql(
        f'''
        drop table if exists {table}
        ''')

    get_spark().sql(
        f'''
        create table {table} as
        select *, current_date() as rundate
        from df_load
        '''
        )

    if pk_cols:
        get_spark().sql(
            f'''
            alter table {table}
            add constraint pk_{"_".join(pk_cols).lower()}
            primary key ({",".join(pk_cols)});
            '''
        )

    return None


def count_null_by_col(df: DataFrame) -> DataFrame:
    """
    Counts nulls in dataframe by column.

    Arguments:
        df -- Dataframe

    Returns:
        dataframe with same column names, each containing null count
    """
    df_n = (
        df.select(
            [F.count(F.when(F.col(c).isNull(), c)).alias(c)
             for c in df.columns]
            )
    )

    return df_n

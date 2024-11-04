from pyspark.sql.types import StructField, StructType
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import sys
from next_ads.utils.dbc import get_spark
from argparse import ArgumentParser


class JobParser(ArgumentParser):
    """
    Child class of argparse.Argument parser designed to parse
    Databricks job parameters
    """
    def __init__(self):
        super().__init__()

    def parse_job_args(self, job_arg_list: list) -> dict:
        """
        Arguments:
            List of option strings to parse e.g. ["--jobname", "--location"]

        Returns:
            Dictionary of parsed (known) arguments
        """
        known_arg_list = [
            "--f",
            "--jobname",
            "--location",
            "--macrolocation"
            ]

        job_args = [j for j in job_arg_list if j in known_arg_list]

        self.add_argument("--f", help="dummy arg for interactive debugging")

        if "--jobname" in job_args:
            self.add_argument("--jobname",
                              nargs="?", const="dev_", type=str)

        if "--location" in job_args:
            self.add_argument("--location",
                              nargs="?", const="HN1", type=str)

        if "--macrolocation" in job_args:
            self.add_argument("--macrolocation",
                              nargs="?", const="HN", type=str)

        known_args, _ = self.parse_known_args()
        pargs = vars(known_args)

        if not pargs["jobname"]:
            job_env = "dev"
        elif pargs["jobname"].startswith("dev_"):
            job_env = "dev"
        else:
            job_env = "prod"

        return pargs, job_env


def map_schema(s: str, schema) -> str:
    return s.format_map({"schema": schema})


def build_spark_field(
        name: str,
        dtype: str,
        null_str: str = "null"
        ) -> StructField:
    """
    Builds Spark StructField from agnostic input.

    Arguments:
        name - Name of field
        dtype - Column data type
        (currently supported: `"string"`, `"int"`, `"float"`)
        null_str - Nullable status of field ("not null" for nullable=False)
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


def build_spark_schema(schema: list[list[str]]) -> StructType:
    """
    Builds Spark StructType object - requires `build_spark_fields`.

    Arguments:
        schema - List of list of strings
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
        df - Dataframe to check
        pk_cols - List of Primary Key columns
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
        pk_cols: list[str] = [],
        del_where: dict = {}) -> None:
    """
    Deletes from table where `rundate` is current date, then inserts df
    with `rundate` as current date.

    Arguments:
        df - PySpark dataframe to load
        table - Table to delete from and load into
        pk_cols - Primary Key columns
        del_where - Also delete where key = value before load
            e.g. {"a": "'b'"} appends and "and a = 'b'
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
        pk_cols: list[str] = []) -> None:
    """
    Truncates table and then inserts df into table with
    `rundate` as current date

    Arguments:
        df - PySpark dataframe to load
        table - Table to truncate and load into
        pk_cols - Primary Key columns
    """

    if pk_cols:
        assert_pk(df, pk_cols)

    df.createOrReplaceTempView("df_load")

    get_spark().sql(
        f"""
        truncate table {table}
        """)

    get_spark().sql(
        f"""
        insert into {table}
        select *, current_date() as rundate
        from df_load
        """
        )

    return None


def count_null_by_column(df: DataFrame) -> DataFrame:
    """
    Counts nulls in Spark dataframe by column.
    """
    df_n = (
        df.select(
            [F.count(F.when(F.col(c).isNull(), c)).alias(c)
             for c in df.columns]
            )
    )

    return df_n

import functools
import operator
from random import randint
from time import sleep
from pyspark.sql.types import StructField, StructType
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import sys
import requests
from next_ads.utils.dbc import get_spark
from argparse import ArgumentParser
from delta.exceptions import ConcurrentAppendException
from delta.tables import DeltaTable


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
            "--droptables"
            ]

        job_args = [j for j in job_arg_list if j in known_arg_list]

        self.add_argument("--f", help="dummy arg for interactive debugging")

        if "--jobname" in job_args:
            self.add_argument("--jobname",
                              nargs="?", const="dev_", type=str)

        if "--location" in job_args:
            self.add_argument("--location",
                              nargs="?", const="HN1", type=str)

        if "--droptables" in job_args:
            self.add_argument("--droptables",
                              nargs="?", const="False", type=str)

        known_args, _ = self.parse_known_args()
        pargs = vars(known_args)

        if not pargs["jobname"]:
            job_env = "dev"
        elif pargs["jobname"].startswith("dev_"):
            job_env = "dev"
        else:
            job_env = "prod"

        return pargs, job_env


def chain_when_and(whens: list[dict]):
    """
    Chains PySpark when conditions together with "and" operator.

    Arguments:
        whens - List of dictionaries, each with "col", "op", "val" keys
        (i.e. column, operator, value)

    e.g.
    ```
    [{"col": "colname", "op": "eq", "val": "value"},
    {"col": "colname2", "op": "le", "val": 0.5}]
    ```

    The above would return the equivalent of:
    `(F.col("colname") == "value") & (F.col("colname2") <= 0.5)`
    """
    ops = sys.modules["operator"]
    wl = [getattr(ops, w["op"])(F.col(w["col"]), w["val"]) for w in whens]
    return functools.reduce(operator.and_, wl)


def chain_when_thens(when_thens: list):
    """
    Chains when-then PySpark conditions together.

    Arguments:
        when_thens - List of dictionaries of the form:
        N.B. "then" dict should have either `col` or `lit` key, depending on
        whether the "then" is a column reference of literal.
    ```
    [{
        "when": [{"col": "colname", "op": "eq", "val": "value"},
                    {"col": "colname2", "op": "le", "val": 0.5}],
        "then": {"lit": "a"}
    },
    {
        "when": [{"col": "colname3", "op": "eq", "val": "othervalue"}],
        "then": {"col": "colname4"}
    }]
    ```

    The above would return the equivalent of:
    ```
    F.when((F.col("colname") == "value") & (F.col("colname2") <= 0.5), "a"))
     .when(F.col("colname3") == "othervalue", F.col("colname4"))
    ```
    """
    wt0 = when_thens[0]
    whens = wt0["when"]
    if "col" in wt0["then"]:
        then = F.col(wt0["then"]["col"])
    else:
        then = F.lit(wt0["then"]["lit"])
    cond = F.when(chain_when_and(whens), then)
    if len(when_thens) == 1:
        return cond
    else:
        for wt in when_thens[1:]:
            whens = wt["when"]
            if "col" in wt["then"]:
                then = F.col(wt["then"]["col"])
            else:
                then = F.lit(wt["then"]["lit"])
            cond = cond.when(chain_when_and(whens), then)
        return cond


def map_schema(s: str, schema) -> str:
    return s.format_map({"schema": schema})


def post_to_webhook(webhook_url: str, message: str) -> None:
    requests.post(webhook_url, json={"text": message})
    return None


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


def table_housekeeping(
        table: str,
        vacuum_hours: int = 168,
        retention_days: int = 731) -> None:

    get_spark().sql(
        f"""
        delete from {table}
        where rundate <= current_date() - {retention_days}
        """)

    get_spark().sql(f"optimize {table}")
    get_spark().sql(f"vacuum {table} retain {vacuum_hours} hours")


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

    i = 0
    max_attempts = 5
    while True:
        i += 1
        if i > max_attempts:
            raise Exception("Max attempts exceded")
        try:
            get_spark().sql(query_del)
            get_spark().sql(
                f"""
                insert into {table}
                select *, current_date() as rundate
                from df_load
                """
                )
            break
        except ConcurrentAppendException:
            print("ConcurrentAppendException encountered during table load")
            wait_seconds = randint(30, 90)
            print(f"Waiting {wait_seconds} seconds before retrying")
            sleep(wait_seconds)

    table_housekeeping(table)

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

    i = 0
    max_attempts = 5
    while True:
        i += 1
        if i > max_attempts:
            raise Exception("Max attempts exceded")
        try:
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
            break
        except ConcurrentAppendException:
            print("ConcurrentAppendException encountered during table load")
            wait_seconds = randint(30, 90)
            print(f"Waiting {wait_seconds} seconds before retrying")
            sleep(wait_seconds)

    table_housekeeping(table)

    return None


def create_table_from_df(
        df: DataFrame,
        table: str,
        partitioned_by: list[str],
        pk_cols: list[str] = [],
        drop_if_exists: bool = False) -> None:

    if pk_cols:
        assert_pk(df, pk_cols)

    if get_spark().catalog.tableExists(table):
        if drop_if_exists:
            get_spark().sql(f"drop table {table}")
        else:
            msg = f"Table {table} exists (and drop_if_exists set to False)"
            raise Exception(msg)

    df.createOrReplaceTempView("df_create")

    get_spark().sql(f"""
                    create table {table} as
                    select *, current_date() as rundate
                    from df_create""")

    get_spark().sql(
        f"""
        replace table {table}
        partitioned by ({','.join(partitioned_by)})
        as select * from {table}
        """
    )

    for pk_col in pk_cols:
        get_spark().sql(
            f"alter table {table} alter column {pk_col} set not null")

    table_name = table.split(".")[-1]
    get_spark().sql(
        f"""alter table {table} add constraint pk_{table_name}
        primary key ({','.join(pk_cols)})""")

    table_housekeeping(table)

    return None


def copy_table_from_to(
        table_from: str,
        table_to: str,
        history_days: int = 1,
        copy_partitioning: bool = False,
        copy_primary_key: bool = False,
        overwrite_table_to: bool = False) -> None:

    if overwrite_table_to:
        get_spark().sql(f"drop table if exists {table_to}")

    get_spark().sql(f"""
                    create table {table_to} as
                    select *
                    from {table_from}
                    where rundate >= current_date() - {history_days}
                    """)

    if copy_partitioning:
        partitioned_by = get_table_partition_cols(table_from)
        if partitioned_by:
            get_spark().sql(
                f"""
                replace table {table_to}
                partitioned by ({','.join(partitioned_by)})
                as select * from {table_to}
                """
            )

    if copy_primary_key:
        pk_cols = get_table_pk_cols(table_from)
        if pk_cols:
            for pk_col in pk_cols:
                get_spark().sql(
                    f"""alter table {table_to}
                    alter column {pk_col} set not null""")

            table_to_name = table_to.split(".")[-1]
            get_spark().sql(
                f"""alter table {table_to} add constraint pk_{table_to_name}
                primary key ({','.join(pk_cols)})""")

    table_housekeeping(table_to)

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


def get_table_partition_cols(table: str) -> list:

    df = DeltaTable.forName(get_spark(), table)

    return df.detail().select("partitionColumns").collect()[0][0]


def get_table_pk_cols(table: str) -> list:

    catalog = table.split(".")[0]
    schema = table.split(".")[1]
    table_name = table.split(".")[2]

    query = f"""
        select column_name
        from `system`.information_schema.key_column_usage cu
        join `system`.information_schema.table_constraints tc
        using (constraint_catalog, constraint_schema, constraint_name)
        where cu.table_catalog = '{catalog}'
        and cu.table_schema = '{schema}'
        and cu.table_name = '{table_name}'
        and tc.constraint_type = 'PRIMARY KEY'
        order by cu.ordinal_position
        """

    df = get_spark().sql(query)

    return [x[0] for x in df.collect()]

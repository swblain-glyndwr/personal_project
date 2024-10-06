from pyspark.sql.types import StructField, StructType
import sys


def build_spark_field(
        name: str,
        dtype: str,
        nullable_str: str
        ) -> StructField:

    spark_types = {
        "string": "StringType",
        "date": "DateType"
    }

    spark_type = getattr(
        sys.modules["pyspark.sql.types"],
        spark_types[dtype]
        )

    nullable_bool = False if nullable_str != "nullable" else True

    return StructField(name, spark_type(), nullable_bool)


def build_spark_schema(schema: list) -> StructType:

    fields = [build_spark_field(*c) for c in schema]

    return StructType(fields)

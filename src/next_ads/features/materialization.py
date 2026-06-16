"""Helpers for writing Next Ads Databricks Feature Engineering tables."""

from __future__ import annotations

from datetime import date
from typing import Any

from next_ads.features.feature_store_registry import (
    FeatureStoreRegistry,
    load_feature_store_registry,
)
from scripts.table_operations.create_tables import extract_create_table_columns


def create_feature_engineering_client():
    """Create the Databricks Feature Engineering client at runtime."""
    try:
        from databricks.feature_engineering import FeatureEngineeringClient
    except ImportError as exc:
        raise ImportError(
            "databricks.feature_engineering is required to write Databricks "
            "feature tables. Run this on a Databricks runtime or install the "
            "Databricks Feature Engineering package in the execution "
            "environment. Original import error: "
            f"{exc}"
        ) from exc

    return FeatureEngineeringClient()


def feature_table_path(
    table_name: str,
    catalog: str | None = None,
    schema: str | None = None,
    registry: FeatureStoreRegistry | None = None,
) -> str:
    """Resolve a registered feature table to a fully qualified path."""
    active_registry = registry or load_feature_store_registry()
    return active_registry.resolved_table_path(
        table_name,
        catalog=catalog,
        schema=schema,
    )


def feature_table_contract_columns(
    table_name: str,
    registry: FeatureStoreRegistry | None = None,
) -> list[tuple[str, str]]:
    """Return contract columns and DDL types for a feature table."""
    active_registry = registry or load_feature_store_registry()
    return extract_create_table_columns(
        active_registry.sql_contract_path(table_name).read_text()
    )


def validate_required_columns(
    df: Any,
    required_columns: list[str] | tuple[str, ...],
    table_name: str,
) -> None:
    """Fail fast when a DataFrame is missing required feature columns."""
    missing_columns = sorted(set(required_columns) - set(df.columns))
    if missing_columns:
        raise ValueError(
            f"{table_name} is missing required columns: "
            f"{', '.join(missing_columns)}"
        )


def _spark_cast_type(ddl_type: str) -> str:
    return ddl_type.upper().replace(" NOT NULL", "").strip()


def align_to_feature_table_contract(
    df: Any,
    table_name: str,
    registry: FeatureStoreRegistry | None = None,
):
    """Select contract columns in order, adding nullable missing fields."""
    from pyspark.sql import functions as F

    contract_columns = feature_table_contract_columns(table_name, registry)
    selected_columns = []
    for column_name, ddl_type in contract_columns:
        spark_type = _spark_cast_type(ddl_type)
        if column_name in df.columns:
            selected_columns.append(
                F.col(column_name).cast(spark_type).alias(column_name)
            )
        else:
            selected_columns.append(F.lit(None).cast(spark_type).alias(column_name))
    return df.select(*selected_columns)


def delete_reference_date_partition(
    spark: Any,
    table_path: str,
    reference_date_column: str,
    reference_date: str | date,
) -> None:
    """Delete one point-in-time partition before a feature-table merge."""
    spark.sql(
        f"DELETE FROM {table_path} "
        f"WHERE {reference_date_column} = DATE '{reference_date}'"
    )


def write_feature_table(
    spark: Any,
    table_name: str,
    df: Any,
    catalog: str | None = None,
    schema: str | None = None,
    reference_date: str | date | None = None,
    reference_date_column: str = "reference_date",
    replace_reference_date: bool = True,
    registry: FeatureStoreRegistry | None = None,
    feature_engineering_client: Any | None = None,
) -> str:
    """Validate and write a feature table through Databricks FE."""
    active_registry = registry or load_feature_store_registry()
    table = active_registry.table_spec(table_name)
    table_path = feature_table_path(table_name, catalog, schema, active_registry)

    validate_required_columns(df, table.primary_keys, table_name)
    aligned_df = align_to_feature_table_contract(df, table_name, active_registry)

    if (
        replace_reference_date
        and reference_date
        and reference_date_column in aligned_df.columns
        and spark.catalog.tableExists(table_path)
    ):
        delete_reference_date_partition(
            spark,
            table_path,
            reference_date_column,
            reference_date,
        )

    client = feature_engineering_client or create_feature_engineering_client()
    client.write_table(name=table_path, df=aligned_df, mode="merge")
    return table_path

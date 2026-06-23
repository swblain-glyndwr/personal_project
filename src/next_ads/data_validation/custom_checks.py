"""Compatibility wrapper for custom Spark validation checks."""

from next_ads.data.validation.custom_checks import (
    isin_spark,
    str_matches_spark,
    unique_spark,
)

__all__ = ["isin_spark", "str_matches_spark", "unique_spark"]

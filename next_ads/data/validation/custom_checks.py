"""Compatibility wrapper for custom Spark validation checks."""

from next_ads.data.validation._src_loader import load_src_validation_module

_custom_checks = load_src_validation_module("custom_checks")

isin_spark = _custom_checks.isin_spark
str_matches_spark = _custom_checks.str_matches_spark
unique_spark = _custom_checks.unique_spark

__all__ = ["isin_spark", "str_matches_spark", "unique_spark"]

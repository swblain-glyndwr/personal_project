import pytest

from scripts.smoke.prod_table_contract_smoke import (
    compare_expected_columns,
    normalize_type,
    validate_prod_route,
)


class Config:
    catalog_write = "marketingdata_prod"
    schema_write = "warehouse"


def test_validate_prod_route_accepts_prod_warehouse():
    validate_prod_route("prod", Config())


def test_validate_prod_route_rejects_non_prod():
    with pytest.raises(ValueError, match="job_env=prod"):
        validate_prod_route("preprod", Config())


def test_normalize_type_handles_simple_sql_and_spark_aliases():
    assert normalize_type("STRING NOT NULL") == "string"
    assert normalize_type("date not null") == "date"
    assert normalize_type("long") == "bigint"
    assert normalize_type("INTEGER") == "int"


def test_normalize_type_skips_complex_types():
    assert normalize_type("STRUCT<AccountNumber: STRING> NOT NULL") is None
    assert normalize_type("ARRAY<STRING>") is None


def test_compare_expected_columns_reports_missing_extra_and_type_mismatches():
    missing_columns, extra_columns, type_mismatches = compare_expected_columns(
        [
            ("UniqueAdID", "STRING NOT NULL"),
            ("Score", "DOUBLE"),
            ("rundate", "DATE NOT NULL"),
            ("payload", "STRUCT<AccountNumber: STRING> NOT NULL"),
        ],
        [
            ("UniqueAdID", "string"),
            ("Score", "float"),
            ("payload", "struct<AccountNumber:string>"),
            ("LegacyColumn", "string"),
        ],
    )

    assert missing_columns == ["rundate"]
    assert extra_columns == ["LegacyColumn"]
    assert type_mismatches == ["Score: expected double, found float"]


def test_compare_expected_columns_can_allow_extra_columns_when_explicit():
    missing_columns, extra_columns, type_mismatches = compare_expected_columns(
        [("UniqueAdID", "STRING")],
        [("UniqueAdID", "string"), ("LegacyColumn", "string")],
        allow_extra_columns=True,
    )

    assert missing_columns == []
    assert extra_columns == []
    assert type_mismatches == []

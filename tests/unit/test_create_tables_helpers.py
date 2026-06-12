from scripts.table_operations.create_tables import (
    build_add_missing_columns_query,
    extract_create_table_columns,
)


def test_extract_create_table_columns_ignores_constraints_and_table_options():
    sql = """
CREATE TABLE {catalog}.{schema}.{client}_example (
  UniqueAdID STRING NOT NULL,
  ClusterID STRING,
  FY20 STRING,
  rundate DATE,
  CONSTRAINT `pk_example` PRIMARY KEY (`UniqueAdID`))
USING delta
PARTITIONED BY (rundate)
"""

    assert extract_create_table_columns(sql) == [
        ("UniqueAdID", "STRING"),
        ("ClusterID", "STRING"),
        ("FY20", "STRING"),
        ("rundate", "DATE"),
    ]


def test_build_add_missing_columns_query_is_additive_only():
    query = build_add_missing_columns_query(
        "marketingdata_dev.nextads_integration.next_uk_nextads_control_sheet_raw",
        [
            ("UniqueAdID", "STRING"),
            ("ClusterID", "STRING"),
            ("FY20", "STRING"),
            ("rundate", "DATE"),
        ],
        ["UniqueAdID", "ClusterID", "rundate"],
    )

    assert query == (
        "ALTER TABLE "
        "marketingdata_dev.nextads_integration.next_uk_nextads_control_sheet_raw "
        "ADD COLUMNS (`FY20` STRING)"
    )


def test_build_add_missing_columns_query_returns_none_when_target_is_current():
    assert (
        build_add_missing_columns_query(
            "catalog.schema.table",
            [("UniqueAdID", "STRING"), ("FY20", "STRING")],
            ["UniqueAdID", "FY20"],
        )
        is None
    )

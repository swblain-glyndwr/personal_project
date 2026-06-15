from scripts.table_operations.create_tables import (
    build_add_missing_columns_query,
    extract_create_table_columns,
    get_unsupported_missing_columns,
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
        ("UniqueAdID", "STRING NOT NULL"),
        ("ClusterID", "STRING"),
        ("FY20", "STRING"),
        ("rundate", "DATE"),
    ]


def test_extract_create_table_columns_handles_multiline_struct_columns():
    sql = """
create table {catalog}.{schema}.{client}_nextads_payload_latest(
  roamingprofileid BIGINT,
  next_ads STRUCT<
    AccountNumber: STRING NOT NULL,
    adFatigueImpressionThreshold: INT NOT NULL,
    experimentId: STRING NOT NULL,
    triggers: ARRAY<STRUCT<t: FLOAT, id: STRING>> NOT NULL,
    control: BOOLEAN NOT NULL,
    fragments: ARRAY<ARRAY<STRUCT<
      pageTypes: ARRAY<STRING>,
      enableAdFatigueRotation: BOOLEAN,
      fragmentIds: ARRAY<STRING>
    >>> NOT NULL,
    adsHash: STRING
  > NOT NULL,
  rundate date not null,
  constraint pk_example primary key (
    roamingprofileid,
    rundate
    )
)
"""

    columns = extract_create_table_columns(sql)

    assert [name for name, _ in columns] == [
        "roamingprofileid",
        "next_ads",
        "rundate",
    ]
    assert columns[1][1].startswith("STRUCT<")
    assert "fragments: ARRAY<ARRAY<STRUCT<" in columns[1][1]


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


def test_build_add_missing_columns_query_skips_complex_or_constrained_columns():
    expected_columns = [
        ("roamingprofileid", "BIGINT"),
        ("next_ads", "STRUCT<AccountNumber: STRING> NOT NULL"),
        ("FY20", "STRING"),
        ("rundate", "date not null"),
    ]
    actual_columns = ["roamingprofileid", "rundate"]

    query = build_add_missing_columns_query(
        "marketingdata_dev.nextads_integration.next_uk_nextads_payload_latest",
        expected_columns,
        actual_columns,
    )

    assert query == (
        "ALTER TABLE "
        "marketingdata_dev.nextads_integration.next_uk_nextads_payload_latest "
        "ADD COLUMNS (`FY20` STRING)"
    )
    assert get_unsupported_missing_columns(expected_columns, actual_columns) == [
        "next_ads",
    ]


def test_build_add_missing_columns_query_returns_none_when_target_is_current():
    assert (
        build_add_missing_columns_query(
            "catalog.schema.table",
            [("UniqueAdID", "STRING"), ("FY20", "STRING")],
            ["UniqueAdID", "FY20"],
        )
        is None
    )

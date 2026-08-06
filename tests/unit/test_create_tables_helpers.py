from pathlib import Path

import pytest

from jobs.table_operations.create_tables import (
    ColumnSpec,
    build_repair_insert_query,
    build_repair_create_table_query,
    repair_table_to_contract,
    can_use_additive_alter_only,
    build_add_missing_columns_query,
    compare_table_schema,
    extract_create_table_columns,
    get_unsupported_missing_columns,
    normalize_data_type,
    parse_column_specs,
    table_matches_selection,
)
from next_ads.common.paths import resolve_sql_contract_path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class FakeSpark:
    def __init__(self):
        self.sql_calls = []

    def sql(self, statement):
        self.sql_calls.append(statement)


class FakeLogger:
    def warning(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass


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
    assert get_unsupported_missing_columns(
        expected_columns, actual_columns
    ) == [
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


def specs(columns):
    return parse_column_specs(columns)


def test_normalize_data_type_handles_spark_and_sql_complex_type_formats():
    assert normalize_data_type("string not null") == "STRING"
    assert normalize_data_type("array<struct<t: float, id: string>>") == (
        "ARRAY<STRUCT<T:FLOAT,ID:STRING>>"
    )


def test_compare_table_schema_exact_schema_passes():
    expected = specs(
        [
            ("AccountNumber", "STRING NOT NULL"),
            ("Audience", "STRING NOT NULL"),
            ("rundate", "DATE NOT NULL"),
        ]
    )
    actual = [
        ColumnSpec("AccountNumber", "STRING", False, "string"),
        ColumnSpec("Audience", "STRING", False, "string"),
        ColumnSpec("rundate", "DATE", False, "date"),
    ]

    assert not compare_table_schema(expected, actual).has_drift


def test_compare_table_schema_reports_missing_nullable_column_before_final_column():
    expected = specs(
        [
            ("UniqueAdID", "STRING NOT NULL"),
            ("ClusterID", "STRING"),
            ("rundate", "DATE NOT NULL"),
        ]
    )
    actual = specs(
        [
            ("UniqueAdID", "STRING NOT NULL"),
            ("rundate", "DATE NOT NULL"),
        ]
    )

    drift = compare_table_schema(expected, actual)

    assert [column.name for column in drift.missing_columns] == ["ClusterID"]
    assert drift.has_drift
    assert can_use_additive_alter_only(expected, actual, drift) is False


def test_compare_table_schema_allows_missing_nullable_column_at_end():
    expected = specs(
        [
            ("UniqueAdID", "STRING NOT NULL"),
            ("rundate", "DATE NOT NULL"),
            ("ClusterID", "STRING"),
        ]
    )
    actual = specs(
        [
            ("UniqueAdID", "STRING NOT NULL"),
            ("rundate", "DATE NOT NULL"),
        ]
    )

    drift = compare_table_schema(expected, actual)

    assert [column.name for column in drift.missing_columns] == ["ClusterID"]
    assert can_use_additive_alter_only(expected, actual, drift) is True


def test_compare_table_schema_reports_wrong_column_order():
    expected = specs(
        [
            ("UniqueAdID", "STRING NOT NULL"),
            ("IsUnderperforming", "BOOLEAN"),
            ("rundate", "DATE NOT NULL"),
        ]
    )
    actual = specs(
        [
            ("UniqueAdID", "STRING NOT NULL"),
            ("rundate", "DATE NOT NULL"),
            ("IsUnderperforming", "BOOLEAN"),
        ]
    )

    drift = compare_table_schema(expected, actual)

    assert drift.order_drift is True
    assert drift.requires_rebuild is True


def test_compare_table_schema_reports_type_and_nullability_drift():
    expected = specs(
        [
            ("AccountNumber", "STRING NOT NULL"),
            ("Audience", "STRING NOT NULL"),
        ]
    )
    actual = [
        ColumnSpec("AccountNumber", "BIGINT", False, "bigint"),
        ColumnSpec("Audience", "STRING", True, "string"),
    ]

    drift = compare_table_schema(expected, actual)

    assert drift.type_drift == ["AccountNumber: expected STRING, found BIGINT"]
    assert drift.nullability_drift == [
        "Audience: expected NOT NULL, found nullable"
    ]
    assert drift.requires_rebuild is True


def test_compare_table_schema_reports_extra_columns():
    expected = specs([("UniqueAdID", "STRING NOT NULL")])
    actual = specs(
        [
            ("UniqueAdID", "STRING NOT NULL"),
            ("UnusedColumn", "STRING"),
        ]
    )

    drift = compare_table_schema(expected, actual)

    assert drift.extra_columns == ["UnusedColumn"]
    assert drift.requires_rebuild is True


def test_control_sheet_with_rundate_before_is_underperforming_requires_rebuild():
    expected = specs(
        [
            ("UniqueAdID", "STRING NOT NULL"),
            ("IsUnderperforming", "BOOLEAN"),
            ("rundate", "DATE NOT NULL"),
        ]
    )
    actual = specs(
        [
            ("UniqueAdID", "STRING NOT NULL"),
            ("rundate", "DATE NOT NULL"),
            ("IsUnderperforming", "BOOLEAN"),
        ]
    )

    drift = compare_table_schema(expected, actual)

    assert drift.order_drift is True
    assert can_use_additive_alter_only(expected, actual, drift) is False
    assert drift.requires_rebuild is True


def test_v2_control_sheet_appended_cluster_and_underperforming_requires_rebuild():
    contract = Path("sql/adsv2/create_table_control_sheet_v2.sql").read_text()
    expected = parse_column_specs(extract_create_table_columns(contract))
    actual = [
        column
        for column in expected
        if column.name not in {"ClusterID", "IsUnderperforming", "rundate"}
    ]
    actual.extend(
        specs(
            [
                ("rundate", "DATE NOT NULL"),
                ("ClusterID", "STRING"),
                ("IsUnderperforming", "BOOLEAN"),
            ]
        )
    )

    drift = compare_table_schema(expected, actual)

    assert drift.order_drift is True
    assert drift.requires_rebuild is True


def test_missing_audience_uses_false_default_in_repair_insert():
    expected = specs(
        [
            ("AccountNumber", "STRING NOT NULL"),
            ("Audience", "STRING NOT NULL"),
            ("rundate", "DATE NOT NULL"),
        ]
    )
    actual = specs(
        [
            ("AccountNumber", "STRING NOT NULL"),
            ("rundate", "DATE NOT NULL"),
        ]
    )

    query = build_repair_insert_query(
        "catalog.schema.customer_cells_repair",
        "catalog.schema.customer_cells_backup",
        expected,
        actual,
    )

    assert "CAST('false' AS STRING) AS `Audience`" in query
    assert (
        "SELECT `AccountNumber`, CAST('false' AS STRING) AS `Audience`, `rundate`"
        in query
    )
    assert query.startswith(
        "INSERT INTO catalog.schema.customer_cells_repair "
        "(`AccountNumber`, `Audience`, `rundate`)"
    )


@pytest.mark.parametrize(
    ("table", "contract_path"),
    [
        (
            "catalog.schema.next_uk_nextads_assignments_build_staging",
            "sql/decisioning/create_table_assignments_build_staging.sql",
        ),
        (
            "catalog.schema.next_uk_nextads_assignments_v2_build_staging",
            "sql/adsv2/create_table_assignments_v2_build_staging.sql",
        ),
        (
            "catalog.schema.next_uk_nextads_assignment_build_events",
            "sql/decisioning/create_table_assignment_build_events.sql",
        ),
    ],
)
def test_assignment_provenance_migration_requires_targeted_recreation(
    table,
    contract_path,
):
    provenance_columns = {
        "CandidateBuildID",
        "CandidateBuildAttemptID",
        "PortfolioID",
        "PortfolioAttemptID",
        "CandidateFoundationSnapshotID",
    }
    contract = (PROJECT_ROOT / contract_path).read_text()
    expected = parse_column_specs(extract_create_table_columns(contract))
    actual = [
        column for column in expected if column.name not in provenance_columns
    ]

    spark = FakeSpark()

    with pytest.raises(ValueError, match="explicit targeted recreation"):
        repair_table_to_contract(
            spark,
            table=table,
            create_table_sql=contract,
            expected_columns=expected,
            actual_columns=actual,
            job_env="dev",
            dry_run=False,
            logger=FakeLogger(),
        )

    assert spark.sql_calls == []


def test_repair_create_table_suffixes_quoted_constraint_name():
    query = build_repair_create_table_query(
        """
CREATE TABLE catalog.schema.customer_cells (
  AccountNumber STRING NOT NULL,
  CONSTRAINT `pk_customer_cells` PRIMARY KEY (AccountNumber)
)
""",
        "catalog.schema.customer_cells__repair_20260724140000",
    )

    assert (
        "CONSTRAINT `pk_customer_cells_customer_cells__repair_20260724140000`"
        in query
    )


def test_repair_table_to_contract_dry_run_does_not_execute_sql():
    spark = FakeSpark()

    repair_table_to_contract(
        spark,
        table="catalog.schema.customer_cells",
        create_table_sql="""
CREATE TABLE catalog.schema.customer_cells (
  AccountNumber STRING NOT NULL,
  Audience STRING NOT NULL,
  rundate DATE NOT NULL,
  CONSTRAINT pk_customer_cells PRIMARY KEY (AccountNumber)
)
""",
        expected_columns=specs(
            [
                ("AccountNumber", "STRING NOT NULL"),
                ("Audience", "STRING NOT NULL"),
                ("rundate", "DATE NOT NULL"),
            ]
        ),
        actual_columns=specs(
            [
                ("AccountNumber", "STRING NOT NULL"),
                ("rundate", "DATE NOT NULL"),
            ]
        ),
        job_env="dev",
        dry_run=True,
        logger=FakeLogger(),
    )

    assert spark.sql_calls == []


def test_repair_table_to_contract_blocks_prod_rebuild():
    with pytest.raises(ValueError, match="PROD rebuild repair is blocked"):
        repair_table_to_contract(
            FakeSpark(),
            table="catalog.schema.customer_cells",
            create_table_sql="CREATE TABLE catalog.schema.customer_cells (id STRING)",
            expected_columns=specs([("id", "STRING")]),
            actual_columns=specs([("old_id", "STRING")]),
            job_env="prod",
            dry_run=False,
            logger=FakeLogger(),
        )


def test_repair_table_to_contract_never_backup_rebuilds_canonical_outputs():
    with pytest.raises(ValueError, match="will not create a backup copy"):
        repair_table_to_contract(
            FakeSpark(),
            table="catalog.schema.next_uk_nextads_candidate_scores",
            create_table_sql=(
                "create table catalog.schema.next_uk_nextads_candidate_scores "
                "(CandidateBuildID string not null)"
            ),
            expected_columns=[
                ColumnSpec(
                    "CandidateBuildID",
                    "string",
                    False,
                    "string not null",
                )
            ],
            actual_columns=[],
            job_env="dev",
            dry_run=False,
            logger=FakeLogger(),
        )


def test_table_selection_accepts_ref_full_name_or_table_name():
    table = (
        "marketingdata_dev.stephen_blain.next_uk_nextads_control_sheet_latest"
    )

    assert table_matches_selection("control_sheet_latest", table, set())
    assert table_matches_selection(
        "control_sheet_latest",
        table,
        {"control_sheet_latest"},
    )
    assert table_matches_selection("control_sheet_latest", table, {table})
    assert table_matches_selection(
        "control_sheet_latest",
        table,
        {"next_uk_nextads_control_sheet_latest"},
    )
    assert not table_matches_selection(
        "control_sheet_latest",
        table,
        {"next_uk_nextads_assignments_latest"},
    )


def test_adsv2_control_sheet_tables_include_underperforming_flag_before_rundate():
    for sql_path in [
        PROJECT_ROOT / "sql/adsv2/create_table_control_sheet_v2.sql",
        PROJECT_ROOT / "sql/adsv2/create_table_control_sheet_latest_v2.sql",
    ]:
        columns = extract_create_table_columns(sql_path.read_text())

        assert ("IsUnderperforming", "BOOLEAN") in columns
        assert columns.index(("ClusterID", "STRING")) < columns.index(
            ("IsUnderperforming", "BOOLEAN")
        )
        assert columns.index(("IsUnderperforming", "BOOLEAN")) < columns.index(
            ("rundate", "DATE NOT NULL")
        )


@pytest.mark.parametrize(
    "table_ref",
    [
        "assignments_build_staging",
        "assignments_v2_build_staging",
        "assignment_build_events",
    ],
)
def test_assignment_publication_tables_have_repo_owned_sql_contracts(
    table_ref,
):
    contract_path = resolve_sql_contract_path(table_ref)

    assert contract_path.is_file()
    assert extract_create_table_columns(contract_path.read_text())


def test_realtime_reranking_advert_features_requires_cms_page_id():
    contract = (
        PROJECT_ROOT
        / "sql/realtime/create_table_nextads_realtime_reranking_advert_features.sql"
    ).read_text()

    assert ("CMSPageID", "STRING NOT NULL") in extract_create_table_columns(
        contract
    )

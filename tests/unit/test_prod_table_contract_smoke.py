import pytest

from jobs.smoke.prod_table_contract_smoke import (
    collect_contract_failures,
    describe_schema_drift,
    validate_prod_route,
)
from jobs.table_operations.create_tables import (
    ColumnSpec,
    compare_table_schema,
)


class Config:
    catalog_write = "marketingdata_prod"
    schema_write = "warehouse"


def test_validate_prod_route_accepts_prod_warehouse():
    validate_prod_route("prod", Config())


def test_validate_prod_route_rejects_non_prod():
    with pytest.raises(ValueError, match="job_env=prod"):
        validate_prod_route("preprod", Config())


def test_describe_schema_drift_reports_table_operations_drift_model():
    drift = compare_table_schema(
        [
            ColumnSpec("UniqueAdID", "STRING", False, "STRING NOT NULL"),
            ColumnSpec("Score", "DOUBLE", True, "DOUBLE"),
            ColumnSpec("rundate", "DATE", False, "DATE NOT NULL"),
        ],
        [
            ColumnSpec("Score", "FLOAT", True, "float"),
            ColumnSpec("UniqueAdID", "STRING", True, "string"),
            ColumnSpec("LegacyColumn", "STRING", True, "string"),
        ],
    )

    assert describe_schema_drift(drift) == [
        "missing columns: rundate",
        "unexpected columns: LegacyColumn",
        "column order does not match SQL contract",
        "type drift: Score: expected DOUBLE, found FLOAT",
        "nullability drift: UniqueAdID: expected NOT NULL, found nullable",
    ]


def test_describe_schema_drift_can_allow_extra_columns_when_explicit():
    drift = compare_table_schema(
        [ColumnSpec("UniqueAdID", "STRING", True, "STRING")],
        [
            ColumnSpec("UniqueAdID", "STRING", True, "string"),
            ColumnSpec("LegacyColumn", "STRING", True, "string"),
        ],
    )

    assert (
        describe_schema_drift(
            drift,
            allow_extra_columns=True,
        )
        == []
    )


class FakeLogger:
    def info(self, *_args, **_kwargs):
        pass


class FakeDataType:
    def __init__(self, value):
        self.value = value

    def simpleString(self):  # noqa: N802 - mirrors Spark API
        return self.value


class FakeField:
    def __init__(self, name, data_type, nullable=True):
        self.name = name
        self.dataType = FakeDataType(data_type)
        self.nullable = nullable


class FakeSchema:
    def __init__(self, fields):
        self.fields = fields


class FakeDataFrame:
    def __init__(self, fields):
        self.schema = FakeSchema(fields)


class FakeCatalog:
    def __init__(self, existing_tables):
        self.existing_tables = existing_tables

    def tableExists(self, table):  # noqa: N802 - mirrors Spark API
        return table in self.existing_tables


class FakeSpark:
    def __init__(self, tables):
        self.tables = tables
        self.catalog = FakeCatalog(set(tables))

    def table(self, table):
        return FakeDataFrame(self.tables[table])


def test_collect_contract_failures_reports_missing_table_and_schema_drift(
    tmp_path,
    monkeypatch,
):
    contract = tmp_path / "contract.sql"
    contract.write_text(
        """
        CREATE TABLE table_name (
          UniqueAdID STRING NOT NULL,
          Score DOUBLE,
          rundate DATE NOT NULL
        )
        """,
    )
    monkeypatch.setattr(
        "jobs.smoke.prod_table_contract_smoke.resolve_sql_contract_path",
        lambda _table_ref: contract,
    )
    spark = FakeSpark(
        {
            "marketingdata_prod.warehouse.present_table": [
                FakeField("Score", "float"),
                FakeField("UniqueAdID", "string"),
                FakeField("LegacyColumn", "string"),
            ],
        }
    )

    failures = collect_contract_failures(
        spark=spark,
        table_contracts={
            "present_table": "marketingdata_prod.warehouse.present_table",
            "missing_table": "marketingdata_prod.warehouse.missing_table",
        },
        logger=FakeLogger(),
    )

    assert failures == [
        "present_table: marketingdata_prod.warehouse.present_table has schema drift",
        "  - missing columns: rundate",
        "  - unexpected columns: LegacyColumn",
        "  - column order does not match SQL contract",
        "  - type drift: Score: expected DOUBLE, found FLOAT",
        "  - nullability drift: UniqueAdID: expected NOT NULL, found nullable",
        "missing_table: missing table marketingdata_prod.warehouse.missing_table",
    ]

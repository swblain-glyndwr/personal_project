from datetime import date
from types import SimpleNamespace

import pytest
from delta.exceptions import DeltaConcurrentModificationException
from pyspark.sql.types import LongType, StringType, StructField, StructType

import next_ads.common.delta_writes as delta_writes
from next_ads.common.delta_writes import (
    DeltaRetryPolicy,
    atomic_append_by_name,
    atomic_replace_where_by_name,
    build_append_statement,
    build_equality_predicate,
    build_replace_where_statement,
    quote_qualified_identifier,
    replace_scope_by_name,
    replace_table_by_name,
    sql_literal,
    typed_table_frame,
    validate_target_columns,
    validate_replace_source_scope,
    validate_unique_non_null_keys,
    validate_typed_table_schema,
)


class FakeExpression:
    def __init__(self, operation):
        self.operation = operation

    def isNull(self):  # noqa: N802
        return FakeExpression(("is_null", self.operation))

    def __or__(self, other):
        """Combine fake predicates for aggregate-expression inspection."""
        return FakeExpression(("or", self.operation, other.operation))

    def __and__(self, other):
        """Combine fake predicates for aggregate-expression inspection."""
        return FakeExpression(("and", self.operation, other.operation))

    def __invert__(self):
        """Invert a fake predicate."""
        return FakeExpression(("not", self.operation))

    def alias(self, name):
        return FakeExpression(("alias", self.operation, name))

    def eqNullSafe(self, other):  # noqa: N802
        return FakeExpression(
            ("eq_null_safe", self.operation, other.operation)
        )

    def otherwise(self, value):
        return FakeExpression(("otherwise", self.operation, value.operation))


@pytest.fixture
def fake_functions(monkeypatch):
    fake = SimpleNamespace(
        col=lambda value: FakeExpression(("col", value)),
        lit=lambda value: FakeExpression(("lit", value)),
        count=lambda value: FakeExpression(("count", value.operation)),
        struct=lambda *values: FakeExpression(
            ("struct", [value.operation for value in values])
        ),
        countDistinct=lambda value: FakeExpression(  # noqa: N815
            ("count_distinct", value.operation)
        ),
        when=lambda condition, value: FakeExpression(
            ("when", condition.operation, value.operation)
        ),
        sum=lambda value: FakeExpression(("sum", value.operation)),
        coalesce=lambda *values: FakeExpression(
            ("coalesce", [value.operation for value in values])
        ),
    )
    monkeypatch.setattr(delta_writes, "F", fake)
    return fake


class FakeAggregateFrame:
    def __init__(self, summary, columns=None):
        self.columns = columns or ["account", "rank"]
        self.summary = summary
        self.aggregate_calls = 0
        self.action_calls = 0

    def agg(self, *expressions):
        self.aggregate_calls += 1
        assert len(expressions) in {1, 3}
        return self

    def first(self):
        self.action_calls += 1
        return self.summary


class FakeFrame:
    def __init__(self, columns):
        self.columns = columns
        self.selected_columns = None
        self.view_name = None

    def select(self, *columns):
        self.selected_columns = list(columns)
        return self

    def createOrReplaceTempView(self, view_name):  # noqa: N802
        self.view_name = view_name


class FakeSpark:
    def __init__(self, outcomes, target_columns=None):
        self.outcomes = list(outcomes)
        self.statements = []
        self.dropped_views = []
        self.target_columns = target_columns or []
        self.catalog = SimpleNamespace(dropTempView=self.dropped_views.append)

    def table(self, _table_name):
        return SimpleNamespace(columns=self.target_columns)

    def sql(self, statement):
        self.statements.append(statement)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_key_validation_accepts_unique_keys_in_one_action(fake_functions):
    df = FakeAggregateFrame(
        {
            "_row_count": 2,
            "_distinct_key_count": 2,
            "_null_key_count": 0,
        }
    )

    summary = validate_unique_non_null_keys(df, ["account", "rank"])

    assert summary.row_count == 2
    assert summary.distinct_key_count == 2
    assert summary.null_key_count == 0
    assert df.aggregate_calls == 1
    assert df.action_calls == 1


@pytest.mark.parametrize(
    ("summary", "message"),
    [
        (
            {
                "_row_count": 2,
                "_distinct_key_count": 1,
                "_null_key_count": 0,
            },
            "Duplicate values",
        ),
        (
            {
                "_row_count": 2,
                "_distinct_key_count": 2,
                "_null_key_count": 1,
            },
            "Null values",
        ),
    ],
)
def test_key_validation_rejects_invalid_keys(
    fake_functions,
    summary,
    message,
):
    df = FakeAggregateFrame(summary)

    with pytest.raises(ValueError, match=message):
        validate_unique_non_null_keys(df, ["account", "rank"])


def test_replace_scope_validation_accepts_only_matching_source_rows(
    fake_functions,
):
    df = FakeAggregateFrame(
        {"_out_of_scope_count": 0},
        columns=["BuildRunID", "Location"],
    )

    validate_replace_source_scope(
        df,
        {"BuildRunID": "v1_123", "Location": None},
    )

    assert df.aggregate_calls == 1
    assert df.action_calls == 1


def test_replace_scope_validation_rejects_out_of_scope_source_rows(
    fake_functions,
):
    df = FakeAggregateFrame(
        {"_out_of_scope_count": 1},
        columns=["BuildRunID", "Location"],
    )

    with pytest.raises(ValueError, match="outside replacement scope"):
        validate_replace_source_scope(
            df,
            {"BuildRunID": "v1_123"},
        )


def test_sql_helpers_quote_names_literals_and_match_columns_by_name():
    assert quote_qualified_identifier("catalog.schema.table`name") == (
        "`catalog`.`schema`.`table``name`"
    )
    assert sql_literal("O'Brien") == "'O''Brien'"
    assert sql_literal(date(2026, 7, 28)) == "DATE '2026-07-28'"
    assert (
        build_equality_predicate({"BuildRunID": "v1_123", "Location": None})
        == "`BuildRunID` = 'v1_123' AND `Location` IS NULL"
    )

    statement = build_replace_where_statement(
        target_table="catalog.schema.target",
        source_view="source_view",
        columns=["b", "a"],
        filters={"BuildRunID": "v1_123"},
    )
    assert statement == (
        "INSERT INTO `catalog`.`schema`.`target`\n"
        "REPLACE WHERE `BuildRunID` = 'v1_123'\n"
        "SELECT `b`, `a`\n"
        "FROM `source_view`"
    )

    assert build_append_statement(
        target_table="catalog.schema.events",
        source_view="event_view",
        columns=["Scope", "Status"],
    ) == (
        "INSERT INTO `catalog`.`schema`.`events` BY NAME\n"
        "SELECT `Scope`, `Status`\n"
        "FROM `event_view`"
    )


def test_target_schema_requires_exact_names_but_not_column_order():
    spark = FakeSpark([], target_columns=["a", "b"])

    target_columns = validate_target_columns(
        spark,
        "catalog.schema.target",
        ["b", "a"],
    )

    assert target_columns == ["a", "b"]

    with pytest.raises(ValueError, match="missing target columns: b"):
        validate_target_columns(
            spark,
            "catalog.schema.target",
            ["a"],
        )


def test_nullable_manifest_rows_always_use_the_explicit_target_schema():
    schema = StructType(
        [
            StructField("BuildID", StringType(), nullable=False),
            StructField("PipelineUpdateID", LongType(), nullable=True),
        ]
    )
    captured = {}
    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=lambda _table: True),
        table=lambda _table: SimpleNamespace(schema=schema),
        createDataFrame=lambda rows, schema: captured.update(
            {"rows": rows, "schema": schema}
        )
        or SimpleNamespace(),
    )

    validate_typed_table_schema(
        spark,
        "catalog.schema.manifest",
        ("BuildID", "PipelineUpdateID"),
        nullable_columns=("PipelineUpdateID",),
    )
    typed_table_frame(
        spark,
        "catalog.schema.manifest",
        [{"BuildID": "build-1", "PipelineUpdateID": None}],
    )

    assert captured["schema"] == schema
    assert captured["rows"][0]["PipelineUpdateID"] is None


def test_replace_retries_only_delta_conflicts_and_drops_temporary_view():
    conflict = DeltaConcurrentModificationException("conflict")
    spark = FakeSpark(
        [conflict, None],
        target_columns=["AccountNumber", "Location"],
    )
    frame = FakeFrame(["AccountNumber", "Location"])
    delays = []
    monkeypatch_scope = {"calls": 0}

    def record_scope(*_):
        monkeypatch_scope["calls"] += 1

    original_validator = delta_writes.validate_replace_source_scope
    delta_writes.validate_replace_source_scope = record_scope

    try:
        result = atomic_replace_where_by_name(
            spark,
            frame,
            target_table="catalog.schema.assignments",
            filters={"Location": "SB1"},
            retry_policy=DeltaRetryPolicy(
                max_attempts=3,
                initial_backoff_seconds=2,
                max_backoff_seconds=10,
                jitter_seconds=0,
            ),
            sleep=delays.append,
        )
    finally:
        delta_writes.validate_replace_source_scope = original_validator

    assert result.attempts == 2
    # Scope validation is enforced by the Delta replace predicate itself;
    # avoiding a full pre-write scan is part of the lean publication contract.
    assert monkeypatch_scope["calls"] == 0
    assert len(spark.statements) == 2
    assert delays == [2]
    assert frame.selected_columns == ["AccountNumber", "Location"]
    assert spark.dropped_views == [frame.view_name]


def test_replace_aligns_source_to_target_by_name_without_dbr_unsupported_syntax():
    spark = FakeSpark([None], target_columns=["a", "b"])
    frame = FakeFrame(["b", "a"])

    result = atomic_replace_where_by_name(
        spark,
        frame,
        target_table="catalog.schema.target",
        replace_all=True,
        sleep=lambda _: None,
    )

    assert frame.selected_columns == ["a", "b"]
    assert "BY NAME" not in result.statement
    assert "REPLACE WHERE TRUE" in result.statement
    assert "SELECT `a`, `b`" in result.statement


def test_replace_does_not_retry_non_concurrency_errors_and_cleans_view():
    spark = FakeSpark(
        [ValueError("schema mismatch")],
        target_columns=["AccountNumber"],
    )
    frame = FakeFrame(["AccountNumber"])

    with pytest.raises(ValueError, match="schema mismatch"):
        atomic_replace_where_by_name(
            spark,
            frame,
            target_table="catalog.schema.assignments",
            replace_all=True,
            sleep=lambda _: None,
        )

    assert len(spark.statements) == 1
    assert spark.dropped_views == [frame.view_name]


def test_append_uses_selected_columns_and_name_alignment():
    spark = FakeSpark([None], target_columns=["Scope", "Status"])
    frame = FakeFrame(["Status", "Scope", "Unused"])

    result = atomic_append_by_name(
        spark,
        frame,
        target_table="catalog.schema.events",
        columns=["Scope", "Status"],
        sleep=lambda _: None,
    )

    assert result.attempts == 1
    assert frame.selected_columns == ["Scope", "Status"]
    assert "BY NAME" in result.statement
    assert "`Unused`" not in result.statement


def test_repo_owned_replace_interfaces_select_table_or_scope(monkeypatch):
    frame = FakeFrame(["id", "rundate"])
    frame.sparkSession = object()
    calls = []

    def replace(spark, df, **kwargs):
        calls.append((spark, df, kwargs))
        return SimpleNamespace(statement="replace", attempts=1)

    monkeypatch.setattr(
        delta_writes,
        "atomic_replace_where_by_name",
        replace,
    )

    replace_table_by_name(
        frame,
        "catalog.schema.latest",
        ["id", "rundate"],
    )
    replace_scope_by_name(
        frame,
        "catalog.schema.history",
        {"rundate": date(2026, 7, 28)},
        ["rundate", "id"],
    )

    assert [call[:2] for call in calls] == [
        (frame.sparkSession, frame),
        (frame.sparkSession, frame),
    ]
    assert calls[0][2]["target_table"] == "catalog.schema.latest"
    assert calls[0][2]["replace_all"] is True
    assert calls[0][2]["columns"] == ["id", "rundate"]
    assert calls[1][2]["target_table"] == "catalog.schema.history"
    assert calls[1][2]["filters"] == {"rundate": date(2026, 7, 28)}
    assert calls[1][2]["columns"] == ["rundate", "id"]

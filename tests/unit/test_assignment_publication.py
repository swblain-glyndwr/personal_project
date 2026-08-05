from datetime import date, datetime, timedelta
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession

import next_ads.decisioning.assignment_publication as publication
from next_ads.common.delta_writes import KeyValidationSummary
from next_ads.decisioning.assignment_publication import (
    NO_ADS,
    READY,
    AssignmentColumnContract,
    AssignmentProvenance,
    AssignmentScopeContract,
    AssignmentScopeEvent,
    AssignmentTableContract,
    stage_assignment_scope,
    validate_and_publish_assignment_build,
)


BUILD_RUN_ID = "v1_123"
BUILD_DATE = date(2026, 7, 29)
COMPLETED_AT = datetime(2026, 7, 29, 20, 0)
TABLES = AssignmentTableContract(
    staging_table="catalog.schema.assignment_staging",
    event_table="catalog.schema.assignment_events",
    history_table="catalog.schema.assignments",
    latest_table="catalog.schema.assignments_latest",
)
COLUMNS = AssignmentColumnContract()
PROVENANCE = AssignmentProvenance(
    candidate_build_id="candidate-build",
    candidate_build_attempt_id="candidate-build:attempt:0",
    portfolio_id="portfolio",
    portfolio_attempt_id="portfolio:attempt:0",
    candidate_foundation_snapshot_id="candidate-foundation",
)
PROVENANCE_COLUMNS = list(PROVENANCE.column_values(COLUMNS))
SCOPE_CONTRACT = AssignmentScopeContract(
    route="v1",
    scope_column="Location",
    expected_scopes=("A", "B"),
    key_columns=("AccountNumber", "Location"),
    public_columns=(
        "AccountNumber",
        "Location",
        "UniqueAdIDAssigned",
        "rundate",
    ),
)


@pytest.fixture
def local_spark():
    try:
        spark = (
            SparkSession.builder.master("local[2]")
            .appName("nextads-assignment-publication-tests")
            .getOrCreate()
        )
    except RuntimeError as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")
    yield spark


class FakeLiteral:
    def cast(self, _data_type):
        return self


class FakeFrame:
    def __init__(self, columns, rows=None):
        self.columns = list(columns)
        self.rows = rows
        self.select_calls = []
        self.persisted = False
        self.unpersisted = False

    def select(self, *columns):
        self.select_calls.append(list(columns))
        return FakeFrame(columns, rows=self.rows)

    def withColumn(self, column, _value):  # noqa: N802
        columns = [existing for existing in self.columns if existing != column]
        return FakeFrame([*columns, column], rows=self.rows)

    def persist(self, _storage_level):
        self.persisted = True
        return self

    def unpersist(self):
        self.unpersisted = True


class FakeSpark:
    def __init__(self):
        self.created_rows = None

    def createDataFrame(self, rows):  # noqa: N802
        self.created_rows = rows
        return FakeFrame(rows[0].keys(), rows=rows)


class FakeEventRow(dict):
    def asDict(self, recursive=False):  # noqa: N802
        return dict(self)


class FakeEventFrame:
    def __init__(self, columns, rows):
        self.columns = list(columns)
        self.rows = rows
        self.selected_columns = None

    def select(self, *columns):
        self.selected_columns = list(columns)
        return self

    def collect(self):
        return self.rows


def _event(
    scope,
    status,
    row_count,
    *,
    task_run_id=100,
    execution_count=1,
    completed_at=COMPLETED_AT,
    build_run_id=BUILD_RUN_ID,
    route="v1",
    build_date=BUILD_DATE,
    provenance=PROVENANCE,
):
    return {
        "BuildRunID": build_run_id,
        "Route": route,
        "Scope": scope,
        "Status": status,
        "RowCount": row_count,
        "BuildDate": build_date,
        "TaskRunID": task_run_id,
        "ExecutionCount": execution_count,
        "CompletedAt": completed_at,
        **provenance.column_values(COLUMNS),
    }


def _selected_event(
    scope="A",
    status=READY,
    row_count=2,
    *,
    task_run_id=100,
    execution_count=1,
):
    return AssignmentScopeEvent(
        scope=scope,
        status=status,
        row_count=row_count,
        build_date=BUILD_DATE,
        task_run_id=task_run_id,
        execution_count=execution_count,
        completed_at=COMPLETED_AT,
        provenance=PROVENANCE,
    )


@pytest.mark.parametrize(
    ("row_count", "expected_status"),
    [(2, READY), (0, NO_ADS)],
)
def test_stage_assignment_scope_replaces_only_one_structured_build_scope(
    monkeypatch,
    row_count,
    expected_status,
):
    source = FakeFrame(["AccountNumber", "Location", "UniqueAdIDAssigned"])
    dated = FakeFrame(SCOPE_CONTRACT.public_columns)
    calls = []
    summary = KeyValidationSummary(row_count, row_count, 0)
    spark = FakeSpark()

    monkeypatch.setattr(
        publication,
        "F",
        SimpleNamespace(
            lit=lambda _value: FakeLiteral(),
            col=lambda _value: FakeLiteral(),
        ),
    )
    monkeypatch.setattr(
        publication,
        "with_run_date",
        lambda df, value, column: (
            calls.append(("run_date", df, value, column)) or dated
        ),
    )

    def fake_replace(
        spark,
        df,
        *,
        table,
        scope,
        key_columns,
        columns,
    ):
        calls.append(
            (
                "stage",
                spark,
                df,
                table,
                scope,
                list(key_columns),
                list(columns),
            )
        )
        return summary

    monkeypatch.setattr(publication, "replace_validated_scope", fake_replace)
    monkeypatch.setattr(
        publication,
        "atomic_append_by_name",
        lambda spark, df, *, target_table, columns: (
            calls.append(
                (
                    "event",
                    spark,
                    df,
                    target_table,
                    list(columns),
                )
            )
            or SimpleNamespace(statement="event", attempts=1)
        ),
    )

    result = stage_assignment_scope(
        spark,
        source,
        tables=TABLES,
        columns=COLUMNS,
        scope_contract=SCOPE_CONTRACT,
        build_run_id=BUILD_RUN_ID,
        build_date=BUILD_DATE,
        scope="A",
        task_run_id=321,
        execution_count=2,
        provenance=PROVENANCE,
        completed_at=COMPLETED_AT,
    )

    assert calls[0][0] == "run_date"
    assert calls[0][2:] == (BUILD_DATE, "rundate")
    stage_call = calls[1]
    assert stage_call[0] == "stage"
    assert stage_call[3] == TABLES.staging_table
    assert stage_call[4] == {
        "BuildRunID": BUILD_RUN_ID,
        "Location": "A",
    }
    assert stage_call[5] == ["AccountNumber", "Location"]
    assert stage_call[6] == [
        "BuildRunID",
        "TaskRunID",
        "ExecutionCount",
        *PROVENANCE_COLUMNS,
        *SCOPE_CONTRACT.public_columns,
    ]
    assert stage_call[2].columns == stage_call[6]
    assert "Route" not in stage_call[6]
    assert "BuildDate" not in stage_call[6]
    assert "Scope" not in stage_call[6]
    event_call = calls[2]
    assert event_call[0] == "event"
    assert event_call[3] == TABLES.event_table
    assert event_call[4] == [
        "BuildRunID",
        "Route",
        "Scope",
        "Status",
        "RowCount",
        "BuildDate",
        "TaskRunID",
        "ExecutionCount",
        "CompletedAt",
        *PROVENANCE_COLUMNS,
    ]
    assert spark.created_rows == [
        {
            "BuildRunID": BUILD_RUN_ID,
            "Route": "v1",
            "Scope": "A",
            "Status": expected_status,
            "RowCount": row_count,
            "BuildDate": BUILD_DATE,
            "TaskRunID": 321,
            "ExecutionCount": 2,
            "CompletedAt": COMPLETED_AT,
            **PROVENANCE.column_values(COLUMNS),
        }
    ]
    assert result.status == expected_status
    assert result.row_count == row_count
    assert result.task_run_id == 321
    assert result.execution_count == 2
    assert result.completed_at == COMPLETED_AT
    assert result.event_write.statement == "event"


def test_stage_rejects_an_unexpected_scope_before_staging(monkeypatch):
    monkeypatch.setattr(
        publication,
        "replace_validated_scope",
        lambda *args, **kwargs: pytest.fail("staging write was reached"),
    )

    with pytest.raises(ValueError, match="Unexpected assignment scope"):
        stage_assignment_scope(
            object(),
            FakeFrame(["AccountNumber", "Location", "UniqueAdIDAssigned"]),
            tables=TABLES,
            columns=COLUMNS,
            scope_contract=SCOPE_CONTRACT,
            build_run_id=BUILD_RUN_ID,
            build_date=BUILD_DATE,
            scope="C",
            task_run_id=321,
            execution_count=2,
            provenance=PROVENANCE,
        )


def test_stage_failure_never_appends_a_completion_event(monkeypatch):
    monkeypatch.setattr(
        publication,
        "F",
        SimpleNamespace(
            lit=lambda _value: FakeLiteral(),
            col=lambda _value: FakeLiteral(),
        ),
    )
    monkeypatch.setattr(
        publication,
        "with_run_date",
        lambda df, value, column: FakeFrame(SCOPE_CONTRACT.public_columns),
    )
    monkeypatch.setattr(
        publication,
        "replace_validated_scope",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("staging failed")
        ),
    )
    monkeypatch.setattr(
        publication,
        "atomic_append_by_name",
        lambda *args, **kwargs: pytest.fail("event append was reached"),
    )

    with pytest.raises(RuntimeError, match="staging failed"):
        stage_assignment_scope(
            FakeSpark(),
            FakeFrame(["AccountNumber", "Location", "UniqueAdIDAssigned"]),
            tables=TABLES,
            columns=COLUMNS,
            scope_contract=SCOPE_CONTRACT,
            build_run_id=BUILD_RUN_ID,
            build_date=BUILD_DATE,
            scope="A",
            task_run_id=321,
            execution_count=2,
            provenance=PROVENANCE,
        )


def test_unexpected_empty_scope_fails_without_a_completion_event(monkeypatch):
    monkeypatch.setattr(
        publication,
        "F",
        SimpleNamespace(
            lit=lambda _value: FakeLiteral(),
            col=lambda _value: FakeLiteral(),
        ),
    )
    monkeypatch.setattr(
        publication,
        "with_run_date",
        lambda df, value, column: FakeFrame(SCOPE_CONTRACT.public_columns),
    )
    monkeypatch.setattr(
        publication,
        "replace_validated_scope",
        lambda *args, **kwargs: KeyValidationSummary(0, 0, 0),
    )
    monkeypatch.setattr(
        publication,
        "atomic_append_by_name",
        lambda *args, **kwargs: pytest.fail("event append was reached"),
    )

    with pytest.raises(ValueError, match="unexpectedly produced no rows"):
        stage_assignment_scope(
            FakeSpark(),
            FakeFrame(["AccountNumber", "Location", "UniqueAdIDAssigned"]),
            tables=TABLES,
            columns=COLUMNS,
            scope_contract=SCOPE_CONTRACT,
            build_run_id=BUILD_RUN_ID,
            build_date=BUILD_DATE,
            scope="A",
            task_run_id=321,
            execution_count=2,
            provenance=PROVENANCE,
            allow_no_ads=False,
        )


def test_event_reader_uses_the_exact_repair_event_schema():
    event_columns = [
        "BuildRunID",
        "Route",
        "Scope",
        "Status",
        "RowCount",
        "BuildDate",
        "TaskRunID",
        "ExecutionCount",
        "CompletedAt",
        *PROVENANCE_COLUMNS,
    ]
    row = FakeEventRow(_event("A", READY, 2))
    frame = FakeEventFrame(event_columns, [row])

    rows = publication._collect_event_rows(frame, COLUMNS)

    assert frame.selected_columns == event_columns
    assert rows == [dict(row)]


def test_repaired_event_selection_uses_all_three_descending_tiebreakers():
    rows = [
        _event(
            "A",
            READY,
            1,
            execution_count=1,
            provenance=PROVENANCE,
            completed_at=COMPLETED_AT + timedelta(hours=2),
            task_run_id=999,
        ),
        _event(
            "A",
            READY,
            2,
            execution_count=2,
            completed_at=COMPLETED_AT,
            task_run_id=100,
        ),
        _event(
            "A",
            READY,
            3,
            execution_count=2,
            completed_at=COMPLETED_AT + timedelta(minutes=1),
            task_run_id=100,
        ),
        _event(
            "A",
            READY,
            4,
            execution_count=2,
            completed_at=COMPLETED_AT + timedelta(minutes=1),
            task_run_id=101,
        ),
        _event("B", NO_ADS, 0, task_run_id=200),
    ]

    selected = publication._select_latest_scope_events(
        rows,
        columns=COLUMNS,
        scope_contract=SCOPE_CONTRACT,
        build_run_id=BUILD_RUN_ID,
        build_date=BUILD_DATE,
    )

    assert [(event.scope, event.row_count) for event in selected] == [
        ("A", 4),
        ("B", 0),
    ]
    assert selected[0].execution_count == 2
    assert selected[0].completed_at == COMPLETED_AT + timedelta(minutes=1)
    assert selected[0].task_run_id == 101


def test_contradictory_latest_events_are_rejected():
    rows = [
        _event("A", READY, 2, task_run_id=101),
        _event("A", NO_ADS, 0, task_run_id=101),
        _event("B", NO_ADS, 0, task_run_id=200),
    ]

    with pytest.raises(ValueError, match="Contradictory latest"):
        publication._select_latest_scope_events(
            rows,
            columns=COLUMNS,
            scope_contract=SCOPE_CONTRACT,
            build_run_id=BUILD_RUN_ID,
            build_date=BUILD_DATE,
        )


def test_assignment_events_must_share_one_candidate_provenance():
    other = AssignmentProvenance(
        candidate_build_id="other-build",
        candidate_build_attempt_id="other-attempt",
        portfolio_id="other-portfolio",
        portfolio_attempt_id="other-portfolio-attempt",
        candidate_foundation_snapshot_id="other-foundation",
    )

    with pytest.raises(ValueError, match="one accepted candidate provenance"):
        publication._select_latest_scope_events(
            [
                _event("A", READY, 1),
                _event("B", NO_ADS, 0, provenance=other),
            ],
            columns=COLUMNS,
            scope_contract=SCOPE_CONTRACT,
            build_run_id=BUILD_RUN_ID,
            build_date=BUILD_DATE,
        )


@pytest.mark.parametrize(
    ("parent_scope", "secondary_scope"),
    [("SB1", "SB2"), ("OC1", "OC2")],
)
def test_v1_no_ads_parent_requires_no_ads_secondary_event(
    parent_scope,
    secondary_scope,
):
    with pytest.raises(
        ValueError,
        match=(
            f"{secondary_scope} must be NO_ADS.*"
            f"{parent_scope} is NO_ADS"
        ),
    ):
        publication._validate_v1_secondary_event_inheritance(
            (
                _selected_event(parent_scope, NO_ADS, 0),
                _selected_event(secondary_scope, READY, 1),
            )
        )


@pytest.mark.parametrize(
    ("parent_status", "secondary_status"),
    [
        (READY, READY),
        (READY, NO_ADS),
        (NO_ADS, NO_ADS),
    ],
)
def test_v1_valid_parent_secondary_event_states_are_accepted(
    parent_status,
    secondary_status,
):
    publication._validate_v1_secondary_event_inheritance(
        (
            _selected_event(
                "SB1",
                parent_status,
                1 if parent_status == READY else 0,
            ),
            _selected_event(
                "SB2",
                secondary_status,
                1 if secondary_status == READY else 0,
            ),
        )
    )


def test_v1_parent_secondary_mismatch_rejects_before_staging_or_live_reads(
    monkeypatch,
):
    contract = AssignmentScopeContract(
        route="v1",
        scope_column="Location",
        expected_scopes=("SB1", "SB2"),
        key_columns=("AccountNumber", "Location"),
        public_columns=(
            "AccountNumber",
            "Location",
            "UniqueAdIDAssigned",
            "rundate",
        ),
    )
    reads = []

    def fake_read(
        spark,
        table,
        *,
        build_run_id_column,
        build_run_id,
    ):
        reads.append(
            (
                spark,
                table,
                build_run_id_column,
                build_run_id,
            )
        )
        return object()

    monkeypatch.setattr(publication, "_read_build_frame", fake_read)
    monkeypatch.setattr(
        publication,
        "_collect_event_rows",
        lambda *args, **kwargs: [
            _event("SB1", NO_ADS, 0, task_run_id=101),
            _event("SB2", READY, 1, task_run_id=102),
        ],
    )

    with pytest.raises(ValueError, match="SB2 must be NO_ADS"):
        validate_and_publish_assignment_build(
            object(),
            tables=TABLES,
            columns=COLUMNS,
            scope_contract=contract,
            build_run_id=BUILD_RUN_ID,
            build_date=BUILD_DATE,
        )

    assert [read[1] for read in reads] == [TABLES.event_table]


def test_invalid_status_in_an_older_event_is_still_rejected():
    rows = [
        _event("A", "FAILED", 1, task_run_id=100, execution_count=1),
        _event("A", READY, 2, task_run_id=101, execution_count=2),
        _event("B", NO_ADS, 0, task_run_id=200),
    ]

    with pytest.raises(ValueError, match="Invalid assignment event status"):
        publication._select_latest_scope_events(
            rows,
            columns=COLUMNS,
            scope_contract=SCOPE_CONTRACT,
            build_run_id=BUILD_RUN_ID,
            build_date=BUILD_DATE,
        )


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        (
            [_event("A", READY, 2)],
            "missing scopes: B",
        ),
        (
            [
                _event("A", READY, 2),
                _event("B", NO_ADS, 0),
                _event("C", NO_ADS, 0),
            ],
            "unexpected scopes: C",
        ),
        (
            [
                _event("A", "FAILED", 2),
                _event("B", NO_ADS, 0),
            ],
            "Invalid assignment event status",
        ),
        (
            [
                _event("A", READY, 0),
                _event("B", NO_ADS, 0),
            ],
            "READY assignment event.*zero rows",
        ),
        (
            [
                _event("A", READY, 2, route="v2"),
                _event("B", NO_ADS, 0),
            ],
            "Route does not match",
        ),
        (
            [
                _event(
                    "A",
                    READY,
                    2,
                    build_date=BUILD_DATE - timedelta(days=1),
                ),
                _event("B", NO_ADS, 0),
            ],
            "BuildDate does not match",
        ),
    ],
)
def test_invalid_event_builds_are_rejected(rows, message):
    with pytest.raises(ValueError, match=message):
        publication._select_latest_scope_events(
            rows,
            columns=COLUMNS,
            scope_contract=SCOPE_CONTRACT,
            build_run_id=BUILD_RUN_ID,
            build_date=BUILD_DATE,
        )


def test_staging_must_match_selected_event_count_and_attempt():
    events = (
        _selected_event(
            "A",
            READY,
            2,
            task_run_id=101,
            execution_count=2,
        ),
        _selected_event(
            "B",
            NO_ADS,
            0,
            task_run_id=201,
            execution_count=2,
        ),
    )

    valid = (
        publication._StagingSummary(
            scope="A",
            publication_date=BUILD_DATE,
            task_run_id=101,
            execution_count=2,
            row_count=2,
            provenance=PROVENANCE,
        ),
    )
    assert (
        publication._validate_staging_against_events(
            valid,
            selected_events=events,
            scope_contract=SCOPE_CONTRACT,
            build_date=BUILD_DATE,
        )
        == 2
    )

    wrong_attempt = (
        publication._StagingSummary(
            scope="A",
            publication_date=BUILD_DATE,
            task_run_id=102,
            execution_count=3,
            row_count=2,
            provenance=PROVENANCE,
        ),
    )
    with pytest.raises(ValueError, match="attempt does not match"):
        publication._validate_staging_against_events(
            wrong_attempt,
            selected_events=events,
            scope_contract=SCOPE_CONTRACT,
            build_date=BUILD_DATE,
        )

    wrong_count = (
        publication._StagingSummary(
            scope="A",
            publication_date=BUILD_DATE,
            task_run_id=101,
            execution_count=2,
            row_count=1,
            provenance=PROVENANCE,
        ),
    )
    with pytest.raises(ValueError, match="count mismatch"):
        publication._validate_staging_against_events(
            wrong_count,
            selected_events=events,
            scope_contract=SCOPE_CONTRACT,
            build_date=BUILD_DATE,
        )


def test_staging_rejects_unexpected_scope_and_build_date():
    events = (
        _selected_event("A", READY, 1),
        _selected_event("B", NO_ADS, 0),
    )
    unexpected = (
        publication._StagingSummary(
            scope="C",
            publication_date=BUILD_DATE,
            task_run_id=100,
            execution_count=1,
            row_count=1,
            provenance=PROVENANCE,
        ),
    )
    with pytest.raises(ValueError, match="Unexpected staged"):
        publication._validate_staging_against_events(
            unexpected,
            selected_events=events,
            scope_contract=SCOPE_CONTRACT,
            build_date=BUILD_DATE,
        )

    wrong_date = (
        publication._StagingSummary(
            scope="A",
            publication_date=BUILD_DATE - timedelta(days=1),
            task_run_id=100,
            execution_count=1,
            row_count=1,
            provenance=PROVENANCE,
        ),
    )
    with pytest.raises(ValueError, match="does not match BuildDate"):
        publication._validate_staging_against_events(
            wrong_date,
            selected_events=events,
            scope_contract=SCOPE_CONTRACT,
            build_date=BUILD_DATE,
        )


def _configure_successful_build(monkeypatch, *, key_error=None):
    event_frame = object()
    staged = FakeFrame(
        [
            *SCOPE_CONTRACT.public_columns,
            "BuildRunID",
            "TaskRunID",
            "ExecutionCount",
            *PROVENANCE_COLUMNS,
        ]
    )
    event_rows = [
        _event(
            "A",
            READY,
            2,
            task_run_id=101,
            execution_count=2,
        ),
        _event(
            "A",
            READY,
            1,
            task_run_id=100,
            execution_count=1,
        ),
        _event(
            "B",
            NO_ADS,
            0,
            task_run_id=201,
            execution_count=2,
        ),
    ]
    staging_summaries = (
        publication._StagingSummary(
            scope="A",
            publication_date=BUILD_DATE,
            task_run_id=101,
            execution_count=2,
            row_count=2,
            provenance=PROVENANCE,
        ),
    )
    calls = []

    def fake_read(
        spark,
        table,
        *,
        build_run_id_column,
        build_run_id,
    ):
        calls.append(
            (
                "read",
                spark,
                table,
                build_run_id_column,
                build_run_id,
            )
        )
        return event_frame if table == TABLES.event_table else staged

    monkeypatch.setattr(publication, "_read_build_frame", fake_read)
    monkeypatch.setattr(
        publication,
        "_collect_event_rows",
        lambda frame, columns: event_rows,
    )
    monkeypatch.setattr(
        publication,
        "_collect_staging_summaries",
        lambda frame, **kwargs: staging_summaries,
    )

    def fake_validate_keys(df, keys):
        calls.append(("keys", df, list(keys)))
        if key_error is not None:
            raise ValueError(key_error)
        return KeyValidationSummary(2, 2, 0)

    monkeypatch.setattr(
        publication,
        "validate_unique_non_null_keys",
        fake_validate_keys,
    )
    monkeypatch.setattr(
        publication,
        "validate_target_columns",
        lambda spark, table, columns: calls.append(
            ("target", spark, table, list(columns))
        ),
    )

    def fake_publish(spark, df, **kwargs):
        calls.append(("publish", spark, df, kwargs))
        return SimpleNamespace(
            validation=KeyValidationSummary(2, 2, 0),
            history_write=SimpleNamespace(
                statement="history",
                attempts=1,
            ),
            latest_write=SimpleNamespace(
                statement="latest",
                attempts=1,
            ),
        )

    monkeypatch.setattr(
        publication,
        "publish_history_and_latest",
        fake_publish,
    )
    monkeypatch.setattr(
        publication,
        "_prepare_v1_public_assignments",
        lambda spark, staged_assignments, **kwargs: (
            calls.append(
                ("prepare", spark, staged_assignments, kwargs)
            )
            or staged_assignments
        ),
    )
    return staged, calls


def test_complete_build_validates_before_history_then_latest_publication(
    monkeypatch,
):
    staged, calls = _configure_successful_build(monkeypatch)

    result = validate_and_publish_assignment_build(
        object(),
        tables=TABLES,
        columns=COLUMNS,
        scope_contract=SCOPE_CONTRACT,
        build_run_id=BUILD_RUN_ID,
        build_date=BUILD_DATE,
    )

    operation_names = [call[0] for call in calls]
    assert operation_names == [
        "read",
        "read",
        "keys",
        "target",
        "target",
        "prepare",
        "publish",
    ]
    key_call = calls[2]
    assert key_call[2] == ["AccountNumber", "Location", "rundate"]
    assert key_call[1].columns == list(SCOPE_CONTRACT.public_columns)
    assert [calls[3][2], calls[4][2]] == [
        TABLES.history_table,
        TABLES.latest_table,
    ]
    prepare_kwargs = calls[5][3]
    assert prepare_kwargs["selected_events"][1].status == NO_ADS
    assert prepare_kwargs["build_date"] == BUILD_DATE
    publish_kwargs = calls[6][3]
    assert publish_kwargs == {
        "history_table": TABLES.history_table,
        "latest_table": TABLES.latest_table,
        "key_columns": SCOPE_CONTRACT.key_columns,
        "run_date": BUILD_DATE,
        "run_date_column": "rundate",
        "columns": SCOPE_CONTRACT.public_columns,
    }
    assert calls[6][2].columns == list(SCOPE_CONTRACT.public_columns)
    assert result.row_count == 2
    assert result.validation == KeyValidationSummary(2, 2, 0)
    assert [(event.scope, event.status) for event in result.events] == [
        ("A", READY),
        ("B", NO_ADS),
    ]
    assert staged.persisted
    assert staged.unpersisted


@pytest.mark.parametrize(
    "key_error",
    [
        "Duplicate values found for assignment keys",
        "Null values found in assignment keys",
    ],
)
def test_invalid_public_keys_abort_before_live_writes(monkeypatch, key_error):
    staged, calls = _configure_successful_build(
        monkeypatch,
        key_error=key_error,
    )

    with pytest.raises(ValueError, match=key_error):
        validate_and_publish_assignment_build(
            object(),
            tables=TABLES,
            columns=COLUMNS,
            scope_contract=SCOPE_CONTRACT,
            build_run_id=BUILD_RUN_ID,
            build_date=BUILD_DATE,
        )

    assert "publish" not in [call[0] for call in calls]
    assert "target" not in [call[0] for call in calls]
    assert staged.unpersisted


@pytest.mark.parametrize(
    "failing_table",
    [TABLES.history_table, TABLES.latest_table],
)
def test_target_schema_failure_aborts_before_publication(
    monkeypatch,
    failing_table,
):
    staged, calls = _configure_successful_build(monkeypatch)

    def fail_selected_target(spark, table, columns):
        calls.append(("target", spark, table, list(columns)))
        if table == failing_table:
            raise ValueError(f"schema mismatch for {table}")

    monkeypatch.setattr(
        publication,
        "validate_target_columns",
        fail_selected_target,
    )

    with pytest.raises(ValueError, match="schema mismatch"):
        validate_and_publish_assignment_build(
            object(),
            tables=TABLES,
            columns=COLUMNS,
            scope_contract=SCOPE_CONTRACT,
            build_run_id=BUILD_RUN_ID,
            build_date=BUILD_DATE,
        )

    assert "publish" not in [call[0] for call in calls]
    assert staged.unpersisted


def test_staging_attempt_mismatch_aborts_before_live_writes(monkeypatch):
    staged, calls = _configure_successful_build(monkeypatch)
    monkeypatch.setattr(
        publication,
        "_collect_staging_summaries",
        lambda frame, **kwargs: (
            publication._StagingSummary(
                scope="A",
                publication_date=BUILD_DATE,
                task_run_id=102,
                execution_count=3,
                row_count=2,
                provenance=PROVENANCE,
            ),
        ),
    )

    with pytest.raises(ValueError, match="attempt does not match"):
        validate_and_publish_assignment_build(
            object(),
            tables=TABLES,
            columns=COLUMNS,
            scope_contract=SCOPE_CONTRACT,
            build_run_id=BUILD_RUN_ID,
            build_date=BUILD_DATE,
        )

    assert "publish" not in [call[0] for call in calls]
    assert "keys" not in [call[0] for call in calls]
    assert staged.unpersisted


def test_v1_no_ads_scope_is_absent_from_the_new_accepted_snapshot(
    local_spark,
):
    contract = AssignmentScopeContract(
        route="v1",
        scope_column="Location",
        expected_scopes=("A", "B"),
        key_columns=("AccountNumber", "Location"),
        public_columns=(
            "AccountNumber",
            "Location",
            "MASID",
            "rundate",
        ),
    )
    staged = local_spark.createDataFrame(
        [("new", "A", "A_NEW", BUILD_DATE)],
        contract.public_columns,
    )
    table_reads = []
    spark_stub = SimpleNamespace(
        table=lambda table: table_reads.append(table)
    )
    selected_events = (
        _selected_event("A", READY, 1),
        _selected_event("B", NO_ADS, 0),
    )

    result = publication._prepare_v1_public_assignments(
        spark_stub,
        staged,
        tables=TABLES,
        scope_contract=contract,
        selected_events=selected_events,
        build_date=BUILD_DATE,
    )

    assert table_reads == []
    assert sorted(
        tuple(row[column] for column in contract.public_columns)
        for row in result.collect()
    ) == [
        ("new", "A", "A_NEW", BUILD_DATE),
    ]


def test_v1_teaser_correction_removes_only_affected_ph3_ph4_rows(
    local_spark,
):
    assignments = local_spark.createDataFrame(
        [
            ("all-control", "PH3", "PH3_Z"),
            ("all-control", "PH4", "PH4_Z"),
            ("duplicate-token", "PH3", "PH3_A"),
            ("duplicate-token", "PH4", "PH4_A"),
            ("duplicate-token", "SB1", "SB1_C"),
            ("missing-teaser", "PH3", "PH3_A"),
            ("missing-teaser", "SB1", "SB1_C"),
            ("mixed-control", "PH3", "PH3_Z"),
            ("mixed-control", "PH4", "PH4_A"),
            ("valid", "PH3", "PH3_A"),
            ("valid", "PH4", "PH4_B"),
        ],
        ["AccountNumber", "Location", "MASID"],
    )

    corrected = publication._remove_invalid_v1_teaser_assignments(assignments)

    assert sorted(
        (row["AccountNumber"], row["Location"], row["MASID"])
        for row in corrected.collect()
    ) == [
        ("all-control", "PH3", "PH3_Z"),
        ("all-control", "PH4", "PH4_Z"),
        ("duplicate-token", "SB1", "SB1_C"),
        ("missing-teaser", "SB1", "SB1_C"),
        ("valid", "PH3", "PH3_A"),
        ("valid", "PH4", "PH4_B"),
    ]


def test_v1_publishes_one_identical_corrected_frame_to_history_and_latest(
    monkeypatch,
    local_spark,
):
    contract = AssignmentScopeContract(
        route="v1",
        scope_column="Location",
        expected_scopes=("PH3", "PH4", "SB1"),
        key_columns=("AccountNumber", "Location"),
        public_columns=(
            "AccountNumber",
            "Location",
            "MASID",
            "rundate",
        ),
    )
    staged = local_spark.createDataFrame(
        [
            (
                BUILD_RUN_ID,
                101,
                1,
                "bad",
                "PH3",
                "PH3_A",
                BUILD_DATE,
            ),
            (
                BUILD_RUN_ID,
                102,
                1,
                "bad",
                "PH4",
                "PH4_A",
                BUILD_DATE,
            ),
            (
                BUILD_RUN_ID,
                103,
                1,
                "bad",
                "SB1",
                "SB1_C",
                BUILD_DATE,
            ),
            (
                BUILD_RUN_ID,
                101,
                1,
                "good",
                "PH3",
                "PH3_A",
                BUILD_DATE,
            ),
            (
                BUILD_RUN_ID,
                102,
                1,
                "good",
                "PH4",
                "PH4_B",
                BUILD_DATE,
            ),
        ],
        [
            "BuildRunID",
            "TaskRunID",
            "ExecutionCount",
            *PROVENANCE_COLUMNS,
            *contract.public_columns,
        ],
    )
    event_rows = [
        _event("PH3", READY, 2, task_run_id=101),
        _event("PH4", READY, 2, task_run_id=102),
        _event("SB1", READY, 1, task_run_id=103),
    ]
    reads = iter((object(), staged))
    monkeypatch.setattr(
        publication,
        "_read_build_frame",
        lambda *args, **kwargs: next(reads),
    )
    monkeypatch.setattr(
        publication,
        "_collect_event_rows",
        lambda *args, **kwargs: event_rows,
    )
    monkeypatch.setattr(
        publication,
        "validate_target_columns",
        lambda *args, **kwargs: None,
    )

    correction_calls = []
    real_correction = publication._remove_invalid_v1_teaser_assignments

    def track_correction(assignments):
        correction_calls.append(assignments)
        return real_correction(assignments)

    monkeypatch.setattr(
        publication,
        "_remove_invalid_v1_teaser_assignments",
        track_correction,
    )
    snapshots = {}

    def capture_publication(_spark, assignments, **_kwargs):
        rows = sorted(
            tuple(row[column] for column in contract.public_columns)
            for row in assignments.collect()
        )
        snapshots["history"] = rows
        snapshots["latest"] = list(rows)
        return SimpleNamespace(
            validation=KeyValidationSummary(3, 3, 0),
            history_write=SimpleNamespace(statement="history", attempts=1),
            latest_write=SimpleNamespace(statement="latest", attempts=1),
        )

    monkeypatch.setattr(
        publication,
        "publish_history_and_latest",
        capture_publication,
    )

    result = validate_and_publish_assignment_build(
        local_spark,
        tables=TABLES,
        columns=COLUMNS,
        scope_contract=contract,
        build_run_id=BUILD_RUN_ID,
        build_date=BUILD_DATE,
    )

    assert len(correction_calls) == 1
    assert snapshots["history"] == snapshots["latest"]
    assert snapshots["latest"] == [
        ("bad", "SB1", "SB1_C", BUILD_DATE),
        ("good", "PH3", "PH3_A", BUILD_DATE),
        ("good", "PH4", "PH4_B", BUILD_DATE),
    ]
    assert result.row_count == 3
    assert result.validation == KeyValidationSummary(3, 3, 0)


def test_scope_contract_supports_v1_and_v2_public_schemas():
    v1 = SCOPE_CONTRACT
    v2 = AssignmentScopeContract(
        route="v2",
        scope_column="PageType",
        expected_scopes=("HomePage", "ShoppingBagPage"),
        key_columns=("AccountNumber", "PageType", "Rank"),
        public_columns=(
            "AccountNumber",
            "PageType",
            "Rank",
            "UniqueAdIDAssigned",
            "TriggerScore",
            "rundate",
        ),
    )

    assert v1.scope_column == "Location"
    assert v2.scope_column == "PageType"
    assert v1.public_columns[-1] == "rundate"
    assert v2.public_columns[-1] == "rundate"


def test_v2_complete_scope_set_accepts_one_no_ads_event():
    build_run_id = "v2_456"
    page_types = (
        "HomePage",
        "ShoppingBagPage",
        "CheckoutPage",
        "ProductListingPage",
        "ForYouPage",
    )
    scope_contract = AssignmentScopeContract(
        route="v2",
        scope_column="PageType",
        expected_scopes=page_types,
        key_columns=("AccountNumber", "PageType", "Rank"),
        public_columns=(
            "AccountNumber",
            "PageType",
            "Rank",
            "UniqueAdIDAssigned",
            "rundate",
        ),
    )
    event_rows = [
        _event(
            scope,
            NO_ADS if scope == "CheckoutPage" else READY,
            0 if scope == "CheckoutPage" else 1,
            task_run_id=100 + index,
            build_run_id=build_run_id,
            route="v2",
        )
        for index, scope in enumerate(page_types)
    ]

    selected_events = publication._select_latest_scope_events(
        event_rows,
        columns=COLUMNS,
        scope_contract=scope_contract,
        build_run_id=build_run_id,
        build_date=BUILD_DATE,
    )
    staging_summaries = tuple(
        publication._StagingSummary(
            scope=event.scope,
            publication_date=BUILD_DATE,
            task_run_id=event.task_run_id,
            execution_count=event.execution_count,
            row_count=event.row_count,
            provenance=event.provenance,
        )
        for event in selected_events
        if event.status == READY
    )

    assert [event.scope for event in selected_events] == list(page_types)
    assert [
        event.scope for event in selected_events if event.status == NO_ADS
    ] == ["CheckoutPage"]
    assert (
        publication._validate_staging_against_events(
            staging_summaries,
            selected_events=selected_events,
            scope_contract=scope_contract,
            build_date=BUILD_DATE,
        )
        == 4
    )


@pytest.mark.parametrize("route", ["", "V1", "legacy"])
def test_scope_contract_rejects_routes_outside_v1_and_v2(route):
    with pytest.raises(ValueError, match="route must be one of"):
        AssignmentScopeContract(
            route=route,
            scope_column="Location",
            expected_scopes=("A",),
            key_columns=("AccountNumber", "Location"),
            public_columns=("AccountNumber", "Location", "rundate"),
        )


@pytest.mark.parametrize(
    ("build_run_id", "task_run_id", "message"),
    [
        ("v2_123", 1, "must start with 'v1_'"),
        ("v1_", 1, "must start with 'v1_'"),
        ("v1_123", 0, "TaskRunID must be at least 1"),
    ],
)
def test_stage_rejects_invalid_build_and_task_run_identity(
    monkeypatch,
    build_run_id,
    task_run_id,
    message,
):
    monkeypatch.setattr(
        publication,
        "replace_validated_scope",
        lambda *args, **kwargs: pytest.fail("staging write was reached"),
    )

    with pytest.raises(ValueError, match=message):
        stage_assignment_scope(
            FakeSpark(),
            FakeFrame(["AccountNumber", "Location", "UniqueAdIDAssigned"]),
            tables=TABLES,
            columns=COLUMNS,
            scope_contract=SCOPE_CONTRACT,
            build_run_id=build_run_id,
            build_date=BUILD_DATE,
            scope="A",
            task_run_id=task_run_id,
            execution_count=1,
            provenance=PROVENANCE,
        )

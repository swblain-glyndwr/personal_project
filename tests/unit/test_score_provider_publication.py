import json
from datetime import date, datetime, timedelta, timezone

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

import next_ads.ranking.provider_publication as publication
from next_ads.ranking.provider_context import ProviderContext
from next_ads.ranking.provider_publication import (
    ProviderOutputSummary,
    publish_provider_build,
    stage_provider_signals,
    summarise_provider_signals,
)
from next_ads.ranking.scoring_manifest import READY_FOR_NEXTADS


RUN_DATE = date(2026, 8, 3)
NOW = datetime(2026, 8, 3, 15, 0, tzinfo=timezone.utc)
SIGNALS_TABLE = "catalog.schema.score_provider_signals"
BUILDS_TABLE = "catalog.schema.score_provider_builds"
SIGNALS_VERSION = 42
MAX_ENTITIES_PER_ACCOUNT = 100

SIGNAL_SCHEMA = StructType(
    [
        StructField("ProviderBuildID", StringType(), True),
        StructField("AccountNumber", StringType(), True),
        StructField("EntityType", StringType(), True),
        StructField("EntityID", StringType(), True),
        StructField("ProviderID", StringType(), True),
        StructField("RunDate", DateType(), True),
        StructField("RawScore", DoubleType(), True),
        StructField("Score", DoubleType(), True),
        StructField("ProviderRank", IntegerType(), True),
    ]
)


def _context() -> ProviderContext:
    foundation_build_id = "account-theme-foundation"
    foundation_attempt_id = "account-theme-foundation:task:0"
    return ProviderContext(
        context_slot="theme_affinity_serving",
        orchestration_run_id=123,
        provider_id="theme_affinity",
        provider_build_id="theme-affinity-build",
        provider_build_attempt_id="theme-affinity-build:task:0",
        input_snapshot_id="scoring-inputs-20260803",
        run_date=RUN_DATE,
        model_uri="models:/catalog.schema.theme_affinity/7",
        bindings_json=json.dumps(
            {
                "foundation": {
                    "scoring_foundation_build_id": foundation_build_id,
                    "scoring_foundation_build_attempt_id": (
                        foundation_attempt_id
                    ),
                    "pipeline_update_id": "pipeline-update-123",
                    "outputs": {},
                }
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        capability="account_theme",
        use_case="theme_ranking",
        invocation_checksum="provider-invocation-checksum",
        expires_at=NOW + timedelta(hours=8),
        scoring_foundation_build_id=foundation_build_id,
        scoring_foundation_build_attempt_id=foundation_attempt_id,
    )


def _provider_config() -> dict:
    return {
        "provider_id": "theme_affinity",
        "provider_version": "theme_affinity/v1",
        "implementation": "theme_affinity",
        "capability": "account_theme",
        "entity_type": "theme",
        "max_entities_per_account": MAX_ENTITIES_PER_ACCOUNT,
    }


def _valid_rows():
    context = _context()
    return [
        (
            context.provider_build_id,
            "account-a",
            "theme",
            "menswear",
            context.provider_id,
            RUN_DATE,
            0.75,
            0.75,
            1,
        ),
        (
            context.provider_build_id,
            "account-a",
            "theme",
            "footwear",
            context.provider_id,
            RUN_DATE,
            0.50,
            0.50,
            2,
        ),
        (
            context.provider_build_id,
            "account-b",
            "theme",
            "menswear",
            context.provider_id,
            RUN_DATE,
            0.25,
            0.25,
            1,
        ),
    ]


def _frame_for_case(spark, case: str):
    rows = _valid_rows()
    if case == "empty":
        rows = []
    elif case == "null_key":
        rows[0] = (rows[0][0], None, *rows[0][2:])
    elif case == "duplicate_key":
        rows = [rows[0], rows[0]]
    elif case == "wrong_date":
        rows[0] = (*rows[0][:5], RUN_DATE - timedelta(days=1), *rows[0][6:])
    elif case == "null_score":
        rows[0] = (*rows[0][:6], None, rows[0][7], rows[0][8])
    elif case == "nan_score":
        rows[0] = (*rows[0][:6], float("nan"), rows[0][7], rows[0][8])
    elif case == "positive_infinite_score":
        rows[0] = (*rows[0][:6], rows[0][6], float("inf"), rows[0][8])
    elif case == "negative_infinite_score":
        rows[0] = (*rows[0][:6], float("-inf"), rows[0][7], rows[0][8])
    elif case == "invalid_rank":
        rows[0] = (*rows[0][:8], MAX_ENTITIES_PER_ACCOUNT + 1)
    else:
        raise ValueError(f"Unsupported provider-output case: {case}")
    return spark.createDataFrame(rows, schema=SIGNAL_SCHEMA)


class _AggregateActionCounter:
    def __init__(self, frame):
        self._frame = frame
        self.aggregate_calls = 0
        self.action_calls = 0

    @property
    def columns(self):
        return self._frame.columns

    def agg(self, *expressions):
        self.aggregate_calls += 1
        aggregated = self._frame.agg(*expressions)
        counter = self

        class _Aggregated:
            def first(self):
                counter.action_calls += 1
                return aggregated.first()

        return _Aggregated()


def test_provider_summary_validates_all_contract_fields_with_one_aggregation(
    spark,
):
    counted = _AggregateActionCounter(
        spark.createDataFrame(_valid_rows(), schema=SIGNAL_SCHEMA)
    )

    summary = summarise_provider_signals(
        counted,
        context=_context(),
        max_entities_per_account=MAX_ENTITIES_PER_ACCOUNT,
    )

    assert isinstance(summary, ProviderOutputSummary)
    assert summary.row_count == 3
    assert summary.account_count == 2
    assert summary.entity_count == 2
    assert summary.null_key_count == 0
    assert summary.duplicate_key_count == 0
    assert summary.invalid_score_count == 0
    assert summary.wrong_metadata_count == 0
    assert summary.invalid_rank_count == 0
    assert summary.output_checksum
    summary.require_valid(_context().provider_build_id)
    assert counted.aggregate_calls == 1
    assert counted.action_calls == 1


@pytest.mark.parametrize(
    ("case", "summary_field", "error_pattern"),
    [
        ("empty", "row_count", "empty"),
        ("null_key", "null_key_count", "null"),
        ("duplicate_key", "duplicate_key_count", "duplicate"),
        ("wrong_date", "wrong_metadata_count", "metadata"),
        ("null_score", "invalid_score_count", "score"),
        ("nan_score", "invalid_score_count", "score"),
        ("positive_infinite_score", "invalid_score_count", "score"),
        ("negative_infinite_score", "invalid_score_count", "score"),
        ("invalid_rank", "invalid_rank_count", "rank"),
    ],
)
def test_provider_summary_rejects_unsafe_output(
    spark,
    case,
    summary_field,
    error_pattern,
):
    summary = summarise_provider_signals(
        _frame_for_case(spark, case),
        context=_context(),
        max_entities_per_account=MAX_ENTITIES_PER_ACCOUNT,
    )

    if case == "empty":
        assert summary.row_count == 0
    else:
        assert getattr(summary, summary_field) > 0
    with pytest.raises(ValueError, match=error_pattern):
        summary.require_valid(_context().provider_build_id)


def test_provider_summary_checksum_is_partition_and_order_stable(spark):
    frame = spark.createDataFrame(_valid_rows(), schema=SIGNAL_SCHEMA)
    variants = (
        frame.repartition(1),
        frame.orderBy(F.desc("EntityID"), F.desc("AccountNumber")).repartition(4),
        frame.orderBy("Score", "AccountNumber").repartition(8),
    )

    summaries = [
        summarise_provider_signals(
            variant,
            context=_context(),
            max_entities_per_account=MAX_ENTITIES_PER_ACCOUNT,
        )
        for variant in variants
    ]

    assert summaries[0] == summaries[1] == summaries[2]
    assert len({summary.output_checksum for summary in summaries}) == 1


def test_provider_signal_staging_is_build_scoped_and_returns_exact_version(
    spark,
    monkeypatch,
):
    versions = iter((41, SIGNALS_VERSION))
    writes = []
    monkeypatch.setattr(
        publication,
        "latest_delta_version",
        lambda _spark, table: (
            next(versions) if table == SIGNALS_TABLE else None
        ),
    )

    def replace(frame, table, scope, columns, *, spark):
        writes.append((frame, table, scope, tuple(columns)))

    monkeypatch.setattr(publication, "replace_scope_by_name", replace)
    frame = spark.createDataFrame(_valid_rows(), schema=SIGNAL_SCHEMA)

    version = stage_provider_signals(
        spark,
        frame,
        context=_context(),
        table=SIGNALS_TABLE,
    )

    assert version == SIGNALS_VERSION
    assert len(writes) == 1
    assert writes[0][1] == SIGNALS_TABLE
    assert writes[0][2]["ProviderBuildID"] == _context().provider_build_id


@pytest.mark.parametrize(
    "case",
    [
        "empty",
        "null_key",
        "duplicate_key",
        "wrong_date",
        "null_score",
        "nan_score",
        "positive_infinite_score",
        "negative_infinite_score",
        "invalid_rank",
    ],
)
def test_rejected_staged_build_never_calls_compatibility_or_live_writers(
    spark,
    monkeypatch,
    case,
):
    operations = []
    staged = _frame_for_case(spark, case)

    def read_version(_spark, table, version):
        operations.append(("read_staged", table, version))
        return staged

    def compatibility_publisher(_frame, _completed_at):
        operations.append(("compatibility",))
        return {"history": 11, "inference_log": 12, "latest": 13}

    def manifest_write(*_args, **_kwargs):
        operations.append(("ready_manifest",))

    monkeypatch.setattr(publication, "read_delta_version", read_version)
    monkeypatch.setattr(publication, "replace_scope_by_name", manifest_write)

    with pytest.raises(ValueError):
        publish_provider_build(
            spark,
            context=_context(),
            signals_table=SIGNALS_TABLE,
            signals_delta_version=SIGNALS_VERSION,
            builds_table=BUILDS_TABLE,
            provider_config=_provider_config(),
            contract_version="account_entity_scores/v1",
            compatibility_publisher=compatibility_publisher,
            task_run_id=456,
            execution_count=0,
            completed_at=NOW,
        )

    assert operations == [("read_staged", SIGNALS_TABLE, SIGNALS_VERSION)]


def test_ready_manifest_is_the_last_write_and_binds_exact_staged_output(
    spark,
    monkeypatch,
):
    operations = []
    manifest_frames = []
    staged = spark.createDataFrame(_valid_rows(), schema=SIGNAL_SCHEMA)
    real_summary = publication.summarise_provider_signals

    def read_version(_spark, table, version):
        operations.append(("read_staged", table, version))
        return staged

    def summarise(frame, *, context, max_entities_per_account):
        operations.append(("validate",))
        return real_summary(
            frame,
            context=context,
            max_entities_per_account=max_entities_per_account,
        )

    compatibility_versions = {
        "history": 11,
        "inference_log": 12,
        "latest": 13,
    }

    def compatibility_publisher(frame, completed_at):
        assert sorted(tuple(row) for row in frame.collect()) == sorted(
            tuple(row) for row in staged.collect()
        )
        assert completed_at == NOW
        operations.extend(
            [("history",), ("inference_log",), ("latest",)]
        )
        return compatibility_versions

    def manifest_write(frame, table, scope, columns, *, spark):
        operations.append(("ready_manifest", table, scope, tuple(columns)))
        manifest_frames.append(frame)

    monkeypatch.setattr(publication, "read_delta_version", read_version)
    monkeypatch.setattr(publication, "summarise_provider_signals", summarise)
    monkeypatch.setattr(publication, "replace_scope_by_name", manifest_write)

    result = publish_provider_build(
        spark,
        context=_context(),
        signals_table=SIGNALS_TABLE,
        signals_delta_version=SIGNALS_VERSION,
        builds_table=BUILDS_TABLE,
        provider_config=_provider_config(),
        contract_version="account_entity_scores/v1",
        compatibility_publisher=compatibility_publisher,
        task_run_id=456,
        execution_count=0,
        completed_at=NOW,
    )

    assert [operation[0] for operation in operations] == [
        "read_staged",
        "validate",
        "history",
        "inference_log",
        "latest",
        "ready_manifest",
    ]
    assert operations[-1][1] == BUILDS_TABLE
    assert operations[-1][2] == {
        "ProviderBuildAttemptID": _context().provider_build_attempt_id
    }
    assert result.compatibility_output_versions == compatibility_versions

    build = result.build
    assert build.status == READY_FOR_NEXTADS
    assert build.provider_build_id == _context().provider_build_id
    assert build.provider_build_attempt_id == (
        _context().provider_build_attempt_id
    )
    assert build.input_snapshot_id == _context().input_snapshot_id
    assert build.run_date == RUN_DATE
    assert build.provider_id == _context().provider_id
    assert build.provider_version == "theme_affinity/v1"
    assert build.contract_version == "account_entity_scores/v1"
    assert build.model_uri == _context().model_uri
    assert build.model_version == "7"
    assert build.pipeline_update_id == "pipeline-update-123"
    assert build.output_table == SIGNALS_TABLE
    assert build.output_delta_version == SIGNALS_VERSION
    assert build.row_count == 3
    assert build.account_count == 2
    assert build.entity_count == 2
    assert build.null_key_count == 0
    assert build.duplicate_key_count == 0
    assert build.invalid_score_count == 0
    assert build.output_checksum
    assert build.scoring_foundation_build_id == (
        _context().scoring_foundation_build_id
    )
    assert build.scoring_foundation_build_attempt_id == (
        _context().scoring_foundation_build_attempt_id
    )
    assert build.task_run_id == 456
    assert build.execution_count == 0
    assert build.completed_at == NOW

    assert len(manifest_frames) == 1
    manifest_row = manifest_frames[0].collect()[0].asDict()
    assert manifest_row["Status"] == READY_FOR_NEXTADS
    assert manifest_row["ProviderBuildAttemptID"] == (
        _context().provider_build_attempt_id
    )
    assert manifest_row["OutputTable"] == SIGNALS_TABLE
    assert manifest_row["OutputDeltaVersion"] == SIGNALS_VERSION

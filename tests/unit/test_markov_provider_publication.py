from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from pyspark.sql import functions as F

from next_ads.ranking import provider_compatibility
from next_ads.ranking.provider_compatibility import (
    build_markov_compatibility_scores,
    configured_compatibility_publisher,
    publish_markov_compatibility_outputs,
)
from next_ads.ranking.provider_context import ProviderContext
from next_ads.ranking.provider_publication import summarise_provider_signals
from next_ads.ranking.provider_signals import (
    adapt_configured_provider_scores,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUN_DATE = date(2026, 8, 5)
NOW = datetime(2026, 8, 5, 13, 0, tzinfo=timezone.utc)


def _context(
    *,
    provider_id="markov",
    capability="account_theme",
    entity_type="theme",
):
    return ProviderContext(
        context_slot=f"{provider_id}_scoring",
        orchestration_run_id=123,
        provider_id=provider_id,
        provider_build_id=f"{provider_id}_build",
        provider_build_attempt_id=f"{provider_id}_build:456:0",
        input_snapshot_id="scoring_inputs_20260805",
        run_date=RUN_DATE,
        model_uri=f"code:/{provider_id}/v1",
        bindings_json="{}",
        capability=capability,
        use_case=f"{entity_type}_ranking",
        invocation_checksum="checksum",
        expires_at=NOW + timedelta(hours=8),
    )


def _markov_config():
    return {
        "provider_id": "markov",
        "provider_version": "markov/v1",
        "implementation": "markov",
        "capability": "account_theme",
        "adapter": "legacy_account_entity_table",
        "entity_type": "theme",
        "score_direction": "higher_is_better",
        "max_entities_per_account": 100,
        "account_number_column": "AccountNumber",
        "entity_id_column": "NextTheme",
        "raw_score_column": "ProbAgg",
        "score_column": "ProbAggRebased",
        "compatibility_publisher": "markov_legacy",
    }


def _rows(frame):
    return sorted(tuple(row[column] for column in frame.columns) for row in frame.collect())


def test_markov_adapter_and_checksum_are_partition_stable(spark):
    source = spark.createDataFrame(
        [
            ("account-a", "menswear", 0.8, 0.2, 0.6),
            ("account-a", "footwear", 0.5, 0.1, 0.4),
            ("account-b", "homeware", 0.7, 0.3, 0.4),
        ],
        [
            "AccountNumber",
            "NextTheme",
            "ProbAgg",
            "ProbBase",
            "ProbAggRebased",
        ],
    )
    context = _context()
    variants = [
        adapt_configured_provider_scores(
            source.repartition(partitions),
            context=context,
            provider_config=_markov_config(),
        )
        for partitions in (1, 4, 8)
    ]
    summaries = [
        summarise_provider_signals(
            frame,
            context=context,
            max_entities_per_account=100,
        )
        for frame in variants
    ]

    assert _rows(variants[0]) == _rows(variants[1]) == _rows(variants[2])
    assert summaries[0] == summaries[1] == summaries[2]
    assert summaries[0].output_checksum


def test_markov_legacy_and_canonical_scores_match_within_tolerance(spark):
    source = spark.createDataFrame(
        [
            ("account-a", "menswear", 0.8, 0.2, 0.6),
            ("account-a", "footwear", 0.5, 0.1, 0.4),
        ],
        [
            "AccountNumber",
            "NextTheme",
            "ProbAgg",
            "ProbBase",
            "ProbAggRebased",
        ],
    )
    canonical = adapt_configured_provider_scores(
        source,
        context=_context(),
        provider_config=_markov_config(),
    )
    compatibility = build_markov_compatibility_scores(canonical)
    differences = (
        source.alias("source")
        .join(
            compatibility.alias("legacy"),
            ["AccountNumber", "NextTheme"],
        )
        .select(
            F.abs(F.col("source.ProbAgg") - F.col("legacy.ProbAgg")).alias(
                "raw_difference"
            ),
            F.abs(
                F.col("source.ProbAggRebased")
                - F.col("legacy.ProbAggRebased")
            ).alias("score_difference"),
            F.abs(F.col("source.ProbBase") - F.col("legacy.ProbBase")).alias(
                "base_difference"
            ),
        )
        .agg(
            F.max("raw_difference").alias("raw_difference"),
            F.max("score_difference").alias("score_difference"),
            F.max("base_difference").alias("base_difference"),
        )
        .first()
    )

    assert max(float(value) for value in differences) <= 1e-6


def test_same_adapter_accepts_a_non_theme_provider(spark):
    context = _context(
        provider_id="ad_ctr",
        capability="account_ad",
        entity_type="ad",
    )
    config = {
        **_markov_config(),
        "provider_id": "ad_ctr",
        "provider_version": "ad_ctr/v1",
        "implementation": "ad_ctr",
        "capability": "account_ad",
        "entity_type": "ad",
        "entity_id_column": "UniqueAdID",
        "raw_score_column": "PredictedCTR",
        "score_column": "PredictedCTR",
        "compatibility_publisher": "none",
    }
    source = spark.createDataFrame(
        [("account-a", "ad-1", 0.25)],
        ["AccountNumber", "UniqueAdID", "PredictedCTR"],
    )

    row = adapt_configured_provider_scores(
        source,
        context=context,
        provider_config=config,
    ).first()

    assert row["EntityType"] == "ad"
    assert row["EntityID"] == "ad-1"
    assert row["ProviderID"] == "ad_ctr"
    assert row["Score"] == 0.25
    publisher = configured_compatibility_publisher(
        spark,
        config=SimpleNamespace(),
        context=context,
        provider_config=config,
    )
    assert publisher(source, NOW) == {}


def test_markov_compatibility_publication_updates_history_then_latest(
    spark,
    monkeypatch,
):
    context = _context()
    signals = adapt_configured_provider_scores(
        spark.createDataFrame(
            [("account-a", "menswear", 0.8, 0.6)],
            ["AccountNumber", "NextTheme", "ProbAgg", "ProbAggRebased"],
        ),
        context=context,
        provider_config=_markov_config(),
    )
    versions = iter((10, 11, 20, 21))
    operations = []
    monkeypatch.setattr(
        provider_compatibility,
        "latest_delta_version",
        lambda _spark, _table: next(versions),
    )
    monkeypatch.setattr(
        provider_compatibility,
        "replace_scope_by_name",
        lambda frame, table, scope, columns, *, spark: operations.append(
            ("history", table, scope, tuple(columns), frame.count())
        ),
    )
    monkeypatch.setattr(
        provider_compatibility,
        "replace_table_by_name",
        lambda frame, table, columns, *, spark: operations.append(
            ("latest", table, tuple(columns), frame.count())
        ),
    )
    config = SimpleNamespace(
        tables_write=SimpleNamespace(
            next_theme_scores="catalog.schema.markov_history",
            next_theme_scores_latest="catalog.schema.markov_latest",
        )
    )

    versions = publish_markov_compatibility_outputs(
        spark,
        config,
        context,
        signals,
        NOW,
    )

    assert [operation[0] for operation in operations] == ["history", "latest"]
    assert operations[0][2] == {"rundate": RUN_DATE}
    assert versions == {"history": 11, "latest": 21}


def test_markov_compatibility_repair_replaces_the_same_scopes(
    spark,
    monkeypatch,
):
    context = _context()
    signals = adapt_configured_provider_scores(
        spark.createDataFrame(
            [("account-a", "menswear", 0.8, 0.6)],
            ["AccountNumber", "NextTheme", "ProbAgg", "ProbAggRebased"],
        ),
        context=context,
        provider_config=_markov_config(),
    )
    operations = []
    next_versions = iter((10, 11, 20, 21, 11, 12, 21, 22))
    monkeypatch.setattr(
        provider_compatibility,
        "latest_delta_version",
        lambda _spark, _table: next(next_versions),
    )
    monkeypatch.setattr(
        provider_compatibility,
        "replace_scope_by_name",
        lambda _frame, table, scope, _columns, *, spark: operations.append(
            ("history", table, scope)
        ),
    )
    monkeypatch.setattr(
        provider_compatibility,
        "replace_table_by_name",
        lambda _frame, table, _columns, *, spark: operations.append(
            ("latest", table)
        ),
    )
    config = SimpleNamespace(
        tables_write=SimpleNamespace(
            next_theme_scores="catalog.schema.markov_history",
            next_theme_scores_latest="catalog.schema.markov_latest",
        )
    )

    first = publish_markov_compatibility_outputs(
        spark,
        config,
        context,
        signals,
        NOW,
    )
    repaired = publish_markov_compatibility_outputs(
        spark,
        config,
        context,
        signals,
        NOW,
    )

    assert operations == [
        ("history", "catalog.schema.markov_history", {"rundate": RUN_DATE}),
        ("latest", "catalog.schema.markov_latest"),
        ("history", "catalog.schema.markov_history", {"rundate": RUN_DATE}),
        ("latest", "catalog.schema.markov_latest"),
    ]
    assert first == {"history": 11, "latest": 21}
    assert repaired == {"history": 12, "latest": 22}


def test_shared_publisher_entrypoint_has_no_model_specific_runtime_import():
    source = (
        PROJECT_ROOT / "jobs/orchestration/publish_score_provider_build.py"
    ).read_text()

    assert "theme_affinity" not in source
    assert "markov" not in source
    assert "configured_compatibility_publisher(" in source
    assert "publish_provider_build(" in source

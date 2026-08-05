import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml
from pyspark.sql import functions as F

from next_ads.ranking.provider_context import (
    ProviderContext,
    build_provider_build_id,
    build_provider_invocation_checksum,
    pinned_item_themes,
)
from next_ads.ranking.provider_signals import adapt_account_theme_scores
from next_ads.ranking.scoring_inputs import (
    ContentSummary,
    build_input_snapshot_id,
    summarise_content,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUN_DATE = date(2026, 7, 30)


def _context():
    return ProviderContext(
        context_slot="theme_affinity_serving",
        orchestration_run_id=123,
        provider_id="theme_affinity",
        provider_build_id="theme_affinity_build",
        provider_build_attempt_id="theme_affinity_build:1:0",
        input_snapshot_id="scoring_inputs_20260730_abc",
        run_date=RUN_DATE,
        model_uri="models:/catalog.schema.model/1",
        bindings_json=json.dumps(
            {
                "item_themes": {
                    "input_snapshot_id": "scoring_inputs_20260730_abc",
                    "run_date": "2026-07-30",
                    "table": "catalog.schema.item_themes",
                    "delta_version": 7,
                }
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        capability="account_theme",
        use_case="theme_ranking",
        invocation_checksum="checksum",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=8),
    )


def test_input_identity_excludes_delta_version_provenance():
    summary = ContentSummary(10, 10, 0, 0, "checksum")

    first = build_input_snapshot_id(
        RUN_DATE,
        {"item_themes": summary},
    )
    rewritten_same_content = build_input_snapshot_id(
        RUN_DATE,
        {"item_themes": summary},
    )

    assert first == rewritten_same_content


def test_input_acceptance_treats_null_logical_dates_as_invalid():
    acceptance = (
        PROJECT_ROOT / "jobs/nextads_control/accept_scoring_inputs.py"
    ).read_text()
    mapping = (
        PROJECT_ROOT / "jobs/nextads_control/parse_theme_mapping.py"
    ).read_text()

    assert 'F.col("RunDate").isNull()' in acceptance
    assert 'F.col("rundate").isNull()' in acceptance
    assert 'F.col("RunDate").isNull()' in mapping


def test_content_checksum_is_partition_and_order_stable(spark):
    frame = spark.createDataFrame(
        [("2", "b", 2), ("1", "a", 1), ("3", "c", 3)],
        ["pid", "theme", "theme_rank"],
    )

    one = summarise_content(
        frame.repartition(1),
        key_columns=("pid", "theme"),
    )
    four = summarise_content(
        frame.orderBy(F.desc("pid")).repartition(4),
        key_columns=("pid", "theme"),
    )

    assert one == four


def test_provider_build_identity_requires_exact_model_and_semantic_config():
    identity = build_provider_build_id(
        provider_id="theme_affinity",
        provider_version="theme_affinity/v1",
        input_snapshot_id="snapshot",
        model_uri="models:/catalog.schema.model/1",
        invocation_checksum="config-a",
        run_date=RUN_DATE,
    )
    changed = build_provider_build_id(
        provider_id="theme_affinity",
        provider_version="theme_affinity/v1",
        input_snapshot_id="snapshot",
        model_uri="models:/catalog.schema.model/1",
        invocation_checksum="config-b",
        run_date=RUN_DATE,
    )

    assert identity != changed
    with pytest.raises(ValueError, match="immutable model URI"):
        build_provider_build_id(
            provider_id="theme_affinity",
            provider_version="theme_affinity/v1",
            input_snapshot_id="snapshot",
            model_uri="models:/catalog.schema.model@champion",
            invocation_checksum="config",
            run_date=RUN_DATE,
        )


def _theme_affinity_invocation_configs():
    provider = {
        "adapter": "legacy_account_entity_table",
        "capability": "account_theme",
        "entity_type": "theme",
        "score_direction": "higher_is_better",
        "max_entities_per_account": 100,
        "account_number_column": "AccountNumber",
        "entity_id_column": "NextTheme",
        "raw_score_column": "ProbAggRebased",
        "score_column": "ProbAggRebased",
        "legacy_source_table": "catalog.dev.theme_affinity_latest",
    }
    ranking_model = {
        "model_uri": "models:/catalog.dev.theme_affinity/1",
        "experiment_path": "/Shared/dev/theme_affinity",
        "model_name": "theme_affinity_ranker",
        "registered_model_name": "dev_theme_affinity_ranker",
        "training_frame": {
            "source_table": "catalog.dev.training",
            "max_accounts": 50_000,
        },
        "predict_rank_filter_threshold": 100,
        "high_repurchase_penalty": 0.25,
        "high_repurchase_manual_themes": ["homesofas"],
        "predict_table_cols": ["account_number", "theme", "prediction"],
        "model_input_cols": ["month", "simple_rules_rank"],
    }
    return provider, ranking_model


def test_theme_affinity_invocation_ignores_environment_and_training_paths():
    provider, ranking_model = _theme_affinity_invocation_configs()
    expected = build_provider_invocation_checksum(
        provider_id="theme_affinity",
        provider_config=provider,
        ranking_model_config=ranking_model,
    )
    environment_changed = {
        **provider,
        "legacy_source_table": "catalog.prod.theme_affinity_latest",
    }
    paths_changed = {
        **ranking_model,
        "model_uri": "models:/catalog.prod.theme_affinity/1",
        "experiment_path": "/Shared/prod/theme_affinity",
        "registered_model_name": "prod_theme_affinity_ranker",
        "training_frame": {
            **ranking_model["training_frame"],
            "source_table": "catalog.prod.training",
        },
    }

    assert (
        build_provider_invocation_checksum(
            provider_id="theme_affinity",
            provider_config=environment_changed,
            ranking_model_config=paths_changed,
        )
        == expected
    )


def test_theme_affinity_build_changes_for_inference_setting_or_model_uri():
    provider, ranking_model = _theme_affinity_invocation_configs()
    checksum = build_provider_invocation_checksum(
        provider_id="theme_affinity",
        provider_config=provider,
        ranking_model_config=ranking_model,
    )
    changed_checksum = build_provider_invocation_checksum(
        provider_id="theme_affinity",
        provider_config=provider,
        ranking_model_config={
            **ranking_model,
            "predict_rank_filter_threshold": 50,
        },
    )
    baseline = build_provider_build_id(
        provider_id="theme_affinity",
        provider_version="theme_affinity/v1",
        input_snapshot_id="snapshot",
        model_uri="models:/catalog.schema.model/1",
        invocation_checksum=checksum,
        run_date=RUN_DATE,
    )
    changed_setting = build_provider_build_id(
        provider_id="theme_affinity",
        provider_version="theme_affinity/v1",
        input_snapshot_id="snapshot",
        model_uri="models:/catalog.schema.model/1",
        invocation_checksum=changed_checksum,
        run_date=RUN_DATE,
    )
    changed_model = build_provider_build_id(
        provider_id="theme_affinity",
        provider_version="theme_affinity/v1",
        input_snapshot_id="snapshot",
        model_uri="models:/catalog.schema.model/2",
        invocation_checksum=checksum,
        run_date=RUN_DATE,
    )

    assert changed_checksum != checksum
    assert changed_setting != baseline
    assert changed_model != baseline


def test_pinned_item_themes_rejects_empty_or_wrong_date(spark, monkeypatch):
    table = "catalog.schema.item_themes"
    context = _context()
    frame = spark.createDataFrame(
        [
            (
                context.input_snapshot_id,
                RUN_DATE,
                "1",
                "menswear",
                1,
            )
        ],
        ["InputSnapshotID", "RunDate", "pid", "theme", "theme_rank"],
    )
    monkeypatch.setattr(
        "next_ads.ranking.scoring_inputs.read_delta_version",
        lambda _spark, name, version: (
            frame if (name, version) == (table, 7) else None
        ),
    )
    assert pinned_item_themes(
        spark,
        context,
        input_table=table,
    ).count() == 1
    wrong_date = ProviderContext(
        **{
            **context.__dict__,
            "run_date": date(2026, 7, 29),
            "bindings_json": json.dumps(
                {
                    "item_themes": {
                        "input_snapshot_id": context.input_snapshot_id,
                        "run_date": "2026-07-29",
                        "table": table,
                        "delta_version": 7,
                    }
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
    )
    with pytest.raises(ValueError, match="empty"):
        pinned_item_themes(
            spark,
            wrong_date,
            input_table=table,
        )


def test_canonical_adapter_allows_constant_finite_scores(spark):
    frame = spark.createDataFrame(
        [("A", "b", 1.0), ("A", "a", 1.0)],
        ["account", "theme", "score"],
    )
    adapted = adapt_account_theme_scores(
        frame,
        provider_build_id="build",
        provider_id="markov",
        run_date=RUN_DATE,
        account_column="account",
        theme_column="theme",
        raw_score_column="score",
        score_column="score",
    )

    assert [
        (row["EntityID"], row["ProviderRank"])
        for row in adapted.orderBy("ProviderRank").collect()
    ] == [("a", 1), ("b", 2)]


def test_jobs_pin_same_day_inputs_and_static_context_slot():
    affinity = yaml.safe_load(
        (
            PROJECT_ROOT
            / "pipelines/databricks/jobs/"
            "mktg_next_uk_nextads_theme_affinity.yml"
        ).read_text()
    )["mktg_next_uk_nextads_theme_affinity_config"][
        "mktg_next_uk_nextads_theme_affinity_cicd"
    ]
    inputs = yaml.safe_load(
        (
            PROJECT_ROOT
            / "pipelines/databricks/jobs/"
            "mktg_next_uk_nextads_theme_inputs.yml"
        ).read_text()
    )["mktg_next_uk_nextads_theme_inputs_config"][
        "mktg_next_uk_nextads_theme_inputs_cicd"
    ]

    assert inputs["schedule"]["quartz_cron_expression"] == "0 15 12 * * ?"
    assert affinity["schedule"]["quartz_cron_expression"] == "0 0 13 * * ?"
    assert "context_slot" not in {
        parameter["name"] for parameter in affinity["parameters"]
    }
    prepare_foundation = next(
        task
        for task in affinity["tasks"]
        if task["task_key"] == "prepare_foundation_context"
    )
    foundation_parameters = prepare_foundation["spark_python_task"][
        "parameters"
    ]
    assert int(
        foundation_parameters[
            foundation_parameters.index("--readiness_wait_seconds") + 1
        ]
    ) == 5400
    assert prepare_foundation["timeout_seconds"] > 5400

    prepare_provider = next(
        task
        for task in affinity["tasks"]
        if task["task_key"] == "prepare_provider_context"
    )
    parameters = prepare_provider["spark_python_task"]["parameters"]
    assert parameters[parameters.index("--context_slot") + 1] == (
        "theme_affinity_serving"
    )
    assert int(
        parameters[parameters.index("--readiness_wait_seconds") + 1]
    ) == 0
    assert prepare_provider["timeout_seconds"] > 0


def _task_parameter_map(task):
    parameters = task["spark_python_task"]["parameters"]
    parsed = {}
    index = 0
    while index < len(parameters):
        name = parameters[index]
        next_index = index + 1
        if (
            next_index == len(parameters)
            or str(parameters[next_index]).startswith("--")
        ):
            parsed[name] = True
            index += 1
        else:
            parsed[name] = parameters[next_index]
            index += 2
    return parsed


def test_provider_lifecycle_tasks_use_neutral_entrypoints_and_complete_args():
    affinity = yaml.safe_load(
        (
            PROJECT_ROOT
            / "pipelines/databricks/jobs/"
            "mktg_next_uk_nextads_theme_affinity.yml"
        ).read_text()
    )["mktg_next_uk_nextads_theme_affinity_config"][
        "mktg_next_uk_nextads_theme_affinity_cicd"
    ]
    markov = yaml.safe_load(
        (
            PROJECT_ROOT
            / "pipelines/databricks/jobs/"
            "mktg_next_uk_nextads_markov_scoring.yml"
        ).read_text()
    )["mktg_next_uk_nextads_markov_scoring_config"][
        "mktg_next_uk_nextads_markov_scoring_cicd"
    ]
    affinity_tasks = {
        task["task_key"]: task for task in affinity["tasks"]
    }
    markov_tasks = {task["task_key"]: task for task in markov["tasks"]}

    neutral_prepare = (
        "../../../jobs/orchestration/prepare_score_provider_context.py"
    )
    neutral_finalize = (
        "../../../jobs/orchestration/finalize_score_provider_context.py"
    )
    neutral_publish = (
        "../../../jobs/orchestration/publish_score_provider_build.py"
    )
    assert (
        affinity_tasks["prepare_provider_context"]["spark_python_task"][
            "python_file"
        ]
        == neutral_prepare
    )
    assert (
        markov_tasks["prepare_provider_context"]["spark_python_task"][
            "python_file"
        ]
        == neutral_prepare
    )
    assert (
        markov_tasks["finalize_provider_context"]["spark_python_task"][
            "python_file"
        ]
        == neutral_finalize
    )
    assert (
        affinity_tasks["finalize_provider_context"]["spark_python_task"][
            "python_file"
        ]
        == neutral_finalize
    )
    assert (
        affinity_tasks["publish_provider_build"]["spark_python_task"][
            "python_file"
        ]
        == neutral_publish
    )
    assert (
        markov_tasks["publish_provider_build"]["spark_python_task"][
            "python_file"
        ]
        == neutral_publish
    )

    expected_prepare = {
        "--client",
        "--job_env",
        "--run_date",
        "--input_snapshot_id",
        "--model_uri",
        "--context_slot",
        "--readiness_wait_seconds",
        "--readiness_poll_seconds",
        "--task_run_id",
        "--execution_count",
        "--orchestration_run_id",
        "--log_level",
    }
    assert set(
        _task_parameter_map(affinity_tasks["prepare_provider_context"])
    ) == expected_prepare | {
        "--allow-serial-run-takeover",
        "--scoring_foundation_build_id",
        "--scoring_foundation_build_attempt_id",
    }
    assert set(
        _task_parameter_map(markov_tasks["prepare_provider_context"])
    ) == expected_prepare | {
        "--allow-serial-run-takeover",
        "--provider_id",
        "--use_case",
    }
    assert set(_task_parameter_map(markov_tasks["build_markov_scores"])) == {
        "--client",
        "--job_env",
        "--refresh_model_date",
        "--run_date",
        "--input_snapshot_id",
        "--provider_build_id",
        "--provider_build_attempt_id",
        "--context_slot",
        "--orchestration_run_id",
    }

    assert set(_task_parameter_map(affinity_tasks["model_predict"])) == {
        "--client",
        "--job_env",
        "--model_uri",
        "--run_date",
        "--input_snapshot_id",
        "--provider_build_id",
        "--provider_build_attempt_id",
        "--context_slot",
        "--log_level",
    }
    assert set(
        _task_parameter_map(affinity_tasks["publish_provider_build"])
    ) == {
        "--client",
        "--job_env",
        "--run_date",
        "--input_snapshot_id",
        "--provider_build_id",
        "--provider_build_attempt_id",
        "--provider_signals_delta_version",
        "--context_slot",
        "--orchestration_run_id",
        "--task_run_id",
        "--execution_count",
        "--log_level",
    }
    assert set(
        _task_parameter_map(markov_tasks["publish_provider_build"])
    ) == {
        "--client",
        "--job_env",
        "--run_date",
        "--input_snapshot_id",
        "--provider_build_id",
        "--provider_build_attempt_id",
        "--provider_signals_delta_version",
        "--context_slot",
        "--orchestration_run_id",
        "--task_run_id",
        "--execution_count",
        "--log_level",
    }
    publish_parameters = _task_parameter_map(
        affinity_tasks["publish_provider_build"]
    )
    assert publish_parameters["--provider_signals_delta_version"] == (
        "{{tasks.model_predict.values.provider_signals_delta_version}}"
    )
    markov_publish_parameters = _task_parameter_map(
        markov_tasks["publish_provider_build"]
    )
    assert markov_publish_parameters[
        "--provider_signals_delta_version"
    ] == (
        "{{tasks.build_markov_scores.values."
        "provider_signals_delta_version}}"
    )
    assert "clean_output" not in affinity_tasks

    expected_finalize = {
        "--client",
        "--job_env",
        "--context_slot",
        "--orchestration_run_id",
        "--status",
        "--log_level",
    }
    assert set(
        _task_parameter_map(affinity_tasks["finalize_provider_context"])
    ) == expected_finalize
    assert affinity_tasks["finalize_provider_context"]["depends_on"] == [
        {"task_key": "publish_provider_build"}
    ]
    assert set(
        _task_parameter_map(markov_tasks["finalize_provider_context"])
    ) == expected_finalize
    assert markov_tasks["finalize_provider_context"]["depends_on"] == [
        {"task_key": "publish_provider_build"}
    ]

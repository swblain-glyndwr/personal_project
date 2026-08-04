import argparse
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from jobs.table_operations.table_maintenance import (
    parse_args,
    parse_run_date,
)
from next_ads.common.config_manager import load_config
from next_ads.decisioning.table_maintenance import (
    ASSIGNMENT_TABLE_SPECS,
    CANDIDATE_FOUNDATION_TABLE_SPECS,
    CONFIGURED_TABLE_SPECS,
    PLP_HISTORY_SPEC,
    SCORING_FOUNDATION_TABLE_SPECS,
    SCORING_INPUT_SNAPSHOT_TABLE_SPECS,
    SCORING_PROVIDER_TABLE_SPECS,
    VACUUM_RETENTION_HOURS,
    build_maintenance_plan,
    execute_maintenance_plan,
    is_weekly_maintenance_day,
    resolve_maintenance_tables,
    validate_qualified_table_name,
)
from tests.job_resource_helpers import load_job


PROJECT_ROOT = Path(__file__).resolve().parents[2]
JOB_RESOURCE = (
    "pipelines/databricks/jobs/"
    "mktg_next_uk_nextads_table_maintenance.yml"
)
JOB_KEY = "mktg_next_uk_nextads_table_maintenance_cicd"

EXPECTED_CONFIG_KEYS = {
    "customer_cells_fixed_latest",
    "customer_cells_transient",
    "customer_cells_transient_latest",
    "customer_cells_latest",
    "control_sheet_raw",
    "control_sheet_raw_latest",
    "control_sheet_plp_raw",
    "control_sheet_plp_raw_latest",
    "control_sheet",
    "control_sheet_latest",
    "multipage_locations",
    "multipage_locations_latest",
    "control_sheet_raw_v2",
    "control_sheet_raw_latest_v2",
    "exclusions",
    "exclusions_latest",
    "control_sheet_v2",
    "control_sheet_latest_v2",
    "attribute_set",
    "attribute_set_latest",
    "item_attributes_latest",
    "theme_mapping",
    "theme_mapping_latest",
    "item_themes",
    "item_themes_latest",
    "scoring_input_snapshots",
    "scoring_input_snapshot_sources",
    "scoring_input_item_themes",
    "scoring_input_theme_mapping_raw",
    "scoring_foundation_builds",
    "scoring_foundation_outputs",
    "scoring_foundation_run_contexts",
    "candidate_foundation_builds",
    "candidate_foundation_sources",
    "candidate_repeat_ad_exposure",
    "candidate_ad_feedback",
    "score_provider_builds",
    "score_provider_signals",
    "score_provider_run_contexts",
    "theme_scoring_events_latest",
    "theme_transitions",
    "theme_transitions_latest",
    "next_theme_scores",
    "next_theme_scores_latest",
    "theme_score_components",
    "theme_score_components_latest",
    "preranked_ads_from_themes_latest",
    "preranked_ads_from_themes_v2_latest",
    "nextads_payload",
    "nextads_payload_latest",
    "sort_order_v2",
    "cms_content",
    "viewed_bought_latest",
    "assignments",
    "assignments_latest",
    "assignments_build_staging",
    "assignments_v2",
    "assignments_v2_latest",
    "assignments_v2_build_staging",
    "assignment_build_events",
}

EXCLUDED_TABLE_CONFIG_KEYS = {
    "customer_cells_fixed_history",
    "theme_affinity_model_latest",
    "theme_affinity_model_full",
    "theme_affinity_inference_log",
}

EXCLUDED_THEME_AFFINITY_PUBLISH_SUFFIXES = {
    "ranked",
    "complete",
    "advanced_features",
    "customer_features",
    "customer_segments",
    "popularity_metrics",
}

LEGACY_731_HISTORY_NAMES = {
    "transient_cell_history",
    "v1_control_raw_history",
    "v1_placement_raw_history",
    "v1_control_history",
    "multipage_location_history",
    "v2_control_raw_history",
    "v2_exclusion_history",
    "v2_control_history",
    "attribute_set_history",
    "theme_mapping_history",
    "item_theme_history",
    "theme_transition_history",
    "next_theme_score_history",
    "theme_score_component_history",
    "v2_payload_history",
    "sort_order_history",
    "cms_content_history",
}

ASSIGNMENT_731_HISTORY_NAMES = {
    "v1_assignment_history",
    "v2_assignment_history",
}


def _config():
    tables_write = {
        spec.config_key: f"catalog.schema.{spec.config_key}"
        for spec in CONFIGURED_TABLE_SPECS
    }
    tables_write.update(
        {
            "assignments": "catalog.schema.assignments",
            "assignments_latest": "catalog.schema.assignments_latest",
            "assignments_build_staging": (
                "catalog.schema.assignments_build_staging"
            ),
            "assignments_v2": "catalog.schema.assignments_v2",
            "assignments_v2_latest": (
                "catalog.schema.assignments_v2_latest"
            ),
            "assignments_v2_build_staging": (
                "catalog.schema.assignments_v2_build_staging"
            ),
            "assignment_build_events": (
                "catalog.schema.assignment_build_events"
            ),
            "nextads_plp_gs_latest": "catalog.schema.plp_history",
        }
    )
    return SimpleNamespace(tables_write=tables_write)


def test_allowlist_covers_every_migrated_active_output_contract():
    resolved = resolve_maintenance_tables(_config())

    assert {spec.config_key for spec in CONFIGURED_TABLE_SPECS} == (
        EXPECTED_CONFIG_KEYS
    )
    assert len(resolved) == 61
    assert len({table.table for table in resolved}) == 61
    assert {table.spec.name for table in resolved}.issuperset(
        {spec.name for spec in ASSIGNMENT_TABLE_SPECS}
    )
    assert resolved[-1].spec == PLP_HISTORY_SPEC
    assert resolved[-1].table == "catalog.schema.plp_history"


def test_actual_next_uk_config_resolves_only_repo_owned_tables(monkeypatch):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "maintenance_test")
    config = load_config("dev", client="next_uk")

    resolved = resolve_maintenance_tables(config)

    assert len(resolved) == 61
    assert all(
        table.table.startswith("marketingdata_dev.maintenance_test.")
        for table in resolved
    )
    assert resolved[-1].table.endswith("next_uk_nextads_plp_gs_latest")


def test_allowlist_excludes_tables_without_a_migrated_maintenance_contract():
    config = _config()
    excluded_physical_tables = {
        f"catalog.schema.{key}" for key in EXCLUDED_TABLE_CONFIG_KEYS
    }
    config.tables_write.update(
        {
            key: f"catalog.schema.{key}"
            for key in EXCLUDED_TABLE_CONFIG_KEYS
        }
    )
    config.tables_write["nextads_plp_gs"] = {
        "next": {
            "gb": {
                "latest": "catalog.schema.plp_current",
            }
        }
    }
    excluded_physical_tables.add("catalog.schema.plp_current")
    excluded_physical_tables.update(
        {
            f"catalog.schema.theme_affinity_predict_{suffix}"
            for suffix in EXCLUDED_THEME_AFFINITY_PUBLISH_SUFFIXES
        }
    )

    resolved_physical_tables = {
        table.table for table in resolve_maintenance_tables(config)
    }

    # Fixed-cell history used direct append and PLP current had no old
    # housekeeping. Theme Affinity had no writer-side housekeeping to move and
    # its independent 09:00 job can still be running at the noon schedule.
    assert resolved_physical_tables.isdisjoint(excluded_physical_tables)


@pytest.mark.parametrize(
    "table",
    [
        "",
        "schema.table",
        "catalog.schema.table.extra",
        "catalog.schema.table;DELETE_FROM_x",
        "catalog.`schema`.table",
        "catalog.bad-schema.table",
    ],
)
def test_table_identifiers_reject_unstructured_or_unsafe_values(table):
    with pytest.raises(ValueError):
        validate_qualified_table_name(table)


def test_allowlist_rejects_missing_routes_and_duplicate_physical_tables():
    missing_plp = _config()
    del missing_plp.tables_write["nextads_plp_gs_latest"]
    with pytest.raises(ValueError, match="nextads_plp_gs_latest"):
        resolve_maintenance_tables(missing_plp)

    duplicate = _config()
    duplicate.tables_write["assignments_v2"] = duplicate.tables_write[
        "assignments"
    ]
    with pytest.raises(ValueError, match="duplicate table"):
        resolve_maintenance_tables(duplicate)


def test_daily_plan_preserves_latest_and_applies_exact_retention_contracts():
    run_date = date(2026, 7, 27)  # Monday

    statements = build_maintenance_plan(_config(), run_date)

    assert {statement.table_name for statement in statements} == {
        *LEGACY_731_HISTORY_NAMES,
        *ASSIGNMENT_731_HISTORY_NAMES,
        "v1_assignment_staging",
        "v2_assignment_staging",
        "assignment_build_events",
        "scoring_input_item_themes",
        "scoring_foundation_builds",
        "scoring_foundation_outputs",
        "candidate_foundation_builds",
        "candidate_foundation_sources",
        "candidate_repeat_ad_exposure",
        "candidate_ad_feedback",
        "score_provider_builds",
        "score_provider_signals",
        "plp_gs_history",
    }
    assert {statement.operation for statement in statements} == {"retention"}
    sql = "\n".join(statement.sql for statement in statements)
    legacy_statements = [
        statement
        for statement in statements
        if statement.table_name in LEGACY_731_HISTORY_NAMES
    ]
    assert len(legacy_statements) == len(LEGACY_731_HISTORY_NAMES)
    assert all(
        (
            "WHERE `rundate` < "
            "date_sub(DATE '2026-07-27', 731)"
        )
        in statement.sql
        for statement in legacy_statements
    )
    assert (
        "DELETE FROM `catalog`.`schema`.`assignments`\n"
        "WHERE `rundate` <= date_sub(DATE '2026-07-27', 731)"
    ) in sql
    assert (
        "DELETE FROM `catalog`.`schema`.`assignments_v2`\n"
        "WHERE `rundate` <= date_sub(DATE '2026-07-27', 731)"
    ) in sql
    assert sql.count(
        "WHERE `rundate` <= date_sub(DATE '2026-07-27', 2)"
    ) == 2
    assert (
        "DELETE FROM `catalog`.`schema`.`assignment_build_events`\n"
        "WHERE `BuildDate` <= date_sub(DATE '2026-07-27', 7)"
    ) in sql
    assert (
        "DELETE FROM `catalog`.`schema`.`scoring_input_item_themes`\n"
        "WHERE `RunDate` <= date_sub(DATE '2026-07-27', 35)"
    ) in sql
    assert (
        "DELETE FROM `catalog`.`schema`.`candidate_foundation_builds`\n"
        "WHERE `RunDate` <= date_sub(DATE '2026-07-27', 35)"
    ) in sql
    assert (
        "DELETE FROM `catalog`.`schema`.`plp_history`\n"
        "WHERE `rundate` <= date_sub(DATE '2026-07-27', 365)"
    ) in sql
    assert "`catalog`.`schema`.`assignments_latest`" not in sql
    assert "`catalog`.`schema`.`assignments_v2_latest`" not in sql
    assert "`catalog`.`schema`.`plp_current`" not in sql
    assert "theme_affinity_model_full" not in sql
    assert "theme_affinity_inference_log" not in sql


def test_legacy_and_new_retention_boundaries_are_not_conflated():
    specs_by_name = {
        spec.name: spec
        for spec in (*CONFIGURED_TABLE_SPECS, PLP_HISTORY_SPEC)
    }

    for name in LEGACY_731_HISTORY_NAMES:
        spec = specs_by_name[name]
        assert (
            spec.retention_days,
            spec.retention_column,
            spec.retention_comparison,
        ) == (731, "rundate", "<")

    for name in ASSIGNMENT_731_HISTORY_NAMES:
        spec = specs_by_name[name]
        assert (
            spec.retention_days,
            spec.retention_column,
            spec.retention_comparison,
        ) == (731, "rundate", "<=")

    assert (
        specs_by_name["v1_assignment_staging"].retention_days,
        specs_by_name["v1_assignment_staging"].retention_comparison,
    ) == (2, "<=")
    assert (
        specs_by_name["assignment_build_events"].retention_days,
        specs_by_name["assignment_build_events"].retention_comparison,
    ) == (7, "<=")
    assert (
        PLP_HISTORY_SPEC.retention_days,
        PLP_HISTORY_SPEC.retention_comparison,
    ) == (365, "<=")


def test_scoring_input_manifests_are_preserved_and_large_snapshot_is_bounded():
    assert {
        spec.config_key for spec in SCORING_INPUT_SNAPSHOT_TABLE_SPECS
    } == {
        "scoring_input_snapshots",
        "scoring_input_snapshot_sources",
        "scoring_input_item_themes",
        "scoring_input_theme_mapping_raw",
    }
    specs = {
        spec.config_key: spec
        for spec in SCORING_INPUT_SNAPSHOT_TABLE_SPECS
    }
    assert all(
        (
            spec.retention_days,
            spec.retention_column,
            spec.retention_comparison,
        )
        == (None, None, None)
        for key, spec in specs.items()
        if key != "scoring_input_item_themes"
    )
    assert (
        specs["scoring_input_item_themes"].retention_days,
        specs["scoring_input_item_themes"].retention_column,
        specs["scoring_input_item_themes"].retention_comparison,
    ) == (35, "RunDate", "<=")

    daily_statements = build_maintenance_plan(
        _config(),
        date(2026, 7, 27),
    )
    snapshot_statements = {
        statement.table_name for statement in daily_statements
    }.intersection(
        {spec.name for spec in SCORING_INPUT_SNAPSHOT_TABLE_SPECS}
    )
    assert snapshot_statements == {"scoring_input_item_themes"}


def test_foundation_manifests_are_bounded_but_active_context_is_preserved():
    specs = {
        spec.config_key: spec for spec in SCORING_FOUNDATION_TABLE_SPECS
    }
    for key in ("scoring_foundation_builds", "scoring_foundation_outputs"):
        spec = specs[key]
        assert (
            spec.retention_days,
            spec.retention_column,
            spec.retention_comparison,
        ) == (35, "RunDate", "<=")
    context = specs["scoring_foundation_run_contexts"]
    assert (
        context.retention_days,
        context.retention_column,
        context.retention_comparison,
    ) == (None, None, None)

    statements = build_maintenance_plan(_config(), date(2026, 7, 27))
    foundation_retention = {
        statement.table_name
        for statement in statements
        if statement.table_name.startswith("scoring_foundation")
    }
    assert foundation_retention == {
        "scoring_foundation_builds",
        "scoring_foundation_outputs",
    }


def test_candidate_foundation_outputs_share_the_35_day_retention_contract():
    assert {
        spec.config_key for spec in CANDIDATE_FOUNDATION_TABLE_SPECS
    } == {
        "candidate_foundation_builds",
        "candidate_foundation_sources",
        "candidate_repeat_ad_exposure",
        "candidate_ad_feedback",
    }
    assert all(
        (
            spec.retention_days,
            spec.retention_column,
            spec.retention_comparison,
        )
        == (35, "RunDate", "<=")
        for spec in CANDIDATE_FOUNDATION_TABLE_SPECS
    )

    statements = build_maintenance_plan(_config(), date(2026, 7, 27))
    candidate_foundation_retention = {
        statement.table_name
        for statement in statements
        if statement.table_name.startswith("candidate_")
    }
    assert candidate_foundation_retention == {
        "candidate_foundation_builds",
        "candidate_foundation_sources",
        "candidate_repeat_ad_exposure",
        "candidate_ad_feedback",
    }


def test_provider_manifests_are_bounded_but_active_context_is_preserved():
    specs = {
        spec.config_key: spec for spec in SCORING_PROVIDER_TABLE_SPECS
    }
    for key in ("score_provider_builds", "score_provider_signals"):
        spec = specs[key]
        assert (
            spec.retention_days,
            spec.retention_column,
            spec.retention_comparison,
        ) == (35, "RunDate", "<=")
    context = specs["score_provider_run_contexts"]
    assert (
        context.retention_days,
        context.retention_column,
        context.retention_comparison,
    ) == (None, None, None)

    statements = build_maintenance_plan(_config(), date(2026, 7, 27))
    provider_retention = {
        statement.table_name
        for statement in statements
        if statement.table_name.startswith("score_provider")
    }
    assert provider_retention == {
        "score_provider_builds",
        "score_provider_signals",
    }


def test_assignment_cutoff_keeps_exactly_731_calendar_dates():
    run_date = date(2026, 7, 27)
    cutoff = run_date - timedelta(days=731)
    oldest_survivor = cutoff + timedelta(days=1)

    assert (run_date - oldest_survivor).days + 1 == 731
    assert cutoff <= cutoff
    assert not oldest_survivor <= cutoff

    legacy_boundary = cutoff
    assert not legacy_boundary < cutoff


def test_weekly_plan_optimizes_and_safely_vacuums_every_allowlisted_table():
    run_date = date(2026, 8, 2)  # Sunday

    statements = build_maintenance_plan(_config(), run_date)
    optimize = [
        statement
        for statement in statements
        if statement.operation == "optimize"
    ]
    vacuum = [
        statement
        for statement in statements
        if statement.operation == "vacuum"
    ]

    assert is_weekly_maintenance_day(run_date)
    assert len(optimize) == 61
    assert len(vacuum) == 61
    assert {statement.table_name for statement in optimize} == {
        table.spec.name for table in resolve_maintenance_tables(_config())
    }
    assert {statement.table_name for statement in vacuum} == {
        table.spec.name for table in resolve_maintenance_tables(_config())
    }
    assert all(
        statement.sql.endswith(
            f"RETAIN {VACUUM_RETENTION_HOURS} HOURS"
        )
        for statement in vacuum
    )
    all_sql = "\n".join(statement.sql for statement in statements).lower()
    assert "retentiondurationcheck" not in all_sql
    assert "set spark.databricks.delta" not in all_sql


def test_weekly_decision_uses_logical_run_date_only():
    assert is_weekly_maintenance_day(date(2026, 8, 2))
    assert not is_weekly_maintenance_day(date(2026, 8, 3))


def test_plan_and_execution_are_idempotent_for_a_logical_date():
    class CurrentDateResult:
        def first(self):
            return {"current_date": date(2026, 8, 2)}

    class Spark:
        def __init__(self):
            self.statements = []

        def sql(self, statement):
            if statement == "SELECT current_date() AS current_date":
                return CurrentDateResult()
            self.statements.append(statement)

    class Logger:
        def info(self, *_args):
            pass

    first_plan = build_maintenance_plan(_config(), date(2026, 8, 2))
    second_plan = build_maintenance_plan(_config(), date(2026, 8, 2))
    spark = Spark()

    execute_maintenance_plan(
        spark,
        first_plan,
        run_date=date(2026, 8, 2),
        logger=Logger(),
    )
    execute_maintenance_plan(
        spark,
        second_plan,
        run_date=date(2026, 8, 2),
        logger=Logger(),
    )

    assert first_plan == second_plan
    assert spark.statements[: len(first_plan)] == spark.statements[
        len(first_plan) :
    ]


def test_future_logical_date_fails_before_any_maintenance_statement():
    class CurrentDateResult:
        def first(self):
            return {"current_date": date(2026, 8, 2)}

    class Spark:
        def __init__(self):
            self.maintenance_statements = []

        def sql(self, statement):
            if statement == "SELECT current_date() AS current_date":
                return CurrentDateResult()
            self.maintenance_statements.append(statement)

    class Logger:
        def info(self, *_args):
            pass

    spark = Spark()
    run_date = date(2026, 8, 3)
    plan = build_maintenance_plan(_config(), run_date)

    with pytest.raises(ValueError, match="cannot be in the future"):
        execute_maintenance_plan(
            spark,
            plan,
            run_date=run_date,
            logger=Logger(),
        )

    assert spark.maintenance_statements == []


def test_entrypoint_requires_logical_date_and_exposes_no_table_override():
    args = parse_args(
        [
            "--job_env",
            "prod",
            "--run_date",
            "2026-08-02",
        ]
    )
    source = (
        PROJECT_ROOT / "jobs/table_operations/table_maintenance.py"
    ).read_text()

    assert args.run_date == date(2026, 8, 2)
    assert args.client == "next_uk"
    assert "--tables" not in source
    assert "--table" not in source

    with pytest.raises(argparse.ArgumentTypeError):
        parse_run_date("02/08/2026")
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--client",
                "next_gb",
                "--job_env",
                "prod",
                "--run_date",
                "2026-08-02",
            ]
        )


def test_maintenance_job_is_independent_and_runs_at_0500_london_time():
    job = load_job(JOB_RESOURCE, JOB_KEY)

    assert job["name"] == "mktg_next_uk_nextads_table_maintenance"
    assert job["schedule"] == {
        "quartz_cron_expression": "0 0 5 * * ?",
        "timezone_id": "Europe/London",
    }
    assert job["parameters"] == [
        {
            "name": "run_date",
            "default": "{{job.start_time.iso_date}}",
        }
    ]
    assert len(job["tasks"]) == 1
    task = job["tasks"][0]
    assert "depends_on" not in task
    assert "run_if" not in task
    assert "run_job_task" not in task
    assert task["spark_python_task"]["parameters"] == [
        "--client",
        "next_uk",
        "--job_env",
        "${var.job_parameter_environment_name}",
        "--run_date",
        "{{job.parameters.run_date}}",
        "--log_level",
        "INFO",
    ]
    assert "--tables" not in task["spark_python_task"]["parameters"]


def test_maintenance_resource_is_target_scoped_and_bundle_included():
    resource = yaml.safe_load((PROJECT_ROOT / JOB_RESOURCE).read_text())
    bundle = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text())

    assert "resources" not in resource
    assert set(resource["targets"]) == {
        "SANDBOX",
        "DEV",
        "DEV_INTEGRATION",
        "PREPROD",
        "PROD",
    }
    for target in resource["targets"].values():
        assert list(target["resources"]["jobs"]) == [JOB_KEY]
    assert JOB_RESOURCE in bundle["include"]


def test_build_and_delivery_jobs_do_not_depend_on_maintenance():
    maintenance_resource_key = (
        "${resources.jobs."
        "mktg_next_uk_nextads_table_maintenance_cicd.id}"
    )
    critical_resources = [
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build.yml",
        "pipelines/databricks/jobs/mktg_next_uk_nextads_page_build_v2.yml",
        "pipelines/databricks/jobs/mktg_next_uk_nextads_payload_export.yml",
        "pipelines/databricks/jobs/mktg_next_uk_nextads_masid_handoff.yml",
        "pipelines/databricks/jobs/mktg_next_uk_nextads_plp_gs_delivery.yml",
    ]

    for relative_path in critical_resources:
        source = (PROJECT_ROOT / relative_path).read_text()
        assert maintenance_resource_key not in source
        assert "mktg_next_uk_nextads_table_maintenance" not in source


def test_event_writes_remain_append_only_outside_maintenance():
    allowed_maintenance_files = {
        (
            PROJECT_ROOT
            / "src/next_ads/decisioning/table_maintenance.py"
        ).resolve(),
        (
            PROJECT_ROOT
            / "jobs/table_operations/table_maintenance.py"
        ).resolve(),
    }
    production_files = [
        *PROJECT_ROOT.glob("jobs/**/*.py"),
        *PROJECT_ROOT.glob("src/**/*.py"),
    ]
    for path in production_files:
        if path.resolve() in allowed_maintenance_files:
            continue
        source = path.read_text(encoding="utf-8", errors="replace")
        if "assignment_build_events" in source:
            assert "DELETE FROM" not in source.upper(), path

    publisher = (
        PROJECT_ROOT
        / "src/next_ads/decisioning/assignment_publication.py"
    ).read_text()
    assert "atomic_append_by_name(" in publisher
    assert "event_write = atomic_append_by_name(" in publisher


def test_event_table_ddl_allows_only_the_maintenance_retention_exception():
    ddl = (
        PROJECT_ROOT
        / "sql/decisioning/create_table_assignment_build_events.sql"
    ).read_text()

    assert "delta.appendOnly" not in ddl
    assert "partitioned by (BuildDate)" in ddl

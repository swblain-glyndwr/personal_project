import json
from datetime import date
from types import SimpleNamespace

import pytest

from jobs.nextads_assignment import publish_build
from next_ads.decisioning.assignment_publication import (
    AssignmentColumnContract,
    AssignmentTableContract,
)


class StubJobParser:
    def __init__(self, values):
        self.values = values
        self.parse_calls = 0

    def _parse_args(self):
        self.parse_calls += 1

    def get_arg(self, name):
        return self.values.get(name)


def valid_job_arguments(**overrides):
    values = {
        "--job_env": "prod",
        "--client": "next_uk",
        "--route": "v1",
        "--run_date": "2026-07-29",
        "--build_run_id": "v1_12345",
        "--scope_manifest_json": json.dumps(
            [
                {"scope": "HP1", "phase": "primary"},
                {
                    "scope": "SB2",
                    "phase": "secondary",
                    "inherit_basic_from": "SB1",
                },
            ]
        ),
        "--log_level": "INFO",
    }
    values.update(overrides)
    return values


def test_scope_manifest_preserves_order_and_optional_metadata():
    manifest = publish_build.parse_scope_manifest_json(
        json.dumps(
            [
                {"scope": "HP1"},
                {
                    "scope": "SB2",
                    "phase": "secondary",
                    "inherit_basic_from": "SB1",
                },
                {"scope": "OC2", "phase": "secondary"},
            ]
        )
    )

    assert manifest == (
        publish_build.ScopeManifestEntry(scope="HP1"),
        publish_build.ScopeManifestEntry(
            scope="SB2",
            phase="secondary",
            inherit_basic_from="SB1",
        ),
        publish_build.ScopeManifestEntry(
            scope="OC2",
            phase="secondary",
        ),
    )


@pytest.mark.parametrize(
    ("raw_manifest", "message"),
    [
        (None, "must be a non-empty string"),
        ("not json", "must contain valid JSON"),
        ("{}", "must be a non-empty JSON list"),
        ("[]", "must be a non-empty JSON list"),
        ('["HP1"]', "entry 0 must be a JSON object"),
        ('[{"phase": "primary"}]', "entry 0.scope"),
        ('[{"scope": "   "}]', "entry 0.scope"),
        ('[{"scope": "HP1", "phase": ""}]', "entry 0.phase"),
        (
            '[{"scope": "HP1", "inherit_basic_from": 12}]',
            "entry 0.inherit_basic_from",
        ),
        (
            '[{"scope": "HP1", "unexpected": true}]',
            "unsupported fields",
        ),
        (
            '[{"scope": "HP1"}, {"scope": " HP1 "}]',
            "duplicate scope",
        ),
    ],
)
def test_scope_manifest_rejects_malformed_or_duplicate_entries(
    raw_manifest,
    message,
):
    with pytest.raises(ValueError, match=message):
        publish_build.parse_scope_manifest_json(raw_manifest)


def test_publish_arguments_are_normalised_and_validated():
    args = publish_build.parse_publish_build_arguments(
        StubJobParser(
            valid_job_arguments(
                **{
                    "--job_env": "PROD",
                    "--route": "V1",
                }
            )
        )
    )

    assert args == publish_build.PublishBuildArguments(
        job_env="prod",
        client="next_uk",
        route="v1",
        run_date=date(2026, 7, 29),
        build_run_id="v1_12345",
        scope_manifest=(
            publish_build.ScopeManifestEntry(
                scope="HP1",
                phase="primary",
            ),
            publish_build.ScopeManifestEntry(
                scope="SB2",
                phase="secondary",
                inherit_basic_from="SB1",
            ),
        ),
        log_level="INFO",
    )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"--job_env": ""}, "--job_env"),
        ({"--job_env": "qa"}, "--job_env must be one of"),
        ({"--client": None}, "--client"),
        ({"--route": "v3"}, "--route must be one of"),
        ({"--run_date": "29/07/2026"}, "--run_date must use ISO format"),
        ({"--build_run_id": "v2_12345"}, "--build_run_id must start"),
        ({"--build_run_id": "v1_"}, "--build_run_id must start"),
        ({"--scope_manifest_json": "[]"}, "must be a non-empty JSON list"),
    ],
)
def test_publish_arguments_reject_invalid_values(overrides, message):
    with pytest.raises(ValueError, match=message):
        publish_build.parse_publish_build_arguments(
            StubJobParser(valid_job_arguments(**overrides))
        )


@pytest.mark.parametrize(
    (
        "route",
        "scope_column",
        "key_columns",
        "public_columns",
    ),
    [
        (
            "v1",
            "Location",
            ("AccountNumber", "Location"),
            publish_build.V1_PUBLIC_COLUMNS,
        ),
        (
            "v2",
            "PageType",
            ("AccountNumber", "PageType", "Rank"),
            publish_build.V2_PUBLIC_COLUMNS,
        ),
    ],
)
def test_scope_contract_matches_public_assignment_schema(
    route,
    scope_column,
    key_columns,
    public_columns,
):
    contract = publish_build.build_assignment_scope_contract(
        route,
        (
            publish_build.ScopeManifestEntry("first"),
            publish_build.ScopeManifestEntry("second"),
        ),
    )

    assert contract.route == route
    assert contract.scope_column == scope_column
    assert contract.expected_scopes == ("first", "second")
    assert contract.key_columns == key_columns
    assert contract.public_columns == public_columns
    assert contract.publication_date_column == "rundate"


@pytest.mark.parametrize(
    ("route", "expected"),
    [
        (
            "v1",
            AssignmentTableContract(
                staging_table="catalog.schema.v1_staging",
                event_table="catalog.schema.events",
                history_table="catalog.schema.v1_history",
                latest_table="catalog.schema.v1_latest",
            ),
        ),
        (
            "v2",
            AssignmentTableContract(
                staging_table="catalog.schema.v2_staging",
                event_table="catalog.schema.events",
                history_table="catalog.schema.v2_history",
                latest_table="catalog.schema.v2_latest",
            ),
        ),
    ],
)
def test_assignment_tables_resolve_from_tables_write(route, expected):
    config = SimpleNamespace(
        tables_write=SimpleNamespace(
            assignments_build_staging="catalog.schema.v1_staging",
            assignments="catalog.schema.v1_history",
            assignments_latest="catalog.schema.v1_latest",
            assignments_v2_build_staging="catalog.schema.v2_staging",
            assignments_v2="catalog.schema.v2_history",
            assignments_v2_latest="catalog.schema.v2_latest",
            assignment_build_events="catalog.schema.events",
        )
    )

    assert publish_build.resolve_assignment_tables(config, route) == expected


def test_assignment_table_resolution_rejects_missing_mapping():
    config = SimpleNamespace(tables_write={"assignments": "history"})

    with pytest.raises(
        ValueError,
        match="tables_write.assignments_build_staging",
    ):
        publish_build.resolve_assignment_tables(config, "v1")


def test_publish_assignment_build_calls_complete_build_publisher(monkeypatch):
    config = SimpleNamespace(
        tables_write={
            "assignments_build_staging": "catalog.schema.staging",
            "assignments": "catalog.schema.history",
            "assignments_latest": "catalog.schema.latest",
            "assignment_build_events": "catalog.schema.events",
        }
    )
    args = publish_build.PublishBuildArguments(
        job_env="prod",
        client="next_uk",
        route="v1",
        run_date=date(2026, 7, 29),
        build_run_id="v1_12345",
        scope_manifest=(
            publish_build.ScopeManifestEntry("HP1"),
            publish_build.ScopeManifestEntry(
                "SB2",
                inherit_basic_from="SB1",
            ),
        ),
    )
    captured = {}
    expected_result = object()

    def fake_publish(spark, **kwargs):
        captured["spark"] = spark
        captured.update(kwargs)
        return expected_result

    monkeypatch.setattr(
        publish_build,
        "validate_and_publish_assignment_build",
        fake_publish,
    )

    spark = object()
    result = publish_build.publish_assignment_build(spark, config, args)

    assert result is expected_result
    assert captured["spark"] is spark
    assert captured["tables"] == AssignmentTableContract(
        staging_table="catalog.schema.staging",
        event_table="catalog.schema.events",
        history_table="catalog.schema.history",
        latest_table="catalog.schema.latest",
    )
    assert captured["columns"] == AssignmentColumnContract()
    assert captured["scope_contract"].expected_scopes == ("HP1", "SB2")
    assert captured["build_run_id"] == "v1_12345"
    assert captured["build_date"] == date(2026, 7, 29)


def test_main_logs_the_published_result_without_import_time_spark(
    monkeypatch,
):
    parser = StubJobParser(valid_job_arguments())
    config = object()
    spark = object()
    logged = []
    configured = []
    result = SimpleNamespace(
        route="v1",
        build_run_id="v1_12345",
        row_count=25,
        events=("HP1", "SB2"),
    )

    class StubLogger:
        def info(self, message, *args):
            logged.append((message, args))

    monkeypatch.setattr(publish_build, "get_job_parser", lambda: parser)
    monkeypatch.setattr(
        publish_build,
        "configure_logging",
        lambda **kwargs: configured.append(kwargs),
    )
    monkeypatch.setattr(
        publish_build, "get_logger", lambda _name: StubLogger()
    )
    monkeypatch.setattr(
        publish_build.config_manager,
        "load_config",
        lambda job_env, *, client: config,
    )
    monkeypatch.setattr(publish_build, "configure_spark", lambda: spark)
    monkeypatch.setattr(
        publish_build,
        "publish_assignment_build",
        lambda resolved_spark, resolved_config, args: result,
    )

    publish_build.main()

    assert parser.parse_calls == 1
    assert configured == [{"log_level": "INFO"}]
    assert logged[-1] == (
        "Published %s assignment build %s: %s rows across %s scopes",
        ("v1", "v1_12345", 25, 2),
    )

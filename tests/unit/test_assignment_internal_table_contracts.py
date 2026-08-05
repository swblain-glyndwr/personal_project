from pathlib import Path

import yaml

from jobs.table_operations.create_tables import (
    extract_create_table_columns,
    extract_table_paths,
)
from next_ads.common.config_manager import load_config
from next_ads.common.paths import resolve_sql_contract_path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _contract(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text()


def _normalised_sql(relative_path: str) -> str:
    return " ".join(_contract(relative_path).split()).lower()


def test_v1_staging_adds_build_identity_to_public_assignment_shape():
    public_columns = extract_create_table_columns(
        _contract("sql/decisioning/create_table_assignments.sql")
    )
    staging_columns = extract_create_table_columns(
        _contract("sql/decisioning/create_table_assignments_build_staging.sql")
    )

    metadata_columns = [
        ("BuildRunID", "STRING NOT NULL"),
        ("TaskRunID", "BIGINT NOT NULL"),
        ("ExecutionCount", "INT NOT NULL"),
        ("CandidateBuildID", "STRING NOT NULL"),
        ("CandidateBuildAttemptID", "STRING NOT NULL"),
        ("PortfolioID", "STRING NOT NULL"),
        ("PortfolioAttemptID", "STRING NOT NULL"),
        ("CandidateFoundationSnapshotID", "STRING NOT NULL"),
    ]
    assert [
        (name, data_type.upper())
        for name, data_type in staging_columns[: len(metadata_columns)]
    ] == metadata_columns
    assert staging_columns[len(metadata_columns) :] == public_columns
    assert "partitioned by (buildrunid, location)" in _normalised_sql(
        "sql/decisioning/create_table_assignments_build_staging.sql"
    )
    sql = _normalised_sql(
        "sql/decisioning/create_table_assignments_build_staging.sql"
    )
    assert "primary key ( buildrunid, accountnumber, location)" in sql
    assert "primary key ( buildrunid, taskrunid" not in sql
    assert " check (" not in sql


def test_v2_staging_adds_build_identity_to_public_assignment_shape():
    public_columns = extract_create_table_columns(
        _contract("sql/adsv2/create_table_assignments_v2.sql")
    )
    staging_columns = extract_create_table_columns(
        _contract("sql/adsv2/create_table_assignments_v2_build_staging.sql")
    )

    metadata_columns = [
        ("BuildRunID", "STRING NOT NULL"),
        ("TaskRunID", "BIGINT NOT NULL"),
        ("ExecutionCount", "INT NOT NULL"),
        ("CandidateBuildID", "STRING NOT NULL"),
        ("CandidateBuildAttemptID", "STRING NOT NULL"),
        ("PortfolioID", "STRING NOT NULL"),
        ("PortfolioAttemptID", "STRING NOT NULL"),
        ("CandidateFoundationSnapshotID", "STRING NOT NULL"),
    ]
    assert [
        (name, data_type.upper())
        for name, data_type in staging_columns[: len(metadata_columns)]
    ] == metadata_columns
    assert staging_columns[len(metadata_columns) :] == public_columns
    assert "partitioned by (buildrunid, pagetype)" in _normalised_sql(
        "sql/adsv2/create_table_assignments_v2_build_staging.sql"
    )
    sql = _normalised_sql(
        "sql/adsv2/create_table_assignments_v2_build_staging.sql"
    )
    assert "primary key ( buildrunid, accountnumber, pagetype, rank)" in sql
    assert "primary key ( buildrunid, taskrunid" not in sql
    assert " check (" not in sql


def test_assignment_build_events_has_validated_retention_compatible_contract():
    relative_path = "sql/decisioning/create_table_assignment_build_events.sql"
    columns = [
        (name, data_type.upper())
        for name, data_type in extract_create_table_columns(
            _contract(relative_path)
        )
    ]
    sql = _normalised_sql(relative_path)

    assert columns == [
        ("BuildRunID", "STRING NOT NULL"),
        ("Route", "STRING NOT NULL"),
        ("Scope", "STRING NOT NULL"),
        ("Status", "STRING NOT NULL"),
        ("RowCount", "BIGINT NOT NULL"),
        ("BuildDate", "DATE NOT NULL"),
        ("TaskRunID", "BIGINT NOT NULL"),
        ("ExecutionCount", "INT NOT NULL"),
        ("CompletedAt", "TIMESTAMP NOT NULL"),
        ("CandidateBuildID", "STRING NOT NULL"),
        ("CandidateBuildAttemptID", "STRING NOT NULL"),
        ("PortfolioID", "STRING NOT NULL"),
        ("PortfolioAttemptID", "STRING NOT NULL"),
        ("CandidateFoundationSnapshotID", "STRING NOT NULL"),
    ]
    # Databricks CREATE TABLE only accepts key constraints inline. The
    # publisher validates route, status, counts and run identity in code.
    assert " check (" not in sql
    assert "partitioned by (builddate)" in sql
    assert "delta.appendonly" not in sql


def test_internal_assignment_tables_resolve_through_create_table_helper(
    monkeypatch,
):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "test_user")
    config = load_config("dev", client="next_uk")
    configured_tables = extract_table_paths(config.tables_write.to_dict())

    expected_tables = {
        "assignments_build_staging": (
            "marketingdata_dev.test_user."
            "next_uk_nextads_assignments_build_staging"
        ),
        "assignments_v2_build_staging": (
            "marketingdata_dev.test_user."
            "next_uk_nextads_assignments_v2_build_staging"
        ),
        "assignment_build_events": (
            "marketingdata_dev.test_user."
            "next_uk_nextads_assignment_build_events"
        ),
    }

    for table_ref, expected_table in expected_tables.items():
        assert configured_tables[table_ref] == expected_table
        assert resolve_sql_contract_path(table_ref).is_file()


def test_internal_assignment_tables_resolve_for_release_environments(
    monkeypatch,
):
    monkeypatch.setenv("DYNACONF_SKIP_ENV", "true")
    monkeypatch.setenv("USER_SCHEMA", "ignored_user")

    for job_env, namespace in [
        ("preprod", "marketingdata_prod.ds_sandbox"),
        ("prod", "marketingdata_prod.warehouse"),
    ]:
        config = load_config(job_env, client="next_uk")
        assert config.tables_write.assignments_build_staging == (
            f"{namespace}.next_uk_nextads_assignments_build_staging"
        )
        assert config.tables_write.assignments_v2_build_staging == (
            f"{namespace}.next_uk_nextads_assignments_v2_build_staging"
        )
        assert config.tables_write.assignment_build_events == (
            f"{namespace}.next_uk_nextads_assignment_build_events"
        )


def test_legacy_next_uk_client_config_exposes_internal_assignment_tables():
    client_config = yaml.safe_load(
        (PROJECT_ROOT / "configs/clients/next_uk.yaml").read_text()
    )
    write_tables = client_config["default"]["tables"]["write"]

    assert write_tables["assignments_build_staging"] == (
        "{catalog}.{schema}.{client}_nextads_assignments_build_staging"
    )
    assert write_tables["assignments_v2_build_staging"] == (
        "{catalog}.{schema}.{client}_nextads_assignments_v2_build_staging"
    )
    assert write_tables["assignment_build_events"] == (
        "{catalog}.{schema}.{client}_nextads_assignment_build_events"
    )

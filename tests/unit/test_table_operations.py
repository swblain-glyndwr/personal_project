import pytest

from jobs.table_operations.table_operations import (
    alter_tables,
    build_drop_table_statement,
    create_missing_tables,
    drop_tables,
    parse_bool,
    recreate_tables,
    resolve_table_name,
    run_configured_table_operation,
    split_table_names,
)


class FakeSpark:
    def __init__(self):
        self.sql_calls = []

    def sql(self, statement):
        self.sql_calls.append(statement)


def test_split_table_names_ignores_empty_values():
    assert split_table_names("table_a, table_b,,") == ["table_a", "table_b"]


def test_resolve_unqualified_table_uses_target_namespace():
    assert resolve_table_name(
        "next_uk_nextads_control_sheet",
        "marketingdata_dev",
        "nextads_integration",
    ) == (
        "marketingdata_dev",
        "nextads_integration",
        "next_uk_nextads_control_sheet",
    )


def test_resolve_fully_qualified_table_requires_matching_namespace():
    assert resolve_table_name(
        "marketingdata_prod.warehouse.next_uk_nextads_model_scores_latest",
        "marketingdata_prod",
        "warehouse",
    ) == (
        "marketingdata_prod",
        "warehouse",
        "next_uk_nextads_model_scores_latest",
    )

    with pytest.raises(ValueError, match="must match --catalog and --schema"):
        resolve_table_name(
            "marketingdata_prod.warehouse.next_uk_nextads_model_scores_latest",
            "marketingdata_prod",
            "ds_sandbox",
        )


def test_resolve_table_rejects_wildcards_and_partial_qualification():
    with pytest.raises(ValueError, match="Wildcard"):
        resolve_table_name("next_uk_nextads_*", "catalog", "schema")

    with pytest.raises(ValueError, match="unqualified or fully qualified"):
        resolve_table_name("schema.table", "catalog", "schema")


def test_build_drop_table_statement_quotes_identifiers():
    assert build_drop_table_statement(("catalog", "schema", "table`name")) == (
        "DROP TABLE IF EXISTS `catalog`.`schema`.`table``name`"
    )


def test_drop_tables_is_dry_run_by_default_behavior():
    spark = FakeSpark()

    statements = drop_tables(
        spark,
        catalog="marketingdata_prod",
        schema="ds_sandbox",
        tables="table_a,marketingdata_prod.ds_sandbox.table_b",
        confirm_destructive=False,
        dry_run=True,
    )

    assert statements == [
        "DROP TABLE IF EXISTS `marketingdata_prod`.`ds_sandbox`.`table_a`",
        "DROP TABLE IF EXISTS `marketingdata_prod`.`ds_sandbox`.`table_b`",
    ]
    assert spark.sql_calls == []


def test_drop_tables_requires_confirmation_when_executing():
    with pytest.raises(ValueError, match="confirm_destructive"):
        drop_tables(
            FakeSpark(),
            catalog="marketingdata_prod",
            schema="warehouse",
            tables="next_uk_nextads_model_scores_latest",
            confirm_destructive=False,
            dry_run=False,
        )


def test_drop_tables_executes_confirmed_drop_statements():
    spark = FakeSpark()

    statements = drop_tables(
        spark,
        catalog="marketingdata_prod",
        schema="warehouse",
        tables="next_uk_nextads_model_scores_latest",
        confirm_destructive=True,
        dry_run=False,
    )

    assert statements == [
        "DROP TABLE IF EXISTS "
        "`marketingdata_prod`.`warehouse`.`next_uk_nextads_model_scores_latest`"
    ]
    assert spark.sql_calls == statements


def test_drop_tables_rejects_empty_tables_when_executing():
    with pytest.raises(ValueError, match="--tables"):
        drop_tables(
            FakeSpark(),
            catalog="marketingdata_prod",
            schema="warehouse",
            tables="",
            confirm_destructive=True,
            dry_run=False,
        )


def test_create_missing_tables_requires_confirmation_when_executing():
    with pytest.raises(ValueError, match="confirm_mutating"):
        create_missing_tables(
            job_env="preprod",
            client="next_uk",
            log_level="INFO",
            confirm_mutating=False,
            dry_run=False,
        )


def test_alter_tables_requires_confirmation_when_executing():
    with pytest.raises(ValueError, match="confirm_mutating"):
        alter_tables(
            job_env="prod",
            client="next_uk",
            log_level="INFO",
            confirm_mutating=False,
            dry_run=False,
        )


def test_recreate_tables_requires_destructive_confirmation_when_executing():
    with pytest.raises(ValueError, match="confirm_destructive"):
        recreate_tables(
            job_env="dev",
            client="next_uk",
            log_level="INFO",
            confirm_destructive=False,
            dry_run=False,
        )


def test_configured_operations_are_dry_run_by_default(monkeypatch):
    calls = []

    def fake_main(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(
        "jobs.table_operations.table_operations.create_tables.main",
        fake_main,
    )

    assert (
        run_configured_table_operation(
            operation="alter_tables",
            job_env="preprod",
            client="next_uk",
            log_level="INFO",
            confirm_mutating=False,
            confirm_destructive=False,
            dry_run=True,
        )
        == []
    )
    assert calls == []


def test_create_missing_tables_delegates_to_create_tables(monkeypatch):
    calls = []

    def fake_main(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(
        "jobs.table_operations.table_operations.create_tables.main",
        fake_main,
    )

    create_missing_tables(
        job_env="preprod",
        client="next_uk",
        log_level="INFO",
        confirm_mutating=True,
        dry_run=False,
    )

    assert calls == [
        {
            "JOB_ENV": "preprod",
            "CLIENT": "next_uk",
            "LOG_LEVEL": "INFO",
            "DROP_TABLES": False,
            "ALTER_TABLES": False,
            "ALLOW_NON_DEV_DROP": False,
            "ALLOW_NON_DEV_ALTER": False,
        }
    ]


def test_alter_tables_delegates_with_non_dev_alter_enabled(monkeypatch):
    calls = []

    def fake_main(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(
        "jobs.table_operations.table_operations.create_tables.main",
        fake_main,
    )

    alter_tables(
        job_env="prod",
        client="next_uk",
        log_level="INFO",
        confirm_mutating=True,
        dry_run=False,
    )

    assert calls[0]["ALTER_TABLES"] is True
    assert calls[0]["ALLOW_NON_DEV_ALTER"] is True
    assert calls[0]["DROP_TABLES"] is False


def test_recreate_tables_delegates_with_non_dev_drop_enabled(monkeypatch):
    calls = []

    def fake_main(**kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(
        "jobs.table_operations.table_operations.create_tables.main",
        fake_main,
    )

    recreate_tables(
        job_env="preprod",
        client="next_uk",
        log_level="INFO",
        confirm_destructive=True,
        dry_run=False,
    )

    assert calls[0]["DROP_TABLES"] is True
    assert calls[0]["ALLOW_NON_DEV_DROP"] is True
    assert calls[0]["ALTER_TABLES"] is False


def test_parse_bool_accepts_expected_values():
    assert parse_bool("true") is True
    assert parse_bool("False") is False

    with pytest.raises(ValueError, match="Unsupported boolean"):
        parse_bool("maybe")

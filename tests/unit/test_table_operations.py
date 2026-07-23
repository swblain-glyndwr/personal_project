import sys
import types
from pathlib import Path

import pytest

import jobs.table_operations.table_operations as table_operations_module
from jobs.table_operations.table_operations import (
    alter_tables,
    build_drop_table_statement,
    copy_prod_tables_to_dev,
    create_missing_tables,
    drop_tables,
    parse_bool,
    recreate_tables,
    resolve_operation_from_flags,
    resolve_project_root,
    resolve_table_name,
    run_configured_table_operation,
    run_operation,
    split_table_names,
)


class FakeSpark:
    def __init__(self):
        self.sql_calls = []

    def sql(self, statement):
        self.sql_calls.append(statement)


class FakeCreateTablesModule:
    def __init__(self):
        self.calls = []

    def main(self, **kwargs):
        self.calls.append(kwargs)


class FakeMirrorProdTablesModule:
    def __init__(self):
        self.calls = []

    def main(self, **kwargs):
        self.calls.append(kwargs)


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


def test_drop_tables_does_not_load_create_tables_module(monkeypatch):
    monkeypatch.delitem(sys.modules, "jobs.table_operations.create_tables", raising=False)
    monkeypatch.setattr(
        table_operations_module,
        "bootstrap_project_imports",
        lambda: pytest.fail("drop_tables must not bootstrap project imports"),
    )

    drop_tables(
        FakeSpark(),
        catalog="marketingdata_prod",
        schema="warehouse",
        tables="next_uk_nextads_theme_affinity_predict_ranked",
        confirm_destructive=True,
        dry_run=False,
    )

    assert "jobs.table_operations.create_tables" not in sys.modules


def test_resolve_project_root_handles_databricks_exec_without_file(monkeypatch):
    class FakeNotebookPath:
        def get(self):
            return (
                "/Workspace/root/MarketingData/DABs/next-ads/.bundle/files/"
                "jobs/table_operations/table_operations.py"
            )

    class FakeContext:
        def notebookPath(self):  # noqa: N802 - mirrors Databricks dbutils API
            return FakeNotebookPath()

    class FakeNotebook:
        def getContext(self):  # noqa: N802 - mirrors Databricks dbutils API
            return FakeContext()

    class FakeEntryPoint:
        def getDbutils(self):  # noqa: N802 - mirrors Databricks dbutils API
            return FakeDbutilsApi()

    class FakeNotebookAccessor:
        entry_point = FakeEntryPoint()

    class FakeDbutilsApi:
        def notebook(self):
            return FakeNotebook()

    class FakeDbutils:
        notebook = FakeNotebookAccessor()

    dsutils_module = types.ModuleType("dsutils")
    dbc_module = types.ModuleType("dsutils.dbc")
    dbc_module.get_dbutils = lambda: FakeDbutils()
    monkeypatch.setitem(sys.modules, "dsutils", dsutils_module)
    monkeypatch.setitem(sys.modules, "dsutils.dbc", dbc_module)
    monkeypatch.delattr(table_operations_module, "__file__", raising=False)

    assert resolve_project_root() == Path(
        "/Workspace/root/MarketingData/DABs/next-ads/.bundle/files"
    )


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


def test_resolve_operation_from_flags_requires_one_selected_action():
    with pytest.raises(ValueError, match="Exactly one run_\\*"):
        resolve_operation_from_flags(
            run_create_missing_tables="false",
            run_alter_tables="false",
            run_recreate_tables="false",
            run_drop_tables="false",
            run_copy_prod_tables_to_dev="false",
        )

    assert (
        resolve_operation_from_flags(run_copy_prod_tables_to_dev="true")
        == "copy_prod_tables_to_dev"
    )


def test_resolve_operation_from_flags_rejects_multiple_selected_actions():
    with pytest.raises(ValueError, match="Exactly one run_\\*"):
        resolve_operation_from_flags(
            run_create_missing_tables="true",
            run_copy_prod_tables_to_dev="true",
        )


def test_resolve_operation_from_flags_keeps_operation_backwards_compatible():
    assert resolve_operation_from_flags(operation="drop_tables") == "drop_tables"


def test_configured_operations_are_dry_run_by_default(monkeypatch):
    create_tables = FakeCreateTablesModule()
    monkeypatch.setattr(
        "jobs.table_operations.table_operations.load_create_tables_module",
        lambda: create_tables,
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
    assert create_tables.calls == []


def test_create_missing_tables_delegates_to_create_tables(monkeypatch):
    create_tables = FakeCreateTablesModule()
    monkeypatch.setattr(
        "jobs.table_operations.table_operations.load_create_tables_module",
        lambda: create_tables,
    )

    create_missing_tables(
        job_env="preprod",
        client="next_uk",
        log_level="INFO",
        confirm_mutating=True,
        dry_run=False,
    )

    assert create_tables.calls == [
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
    create_tables = FakeCreateTablesModule()
    monkeypatch.setattr(
        "jobs.table_operations.table_operations.load_create_tables_module",
        lambda: create_tables,
    )

    alter_tables(
        job_env="prod",
        client="next_uk",
        log_level="INFO",
        confirm_mutating=True,
        dry_run=False,
    )

    assert create_tables.calls[0]["ALTER_TABLES"] is True
    assert create_tables.calls[0]["ALLOW_NON_DEV_ALTER"] is True
    assert create_tables.calls[0]["DROP_TABLES"] is False


def test_recreate_tables_delegates_with_non_dev_drop_enabled(monkeypatch):
    create_tables = FakeCreateTablesModule()
    monkeypatch.setattr(
        "jobs.table_operations.table_operations.load_create_tables_module",
        lambda: create_tables,
    )

    recreate_tables(
        job_env="preprod",
        client="next_uk",
        log_level="INFO",
        confirm_destructive=True,
        dry_run=False,
    )

    assert create_tables.calls[0]["DROP_TABLES"] is True
    assert create_tables.calls[0]["ALLOW_NON_DEV_DROP"] is True
    assert create_tables.calls[0]["ALTER_TABLES"] is False


def test_copy_prod_tables_to_dev_is_dev_only():
    with pytest.raises(ValueError, match="only supports job_env=dev"):
        copy_prod_tables_to_dev(
            job_env="preprod",
            client="next_uk",
            log_level="INFO",
            confirm_mutating=True,
            dry_run=False,
            history_days=1,
            input_tables_only=True,
        )


def test_copy_prod_tables_to_dev_requires_confirmation_when_executing():
    with pytest.raises(ValueError, match="confirm_mutating"):
        copy_prod_tables_to_dev(
            job_env="dev",
            client="next_uk",
            log_level="INFO",
            confirm_mutating=False,
            dry_run=False,
            history_days=1,
            input_tables_only=True,
        )


def test_copy_prod_tables_to_dev_dry_run_does_not_load_mirror_module(monkeypatch):
    monkeypatch.setattr(
        "jobs.table_operations.table_operations.load_mirror_prod_tables_module",
        lambda: pytest.fail("dry-run copy must not load mirror module"),
    )

    assert (
        copy_prod_tables_to_dev(
            job_env="dev",
            client="next_uk",
            log_level="INFO",
            confirm_mutating=False,
            dry_run=True,
            history_days=1,
            input_tables_only=True,
        )
        == []
    )


def test_copy_prod_tables_to_dev_delegates_to_mirror_module(monkeypatch):
    mirror_prod_tables = FakeMirrorProdTablesModule()
    monkeypatch.setattr(
        "jobs.table_operations.table_operations.load_mirror_prod_tables_module",
        lambda: mirror_prod_tables,
    )

    copy_prod_tables_to_dev(
        job_env="dev",
        client="next_uk",
        log_level="INFO",
        confirm_mutating=True,
        dry_run=False,
        history_days=3,
        input_tables_only=False,
    )

    assert mirror_prod_tables.calls == [
        {
            "job_env": "dev",
            "client": "next_uk",
            "log_level": "INFO",
            "history_days": 3,
            "input_tables_only": False,
        }
    ]


def test_run_operation_uses_explicit_action_flags(monkeypatch):
    mirror_prod_tables = FakeMirrorProdTablesModule()
    monkeypatch.setattr(
        "jobs.table_operations.table_operations.load_mirror_prod_tables_module",
        lambda: mirror_prod_tables,
    )

    run_operation(
        FakeSpark(),
        operation=None,
        job_env="dev",
        client="next_uk",
        catalog="marketingdata_dev",
        schema="ds_sandbox",
        tables="",
        confirm_mutating=True,
        confirm_destructive=False,
        dry_run=False,
        history_days=2,
        input_tables_only=True,
        run_copy_prod_tables_to_dev="true",
    )

    assert mirror_prod_tables.calls[0]["history_days"] == 2
    assert mirror_prod_tables.calls[0]["input_tables_only"] is True


def test_parse_bool_accepts_expected_values():
    assert parse_bool("true") is True
    assert parse_bool("False") is False

    with pytest.raises(ValueError, match="Unsupported boolean"):
        parse_bool("maybe")

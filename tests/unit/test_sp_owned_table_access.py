import logging

import pytest

from jobs.table_operations.grant_sp_owned_table_access import (
    ACCESS_RECIPIENTS,
    PROD_CATALOG,
    PROD_JOB_ENV,
    PROD_SCHEMA,
    PROD_SERVICE_PRINCIPAL,
    Relation,
    build_grant_plan,
    qualified_name,
    reconcile_access,
    relation_grant_policy,
    show_grants_statement,
    validate_runtime_scope,
)


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0]


class FakeSpark:
    def __init__(
        self,
        relations,
        *,
        principal=PROD_SERVICE_PRINCIPAL,
        omit_verified_grant=None,
    ):
        self.relations = relations
        self.principal = principal
        self.omit_verified_grant = omit_verified_grant
        self.sql_calls = []

    def sql(self, statement):
        self.sql_calls.append(statement)
        if statement == "SELECT current_user() AS principal":
            return FakeResult([{"principal": self.principal}])
        if "information_schema.tables" in statement:
            return FakeResult(
                [
                    {
                        "table_catalog": relation.catalog,
                        "table_schema": relation.schema,
                        "table_name": relation.name,
                        "table_type": relation.relation_type,
                        "table_owner": relation.owner,
                    }
                    for relation in self.relations
                ]
            )
        if statement.startswith("GRANT "):
            return FakeResult([])
        if statement.startswith("SHOW GRANTS ON "):
            relation = next(
                relation
                for relation in self.relations
                if statement == show_grants_statement(relation)
            )
            _, privileges = relation_grant_policy(relation.relation_type)
            rows = []
            for principal in ACCESS_RECIPIENTS:
                for privilege in privileges:
                    if self.omit_verified_grant == (principal, privilege):
                        continue
                    rows.append(
                        {"principal": principal, "actionType": privilege}
                    )
            return FakeResult(rows)
        raise AssertionError(f"Unexpected SQL: {statement}")


def relation(name, relation_type="MANAGED"):
    return Relation(
        catalog=PROD_CATALOG,
        schema=PROD_SCHEMA,
        name=name,
        relation_type=relation_type,
        owner=PROD_SERVICE_PRINCIPAL,
    )


def test_access_recipients_are_fixed_to_requested_users():
    assert ACCESS_RECIPIENTS == (
        "stephen_blain@next.co.uk",
        "claire_wilsonbarnes@next.co.uk",
        "hadi_miah@next.co.uk",
    )


def test_runtime_scope_accepts_only_the_prod_namespace_and_sp():
    validate_runtime_scope(
        job_env=PROD_JOB_ENV,
        catalog=PROD_CATALOG,
        schema=PROD_SCHEMA,
        expected_owner=PROD_SERVICE_PRINCIPAL,
    )

    for override in (
        {"job_env": "dev"},
        {"catalog": "marketingdata_dev"},
        {"schema": "ds_sandbox"},
        {"expected_owner": "another-principal"},
    ):
        scope = {
            "job_env": PROD_JOB_ENV,
            "catalog": PROD_CATALOG,
            "schema": PROD_SCHEMA,
            "expected_owner": PROD_SERVICE_PRINCIPAL,
        }
        scope.update(override)
        with pytest.raises(ValueError, match="fixed PROD scope"):
            validate_runtime_scope(**scope)


def test_build_grant_plan_maps_types_and_quotes_names():
    relations = [
        relation("managed`table", "MANAGED"),
        relation("external_table", "EXTERNAL"),
        relation("normal_view", "VIEW"),
        relation("materialised_view", "MATERIALIZED_VIEW"),
    ]

    statements = [
        operation.statement
        for operation in build_grant_plan(relations, ["person`@next.co.uk"])
    ]

    assert statements == [
        "GRANT SELECT, MODIFY ON TABLE "
        "`marketingdata_prod`.`warehouse`.`external_table` "
        "TO `person``@next.co.uk`",
        "GRANT SELECT, MODIFY ON TABLE "
        "`marketingdata_prod`.`warehouse`.`managed``table` "
        "TO `person``@next.co.uk`",
        "GRANT SELECT ON MATERIALIZED VIEW "
        "`marketingdata_prod`.`warehouse`.`materialised_view` "
        "TO `person``@next.co.uk`",
        "GRANT SELECT ON VIEW "
        "`marketingdata_prod`.`warehouse`.`normal_view` "
        "TO `person``@next.co.uk`",
    ]


def test_dry_run_discovers_and_logs_without_granting(caplog):
    spark = FakeSpark([relation("table_b"), relation("table_a", "VIEW")])

    with caplog.at_level(logging.INFO):
        result = reconcile_access(
            spark,
            confirm_mutating=False,
            dry_run=True,
        )

    assert result["status"] == "DRY_RUN"
    assert result["relation_count"] == 2
    assert result["statement_count"] == 6
    assert not any(call.startswith("GRANT ") for call in spark.sql_calls)
    assert not any(call.startswith("SHOW GRANTS") for call in spark.sql_calls)
    assert "DRY_RUN GRANT SELECT ON VIEW" in caplog.text


def test_apply_requires_explicit_confirmation_before_sql():
    spark = FakeSpark([relation("table_a")])

    with pytest.raises(ValueError, match="confirm_mutating"):
        reconcile_access(
            spark,
            confirm_mutating=False,
            dry_run=False,
        )

    assert spark.sql_calls == []


def test_apply_grants_and_verifies_every_relation():
    relations = [relation("table_a"), relation("view_a", "VIEW")]
    spark = FakeSpark(relations)

    result = reconcile_access(
        spark,
        confirm_mutating=True,
        dry_run=False,
    )

    grant_calls = [
        call for call in spark.sql_calls if call.startswith("GRANT ")
    ]
    show_calls = [
        call for call in spark.sql_calls if call.startswith("SHOW GRANTS ON ")
    ]
    assert result["status"] == "SUCCEEDED"
    assert grant_calls == [
        operation.statement for operation in build_grant_plan(relations)
    ]
    assert show_calls == [show_grants_statement(item) for item in relations]


def test_apply_fails_when_readback_is_missing_an_expected_grant():
    spark = FakeSpark(
        [relation("table_a")],
        omit_verified_grant=(ACCESS_RECIPIENTS[0], "MODIFY"),
    )

    with pytest.raises(RuntimeError, match="Grant verification failed"):
        reconcile_access(
            spark,
            confirm_mutating=True,
            dry_run=False,
        )


def test_wrong_execution_identity_fails_before_discovery_or_grants():
    spark = FakeSpark([relation("table_a")], principal="someone-else")

    with pytest.raises(
        RuntimeError, match="expected production service principal"
    ):
        reconcile_access(
            spark,
            confirm_mutating=False,
            dry_run=True,
        )

    assert spark.sql_calls == ["SELECT current_user() AS principal"]


def test_empty_or_unsupported_relation_inventory_fails_closed():
    with pytest.raises(RuntimeError, match="No production"):
        reconcile_access(
            FakeSpark([]),
            confirm_mutating=False,
            dry_run=True,
        )

    with pytest.raises(RuntimeError, match="Unsupported relation type"):
        reconcile_access(
            FakeSpark([relation("streaming_table", "STREAMING_TABLE")]),
            confirm_mutating=False,
            dry_run=True,
        )


def test_qualified_name_quotes_every_identifier_part():
    assert (
        qualified_name(
            Relation("cat`alog", "sche`ma", "ta`ble", "MANAGED", "owner")
        )
        == "`cat``alog`.`sche``ma`.`ta``ble`"
    )

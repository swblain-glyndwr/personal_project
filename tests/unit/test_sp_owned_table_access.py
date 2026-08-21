import logging

import pytest

from jobs.table_operations.grant_sp_owned_table_access import (
    ACCESS_RECIPIENTS,
    DEV_CATALOG,
    DEV_JOB_ENV,
    DEV_RELATION_SCOPE,
    DEV_SERVICE_PRINCIPAL,
    PROD_CATALOG,
    PROD_JOB_ENV,
    PROD_RELATION_SCOPE,
    PROD_SCHEMAS,
    PROD_SERVICE_PRINCIPAL,
    Relation,
    RuntimeScope,
    build_grant_plan,
    qualified_name,
    reconcile_access,
    relation_grant_policy,
    show_grants_statement,
    validate_runtime_scope,
)


PROD_SCOPE = RuntimeScope(
    PROD_JOB_ENV,
    PROD_CATALOG,
    PROD_RELATION_SCOPE,
    PROD_SERVICE_PRINCIPAL,
)
DEV_SCOPE = RuntimeScope(
    DEV_JOB_ENV,
    DEV_CATALOG,
    DEV_RELATION_SCOPE,
    DEV_SERVICE_PRINCIPAL,
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


def relation(name, relation_type="MANAGED", scope=PROD_SCOPE, schema=None):
    return Relation(
        catalog=scope.catalog,
        schema=schema or PROD_SCHEMAS[0],
        name=name,
        relation_type=relation_type,
        owner=scope.owner,
    )


def test_access_recipients_are_fixed_to_requested_users():
    assert ACCESS_RECIPIENTS == (
        "stephen_blain@next.co.uk",
        "claire_wilsonbarnes@next.co.uk",
        "hadi_miah@next.co.uk",
    )


def test_runtime_scope_accepts_the_fixed_prod_schemas_and_sp():
    assert (
        validate_runtime_scope(
            job_env=PROD_JOB_ENV,
            catalog=PROD_CATALOG,
            relation_scope=PROD_RELATION_SCOPE,
            expected_owner=PROD_SERVICE_PRINCIPAL,
        )
        == PROD_SCOPE
    )

    for override in (
        {"job_env": "dev"},
        {"catalog": "marketingdata_dev"},
        {"relation_scope": "warehouse"},
        {"expected_owner": "another-principal"},
    ):
        scope = {
            "job_env": PROD_JOB_ENV,
            "catalog": PROD_CATALOG,
            "relation_scope": PROD_RELATION_SCOPE,
            "expected_owner": PROD_SERVICE_PRINCIPAL,
        }
        scope.update(override)
        with pytest.raises(ValueError, match="fixed PROD warehouse"):
            validate_runtime_scope(**scope)


def test_runtime_scope_accepts_the_dev_whole_catalog_owner_scope():
    assert (
        validate_runtime_scope(
            job_env=DEV_JOB_ENV,
            catalog=DEV_CATALOG,
            relation_scope=DEV_RELATION_SCOPE,
            expected_owner=DEV_SERVICE_PRINCIPAL,
        )
        == DEV_SCOPE
    )


@pytest.mark.parametrize(
    ("relation_scope", "owner"),
    (
        ("stephen_blain", DEV_SERVICE_PRINCIPAL),
        ("nextads_integration", DEV_SERVICE_PRINCIPAL),
        (DEV_RELATION_SCOPE, PROD_SERVICE_PRINCIPAL),
    ),
)
def test_runtime_scope_rejects_narrow_or_wrong_owner_dev_scopes(
    relation_scope, owner
):
    with pytest.raises(ValueError, match="whole-catalog owner scope"):
        validate_runtime_scope(
            job_env=DEV_JOB_ENV,
            catalog=DEV_CATALOG,
            relation_scope=relation_scope,
            expected_owner=owner,
        )


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
        "GRANT ALL PRIVILEGES ON TABLE "
        "`marketingdata_prod`.`warehouse`.`external_table` "
        "TO `person``@next.co.uk`",
        "GRANT MANAGE ON TABLE "
        "`marketingdata_prod`.`warehouse`.`external_table` "
        "TO `person``@next.co.uk`",
        "GRANT ALL PRIVILEGES ON TABLE "
        "`marketingdata_prod`.`warehouse`.`managed``table` "
        "TO `person``@next.co.uk`",
        "GRANT MANAGE ON TABLE "
        "`marketingdata_prod`.`warehouse`.`managed``table` "
        "TO `person``@next.co.uk`",
        "GRANT ALL PRIVILEGES ON MATERIALIZED VIEW "
        "`marketingdata_prod`.`warehouse`.`materialised_view` "
        "TO `person``@next.co.uk`",
        "GRANT MANAGE ON MATERIALIZED VIEW "
        "`marketingdata_prod`.`warehouse`.`materialised_view` "
        "TO `person``@next.co.uk`",
        "GRANT ALL PRIVILEGES ON VIEW "
        "`marketingdata_prod`.`warehouse`.`normal_view` "
        "TO `person``@next.co.uk`",
        "GRANT MANAGE ON VIEW "
        "`marketingdata_prod`.`warehouse`.`normal_view` "
        "TO `person``@next.co.uk`",
    ]


def test_dry_run_discovers_and_logs_without_granting(caplog):
    spark = FakeSpark([relation("table_b"), relation("table_a", "VIEW")])

    with caplog.at_level(logging.INFO):
        result = reconcile_access(
            spark,
            scope=PROD_SCOPE,
            confirm_mutating=False,
            dry_run=True,
        )

    assert result["status"] == "DRY_RUN"
    assert result["relation_count"] == 2
    assert result["statement_count"] == 12
    assert not any(call.startswith("GRANT ") for call in spark.sql_calls)
    assert not any(call.startswith("SHOW GRANTS") for call in spark.sql_calls)
    assert "DRY_RUN GRANT ALL PRIVILEGES ON VIEW" in caplog.text
    assert "DRY_RUN GRANT MANAGE ON VIEW" in caplog.text


def test_apply_requires_explicit_confirmation_before_sql():
    spark = FakeSpark([relation("table_a")])

    with pytest.raises(ValueError, match="confirm_mutating"):
        reconcile_access(
            spark,
            scope=PROD_SCOPE,
            confirm_mutating=False,
            dry_run=False,
        )

    assert spark.sql_calls == []


def test_apply_grants_and_verifies_every_relation():
    relations = [relation("table_a"), relation("view_a", "VIEW")]
    spark = FakeSpark(relations)

    result = reconcile_access(
        spark,
        scope=PROD_SCOPE,
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
        omit_verified_grant=(ACCESS_RECIPIENTS[0], "MANAGE"),
    )

    with pytest.raises(RuntimeError, match="Grant verification failed"):
        reconcile_access(
            spark,
            scope=PROD_SCOPE,
            confirm_mutating=True,
            dry_run=False,
        )


def test_wrong_execution_identity_fails_before_discovery_or_grants():
    spark = FakeSpark([relation("table_a")], principal="someone-else")

    with pytest.raises(RuntimeError, match="expected service principal"):
        reconcile_access(
            spark,
            scope=PROD_SCOPE,
            confirm_mutating=False,
            dry_run=True,
        )

    assert spark.sql_calls == ["SELECT current_user() AS principal"]


def test_empty_or_unsupported_relation_inventory_fails_closed():
    with pytest.raises(RuntimeError, match="No service-principal-owned"):
        reconcile_access(
            FakeSpark([]),
            scope=PROD_SCOPE,
            confirm_mutating=False,
            dry_run=True,
        )

    with pytest.raises(RuntimeError, match="Unsupported relation type"):
        reconcile_access(
            FakeSpark([relation("streaming_table", "STREAMING_TABLE")]),
            scope=PROD_SCOPE,
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


def test_dev_scope_uses_the_dev_identity_across_all_owned_schemas():
    spark = FakeSpark(
        [
            relation(
                "next_uk_nextads_example",
                scope=DEV_SCOPE,
                schema="stephen_blain",
            ),
            relation(
                "next_uk_nextads_shared",
                scope=DEV_SCOPE,
                schema="nextads_integration",
            ),
        ],
        principal=DEV_SERVICE_PRINCIPAL,
    )

    result = reconcile_access(
        spark,
        scope=DEV_SCOPE,
        confirm_mutating=False,
        dry_run=True,
    )

    discovery_sql = next(
        call for call in spark.sql_calls if "information_schema.tables" in call
    )
    assert result["job_env"] == DEV_JOB_ENV
    assert result["catalog"] == DEV_CATALOG
    assert result["relation_scope"] == DEV_RELATION_SCOPE
    assert result["schema_count"] == 2
    assert result["schemas"] == ["nextads_integration", "stephen_blain"]
    assert result["run_as"] == DEV_SERVICE_PRINCIPAL
    assert (
        "FROM `marketingdata_dev`.information_schema.tables" in discovery_sql
    )
    assert "table_schema =" not in discovery_sql
    assert "ORDER BY table_schema, table_name, table_type" in discovery_sql


def test_prod_scope_discovers_warehouse_and_ds_sandbox_only():
    spark = FakeSpark(
        [
            relation("warehouse_table", schema="warehouse"),
            relation("sandbox_table", schema="ds_sandbox"),
        ]
    )

    result = reconcile_access(
        spark,
        scope=PROD_SCOPE,
        confirm_mutating=False,
        dry_run=True,
    )

    discovery_sql = next(
        call for call in spark.sql_calls if "information_schema.tables" in call
    )
    assert result["relation_scope"] == PROD_RELATION_SCOPE
    assert result["schemas"] == ["ds_sandbox", "warehouse"]
    assert "table_schema IN ('warehouse', 'ds_sandbox')" in discovery_sql

    with pytest.raises(RuntimeError, match="Unexpected relation namespace"):
        reconcile_access(
            FakeSpark([relation("other_table", schema="other_schema")]),
            scope=PROD_SCOPE,
            confirm_mutating=False,
            dry_run=True,
        )

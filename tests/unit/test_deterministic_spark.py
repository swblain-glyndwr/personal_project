from types import SimpleNamespace

import pytest

import next_ads.common.determinism as determinism
from next_ads.common.determinism import (
    stable_bucket,
    stable_fraction,
    stable_hash,
    stable_hash64,
    stable_order,
)


class FakeExpression:
    def __init__(self, operation):
        self.operation = operation

    def asc(self):
        return FakeExpression(("asc", self.operation))

    def desc(self):
        return FakeExpression(("desc", self.operation))

    def asc_nulls_first(self):
        return FakeExpression(("asc_nulls_first", self.operation))

    def cast(self, data_type):
        return FakeExpression(("cast", self.operation, data_type))

    def __truediv__(self, other):
        """Represent deterministic fraction division."""
        return FakeExpression(("divide", self.operation, other.operation))


@pytest.fixture
def fake_functions(monkeypatch):
    fake = SimpleNamespace(
        col=lambda value: FakeExpression(("col", value)),
        lit=lambda value: FakeExpression(("lit", value)),
        xxhash64=lambda *values: FakeExpression(
            ("xxhash64", [value.operation for value in values])
        ),
        pmod=lambda value, modulus: FakeExpression(
            ("pmod", value.operation, modulus.operation)
        ),
    )
    monkeypatch.setattr(determinism, "F", fake)
    return fake


def test_stable_hash_includes_seed_and_all_business_keys(fake_functions):
    expression = stable_hash("group", "ad_id", seed=99)

    assert expression.operation == (
        "pmod",
        (
            "xxhash64",
            [("col", "group"), ("col", "ad_id"), ("lit", 99)],
        ),
        ("lit", 9_223_372_036_854_775_807),
    )
    assert stable_hash64("group", "ad_id", seed=99).operation == (
        expression.operation
    )


def test_stable_hash_supports_namespaced_contract_versions(fake_functions):
    expression = stable_hash(
        "group",
        "ad_id",
        seed=99,
        namespace="basic-allocation",
        version=2,
    )

    assert expression.operation == (
        "pmod",
        (
            "xxhash64",
            [
                ("col", "group"),
                ("col", "ad_id"),
                ("lit", "basic-allocation"),
                ("lit", "2"),
                ("lit", 99),
            ],
        ),
        ("lit", 9_223_372_036_854_775_807),
    )


def test_stable_fraction_uses_positive_hash_range(fake_functions):
    expression = stable_fraction("group", "account", seed=12)

    assert expression.operation == (
        "divide",
        (
            "cast",
            (
                "pmod",
                (
                    "xxhash64",
                    [
                        ("col", "group"),
                        ("col", "account"),
                        ("lit", 12),
                    ],
                ),
                ("lit", 9_223_372_036_854_775_807),
            ),
            "double",
        ),
        ("lit", float(9_223_372_036_854_775_807)),
    )


def test_stable_bucket_uses_positive_modulus(fake_functions):
    expression = stable_bucket(
        "group",
        "account",
        bucket_count=7,
        seed=12,
    )

    assert expression.operation == (
        "pmod",
        (
            "pmod",
            (
                "xxhash64",
                [
                    ("col", "group"),
                    ("col", "account"),
                    ("lit", 12),
                ],
            ),
            ("lit", 9_223_372_036_854_775_807),
        ),
        ("lit", 7),
    )


def test_stable_order_uses_hash_then_complete_key_fallbacks(fake_functions):
    expressions = stable_order(
        ["AccountNumber", "UniqueAdID"],
        seed=99,
        hash_descending=True,
    )

    assert [expression.operation for expression in expressions] == [
        (
            "desc",
            (
                "pmod",
                (
                    "xxhash64",
                    [
                        ("col", "AccountNumber"),
                        ("col", "UniqueAdID"),
                        ("lit", 99),
                    ],
                ),
                ("lit", 9_223_372_036_854_775_807),
            ),
        ),
        ("asc_nulls_first", ("col", "AccountNumber")),
        ("asc_nulls_first", ("col", "UniqueAdID")),
    ]


def test_stable_helpers_reject_incomplete_contracts(fake_functions):
    with pytest.raises(ValueError, match="At least one column"):
        stable_hash64()
    with pytest.raises(ValueError, match="namespace cannot be empty"):
        stable_hash("id", namespace="")
    with pytest.raises(ValueError, match="greater than zero"):
        stable_bucket("id", bucket_count=0)
    with pytest.raises(ValueError, match="Key columns must be unique"):
        stable_order(["id", "id"])

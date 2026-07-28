from collections.abc import Sequence

from pyspark.sql import Column
from pyspark.sql import functions as F


__all__ = [
    "stable_bucket",
    "stable_fraction",
    "stable_hash",
    "stable_hash64",
    "stable_order",
]


ColumnReference = str | Column
_STABLE_HASH_MODULUS = 9_223_372_036_854_775_807


def _as_column(value: ColumnReference) -> Column:
    if isinstance(value, str):
        return F.col(value)
    return value


def stable_hash(
    *columns: ColumnReference,
    seed: int = 0,
    namespace: str | None = None,
    version: int | str = 1,
) -> Column:
    """Return a stable, non-negative Spark hash for the supplied keys."""
    if not columns:
        raise ValueError("At least one column is required")
    if namespace == "":
        raise ValueError("namespace cannot be empty")

    values = [_as_column(column) for column in columns]
    if namespace is not None:
        values.extend([F.lit(namespace), F.lit(str(version))])
    values.append(F.lit(seed))
    return F.pmod(
        F.xxhash64(*values),
        F.lit(_STABLE_HASH_MODULUS),
    )


def stable_hash64(
    *columns: ColumnReference,
    seed: int = 0,
    namespace: str | None = None,
    version: int | str = 1,
) -> Column:
    """Return the stable hash using the legacy helper name."""
    return stable_hash(
        *columns,
        seed=seed,
        namespace=namespace,
        version=version,
    )


def stable_fraction(
    *columns: ColumnReference,
    seed: int = 0,
    namespace: str | None = None,
    version: int | str = 1,
) -> Column:
    """Map keys deterministically into the half-open interval [0, 1)."""
    return stable_hash(
        *columns,
        seed=seed,
        namespace=namespace,
        version=version,
    ).cast("double") / F.lit(float(_STABLE_HASH_MODULUS))


def stable_bucket(
    *columns: ColumnReference,
    bucket_count: int | Column,
    seed: int = 0,
    namespace: str | None = None,
    version: int | str = 1,
) -> Column:
    """Map keys to a stable zero-based bucket."""
    if isinstance(bucket_count, int):
        if bucket_count <= 0:
            raise ValueError("bucket_count must be greater than zero")
        bucket_count_column = F.lit(bucket_count)
    else:
        bucket_count_column = bucket_count

    return F.pmod(
        stable_hash(
            *columns,
            seed=seed,
            namespace=namespace,
            version=version,
        ),
        bucket_count_column,
    )


def stable_order(
    key_columns: Sequence[str],
    *,
    seed: int = 0,
    namespace: str | None = None,
    version: int | str = 1,
    hash_descending: bool = False,
) -> list[Column]:
    """Return a stable hash order with complete key-column fallbacks."""
    if not key_columns:
        raise ValueError("At least one key column is required")
    if len(set(key_columns)) != len(key_columns):
        raise ValueError("Key columns must be unique")

    hash_column = stable_hash(
        *key_columns,
        seed=seed,
        namespace=namespace,
        version=version,
    )
    hash_order = hash_column.desc() if hash_descending else hash_column.asc()
    return [
        hash_order,
        *[F.col(column).asc_nulls_first() for column in key_columns],
    ]

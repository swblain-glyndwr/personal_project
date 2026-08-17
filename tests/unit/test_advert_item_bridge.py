from datetime import date
import inspect

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from next_ads.features import advert_items as advert_items_module
from next_ads.features.advert_items import (
    CANONICAL_ADVERT_ITEM_COLUMNS,
    build_advert_item_bridge,
)


SORT_V2_SCHEMA = StructType(
    [
        StructField("UniqueAdID", StringType(), True),
        StructField("item", StringType(), True),
        StructField("item_pos", LongType(), True),
        StructField("rundate", DateType(), True),
    ]
)
SORT_LEGACY_SCHEMA = StructType(
    [
        StructField("UniqueAdID", StringType(), True),
        StructField("items", StringType(), True),
        StructField("item_pos", LongType(), True),
        StructField("rundate", DateType(), True),
    ]
)
REPRESENTATIVE_SCHEMA = StructType(
    [
        StructField("UniqueAdID", StringType(), True),
        StructField(
            "RepresentativeItems", ArrayType(StringType()), True
        ),
        StructField("rundate", DateType(), True),
    ]
)
V2_CONTROL_SCHEMA = StructType(
    [
        StructField("UniqueAdID", StringType(), True),
        StructField("PageType", StringType(), True),
        StructField("Items", StringType(), True),
        StructField("rundate", DateType(), True),
    ]
)
V1_CONTROL_SCHEMA = StructType(
    [
        StructField("UniqueAdID", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("Items", StringType(), True),
        StructField("rundate", DateType(), True),
    ]
)


@pytest.fixture(scope="module")
def local_spark():
    try:
        return (
            SparkSession.builder.master("local[2]")
            .appName("next-ads-advert-item-bridge-tests")
            .getOrCreate()
        )
    except (RuntimeError, ValueError) as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")


def _inputs(
    spark,
    *,
    v2_sort=(),
    legacy_sort=(),
    representative=(),
    v2_control=(),
    v1_control=(),
):
    return {
        "v2_sort_history": spark.createDataFrame(v2_sort, SORT_V2_SCHEMA),
        "legacy_sort_history": spark.createDataFrame(
            legacy_sort, SORT_LEGACY_SCHEMA
        ),
        "representative_items": spark.createDataFrame(
            representative, REPRESENTATIVE_SCHEMA
        ),
        "v2_control": spark.createDataFrame(v2_control, V2_CONTROL_SCHEMA),
        "v1_control": spark.createDataFrame(v1_control, V1_CONTROL_SCHEMA),
    }


def _rows_by_advert(frame):
    rows = frame.orderBy("advert_id", "item_rank").collect()
    return {
        advert_id: [row for row in rows if row.advert_id == advert_id]
        for advert_id in sorted({row.advert_id for row in rows})
    }


def test_bridge_uses_point_in_time_precedence_and_latest_snapshot(local_spark):
    day_1 = date(2026, 8, 1)
    day_5 = date(2026, 8, 5)
    day_6 = date(2026, 8, 6)
    future = date(2026, 8, 11)
    inputs = _inputs(
        local_spark,
        v2_sort=[
            ("ad_v2", "old", 1, day_1),
            ("ad_v2", "v2_second", 2, day_5),
            ("ad_v2", "v2_first", 1, day_5),
            ("ad_v2", "future", 1, future),
            ("ad_blank_latest", "old_not_reused", 1, day_5),
            ("ad_blank_latest", None, 1, day_6),
        ],
        legacy_sort=[
            ("ad_v2", "legacy_ignored", 1, day_6),
            ("ad_legacy", "legacy", 1, day_5),
            ("ad_blank_latest", "legacy_fallback", 1, day_5),
        ],
        representative=[("ad_representative", ["rep_1", "rep_2"], day_5)],
        v2_control=[
            ("ad_v2_control", "HomePage", "control_1, control_2", day_5),
            (
                "ad_v2_control",
                "ShoppingBagPage",
                "control_1 | control_2",
                day_5,
            ),
        ],
        v1_control=[("ad_v1_control", "Homepage", "v1_1 v1_2", day_5)],
    )

    result = build_advert_item_bridge(
        **inputs,
        feature_date=date(2026, 8, 10),
        cutoff_date=date(2026, 8, 8),
    )
    rows = _rows_by_advert(result)

    assert result.columns == list(CANONICAL_ADVERT_ITEM_COLUMNS)
    assert set(rows) == {
        "ad_blank_latest",
        "ad_legacy",
        "ad_representative",
        "ad_v1_control",
        "ad_v2",
        "ad_v2_control",
    }
    assert [row.item_id for row in rows["ad_v2"]] == [
        "v2_first",
        "v2_second",
    ]
    assert [row.item_source for row in rows["ad_v2"]] == [
        "v2_sort_history",
        "v2_sort_history",
    ]
    assert rows["ad_v2"][0].source_rundate == day_5
    assert rows["ad_v2"][0].feature_date == date(2026, 8, 10)
    assert rows["ad_v2"][0].item_weight == pytest.approx(2.0 / 3.0)
    assert rows["ad_v2"][1].item_weight == pytest.approx(1.0 / 3.0)
    assert rows["ad_blank_latest"][0].item_id == "legacy_fallback"
    assert rows["ad_blank_latest"][0].item_source == "legacy_sort_history"
    assert rows["ad_legacy"][0].item_source == "legacy_sort_history"
    assert rows["ad_representative"][0].item_source == "representative_items"
    assert rows["ad_v2_control"][0].item_source == "v2_control"
    assert rows["ad_v1_control"][0].item_source == "v1_control"
    assert all(row.item_id not in {"future", "old_not_reused"} for row in result.collect())


def test_bridge_deduplicates_deterministically_and_keeps_top_ten(local_spark):
    source_date = date(2026, 8, 8)
    rows = [
        ("ad", f"item_{position:02d}", position, source_date)
        for position in range(1, 13)
    ]
    rows.extend(
        [
            ("ad", "item_01", 12, source_date),
            ("ad", "item_05", 20, source_date),
        ]
    )

    outputs = []
    for partitions in (1, 4):
        inputs = _inputs(local_spark, v2_sort=rows)
        inputs["v2_sort_history"] = inputs["v2_sort_history"].repartition(
            partitions
        )
        output = build_advert_item_bridge(
            **inputs,
            feature_date=source_date,
            cutoff_date=source_date,
        ).orderBy("item_rank")
        outputs.append(output.collect())

    assert outputs[0] == outputs[1]
    assert [row.item_id for row in outputs[0]] == [
        f"item_{position:02d}" for position in range(1, 11)
    ]
    assert [row.item_rank for row in outputs[0]] == list(range(1, 11))
    assert len({row.item_id for row in outputs[0]}) == 10
    assert sum(row.item_weight for row in outputs[0]) == pytest.approx(1.0)
    harmonic_ten = sum(1.0 / rank for rank in range(1, 11))
    assert outputs[0][4].item_weight == pytest.approx((1.0 / 5) / harmonic_ten)


def test_control_item_parsing_preserves_case_and_item_punctuation(local_spark):
    source_date = date(2026, 8, 8)
    inputs = _inputs(
        local_spark,
        v2_control=[
            (
                "ad",
                "HomePage",
                "AbC-123; xy_Z.9 | Mixed/Case",
                source_date,
            )
        ],
    )

    result = build_advert_item_bridge(
        **inputs,
        feature_date=source_date,
        cutoff_date=source_date,
    ).orderBy("item_rank")

    assert [row.item_id for row in result.collect()] == [
        "AbC-123",
        "xy_Z.9",
        "Mixed/Case",
    ]


@pytest.mark.parametrize(
    ("source", "expected_message"),
    [
        ("v2", "v2_control has conflicting Items values"),
        ("v1", "v1_control has conflicting Items values"),
    ],
)
def test_bridge_rejects_conflicting_control_items(
    local_spark, source, expected_message
):
    source_date = date(2026, 8, 8)
    v2_rows = []
    v1_rows = []
    if source == "v2":
        v2_rows = [
            ("ad", "HomePage", "100,200", source_date),
            ("ad", "ShoppingBagPage", "100,300", source_date),
        ]
    else:
        v1_rows = [
            ("ad", "Homepage", "100,200", source_date),
            ("ad", "PLP", "100,300", source_date),
        ]
    inputs = _inputs(
        local_spark,
        v2_control=v2_rows,
        v1_control=v1_rows,
    )

    with pytest.raises(ValueError, match=expected_message):
        build_advert_item_bridge(
            **inputs,
            feature_date=source_date,
            cutoff_date=source_date,
        )


@pytest.mark.parametrize("invalid_position", [None, 0, -1])
def test_bridge_rejects_invalid_sort_positions(
    local_spark, invalid_position
):
    source_date = date(2026, 8, 8)
    inputs = _inputs(
        local_spark,
        v2_sort=[("ad", "item", invalid_position, source_date)],
    )

    with pytest.raises(ValueError, match="invalid item_pos"):
        build_advert_item_bridge(
            **inputs,
            feature_date=source_date,
            cutoff_date=source_date,
        )


def test_bridge_rejects_conflicting_sort_position(local_spark):
    source_date = date(2026, 8, 8)
    inputs = _inputs(
        local_spark,
        v2_sort=[
            ("ad", "item-a", 1, source_date),
            ("ad", "item-b", 1, source_date),
        ],
    )

    with pytest.raises(ValueError, match="conflicting items at item_pos=1"):
        build_advert_item_bridge(
            **inputs,
            feature_date=source_date,
            cutoff_date=source_date,
        )


def test_bridge_rejects_conflicting_representative_snapshots(local_spark):
    source_date = date(2026, 8, 8)
    inputs = _inputs(
        local_spark,
        representative=[
            ("ad", ["item-a", "item-b"], source_date),
            ("ad", ["item-a", "item-c"], source_date),
        ],
    )

    with pytest.raises(ValueError, match="conflicting arrays"):
        build_advert_item_bridge(
            **inputs,
            feature_date=source_date,
            cutoff_date=source_date,
        )


def test_bridge_rejects_null_and_populated_representative_snapshots(
    local_spark,
):
    source_date = date(2026, 8, 8)
    inputs = _inputs(
        local_spark,
        representative=[
            ("ad", None, source_date),
            ("ad", ["item-a"], source_date),
        ],
    )

    with pytest.raises(ValueError, match="conflicting arrays"):
        build_advert_item_bridge(
            **inputs,
            feature_date=source_date,
            cutoff_date=source_date,
        )


def test_bridge_ignores_invalid_positions_for_blank_advert_ids(local_spark):
    source_date = date(2026, 8, 8)
    inputs = _inputs(
        local_spark,
        v2_sort=[
            (" ", "invalid", 0, source_date),
            ("ad", "valid", 1, source_date),
        ],
    )

    rows = build_advert_item_bridge(
        **inputs,
        feature_date=source_date,
        cutoff_date=source_date,
    ).collect()

    assert [(row.advert_id, row.item_id) for row in rows] == [("ad", "valid")]


def test_bridge_fails_closed_on_a_corrupt_lower_priority_source(local_spark):
    source_date = date(2026, 8, 8)
    inputs = _inputs(
        local_spark,
        v2_sort=[("ad", "preferred", 1, source_date)],
        v1_control=[
            ("ad", "Homepage", "fallback-a", source_date),
            ("ad", "PLP", "fallback-b", source_date),
        ],
    )

    with pytest.raises(ValueError, match="v1_control has conflicting Items"):
        build_advert_item_bridge(
            **inputs,
            feature_date=source_date,
            cutoff_date=source_date,
        )


def test_bridge_rejects_a_cutoff_after_the_feature_date():
    with pytest.raises(ValueError, match="cutoff_date cannot be after feature_date"):
        build_advert_item_bridge(
            v2_sort_history=None,
            legacy_sort_history=None,
            representative_items=None,
            v2_control=None,
            v1_control=None,
            feature_date="2026-08-08",
            cutoff_date="2026-08-09",
        )


def test_bridge_validates_the_current_v2_sort_contract_before_spark_work():
    class MissingPositionFrame:
        columns = ["UniqueAdID", "item", "rundate"]

    with pytest.raises(
        ValueError,
        match="v2_sort_history is missing required columns: item_pos",
    ):
        build_advert_item_bridge(
            v2_sort_history=MissingPositionFrame(),
            legacy_sort_history=None,
            representative_items=None,
            v2_control=None,
            v1_control=None,
            feature_date="2026-08-08",
            cutoff_date="2026-08-08",
        )


def test_bridge_has_no_table_read_dependency():
    source = inspect.getsource(advert_items_module)

    assert "spark.table" not in source

from datetime import date
import json
from pathlib import Path
import sys

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[3]
except NameError:
    from dsutils.dbc import get_dbutils

    dbutils = get_dbutils()
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[3]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from next_ads.features.advert_items import (
    CANONICAL_ADVERT_ITEM_COLUMNS,
    build_advert_item_bridge,
)


SMOKE_MANIFEST_PREFIX = "ADVERT_ITEM_BRIDGE_SMOKE="


def _empty_frame(spark, schema):
    return spark.createDataFrame([], schema)


def run_smoke(spark):
    source_date = date(2026, 8, 8)
    feature_date = date(2026, 8, 9)
    future_date = date(2026, 8, 10)
    v2_schema = StructType(
        [
            StructField("UniqueAdID", StringType()),
            StructField("item", StringType()),
            StructField("item_pos", LongType()),
            StructField("rundate", DateType()),
        ]
    )
    legacy_schema = StructType(
        [
            StructField("UniqueAdID", StringType()),
            StructField("items", StringType()),
            StructField("item_pos", LongType()),
            StructField("rundate", DateType()),
        ]
    )
    representative_schema = StructType(
        [
            StructField("UniqueAdID", StringType()),
            StructField(
                "RepresentativeItems",
                ArrayType(StringType()),
            ),
            StructField("rundate", DateType()),
        ]
    )
    v2_control_schema = StructType(
        [
            StructField("UniqueAdID", StringType()),
            StructField("PageType", StringType()),
            StructField("Items", StringType()),
            StructField("rundate", DateType()),
        ]
    )
    v1_control_schema = StructType(
        [
            StructField("UniqueAdID", StringType()),
            StructField("Location", StringType()),
            StructField("Items", StringType()),
            StructField("rundate", DateType()),
        ]
    )

    result = build_advert_item_bridge(
        v2_sort_history=spark.createDataFrame(
            [
                ("advert-v2", "Item-B", 2, source_date),
                ("advert-v2", "Item-A", 1, source_date),
                ("advert-v2", "future-item", 1, future_date),
            ],
            v2_schema,
        ),
        legacy_sort_history=spark.createDataFrame(
            [
                ("advert-v2", "legacy-ignored", 1, source_date),
                ("advert-legacy", "Legacy-1", 1, source_date),
            ],
            legacy_schema,
        ),
        representative_items=spark.createDataFrame(
            [("advert-representative", ["Rep-1", "Rep-2"], source_date)],
            representative_schema,
        ),
        v2_control=spark.createDataFrame(
            [
                ("advert-v2-control", "HomePage", "Ctrl-1,Ctrl-2", source_date),
                (
                    "advert-v2-control",
                    "ShoppingBagPage",
                    "Ctrl-1 | Ctrl-2",
                    source_date,
                ),
            ],
            v2_control_schema,
        ),
        v1_control=spark.createDataFrame(
            [("advert-v1-control", "Homepage", "V1-1;V1-2", source_date)],
            v1_control_schema,
        ),
        feature_date=feature_date,
        cutoff_date=source_date,
    )
    rows = result.orderBy("advert_id", "item_rank").collect()
    if result.columns != list(CANONICAL_ADVERT_ITEM_COLUMNS):
        raise AssertionError("Advert-item bridge columns do not match contract")
    if any(row.item_id == "future-item" for row in rows):
        raise AssertionError("Advert-item bridge consumed a future source row")

    grouped = {}
    for row in rows:
        grouped.setdefault(row.advert_id, []).append(row)
    expected_sources = {
        "advert-v2": "v2_sort_history",
        "advert-legacy": "legacy_sort_history",
        "advert-representative": "representative_items",
        "advert-v2-control": "v2_control",
        "advert-v1-control": "v1_control",
    }
    if set(grouped) != set(expected_sources):
        raise AssertionError("Advert-item bridge source coverage is incomplete")
    for advert_id, expected_source in expected_sources.items():
        if {row.item_source for row in grouped[advert_id]} != {
            expected_source
        }:
            raise AssertionError(
                f"Advert-item bridge chose the wrong source for {advert_id}"
            )
        weight = sum(row.item_weight for row in grouped[advert_id])
        if abs(weight - 1.0) > 1e-12:
            raise AssertionError(
                f"Advert-item weights do not sum to one for {advert_id}"
            )
    if [row.item_id for row in grouped["advert-v2"]] != [
        "Item-A",
        "Item-B",
    ]:
        raise AssertionError("Advert-item rank order is not deterministic")

    duplicate_keys = (
        result.groupBy("advert_id", "feature_date", "item_id")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    if duplicate_keys:
        raise AssertionError("Advert-item bridge emitted duplicate keys")

    conflicting_position_rejected = False
    try:
        build_advert_item_bridge(
            v2_sort_history=spark.createDataFrame(
                [
                    ("conflict", "item-a", 1, source_date),
                    ("conflict", "item-b", 1, source_date),
                ],
                v2_schema,
            ),
            legacy_sort_history=_empty_frame(spark, legacy_schema),
            representative_items=_empty_frame(
                spark,
                representative_schema,
            ),
            v2_control=_empty_frame(spark, v2_control_schema),
            v1_control=_empty_frame(spark, v1_control_schema),
            feature_date=feature_date,
            cutoff_date=source_date,
        )
    except ValueError as exc:
        conflicting_position_rejected = "conflicting items" in str(exc)
    if not conflicting_position_rejected:
        raise AssertionError(
            "Advert-item bridge accepted a conflicting sort position"
        )
    return {
        "status": "PASS",
        "row_count": len(rows),
        "advert_count": len(grouped),
        "duplicate_key_count": duplicate_keys,
        "future_rows_consumed": 0,
        "conflicting_position_rejected": True,
        "source_precedence_checked": True,
        "weights_checked": True,
        "writes_performed": False,
    }


def main():
    logger = get_logger(__name__)
    spark = configure_spark()
    logger.info("Running read-only advert-item bridge smoke")
    manifest = run_smoke(spark)
    logger.info(
        "%s%s",
        SMOKE_MANIFEST_PREFIX,
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
    )
    logger.info("Advert-item bridge smoke passed without altering tables")
    return manifest


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    LOG_LEVEL = jobparser.get_arg("--log_level")
    configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
    main()

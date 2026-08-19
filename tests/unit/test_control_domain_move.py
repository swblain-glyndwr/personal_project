from pathlib import Path

import pytest
import yaml
from pyspark.sql import SparkSession

from next_ads.control.item_attributes import (
    build_attribute_set,
    build_attributes_master,
    build_item_attribute_catalog,
    extract_attribute_values,
)
from next_ads.control.theme_mapping import (
    build_item_themes,
    filter_valid_theme_ranks,
    normalise_theme_mapping,
    rank_item_themes,
)
from next_ads.control.theme_mapping_sync import build_theme_mapping_differences
from tests.job_resource_helpers import load_job


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def local_spark():
    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("next-ads-control-domain-move-tests")
            .getOrCreate()
        )
    except RuntimeError as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")
    yield spark


def _sorted_rows(df, *cols):
    return sorted(tuple(row[col] for col in cols) for row in df.collect())


def test_item_attribute_catalog_preserves_legacy_normalisation(local_spark):
    spark = local_spark
    df_catalog = spark.createDataFrame(
        [
            (
                "1",
                "Dresses",
                "Black",
                "Women",
                "adult",
                "Next",
                "Signature blazer",
                "Next Signature",
                "menswear",
            ),
            (
                "2",
                "Shirts",
                "Blue",
                "Men",
                "adult",
                "Other",
                "Plain shirt",
                "NPremium|Other",
                "Home",
            ),
        ],
        [
            "pid",
            "next_category",
            "next_colour",
            "next_gender",
            "gender",
            "brand",
            "title",
            "range",
            "department",
        ],
    )

    result = build_item_attribute_catalog(
        df_catalog,
        ["gender", "lifestage", "department", "brand", "category", "colour"],
    )

    assert _sorted_rows(
        result,
        "pid",
        "gender",
        "lifestage",
        "department",
        "brand",
        "category",
        "colour",
    ) == [
        ("1", "women", "adult", "fashion", "nextsignature", "Dresses", "Black"),
        ("2", "men", "adult", "home", "npremium", "Shirts", "Blue"),
    ]


def test_attribute_values_and_master_outputs(local_spark):
    spark = local_spark
    df_catalog = spark.createDataFrame(
        [("1", "Red| BLUE "), ("2", "Blue"), ("3", "")],
        ["pid", "colour"],
    )

    colour_values = extract_attribute_values(df_catalog, "colour")
    master = build_attributes_master(spark, {"colour": colour_values})
    attribute_set = build_attribute_set(master)

    assert _sorted_rows(master, "pid", "attribute", "value") == [
        ("1", "colour", "blue"),
        ("1", "colour", "red"),
        ("2", "colour", "blue"),
    ]
    assert _sorted_rows(attribute_set, "attribute", "value") == [
        ("colour", "blue"),
        ("colour", "red"),
    ]


def test_theme_mapping_filters_and_ranks_item_themes(local_spark):
    spark = local_spark
    df_themes = spark.createDataFrame(
        [
            (" Summer ", "1", "2"),
            ("winter", "bad", "1"),
        ],
        ["Theme", "ThemeTypeRank", "AdTypeRank"],
    )
    item_attributes = spark.createDataFrame(
        [
            ("sku1", "category", "dress"),
            ("sku1", "gender", "women"),
            ("sku2", "category", "shirt"),
        ],
        ["pid", "attribute", "value"],
    )
    theme_attributes = spark.createDataFrame(
        [
            ("summer", "category", "dress"),
            ("summer", "gender", "women"),
            ("winter", "category", "coat"),
        ],
        ["Theme", "attribute", "value"],
    )

    valid_themes = filter_valid_theme_ranks(normalise_theme_mapping(df_themes))
    item_themes = build_item_themes(item_attributes, theme_attributes)
    ranked = rank_item_themes(item_themes, valid_themes, "adtype-themetype")

    assert _sorted_rows(valid_themes, "Theme") == [("summer",)]
    assert _sorted_rows(item_themes, "pid", "theme") == [("sku1", "summer")]
    assert _sorted_rows(ranked, "pid", "theme", "theme_rank") == [
        ("sku1", "summer", 1)
    ]


def test_theme_mapping_sync_detects_v2_source_differences(local_spark):
    spark = local_spark
    columns = [
        "Theme",
        "TargetingAttributes",
        "ThemeType",
        "ThemeTypeRank",
        "AdType",
        "AdTypeRank",
    ]
    v1 = spark.createDataFrame(
        [
            (" Summer ", "gender:women", "Seasonal", "01", "Promo", "2"),
            ("party", "occasion:party", "Occasion", "2", "Promo", "3"),
        ],
        columns,
    )
    v2 = spark.createDataFrame(
        [
            ("summer", "GENDER:WOMEN", "seasonal", "1", "promo", "02"),
            ("denim", "category:jeans", "Category", "2", "Promo", "3"),
        ],
        columns,
    )

    differences = build_theme_mapping_differences(v1, v2)

    assert _sorted_rows(differences, "difference_type", "Theme") == [
        ("v1_only", "party"),
        ("v2_only", "denim"),
    ]


def test_main_job_uses_control_domain_entrypoints():
    candidate_job = load_job(
        "pipelines/databricks/jobs/mktg_next_uk_nextads.yml",
        "mktg_next_uk_nextads_cicd",
    )
    candidate_tasks = {
        task["task_key"]: task for task in candidate_job["tasks"]
    }

    assert candidate_tasks["load_control_sheet_v1"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_control/load_control_sheet.py"
    assert candidate_tasks["refresh_item_attributes"]["spark_python_task"][
        "python_file"
    ] == "../../../jobs/nextads_control/parse_attributes.py"
    assert candidate_tasks["build_authoritative_item_themes"][
        "spark_python_task"
    ][
        "python_file"
    ] == "../../../jobs/nextads_control/parse_theme_mapping.py"
    assert candidate_tasks["land_authoritative_theme_mapping"][
        "spark_python_task"
    ][
        "python_file"
    ] == "../../../jobs/nextads_control/land_scoring_theme_mapping.py"


def test_databricks_sync_includes_job_entrypoints():
    bundle_config = yaml.safe_load((PROJECT_ROOT / "databricks.yml").read_text())

    assert "jobs/**" in bundle_config["sync"]["include"]

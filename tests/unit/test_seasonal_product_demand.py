import ast
from datetime import date
import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

from next_ads.features import seasonal_product_demand as transforms
from next_ads.features.seasonal_product_demand import (
    SEASONAL_PRODUCT_DEMAND_OUTPUT_COLUMNS,
    SeasonalEmbeddingLineage,
    build_seasonal_product_demand_frame,
    resolve_seasonal_demand_windows,
)


REFERENCE_DATE = date(2026, 8, 17)
MODEL_NAME = "marketingdata_dev.nextads_models.product_embedding"
MODEL_VERSION = "7"
MODEL_URI = f"models:/{MODEL_NAME}/{MODEL_VERSION}"
SOURCE_RUN_ID = "95be978bd9e24783afe4e68def0c9845"
ARTIFACT_SHA256 = "a" * 64


def _vector(first_value: float = 1.0) -> list[float]:
    return [first_value] + [0.0] * 383


def _binding():
    return SimpleNamespace(
        model=SimpleNamespace(
            registered_model_name=MODEL_NAME,
            registered_model_version=int(MODEL_VERSION),
            model_uri=MODEL_URI,
        ),
        source_run_id=SOURCE_RUN_ID,
        artifact_sha256=ARTIFACT_SHA256,
    )


@pytest.fixture(scope="module")
def local_spark():
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        pytest.skip(f"PySpark unavailable: {exc}")
    try:
        return (
            SparkSession.builder.master("local[2]")
            .appName("next-ads-seasonal-product-demand-tests")
            .getOrCreate()
        )
    except (RuntimeError, ValueError) as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")


def test_module_keeps_pyspark_imports_inside_transform_functions():
    tree = ast.parse(inspect.getsource(transforms))
    top_level_imports = [
        node
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]

    assert all(
        not (
            isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith("pyspark")
        )
        and not (
            isinstance(node, ast.Import)
            and any(alias.name.startswith("pyspark") for alias in node.names)
        )
        for node in top_level_imports
    )


def test_windows_are_half_open_and_same_month_last_year_is_complete():
    windows = resolve_seasonal_demand_windows(REFERENCE_DATE)

    assert windows.recent_7d_start == date(2026, 8, 10)
    assert windows.recent_30d_start == date(2026, 7, 18)
    assert windows.prior_year_month_start == date(2025, 8, 1)
    assert windows.prior_year_month_end == date(2025, 9, 1)
    assert windows.in_recent_7d("2026-08-10")
    assert windows.in_recent_7d("2026-08-16")
    assert not windows.in_recent_7d("2026-08-09")
    assert not windows.in_recent_7d("2026-08-17")
    assert windows.in_recent_30d("2026-07-18")
    assert not windows.in_recent_30d("2026-07-17")
    assert not windows.in_recent_30d("2026-08-17")
    assert windows.in_prior_year_month("2025-08-01")
    assert windows.in_prior_year_month("2025-08-31")
    assert not windows.in_prior_year_month("2025-09-01")
    assert windows.contributes_membership("2025-08-31")
    assert not windows.contributes_membership("2026-06-01")


def test_output_contract_keeps_declared_key_and_exact_embedding_lineage():
    assert SEASONAL_PRODUCT_DEMAND_OUTPUT_COLUMNS[:4] == (
        "entity_type",
        "entity_id",
        "item_id",
        "feature_date",
    )
    assert SEASONAL_PRODUCT_DEMAND_OUTPUT_COLUMNS[11:19] == (
        "embedding_model_name",
        "embedding_model_version",
        "embedding_model_uri",
        "embedding_source_run_id",
        "embedding_artifact_sha256",
        "product_embedding_text_hash",
        "seasonal_product_embedding",
        "seasonal_product_embedding_dimension",
    )


def test_table_sql_keeps_the_declared_key_and_persists_lineage():
    project_root = Path(__file__).resolve().parents[2]
    ddl = (
        project_root
        / "sql"
        / "features"
        / "nextads"
        / "create_table_next_uk_nextads_fs_seasonal_product_demand_daily.sql"
    ).read_text(encoding="utf-8")

    assert (
        "PRIMARY KEY (\n    entity_type,\n    entity_id,\n    item_id,\n"
        "    feature_date\n  )"
    ) in ddl
    for definition in (
        "embedding_model_name STRING NOT NULL",
        "embedding_model_version STRING NOT NULL",
        "embedding_model_uri STRING NOT NULL",
        "embedding_source_run_id STRING NOT NULL",
        "embedding_artifact_sha256 STRING NOT NULL",
        "product_embedding_text_hash STRING",
        "seasonal_product_embedding ARRAY<DOUBLE>",
        "seasonal_product_embedding_dimension INT NOT NULL",
    ):
        assert definition in ddl


def test_embedding_lineage_requires_one_exact_numeric_model_artifact():
    lineage = SeasonalEmbeddingLineage(
        MODEL_NAME,
        MODEL_VERSION,
        MODEL_URI,
        SOURCE_RUN_ID,
        ARTIFACT_SHA256,
        384,
    )

    assert lineage.model_name == MODEL_NAME
    assert lineage.model_version == MODEL_VERSION
    assert lineage.model_uri == MODEL_URI

    with pytest.raises(ValueError, match="positive numeric version"):
        SeasonalEmbeddingLineage(
            MODEL_NAME,
            "latest",
            f"models:/{MODEL_NAME}/latest",
            SOURCE_RUN_ID,
            ARTIFACT_SHA256,
            384,
        )
    with pytest.raises(ValueError, match="64-character lowercase"):
        SeasonalEmbeddingLineage(
            MODEL_NAME,
            MODEL_VERSION,
            MODEL_URI,
            SOURCE_RUN_ID,
            "A" * 64,
            384,
        )
    with pytest.raises(ValueError, match="exact registered model"):
        SeasonalEmbeddingLineage(
            MODEL_NAME,
            MODEL_VERSION,
            f"models:/{MODEL_NAME}/8",
            SOURCE_RUN_ID,
            ARTIFACT_SHA256,
            384,
        )
    with pytest.raises(ValueError, match="exactly 384"):
        SeasonalEmbeddingLineage(
            MODEL_NAME,
            MODEL_VERSION,
            MODEL_URI,
            SOURCE_RUN_ID,
            ARTIFACT_SHA256,
            32,
        )


def test_transform_checks_source_contracts_before_spark_work():
    class Frame:
        def __init__(self, columns):
            self.columns = columns

    with pytest.raises(ValueError, match="productSku"):
        build_seasonal_product_demand_frame(
            account_views=Frame(["account_number", "date"]),
            account_purchases=None,
            advert_item_bridge=None,
            product_embeddings=None,
            approved_binding=_binding(),
            reference_date=REFERENCE_DATE,
        )


def test_rows_use_global_demand_for_account_and_advert_memberships(
    local_spark,
):
    views = local_spark.createDataFrame(
        [
            ("account-a", "ITEM-1", date(2026, 8, 10)),
            ("account-b", "item1", date(2026, 8, 16)),
            ("account-a", "item-1", date(2026, 8, 17)),
            ("account-a", "item-2", date(2026, 7, 18)),
            ("account-c", "item-3", date(2025, 8, 31)),
            ("outside", "item-4", date(2026, 7, 17)),
        ],
        "account_number string, productSku string, date date",
    )
    purchases = local_spark.createDataFrame(
        [
            ("account-a", "item-1", date(2026, 8, 16)),
            ("account-d", "item-1", date(2025, 8, 1)),
            ("account-e", "item-3", date(2025, 9, 1)),
        ],
        "account_number string, itemno string, order_date date",
    )
    advert_items = local_spark.createDataFrame(
        [
            (
                "advert-a",
                REFERENCE_DATE,
                "item1",
                1,
                0.5,
                "v2_sort_history",
                date(2026, 8, 16),
            ),
            (
                "advert-a",
                REFERENCE_DATE,
                "item4",
                2,
                0.5,
                "v2_sort_history",
                date(2026, 8, 16),
            ),
        ],
        "advert_id string, feature_date date, item_id string, item_rank int, "
        "item_weight double, item_source string, source_rundate date",
    )
    embeddings = local_spark.createDataFrame(
        [
            (
                "item1",
                MODEL_NAME,
                MODEL_VERSION,
                MODEL_URI,
                SOURCE_RUN_ID,
                ARTIFACT_SHA256,
                _vector(),
                384,
                "1" * 64,
            ),
            (
                "item2",
                MODEL_NAME,
                MODEL_VERSION,
                MODEL_URI,
                SOURCE_RUN_ID,
                ARTIFACT_SHA256,
                _vector(),
                384,
                "2" * 64,
            ),
        ],
        "item_id string, embedding_model_name string, "
        "embedding_model_version string, embedding_model_uri string, "
        "embedding_source_run_id string, embedding_artifact_sha256 string, "
        "embedding array<double>, embedding_dimension int, "
        "embedding_text_hash string",
    )

    rows = {
        (
            row["entity_type"],
            row["entity_id"],
            row["item_id"],
        ): row.asDict()
        for row in build_seasonal_product_demand_frame(
            account_views=views,
            account_purchases=purchases,
            advert_item_bridge=advert_items,
            product_embeddings=embeddings,
            approved_binding=_binding(),
            reference_date=REFERENCE_DATE,
        ).collect()
    }

    assert set(rows) == {
        ("ACCOUNT", "account-a", "item1"),
        ("ACCOUNT", "account-a", "item2"),
        ("ACCOUNT", "account-b", "item1"),
        ("ACCOUNT", "account-c", "item3"),
        ("ACCOUNT", "account-d", "item1"),
        ("ADVERT", "advert-a", "item1"),
        ("ADVERT", "advert-a", "item4"),
    }
    account_item = rows[("ACCOUNT", "account-a", "item1")]
    advert_item = rows[("ADVERT", "advert-a", "item1")]
    assert account_item["product_views_7d"] == 2
    assert account_item["product_views_30d"] == 2
    assert account_item["product_purchases_7d"] == 1
    assert account_item["product_purchases_30d"] == 1
    assert account_item["product_purchases_ly_same_month"] == 1
    assert account_item["product_trending_7x30"] == pytest.approx(30 / 7)
    assert {
        key: account_item[key]
        for key in (
            "product_views_7d",
            "product_views_30d",
            "product_purchases_7d",
            "product_purchases_30d",
            "product_views_ly_same_month",
            "product_purchases_ly_same_month",
        )
    } == {
        key: advert_item[key]
        for key in (
            "product_views_7d",
            "product_views_30d",
            "product_purchases_7d",
            "product_purchases_30d",
            "product_views_ly_same_month",
            "product_purchases_ly_same_month",
        )
    }
    assert account_item["embedding_model_name"] == MODEL_NAME
    assert account_item["embedding_model_version"] == MODEL_VERSION
    assert account_item["embedding_model_uri"] == MODEL_URI
    assert account_item["embedding_source_run_id"] == SOURCE_RUN_ID
    assert account_item["embedding_artifact_sha256"] == ARTIFACT_SHA256
    assert account_item["product_embedding_text_hash"] == "1" * 64
    assert account_item["seasonal_product_embedding"] == _vector()
    assert account_item["seasonal_product_embedding_dimension"] == 384
    assert account_item["seasonal_product_embedding_coverage"] == 1.0

    missing_embedding = rows[("ADVERT", "advert-a", "item4")]
    assert missing_embedding["product_views_30d"] == 0
    assert missing_embedding["product_trending_7x30"] == 0.0
    assert missing_embedding["seasonal_product_embedding"] is None
    assert missing_embedding["product_embedding_text_hash"] is None
    assert missing_embedding["seasonal_product_embedding_dimension"] == 384
    assert missing_embedding["seasonal_product_embedding_coverage"] == 0.0


def test_transform_rejects_duplicate_advert_membership(local_spark):
    views = local_spark.createDataFrame(
        [],
        "account_number string, productSku string, date date",
    )
    purchases = local_spark.createDataFrame(
        [],
        "account_number string, itemno string, order_date date",
    )
    advert_items = local_spark.createDataFrame(
        [
            (
                "advert-a",
                REFERENCE_DATE,
                "item1",
                1,
                1.0,
                "v2_sort_history",
                REFERENCE_DATE,
            ),
            (
                "advert-a",
                REFERENCE_DATE,
                "item1",
                2,
                1.0,
                "v2_sort_history",
                REFERENCE_DATE,
            ),
        ],
        "advert_id string, feature_date date, item_id string, item_rank int, "
        "item_weight double, item_source string, source_rundate date",
    )
    embeddings = local_spark.createDataFrame(
        [
            (
                "item1",
                MODEL_NAME,
                MODEL_VERSION,
                MODEL_URI,
                SOURCE_RUN_ID,
                ARTIFACT_SHA256,
                _vector(),
                384,
                "1" * 64,
            )
        ],
        "item_id string, embedding_model_name string, "
        "embedding_model_version string, embedding_model_uri string, "
        "embedding_source_run_id string, embedding_artifact_sha256 string, "
        "embedding array<double>, embedding_dimension int, "
        "embedding_text_hash string",
    )

    with pytest.raises(ValueError, match="duplicate advert-item"):
        build_seasonal_product_demand_frame(
            account_views=views,
            account_purchases=purchases,
            advert_item_bridge=advert_items,
            product_embeddings=embeddings,
            approved_binding=_binding(),
            reference_date=REFERENCE_DATE,
        )

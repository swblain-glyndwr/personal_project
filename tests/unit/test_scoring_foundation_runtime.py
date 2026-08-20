from inspect import getsource
from pathlib import Path

from jobs.table_operations.create_tables import extract_create_table_columns
from next_ads.ranking.theme_affinity.data_prep import (
    build_account_theme_spine,
    build_ranked_sql,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_account_theme_spine_builds_only_the_complete_required_universe():
    source = getsource(build_account_theme_spine)

    assert '"left_semi"' in source
    assert "F.broadcast(item_ids)" in source
    assert "crossJoin(F.broadcast(theme_catalogue))" in source
    assert "UNION" not in source.upper()
    assert "algo_" not in source
    assert "_bytheme" not in source


def test_complete_ranking_has_one_total_order_without_global_sort_or_dedup():
    sql = build_ranked_sql("catalog.schema.complete")
    normalised_sql = " ".join(sql.upper().split())

    assert sql.count("ROW_NUMBER() OVER") == 1
    assert "theme_clean\n  ) AS simple_rules_rank" in sql
    assert "GROUP BY" not in normalised_sql
    assert "SELECT DISTINCT" not in normalised_sql
    assert " WHERE " not in f" {normalised_sql} "
    assert " JOIN " not in f" {normalised_sql} "
    assert " UNION " not in f" {normalised_sql} "
    assert "ORDER BY account_number, simple_rules_rank" not in sql


def test_master_join_projects_only_features_used_by_the_foundation_contract():
    sql = (
        PROJECT_ROOT
        / "src/next_ads/ranking/theme_affinity/sql/6_master_assoc.sql"
    ).read_text()

    assert "SELECT *" not in sql.upper()
    assert "GROUP BY ALL" not in sql.upper()
    for required_feature in (
        "theme_clean2_atbs_lift",
        "theme_clean2_baskets_cs",
        "theme_clean2_views_lift",
        "user_total_views",
        "GmaName",
        "trending_7x30",
    ):
        assert required_feature in sql


def test_declarative_pipeline_reuses_the_shared_spine_and_ranking_contracts():
    source = (
        PROJECT_ROOT
        / "src/next_ads/ranking/theme_affinity/dlt_pipeline.py"
    ).read_text()

    assert "build_account_theme_spine(" in source
    assert "build_ranked_sql(" in source
    assert "GROUP BY ALL" not in source.upper()
    assert " UNION\n" not in source.upper()


def test_foundation_table_contracts_preserve_pipeline_numeric_types():
    expected_types = {
        "algo_baskets5__freq12_top10": "bigint",
        "views_behavior__recency": "int",
        "views_behavior__frequency": "bigint",
        "atbs_behavior__frequency": "bigint",
        "baskets_behavior__frequency": "bigint",
        "user_total_views": "bigint",
        "views_ly_7": "bigint",
        "views_ly_30": "bigint",
        "baskets_ly_7": "bigint",
        "baskets_ly_30": "bigint",
    }

    for output_name in ("complete", "ranked"):
        contract_path = (
            PROJECT_ROOT
            / "sql/ranking/theme_affinity"
            / f"create_table_account_theme_foundation_{output_name}.sql"
        )
        contract_types = dict(
            extract_create_table_columns(contract_path.read_text())
        )
        assert {
            column: contract_types[column]
            for column in expected_types
        } == expected_types

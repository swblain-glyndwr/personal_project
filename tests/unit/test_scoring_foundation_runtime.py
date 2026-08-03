from inspect import getsource
from pathlib import Path

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

    assert sql.count("ROW_NUMBER() OVER") == 1
    assert "theme_clean\n  ) AS simple_rules_rank" in sql
    assert "GROUP BY ALL" not in sql.upper()
    assert "SELECT DISTINCT" not in sql.upper()
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

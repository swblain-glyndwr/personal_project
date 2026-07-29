from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_vatb_top_themes_use_a_total_order():
    source = (
        PROJECT_ROOT
        / "src/next_ads/ranking/theme_affinity/sql/1a_vatb.sql"
    ).read_text()

    assert "ORDER BY freq12 DESC, theme_clean2 ASC" in source
    assert ") < 100" in source

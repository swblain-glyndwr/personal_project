from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_training_sampling_uses_stable_business_key_hashes():
    source = (
        PROJECT_ROOT
        / "src/next_ads/ranking/theme_affinity/training_data.py"
    ).read_text()

    assert "F.rand(" not in source
    assert ".sampleBy(" not in source
    assert "stable_hash(" in source
    assert "stable_fraction(" in source
    assert "_stable_hash_expr" not in source
    assert 'F.col(ACCOUNT_COL).asc()' in source
    assert "F.min(\"repurchase_stage\")" in source
    assert "F.min(\"GmaName\")" in source
    assert "*[F.col(column).asc_nulls_first()" in source

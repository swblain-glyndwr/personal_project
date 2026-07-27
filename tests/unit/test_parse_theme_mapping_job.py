import pytest

from jobs.nextads_control.parse_theme_mapping import parse_bool


@pytest.mark.parametrize("value", [True, "true", "1", "yes", "y"])
def test_parse_bool_accepts_true_values(value):
    assert parse_bool(value) is True


@pytest.mark.parametrize("value", [False, None, "", "false", "0", "no", "n"])
def test_parse_bool_accepts_false_values(value):
    assert parse_bool(value) is False


def test_parse_bool_rejects_unknown_values():
    with pytest.raises(ValueError, match="Unsupported boolean value"):
        parse_bool("sometimes")

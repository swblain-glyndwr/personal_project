from datetime import date
from unittest.mock import MagicMock, Mock

import pytest

from jobs.nextads_control import parse_theme_mapping
from jobs.nextads_control.parse_theme_mapping import (
    parse_bool,
    read_landed_theme_mapping,
)


@pytest.mark.parametrize("value", [True, "true", "1", "yes", "y"])
def test_parse_bool_accepts_true_values(value):
    assert parse_bool(value) is True


@pytest.mark.parametrize("value", [False, None, "", "false", "0", "no", "n"])
def test_parse_bool_accepts_false_values(value):
    assert parse_bool(value) is False


def test_parse_bool_rejects_unknown_values():
    with pytest.raises(ValueError, match="Unsupported boolean value"):
        parse_bool("sometimes")


def test_read_landed_theme_mapping_uses_pinned_authoritative_landing(
    monkeypatch,
):
    reader = Mock()
    source = Mock()
    landed = Mock()
    invalid_date = Mock()
    selected = Mock()
    mock_spark = Mock()
    mock_spark.read = reader
    functions = Mock()
    functions.col.side_effect = lambda name: MagicMock(name=name)
    functions.lit.return_value = MagicMock(name="run_date")
    monkeypatch.setattr(parse_theme_mapping, "F", functions)

    reader.option.return_value = reader
    reader.table.return_value = source
    source.where.return_value = landed
    landed.where.return_value = invalid_date
    invalid_date.limit.return_value = invalid_date
    invalid_date.count.return_value = 0
    landed.select.return_value = selected
    selected.limit.return_value = selected
    selected.count.return_value = 1

    result = read_landed_theme_mapping(
        mock_spark,
        table="catalog.schema.scoring_input_theme_mapping_raw",
        landing_id="theme_mapping_20260803_abc",
        mapping_version="7",
        run_date=date(2026, 8, 3),
        mapping_columns=["Theme", "attribute", "value"],
    )

    assert result is selected
    reader.option.assert_called_once_with("versionAsOf", 7)
    reader.table.assert_called_once_with(
        "catalog.schema.scoring_input_theme_mapping_raw"
    )
    source.where.assert_called_once()
    landed.select.assert_called_once_with("Theme", "attribute", "value")

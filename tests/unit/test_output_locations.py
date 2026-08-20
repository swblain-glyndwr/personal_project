import json
import logging

import pytest

from next_ads.common.output_locations import (
    OUTPUT_DESTINATION_PREFIX,
    log_output_location,
)


def test_output_location_is_compact_sorted_and_searchable(caplog):
    with caplog.at_level(logging.INFO, logger="next_ads.outputs"):
        payload = log_output_location(
            " marketingdata_dev.stephen_blain.output ",
            kind="delta_table",
            details={"row_count": 12, "delta_version": 3, "empty": None},
        )

    assert payload == {
        "destination": "marketingdata_dev.stephen_blain.output",
        "kind": "delta_table",
        "delta_version": 3,
        "row_count": 12,
    }
    message = caplog.records[-1].getMessage()
    assert message.startswith(OUTPUT_DESTINATION_PREFIX)
    assert (
        json.loads(message.removeprefix(OUTPUT_DESTINATION_PREFIX)) == payload
    )


@pytest.mark.parametrize("field", ["destination", "kind"])
def test_output_location_rejects_blank_required_fields(field):
    values = {"destination": "table", "kind": "delta_table"}
    values[field] = "  "

    with pytest.raises(ValueError, match=f"Output {field}"):
        log_output_location(values["destination"], kind=values["kind"])

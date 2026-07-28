import requests
import pytest
from pyspark.sql import SparkSession

from next_ads.reporting.autotrading import (
    GOOGLE_CHAT_MAX_MESSAGE_BYTES,
    post_autotrading_message,
    select_previous_campaign_ads,
    split_message_by_bytes,
)


class SuccessfulResponse:
    def __init__(self):
        self.raise_called = False

    def raise_for_status(self):
        self.raise_called = True


class RejectedResponse:
    def raise_for_status(self):
        raise requests.HTTPError("webhook rejected")


@pytest.fixture
def local_spark():
    try:
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("next-ads-autotrading-reporting-tests")
            .getOrCreate()
        )
    except RuntimeError as exc:
        pytest.skip(f"Local Spark unavailable: {exc}")
    yield spark


def test_previous_campaign_ads_only_returns_earlier_matching_pots(
    local_spark,
):
    ad_results = local_spark.createDataFrame(
        [
            ("P100_C10_Current_Home",),
            ("P100_C10_Current_PLP",),
            ("P090_C10_Previous_Home",),
            ("P080_C10_Previous_PLP",),
            ("P070_C11_Unrelated_Home",),
        ],
        ["UniqueAdID"],
    )
    newly_flagged = local_spark.createDataFrame(
        [("P100_C10_Current_Home",)],
        ["UniqueAdID"],
    )

    selected = {
        row["UniqueAdID"]
        for row in select_previous_campaign_ads(
            ad_results,
            newly_flagged,
        ).collect()
    }

    assert selected == {
        "P090_C10_Previous_Home",
        "P080_C10_Previous_PLP",
    }


def test_previous_campaign_ads_is_empty_when_nothing_is_newly_flagged(
    local_spark,
):
    ad_results = local_spark.createDataFrame(
        [("P090_C10_Previous_Home",)],
        ["UniqueAdID"],
    )
    newly_flagged = local_spark.createDataFrame([], ad_results.schema)

    selected = select_previous_campaign_ads(ad_results, newly_flagged)

    assert selected.isEmpty()


def test_split_message_respects_utf8_byte_limit():
    chunks = split_message_by_bytes("£" * 20, max_bytes=15)

    assert "".join(chunks) == "£" * 20
    assert all(len(chunk.encode("utf-8")) <= 15 for chunk in chunks)


def test_post_autotrading_message_chunks_and_checks_each_response():
    calls = []
    responses = []

    def fake_post(url, *, json, timeout):
        response = SuccessfulResponse()
        calls.append((url, json, timeout))
        responses.append(response)
        return response

    chunk_count = post_autotrading_message(
        "https://example.test/webhook",
        "a" * 60,
        max_bytes=25,
        timeout_seconds=12,
        post=fake_post,
    )

    assert chunk_count == 3
    assert len(calls) == 3
    assert all(call[0] == "https://example.test/webhook" for call in calls)
    assert all(call[2] == 12 for call in calls)
    assert all(response.raise_called for response in responses)
    assert all(
        len(call[1]["text"].encode("utf-8"))
        <= GOOGLE_CHAT_MAX_MESSAGE_BYTES
        for call in calls
    )
    assert calls[0][1]["text"].startswith(
        "AutoTrading notification (1/3)\n"
    )


def test_post_autotrading_message_raises_on_rejected_delivery():
    def fake_post(url, *, json, timeout):
        return RejectedResponse()

    with pytest.raises(requests.HTTPError, match="webhook rejected"):
        post_autotrading_message(
            "https://example.test/webhook",
            "message",
            post=fake_post,
        )

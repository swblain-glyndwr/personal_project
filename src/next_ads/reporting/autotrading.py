"""AutoTrading reporting helpers."""

from collections.abc import Callable
from typing import Any

import requests
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


GOOGLE_CHAT_MAX_MESSAGE_BYTES = 32_000
AUTO_TRADING_MESSAGE_BYTES = 30_000
WEBHOOK_TIMEOUT_SECONDS = 30


def select_previous_campaign_ads(
    df_ad_results: DataFrame,
    df_newly_flagged: DataFrame,
) -> DataFrame:
    """Return ads from earlier pots for newly flagged campaigns."""
    flagged_pot_campaigns = (
        df_newly_flagged.select("UniqueAdID")
        .distinct()
        .withColumn(
            "_PotNumber",
            F.split_part(F.col("UniqueAdID"), F.lit("_"), F.lit(1)),
        )
        .withColumn(
            "_CampaignNumber",
            F.split_part(F.col("UniqueAdID"), F.lit("_"), F.lit(2)),
        )
        .select("_PotNumber", "_CampaignNumber")
        .distinct()
    )
    flagged_campaigns = flagged_pot_campaigns.select(
        "_CampaignNumber"
    ).distinct()

    keyed_results = (
        df_ad_results.withColumn(
            "_PotNumber",
            F.split_part(F.col("UniqueAdID"), F.lit("_"), F.lit(1)),
        )
        .withColumn(
            "_CampaignNumber",
            F.split_part(F.col("UniqueAdID"), F.lit("_"), F.lit(2)),
        )
        .join(
            flagged_campaigns,
            on="_CampaignNumber",
            how="inner",
        )
    )

    return (
        keyed_results.join(
            flagged_pot_campaigns,
            on=["_PotNumber", "_CampaignNumber"],
            how="left_anti",
        )
        .drop("_PotNumber", "_CampaignNumber")
    )


def split_message_by_bytes(
    message: str,
    max_bytes: int = AUTO_TRADING_MESSAGE_BYTES,
) -> list[str]:
    """Split text without breaking UTF-8 characters."""
    if max_bytes <= 0:
        raise ValueError("max_bytes must be greater than zero")
    if not message:
        return [message]

    chunks: list[str] = []
    current: list[str] = []
    current_bytes = 0

    for character in message:
        character_bytes = len(character.encode("utf-8"))
        if character_bytes > max_bytes:
            raise ValueError("max_bytes is too small for a UTF-8 character")
        if current and current_bytes + character_bytes > max_bytes:
            chunks.append("".join(current))
            current = []
            current_bytes = 0
        current.append(character)
        current_bytes += character_bytes

    if current:
        chunks.append("".join(current))

    return chunks


def post_autotrading_message(
    webhook_url: str,
    message: str,
    *,
    max_bytes: int = AUTO_TRADING_MESSAGE_BYTES,
    timeout_seconds: int = WEBHOOK_TIMEOUT_SECONDS,
    post: Callable[..., Any] = requests.post,
) -> int:
    """Post size-bounded chunks and raise when delivery is rejected."""
    chunks = split_message_by_bytes(message, max_bytes=max_bytes)
    chunk_count = len(chunks)

    for index, chunk in enumerate(chunks, start=1):
        prefix = (
            f"AutoTrading notification ({index}/{chunk_count})\n"
            if chunk_count > 1
            else ""
        )
        payload = prefix + chunk
        if len(payload.encode("utf-8")) > GOOGLE_CHAT_MAX_MESSAGE_BYTES:
            raise ValueError("AutoTrading webhook payload exceeds 32,000 bytes")

        response = post(
            webhook_url,
            json={"text": payload},
            timeout=timeout_seconds,
        )
        response.raise_for_status()

    return chunk_count

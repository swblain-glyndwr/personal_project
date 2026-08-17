"""Reusable account-advert affinity and session-context feature builders."""

from __future__ import annotations

from datetime import date
from typing import Any

from next_ads.features.analytics_pctr_source import (
    DeltaSourceBinding,
    bind_analytics_pctr_source,
    parse_optional_delta_version,
    parse_reference_date,
    serialise_source_binding,
)


__all__ = [
    "DeltaSourceBinding",
    "ANALYTICS_PCTR_MODEL_INPUT_COLUMNS",
    "bind_analytics_pctr_source",
    "build_analytics_pctr_model_input_frame",
    "parse_optional_delta_version",
    "parse_reference_date",
    "serialise_source_binding",
]


ANALYTICS_PCTR_MODEL_INPUT_COLUMNS = (
    "account_number",
    "advert_id",
    "reference_date",
    "ad_clicked",
    "treatment_type",
    "location",
    "age",
    "cash_acc",
    "advert_ctr",
    "device_ctr",
    "geo_ctr",
    "gender_ctr",
    "dod_ctr_change",
    "wow_ctr_change",
    "number_pages_viewed",
    "prior_30_day_order_value",
    "customer_total_clicks",
    "customer_total_unique_adverts_clicked",
    "customer_advert_previous_click_number",
    "number_clicks_same_algodivision",
    "advert_impressions",
    "device_impressions",
    "geo_impressions",
    "gender_impressions",
    "day_impressions",
    "prior_day_impressions",
    "view_theme_score",
    "perc_order_value_cat_affinity",
    "perc_30_day_order_value_cat_affinity",
    "perc_order_qty_cat_affinity",
    "view_highest_catid_weight",
    "view_lift_adjusted",
    "purchase_highest_catid_weight",
    "purchase_lift_adjusted",
    "purchase_theme_affinity",
    "created_at",
    "updated_at",
)


ACCOUNT_ADVERT_AFFINITY_COLUMNS = (
    "account_number",
    "advert_id",
    "reference_date",
    "viewed_latest_advert_catid_affinity",
    "purchased_latest_advert_catid_affinity",
    "customer_advert_impressions_7d",
    "customer_advert_impressions_30d",
    "rules_based_pctr",
    "advert_algodivision_impressions",
    "created_at",
    "updated_at",
)

SESSION_CONTEXT_COLUMNS = (
    "account_number",
    "session_id",
    "session_date",
    "device_simple",
    "channel_simple",
    "geocountry_simple",
    "session_hour",
    "session_dayofweek",
    "session_month",
    "session_weekofyear",
    "session_is_weekend",
    "pages_in_session",
    "shopping_bag_pages_in_session",
    "created_at",
    "updated_at",
)

CUSTOMER_ADVERT_IMPRESSIONS_30D_SOURCE_COLUMNS = (
    "customer_advert_impressions_30d",
)
RULES_BASED_PCTR_SOURCE_COLUMNS = ("rules_based_pctr",)


def _column_lookup(frame: Any, source_name: str) -> dict[str, str]:
    lookup: dict[str, str] = {}
    duplicates: set[str] = set()
    for column in frame.columns:
        normalised = str(column).lower()
        if normalised in lookup and lookup[normalised] != column:
            duplicates.add(normalised)
        lookup[normalised] = column
    if duplicates:
        raise ValueError(
            f"{source_name} contains case-ambiguous columns: "
            + ", ".join(sorted(duplicates))
        )
    return lookup


def _resolve_column(
    lookup: dict[str, str],
    aliases: tuple[str, ...],
    *,
    description: str,
    required: bool = True,
) -> str | None:
    for alias in aliases:
        actual = lookup.get(alias.lower())
        if actual is not None:
            return actual
    if required:
        raise ValueError(
            f"Could not resolve {description}; expected one of "
            + ", ".join(aliases)
        )
    return None


def _optional_cast(
    lookup: dict[str, str],
    aliases: tuple[str, ...],
    spark_type: str,
):
    from pyspark.sql import functions as F

    column = _resolve_column(
        lookup,
        aliases,
        description=aliases[0],
        required=False,
    )
    if column is None:
        return F.lit(None).cast(spark_type)
    return F.col(column).cast(spark_type)


def _required_cast(
    lookup: dict[str, str],
    aliases: tuple[str, ...],
    spark_type: str,
):
    from pyspark.sql import functions as F

    column = _resolve_column(
        lookup,
        aliases,
        description=aliases[0],
    )
    return F.col(column).cast(spark_type)


def _invalid_key_condition(columns: tuple[str, ...]):
    from functools import reduce
    from operator import or_

    from pyspark.sql import functions as F

    return reduce(
        or_,
        (
            F.col(column).isNull()
            | (F.trim(F.col(column).cast("string")) == F.lit(""))
            for column in columns
        ),
    )


def _validate_non_empty(frame: Any, description: str) -> None:
    if frame.limit(1).count() == 0:
        raise ValueError(f"{description} produced no rows")


def _validate_keys(
    frame: Any,
    keys: tuple[str, ...],
    *,
    description: str,
) -> None:
    from pyspark.sql import functions as F

    if frame.where(_invalid_key_condition(keys)).limit(1).count() > 0:
        raise ValueError(f"{description} contains null or blank key values")
    duplicate = (
        frame.groupBy(*keys)
        .agg(F.count(F.lit(1)).alias("row_count"))
        .where(F.col("row_count") > 1)
        .limit(1)
        .collect()
    )
    if duplicate:
        key_values = ", ".join(f"{key}={duplicate[0][key]}" for key in keys)
        raise ValueError(
            f"{description} contains duplicate keys; example {key_values}"
        )


def build_account_advert_affinity_frame(
    analytics_pctr_output: Any,
    reference_date: str | date,
):
    """Map one exact Analytics pCTR output to the reusable affinity contract.

    The scheduled Analytics feature output does not currently publish exact
    7-day or 30-day account-advert exposures, or a rules-based pCTR score.
    Those optional contract fields remain null unless the approved source adds
    explicitly named columns; legacy exposure windows and generic advert
    scores are never silently relabelled.
    """
    from pyspark.sql import functions as F

    resolved_date = parse_reference_date(reference_date)
    lookup = _column_lookup(analytics_pctr_output, "Analytics pCTR output")
    account_column = _resolve_column(
        lookup,
        ("account_number",),
        description="Analytics pCTR account number",
    )
    advert_column = _resolve_column(
        lookup,
        ("uniqueadid", "unique_ad_id", "advert_id"),
        description="Analytics pCTR advert ID",
    )
    source_date_column = _resolve_column(
        lookup,
        ("rundate", "reference_date"),
        description="Analytics pCTR reference date",
    )

    output = analytics_pctr_output.where(
        F.to_date(F.col(source_date_column)) == F.lit(resolved_date)
    ).select(
        F.trim(F.col(account_column).cast("string")).alias("account_number"),
        F.trim(F.col(advert_column).cast("string")).alias("advert_id"),
        F.lit(resolved_date).cast("date").alias("reference_date"),
        _required_cast(
            lookup,
            (
                "viewed_latest_advert_catid_affinity",
                "view_lift_adjusted",
            ),
            "double",
        ).alias("viewed_latest_advert_catid_affinity"),
        _required_cast(
            lookup,
            (
                "purchased_latest_advert_catid_affinity",
                "purchase_lift_adjusted",
            ),
            "double",
        ).alias("purchased_latest_advert_catid_affinity"),
        _optional_cast(
            lookup,
            (
                "customer_advert_impressions_7d",
                "customer_advert_previous_impression_number_7d",
            ),
            "bigint",
        ).alias("customer_advert_impressions_7d"),
        _optional_cast(
            lookup,
            CUSTOMER_ADVERT_IMPRESSIONS_30D_SOURCE_COLUMNS,
            "bigint",
        ).alias("customer_advert_impressions_30d"),
        _optional_cast(
            lookup,
            RULES_BASED_PCTR_SOURCE_COLUMNS,
            "double",
        ).alias("rules_based_pctr"),
        _required_cast(
            lookup,
            (
                "advert_algodivision_impressions",
                "number_impressions_same_algodivision",
            ),
            "bigint",
        ).alias("advert_algodivision_impressions"),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )

    _validate_non_empty(
        output,
        f"Analytics pCTR affinity mapping for {resolved_date.isoformat()}",
    )
    _validate_keys(
        output,
        ("account_number", "advert_id", "reference_date"),
        description="Account-advert affinity output",
    )
    return output.select(*ACCOUNT_ADVERT_AFFINITY_COLUMNS)


def build_analytics_pctr_model_input_frame(
    analytics_pctr_output: Any,
    reference_date: str | date,
):
    """Publish the exact inputs used by the existing Analytics pCTR models."""
    from pyspark.sql import functions as F

    resolved_date = parse_reference_date(reference_date)
    lookup = _column_lookup(analytics_pctr_output, "Analytics pCTR output")
    account_column = _resolve_column(
        lookup,
        ("account_number",),
        description="Analytics pCTR account number",
    )
    advert_column = _resolve_column(
        lookup,
        ("uniqueadid", "unique_ad_id", "advert_id"),
        description="Analytics pCTR advert ID",
    )
    source_date_column = _resolve_column(
        lookup,
        ("rundate", "reference_date"),
        description="Analytics pCTR reference date",
    )

    typed_source_columns = {
        "ad_clicked": "int",
        "treatment_type": "string",
        "location": "int",
        "age": "int",
        "cash_acc": "int",
        "advert_ctr": "double",
        "device_ctr": "double",
        "geo_ctr": "double",
        "gender_ctr": "double",
        "dod_ctr_change": "double",
        "wow_ctr_change": "double",
        "number_pages_viewed": "int",
        "prior_30_day_order_value": "double",
        "customer_total_clicks": "int",
        "customer_total_unique_adverts_clicked": "int",
        "customer_advert_previous_click_number": "int",
        "number_clicks_same_algodivision": "int",
        "advert_impressions": "int",
        "device_impressions": "int",
        "geo_impressions": "int",
        "gender_impressions": "int",
        "day_impressions": "int",
        "prior_day_impressions": "int",
        "view_theme_score": "double",
        "perc_order_value_cat_affinity": "double",
        "perc_30_day_order_value_cat_affinity": "double",
        "perc_order_qty_cat_affinity": "double",
        "view_highest_catid_weight": "double",
        "view_lift_adjusted": "double",
        "purchase_highest_catid_weight": "double",
        "purchase_lift_adjusted": "double",
        "purchase_theme_affinity": "double",
    }
    output = analytics_pctr_output.where(
        F.to_date(F.col(source_date_column)) == F.lit(resolved_date)
    ).select(
        F.trim(F.col(account_column).cast("string")).alias("account_number"),
        F.trim(F.col(advert_column).cast("string")).alias("advert_id"),
        F.lit(resolved_date).cast("date").alias("reference_date"),
        *(
            _required_cast(lookup, (column_name,), spark_type).alias(
                column_name
            )
            for column_name, spark_type in typed_source_columns.items()
        ),
        F.current_timestamp().alias("created_at"),
        F.current_timestamp().alias("updated_at"),
    )

    _validate_non_empty(
        output,
        f"Analytics pCTR model input for {resolved_date.isoformat()}",
    )
    _validate_keys(
        output,
        ("account_number", "advert_id", "reference_date"),
        description="Analytics pCTR model input",
    )
    return output.select(*ANALYTICS_PCTR_MODEL_INPUT_COLUMNS)


def _validate_country_mapping(country_mapping: Any) -> Any:
    from pyspark.sql import functions as F

    lookup = _column_lookup(country_mapping, "country mapping")
    country_column = _resolve_column(
        lookup,
        ("country_name",),
        description="country mapping country_name",
    )
    segment_column = _resolve_column(
        lookup,
        ("segment_name",),
        description="country mapping segment_name",
    )
    normalised = (
        country_mapping.select(
            F.lower(F.trim(F.col(country_column).cast("string"))).alias(
                "_country_key"
            ),
            F.trim(F.col(segment_column).cast("string")).alias(
                "_country_segment"
            ),
        )
        .where(F.col("_country_key").isNotNull())
        .where(F.col("_country_key") != F.lit(""))
        .where(F.col("_country_segment").isNotNull())
        .where(F.col("_country_segment") != F.lit(""))
    )
    ambiguous = (
        normalised.groupBy("_country_key")
        .agg(F.countDistinct("_country_segment").alias("segment_count"))
        .where(F.col("segment_count") > 1)
        .limit(1)
        .collect()
    )
    if ambiguous:
        raise ValueError(
            "Country mapping assigns more than one segment to "
            f"{ambiguous[0]['_country_key']}"
        )
    return normalised.dropDuplicates(["_country_key"])


def _normalised_page_path(column: Any):
    from pyspark.sql import functions as F

    normalised = F.lower(F.trim(column.cast("string")))
    return F.when(normalised == F.lit("/"), normalised).otherwise(
        F.regexp_replace(normalised, "/+$", "")
    )


def build_session_context_frame(
    web_sessions: Any,
    app_sessions: Any,
    account_mappings: Any,
    customer_accounts: Any,
    page_events: Any,
    country_mapping: Any,
    session_date: str | date,
):
    """Build one web/app session row and reject ambiguous account assignment."""
    from pyspark.sql import functions as F

    resolved_date = parse_reference_date(session_date)
    def select_session_rows(sessions: Any, source_name: str, default_device: str):
        session_lookup = _column_lookup(sessions, source_name)
        session_id_column = _resolve_column(
            session_lookup,
            ("uniquevisitid",),
            description=f"{source_name} UniqueVisitID",
        )
        session_date_column = _resolve_column(
            session_lookup,
            ("date", "session_date"),
            description=f"{source_name} session date",
        )
        session_rpid_column = _resolve_column(
            session_lookup,
            ("rpid", "roamingprofileid"),
            description=f"{source_name} RPID",
        )
        device_column = _resolve_column(
            session_lookup,
            ("device",),
            description=f"{source_name} device",
            required=False,
        )
        channel_column = _resolve_column(
            session_lookup,
            ("channel",),
            description=f"{source_name} channel",
        )
        country_column = _resolve_column(
            session_lookup,
            ("geocountry", "geo_country"),
            description=f"{source_name} country",
        )
        hour_column = _resolve_column(
            session_lookup,
            ("visitstarthour", "session_hour"),
            description=f"{source_name} start hour",
            required=False,
        )
        device_value = (
            F.trim(F.col(device_column).cast("string"))
            if device_column is not None
            else F.lit(None).cast("string")
        )
        return sessions.where(
            F.to_date(F.col(session_date_column)) == F.lit(resolved_date)
        ).select(
            F.col(session_id_column).cast("string").alias("session_id"),
            F.lit(resolved_date).cast("date").alias("session_date"),
            F.trim(F.col(session_rpid_column).cast("string")).alias("_rpid"),
            F.when(
                device_value.isNull() | (device_value == F.lit("")),
                F.lit(default_device),
            )
            .otherwise(device_value)
            .alias("_device"),
            F.trim(F.col(channel_column).cast("string")).alias("_channel"),
            F.trim(F.col(country_column).cast("string")).alias("_country"),
            (
                F.col(hour_column).cast("int")
                if hour_column is not None
                else F.lit(None).cast("int")
            ).alias("session_hour"),
        )

    account_lookup = _column_lookup(account_mappings, "RPID account mapping")
    account_rpid_column = _resolve_column(
        account_lookup,
        ("roamingprofileid", "rpid"),
        description="account mapping RPID",
    )
    account_number_column = _resolve_column(
        account_lookup,
        ("account_number", "accountnumber"),
        description="account mapping account number",
    )

    raw_sessions = select_session_rows(
        web_sessions,
        "BQ web sessions",
        "Other",
    ).unionByName(
        select_session_rows(
            app_sessions,
            "BQ app sessions",
            "App",
        )
    )
    mapped_accounts = account_mappings.select(
        F.trim(F.col(account_rpid_column).cast("string")).alias("_rpid"),
        F.trim(F.col(account_number_column).cast("string")).alias(
            "account_number"
        ),
    ).dropDuplicates(["_rpid", "account_number"])
    customer_lookup = _column_lookup(customer_accounts, "NEXT customer")
    customer_account_column = _resolve_column(
        customer_lookup,
        ("account_number", "accountnumber"),
        description="NEXT customer account number",
    )
    customer_country_column = _resolve_column(
        customer_lookup,
        ("countrycode", "country_code"),
        description="NEXT customer country code",
    )
    customer_client_column = _resolve_column(
        customer_lookup,
        ("client", "client_name"),
        description="NEXT customer client",
    )
    eligible_accounts = (
        customer_accounts.where(
            (F.upper(F.trim(F.col(customer_country_column))) == F.lit("GB"))
            & (F.upper(F.trim(F.col(customer_client_column))) == F.lit("NEXT"))
        )
        .select(
            F.trim(F.col(customer_account_column).cast("string")).alias(
                "account_number"
            )
        )
        .where(F.col("account_number").isNotNull())
        .dropDuplicates(["account_number"])
    )
    accounts = mapped_accounts.join(
        eligible_accounts,
        on="account_number",
        how="inner",
    )

    mapped = raw_sessions.join(accounts, on="_rpid", how="inner")
    _validate_non_empty(
        mapped,
        f"Mapped BQ session context for {resolved_date.isoformat()}",
    )
    if (
        mapped.where(
            (F.col("session_hour") < 0) | (F.col("session_hour") > 23)
        )
        .limit(1)
        .count()
        > 0
    ):
        raise ValueError("BQ sessions contain a start hour outside 0 to 23")

    ambiguous_accounts = (
        mapped.groupBy("session_id")
        .agg(F.countDistinct("account_number").alias("account_count"))
        .where(F.col("account_count") > 1)
        .orderBy("session_id")
        .limit(1)
        .collect()
    )
    if ambiguous_accounts:
        raise ValueError(
            "UniqueVisitID maps to more than one account: "
            f"{ambiguous_accounts[0]['session_id']}"
        )

    countries = _validate_country_mapping(country_mapping)
    uk_and_ireland = (
        "united kingdom",
        "ireland",
        "jersey",
        "isle of man",
        "guernsey",
    )
    allowed_channels = (
        "Paid Search",
        "Organic Search",
        "Paid Social",
        "Organic Social",
        "Direct",
        "Email",
        "Referral",
        "SMS",
    )
    enriched = (
        mapped.withColumn("_country_key", F.lower(F.trim(F.col("_country"))))
        .join(countries, on="_country_key", how="left")
        .withColumn(
            "device_simple",
            F.when(F.col("_device") == "Mobile", F.lit("Mobile"))
            .when(F.col("_device") == "Desktop", F.lit("Desktop"))
            .when(F.col("_device") == "App", F.lit("App"))
            .otherwise(F.lit("Other")),
        )
        .withColumn(
            "channel_simple",
            F.when(
                F.col("_channel").isin(*allowed_channels), F.col("_channel")
            )
            .when(F.col("_channel").rlike("^.*(Paid).*$"), F.lit("Paid Other"))
            .when(
                F.col("_channel").rlike("^.*(Organic).*$"),
                F.lit("Organic Other"),
            )
            .otherwise(F.lit("Other")),
        )
        .withColumn(
            "geocountry_simple",
            F.when(
                F.col("_country_key").isin(*uk_and_ireland),
                F.lit("UK & Ireland"),
            )
            .when(
                F.col("_country_segment").isNotNull(),
                F.col("_country_segment"),
            )
            .otherwise(F.lit("Other")),
        )
    )

    context_conflicts = (
        enriched.groupBy("account_number", "session_id", "session_date")
        .agg(
            F.countDistinct(
                F.struct(
                    "device_simple",
                    "channel_simple",
                    "geocountry_simple",
                    "session_hour",
                )
            ).alias("context_count")
        )
        .where(F.col("context_count") > 1)
        .orderBy("session_id")
        .limit(1)
        .collect()
    )
    if context_conflicts:
        raise ValueError(
            "BQ sessions contain conflicting context for UniqueVisitID: "
            f"{context_conflicts[0]['session_id']}"
        )

    canonical_sessions = enriched.select(
        "account_number",
        "session_id",
        "session_date",
        "device_simple",
        "channel_simple",
        "geocountry_simple",
        "session_hour",
    ).dropDuplicates(["account_number", "session_id", "session_date"])

    page_lookup = _column_lookup(page_events, "BQ page events")
    page_session_column = _resolve_column(
        page_lookup,
        ("uniquevisitid",),
        description="BQ page UniqueVisitID",
    )
    page_date_column = _resolve_column(
        page_lookup,
        ("date", "session_date"),
        description="BQ page date",
    )
    page_path_column = _resolve_column(
        page_lookup,
        ("pagepath", "page_path"),
        description="BQ page path",
    )
    page_counts = (
        page_events.where(
            F.to_date(F.col(page_date_column)) == F.lit(resolved_date)
        )
        .select(
            F.trim(F.col(page_session_column).cast("string")).alias(
                "session_id"
            ),
            F.lit(resolved_date).cast("date").alias("session_date"),
            _normalised_page_path(F.col(page_path_column)).alias("_page_path"),
        )
        .groupBy("session_id", "session_date")
        .agg(
            F.count(F.lit(1)).cast("bigint").alias("pages_in_session"),
            F.sum(
                F.when(F.col("_page_path") == "/shoppingbag", 1).otherwise(0)
            )
            .cast("bigint")
            .alias("shopping_bag_pages_in_session"),
        )
    )

    output = (
        canonical_sessions.join(
            page_counts,
            on=["session_id", "session_date"],
            how="left",
        )
        .withColumn("session_dayofweek", F.dayofweek("session_date"))
        .withColumn("session_month", F.month("session_date"))
        .withColumn("session_weekofyear", F.weekofyear("session_date"))
        .withColumn(
            "session_is_weekend",
            F.when(F.col("session_dayofweek").isin(1, 7), 1).otherwise(0),
        )
        .withColumn(
            "pages_in_session",
            F.coalesce(F.col("pages_in_session"), F.lit(0)).cast("bigint"),
        )
        .withColumn(
            "shopping_bag_pages_in_session",
            F.coalesce(F.col("shopping_bag_pages_in_session"), F.lit(0)).cast(
                "bigint"
            ),
        )
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
        .select(*SESSION_CONTEXT_COLUMNS)
    )
    _validate_keys(
        output,
        ("account_number", "session_id", "session_date"),
        description="Session context output",
    )
    return output

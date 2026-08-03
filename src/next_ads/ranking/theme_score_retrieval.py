from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructField, StructType

from next_ads.ranking.scoring_inputs import read_delta_version


def load_control_ads(spark, control_sheet_latest: str):
    return spark.table(control_sheet_latest)


def build_theme_to_ad_mapping(df_ads):
    return (
        df_ads.where(F.col("AudienceOnly") != 1)
        .select("Themes", "UniqueAdID", "AdVariant")
        .where(F.col("Themes").isNotNull())
        .where(F.col("Themes") != "")
        .distinct()
    )


def load_customer_base(spark, customer_cells_latest: str):
    return customer_base_from_cells(spark.table(customer_cells_latest))


def customer_base_from_cells(customer_cells):
    return customer_cells.select("AccountNumber")


def load_theme_scores(spark, theme_scores_latest: str, customer_base_df):
    return spark.table(theme_scores_latest).join(
        customer_base_df,
        on="AccountNumber",
        how="inner",
    )


def load_provider_theme_scores(
    spark,
    *,
    provider_signals_table: str,
    provider_signals_delta_version: int,
    provider_build_id: str,
    provider_source_run_date,
    customer_base_df,
):
    """Read one immutable provider build through the canonical score contract."""
    source = read_delta_version(
        spark,
        provider_signals_table,
        provider_signals_delta_version,
    )
    required_columns = {
        "ProviderBuildID",
        "AccountNumber",
        "EntityType",
        "EntityID",
        "RunDate",
        "RawScore",
        "Score",
    }
    missing = sorted(required_columns.difference(source.columns))
    if missing:
        raise ValueError(
            "Provider signal table is missing columns: "
            + ", ".join(missing)
        )

    selected = (
        source.where(F.col("ProviderBuildID") == provider_build_id)
        .where(F.col("EntityType") == "theme")
        .where(F.col("RunDate") == F.lit(provider_source_run_date))
        .select(
            "AccountNumber",
            F.col("EntityID").alias("NextTheme"),
            F.col("RawScore").alias("ProbBase"),
            F.col("RawScore").alias("ProbAgg"),
            F.col("Score").alias("ProbAggRebased"),
        )
    )
    if selected.limit(1).count() == 0:
        raise ValueError(
            "Selected provider build contains no theme signals at its "
            "recorded Delta version"
        )

    customer_scores = selected.join(
        customer_base_df,
        on="AccountNumber",
        how="inner",
    )
    if customer_scores.limit(1).count() == 0:
        raise ValueError(
            "Selected provider build contains no signals for the accepted "
            "customer base"
        )
    return customer_scores


def build_ad_group_mappings(
    spark,
    control_sheet_latest: str,
    logger,
    group_col: str = "Location",
):
    source_ad2group = (
        spark.table(control_sheet_latest)
        .where(F.col("AudienceOnly") != 1)
        .select("UniqueAdID", group_col)
        .distinct()
    )

    # The control mapping is small, but it is reused by several later actions.
    # Materialise it once on the driver so worker loss cannot replay unordered
    # collect_list/collect_set aggregates with different shuffle output.
    source_rows = source_ad2group.collect()
    ad_group_rows = sorted(
        ((row["UniqueAdID"], row[group_col]) for row in source_rows),
        key=lambda row: (
            row[1] is None,
            str(row[1]),
            row[0] is None,
            str(row[0]),
        ),
    )

    group_to_ads: dict[object, set[object]] = {}
    for ad_id, group in ad_group_rows:
        if ad_id is None or group is None:
            continue
        group_to_ads.setdefault(group, set()).add(ad_id)

    adset_to_groups: dict[tuple[object, ...], set[object]] = {}
    for group, ad_ids in group_to_ads.items():
        adset = tuple(sorted(ad_ids, key=str))
        adset_to_groups.setdefault(adset, set()).add(group)

    ordered_adsets = sorted(
        adset_to_groups,
        key=lambda adset: "|".join(str(ad_id) for ad_id in adset),
    )
    adset_ids = {
        adset: adset_id
        for adset_id, adset in enumerate(ordered_adsets, start=1)
    }

    adset_group_rows = [
        (adset_ids[adset], group)
        for adset in ordered_adsets
        for group in sorted(adset_to_groups[adset], key=str)
    ]
    ad_adset_rows = sorted(
        {
            (ad_id, adset_ids[adset])
            for adset in ordered_adsets
            for ad_id in adset
        },
        key=lambda row: (str(row[0]), row[1]),
    )

    ad_group_schema = source_ad2group.schema
    adset_group_schema = StructType(
        [
            StructField("AdSetID", IntegerType(), nullable=False),
            StructField(
                group_col,
                ad_group_schema[group_col].dataType,
                nullable=True,
            ),
        ]
    )
    ad_adset_schema = StructType(
        [
            StructField(
                "UniqueAdID",
                ad_group_schema["UniqueAdID"].dataType,
                nullable=True,
            ),
            StructField("AdSetID", IntegerType(), nullable=False),
        ]
    )

    df_ad2group = spark.createDataFrame(ad_group_rows, schema=ad_group_schema)
    df_adset2group = spark.createDataFrame(
        adset_group_rows,
        schema=adset_group_schema,
    )
    df_ad2adset = spark.createDataFrame(
        ad_adset_rows,
        schema=ad_adset_schema,
    )

    n_groups = len(group_to_ads)
    n_ad_sets = len(ordered_adsets)
    logger.info(
        f"{n_ad_sets:,} distinct ad sets found across "
        f"{n_groups:,} {group_col} values"
    )

    for adset in ordered_adsets:
        groups = ", ".join(
            str(group) for group in sorted(adset_to_groups[adset], key=str)
        )
        logger.info(f"AdSetID {adset_ids[adset]}: {group_col} [{groups}]")

    return df_ad2group, df_adset2group, df_ad2adset


def build_ad_location_mappings(spark, control_sheet_latest: str, logger):
    return build_ad_group_mappings(
        spark,
        control_sheet_latest,
        logger,
        group_col="Location",
    )

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark import StorageLevel

from dsutils.etl import chain_when_thens

from next_ads.decisioning.assignment import (
    assign_candidate_ads,
    assign_candidate_ads_v2,
    assign_nextgenads,
    assign_nextgenads_v2,
    assign_random_ads,
    assign_random_ads_v2,
    assign_random_ads_with_exclusions,
    with_random_assignment_ordinals,
)


V1_OUTPUT_COLUMNS = (
    "AccountNumber",
    "Location",
    "UniqueAdIDBasic",
    "UniqueAdIDBest",
    "UniqueAdIDBestChallenger",
    "UniqueAdIDNextGenAds",
    "Treatment",
    "UniqueAdIDMeasurement",
    "UniqueAdIDAssigned",
    "MASID",
)
V2_OUTPUT_COLUMNS = (
    "AccountNumber",
    "PageType",
    "Rank",
    "UniqueAdIDBasic",
    "UniqueAdIDBest",
    "UniqueAdIDBestChallenger",
    "UniqueAdIDNextGenAds",
    "Treatment",
    "UniqueAdIDMeasurement",
    "UniqueAdIDAssigned",
    "TriggerScore",
)


def _union(frames: Sequence[DataFrame]) -> DataFrame:
    if not frames:
        raise ValueError("Bulk assignment requires at least one scope")
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def _uses_nextgen(cell_map: Mapping[str, Any]) -> bool:
    return any(
        step.get("then", {}).get("col") == "UniqueAdIDNextGenAds"
        for step in cell_map.get("map", [])
    )


def _empty_v1_assignment(cells: DataFrame) -> DataFrame:
    return cells.limit(0).select(
        "AccountNumber",
        F.lit(None).cast("string").alias("UniqueAdID"),
    )


def _empty_v2_assignment(cells: DataFrame) -> DataFrame:
    return cells.limit(0).select(
        "AccountNumber",
        F.lit(None).cast("string").alias("UniqueAdID"),
        F.lit(None).cast("int").alias("Rank"),
        F.lit(None).cast("double").alias("TriggerScore"),
    )


def _assignable_marker(
    targeted_ads: DataFrame, nextgen_ads: DataFrame
) -> DataFrame:
    return (
        targeted_ads.select(F.lit(1).alias("_assignable"))
        .limit(1)
        .unionByName(
            nextgen_ads.select(F.lit(1).alias("_assignable")).limit(1)
        )
        .limit(1)
    )


def _incremental_ad_metrics(
    results: DataFrame,
    *,
    cfg: Mapping[str, Any],
    run_date: date,
) -> DataFrame:
    incrementality = cfg["incrementality"]
    start_date = run_date - timedelta(
        days=incrementality["incremental_lookback"] + 1
    )
    suffix = incrementality["incremental_ads_suffix"]
    return (
        results.where(
            (F.col("SessionDate") >= F.lit(start_date))
            & F.col("UniqueAdID").rlike(suffix + "$"),
        )
        .groupBy("UniqueAdID")
        .agg(
            F.sum("ApportionedRevenue").alias("ApportionedRevenue"),
            F.sum("Sessions").alias("Sessions"),
            F.sum("C_ApportionedRevenue").alias("C_ApportionedRevenue"),
            F.sum("C_Sessions").alias("C_Sessions"),
            F.when(
                F.sum("Sessions") > 0,
                F.sum(F.col("SessionOverlapRatio") * F.col("Sessions"))
                / F.sum("Sessions"),
            ).alias("SessionOverlapRatio"),
        )
        .withColumn(
            "ARPS",
            F.when(
                F.col("Sessions") > 0,
                F.col("ApportionedRevenue") / F.col("Sessions"),
            ),
        )
        .withColumn(
            "C_ARPS",
            F.when(
                F.col("C_Sessions") > 0,
                F.col("C_ApportionedRevenue") / F.col("C_Sessions"),
            ),
        )
        .withColumn("IncARPS", F.col("ARPS") - F.col("C_ARPS"))
        .withColumn(
            "IncARPSAdj",
            F.when(
                F.col("SessionOverlapRatio") > 0,
                F.col("IncARPS") / F.col("SessionOverlapRatio"),
            ),
        )
        .withColumn("EstContribution", F.col("IncARPSAdj") * F.col("Sessions"))
        .select(
            F.col("UniqueAdID").alias("UniqueAdIDAssigned"),
            "C_Sessions",
            "EstContribution",
        )
    )


def _build_v1_scope(
    spark: Any,
    *,
    location: str,
    inherit_basic_from: str | None,
    cfg: Mapping[str, Any],
    control: DataFrame,
    cells: DataFrame,
    candidate_inputs: Any,
    nextgen_assignments_table: str,
    parent_assignments: DataFrame | None,
    incremental_metrics: DataFrame | None,
) -> DataFrame:
    cell_map = cfg["locations"][location]
    ads = control.where(F.col("Location") == location).select(
        "UniqueAdID",
        "UniqueAdIDPremium",
        "AlgoDivision",
        "MASIDToken",
        "TargetingCriteria",
        "AudienceOnly",
        "Tags",
        "Themes",
        "ClusterID",
    )
    targeted_ads = ads.fillna(0, subset=["AudienceOnly"]).where(
        F.col("AudienceOnly") != 1
    )
    best_ads = targeted_ads.where(
        F.col("Themes").isNotNull() & (F.col("Themes") != "")
    )
    nextgen_ads = ads.where(
        F.col("ClusterID").isNotNull() & F.col("Themes").isNull()
    )
    assignable = _assignable_marker(targeted_ads, nextgen_ads)
    ad_columns = [
        "UniqueAdID",
        "UniqueAdIDPremium",
        "AlgoDivision",
        "MASIDToken",
        "TargetingCriteria",
    ]
    ads_for_output = ads.select(*ad_columns)
    targeted_ads = targeted_ads.select(*ad_columns)
    best_ads = best_ads.select(*ad_columns)

    basic_within = cell_map.get("basic_within", "global")
    best_kwargs = cell_map.get("best_kwargs", {"return_ranks": [1]})
    candidate_scope = best_kwargs.get("inherit_rank_from_location", location)
    if inherit_basic_from:
        if parent_assignments is None:
            raise ValueError(
                f"V1 scope {location} is missing parent {inherit_basic_from}"
            )
        excluded = parent_assignments.where(
            F.col("UniqueAdIDBasic").isNotNull()
        ).select(
            "AccountNumber",
            F.col("UniqueAdIDBasic").alias("ExcludedAdID"),
        )
        cells_for_basic = cells.join(excluded, "AccountNumber", "left")
        if basic_within == "global":
            basic = assign_random_ads_with_exclusions(
                targeted_ads.select("UniqueAdID"),
                cells_for_basic.select("AccountNumber", "ExcludedAdID"),
            )
        else:
            basic = assign_random_ads_with_exclusions(
                targeted_ads.select("UniqueAdID", basic_within),
                cells_for_basic.select(
                    "AccountNumber", basic_within, "ExcludedAdID"
                ),
                grp_col=basic_within,
            )
    elif basic_within == "global":
        basic = assign_random_ads(
            targeted_ads.select("UniqueAdID"),
            cells.select(
                "AccountNumber",
                "global",
                "_basic_global_row",
                "_basic_global_count",
            ),
            customer_row_column="_basic_global_row",
            customer_count_column="_basic_global_count",
        )
    else:
        basic = assign_random_ads(
            targeted_ads.select("UniqueAdID", basic_within),
            cells.select(
                "AccountNumber",
                basic_within,
                "_basic_group_row",
                "_basic_group_count",
            ),
            grp_col=basic_within,
            customer_row_column="_basic_group_row",
            customer_count_column="_basic_group_count",
        )

    best = assign_candidate_ads(
        df_ads=best_ads,
        candidate_scores=candidate_inputs.candidates_for_scope(
            "best", candidate_scope
        ),
        df_cust=cells.select("AccountNumber"),
        return_ranks=best_kwargs["return_ranks"],
    )
    challenger = assign_candidate_ads(
        df_ads=best_ads,
        candidate_scores=candidate_inputs.candidates_for_scope(
            "best_challenger", candidate_scope
        ),
        df_cust=cells.select("AccountNumber"),
        return_ranks=best_kwargs["return_ranks"],
    )
    nextgen = (
        assign_nextgenads(
            df_ads=nextgen_ads,
            customer_to_cluster_table=nextgen_assignments_table,
            df_cust=cells.select("AccountNumber"),
            return_ranks=best_kwargs["return_ranks"],
        )
        if _uses_nextgen(cell_map)
        else _empty_v1_assignment(cells)
    )
    joined = (
        cells.withColumn("AdSuppressed", F.lit("AdSuppressed"))
        .join(
            basic.select("AccountNumber", "UniqueAdID").withColumnRenamed(
                "UniqueAdID", "UniqueAdIDBasic"
            ),
            "AccountNumber",
            "left",
        )
        .join(
            best.select("AccountNumber", "UniqueAdID").withColumnRenamed(
                "UniqueAdID", "UniqueAdIDBest"
            ),
            "AccountNumber",
            "left",
        )
        .join(
            challenger.select("AccountNumber", "UniqueAdID").withColumnRenamed(
                "UniqueAdID", "UniqueAdIDBestChallenger"
            ),
            "AccountNumber",
            "left",
        )
        .join(
            nextgen.select("AccountNumber", "UniqueAdID").withColumnRenamed(
                "UniqueAdID", "UniqueAdIDNextGenAds"
            ),
            "AccountNumber",
            "left",
        )
    )
    assigned = (
        joined.withColumn(
            "UniqueAdIDMeasurement", chain_when_thens(cell_map["map"])
        )
        .join(
            ads_for_output.select(
                "UniqueAdID", "UniqueAdIDPremium"
            ).withColumnRenamed("UniqueAdID", "UniqueAdIDMeasurement"),
            "UniqueAdIDMeasurement",
            "left",
        )
        .withColumn(
            "UniqueAdIDMeasurement",
            F.when(
                (F.col("IsPremium") == 1)
                & F.col("UniqueAdIDPremium").isNotNull(),
                F.col("UniqueAdIDPremium"),
            ).otherwise(F.col("UniqueAdIDMeasurement")),
        )
        .fillna("NoAdFound", subset=["UniqueAdIDMeasurement"])
        .withColumn(
            "UniqueAdIDAssigned",
            F.when(
                F.col("FallowControl") == cfg["fallow_control"]["true_label"],
                F.lit("NoAd"),
            ).otherwise(F.col("UniqueAdIDMeasurement")),
        )
    )
    treatments = (
        joined.drop(
            "AdSuppressed",
            "UniqueAdIDBasic",
            "UniqueAdIDBest",
            "UniqueAdIDBestChallenger",
            "UniqueAdIDNextGenAds",
        )
        .withColumns(
            {
                "AdSuppressed": F.lit("AdSuppressed"),
                "UniqueAdIDBasic": F.lit("Basic"),
                "UniqueAdIDBest": F.lit("Best"),
                "UniqueAdIDBestChallenger": F.lit("BestChallenger"),
                "UniqueAdIDNextGenAds": F.lit("NextGenAds"),
            }
        )
        .withColumn("Treatment", chain_when_thens(cell_map["map"]))
        .select("AccountNumber", "Treatment")
    )
    assigned = assigned.join(treatments, "AccountNumber", "left").withColumn(
        "Treatment",
        F.when(
            (F.col("IsPremium") == 1) & F.col("UniqueAdIDPremium").isNotNull(),
            F.concat(F.col("Treatment"), F.lit("Prem")),
        ).otherwise(F.col("Treatment")),
    )
    isolation = cfg["page_type_isolation"]
    if isolation["enabled"]:
        allowed = [
            group
            for group, locations in isolation["page_type_map"].items()
            if location in locations
        ] + ["AllPages"]
        assigned = assigned.withColumn(
            "UniqueAdIDAssigned",
            F.when(
                F.col("PageTypeIsolation").isNotNull()
                & ~F.col("PageTypeIsolation").isin(allowed),
                F.lit("NoAd"),
            ).otherwise(F.col("UniqueAdIDAssigned")),
        )

    masids = (
        ads_for_output.select("UniqueAdID", "MASIDToken")
        .withColumn(
            "MASID", F.concat(F.lit(location + "_"), F.col("MASIDToken"))
        )
        .drop("MASIDToken")
        .distinct()
        .unionByName(
            spark.createDataFrame(
                [
                    ("NoAd", f"{location}_Z"),
                    ("AdSuppressed", f"{location}_Z"),
                    ("NoAdFound", f"{location}_Z"),
                ],
                schema="UniqueAdID string not null, MASID string not null",
            )
        )
    )
    assigned = (
        assigned.join(
            masids,
            assigned.UniqueAdIDAssigned == masids.UniqueAdID,
            "left",
        )
        .drop(masids.UniqueAdID)
        .where(
            F.col("Treatment").isNotNull()
            & F.col("MASID").isNotNull()
            & F.col("UniqueAdIDMeasurement").isNotNull()
        )
    )
    if incremental_metrics is not None:
        incrementality = cfg["incrementality"]
        suppress = (
            F.lit(location).isin(incrementality["locations"])
            & F.col("Treatment").isin(incrementality["treatments"])
            & (
                F.col("EstContribution")
                < incrementality["incremental_value_threshold"]
            )
            & (F.col("EstContribution") < 0)
            & (F.col("C_Sessions") >= cfg["results_prm"]["min_c_sessions"])
        )
        assigned = (
            assigned.join(incremental_metrics, "UniqueAdIDAssigned", "left")
            .withColumn(
                "UniqueAdIDAssigned",
                F.when(
                    suppress, F.lit(incrementality["ads_switch_label"])
                ).otherwise(F.col("UniqueAdIDAssigned")),
            )
            .withColumn(
                "MASID",
                F.when(
                    suppress,
                    F.lit(f"{location}_{incrementality['masid_test_token']}"),
                ).otherwise(F.col("MASID")),
            )
        )
    result = assigned.withColumn("Location", F.lit(location)).select(
        *V1_OUTPUT_COLUMNS
    )
    result = result.crossJoin(assignable)
    if parent_assignments is not None:
        result = result.crossJoin(
            parent_assignments.select(F.lit(1).alias("_parent_ready")).limit(1)
        )
    return result.drop("_assignable", "_parent_ready")


def build_v1_assignments(
    spark: Any,
    *,
    cfg: Mapping[str, Any],
    scope_manifest: Sequence[Any],
    control: DataFrame,
    customer_cells: DataFrame,
    candidate_inputs: Any,
    nextgen_assignments_table: str,
    results: DataFrame,
    run_date: date,
) -> DataFrame:
    prepared_cells = customer_cells.withColumn("global", F.lit(1))
    prepared_cells = with_random_assignment_ordinals(
        prepared_cells,
        grp_col="global",
        row_column="_basic_global_row",
        count_column="_basic_global_count",
    )
    prepared_cells = with_random_assignment_ordinals(
        prepared_cells,
        grp_col="AlgoDivision",
        row_column="_basic_group_row",
        count_column="_basic_group_count",
    ).persist(StorageLevel.MEMORY_AND_DISK)
    incremental_metrics = (
        _incremental_ad_metrics(results, cfg=cfg, run_date=run_date)
        if cfg["incrementality"]["incrementality_ads_suppression_switch"]
        else None
    )
    by_scope: dict[str, DataFrame] = {}
    for entry in scope_manifest:
        parent = (
            by_scope.get(entry.inherit_basic_from)
            if entry.inherit_basic_from
            else None
        )
        by_scope[entry.scope] = _build_v1_scope(
            spark,
            location=entry.scope,
            inherit_basic_from=entry.inherit_basic_from,
            cfg=cfg,
            control=control,
            cells=prepared_cells,
            candidate_inputs=candidate_inputs,
            nextgen_assignments_table=nextgen_assignments_table,
            parent_assignments=parent,
            incremental_metrics=incremental_metrics,
        )
    return _union([by_scope[entry.scope] for entry in scope_manifest]).select(
        *V1_OUTPUT_COLUMNS
    )


def _build_v2_scope(
    spark: Any,
    *,
    page_type: str,
    cfg: Mapping[str, Any],
    control: DataFrame,
    cells: DataFrame,
    candidate_inputs: Any,
    nextgen_assignments_table: str,
) -> DataFrame:
    cell_map = cfg["page_types"][page_type]
    ads = control.where(F.col("PageType") == page_type).select(
        "UniqueAdID",
        "UniqueAdIDPremium",
        "AlgoDivision",
        "TargetingCriteria",
        "AudienceOnly",
        "Tags",
        "Themes",
        "ClusterID",
    )
    targeted_ads = ads.fillna(0, subset=["AudienceOnly"]).where(
        F.col("AudienceOnly") != 1
    )
    best_ads = targeted_ads.where(
        F.col("Themes").isNotNull() & (F.col("Themes") != "")
    )
    nextgen_ads = ads.where(
        F.col("ClusterID").isNotNull() & F.col("Themes").isNull()
    )
    assignable = _assignable_marker(targeted_ads, nextgen_ads)
    basic_within = cell_map["basic_within"]
    basic = assign_random_ads_v2(
        targeted_ads.select("UniqueAdID", basic_within),
        cells.select("AccountNumber", basic_within),
        grp_col=basic_within,
    )
    trigger_scores = candidate_inputs.candidates_for_scope(
        "best", page_type
    ).select(
        "AccountNumber",
        "UniqueAdID",
        F.col("TriggerScore").alias("TriggerScoreLookup"),
    )
    basic = basic.join(
        trigger_scores, ["AccountNumber", "UniqueAdID"], "left"
    ).withColumnRenamed("TriggerScoreLookup", "TriggerScore")
    best = assign_candidate_ads_v2(
        df_ads=best_ads,
        candidate_scores=candidate_inputs.candidates_for_scope(
            "best", page_type
        ),
        df_cust=cells,
    )
    challenger = assign_candidate_ads_v2(
        df_ads=best_ads,
        candidate_scores=candidate_inputs.candidates_for_scope(
            "best_challenger", page_type
        ),
        df_cust=cells,
    )
    nextgen = (
        assign_nextgenads_v2(
            df_ads=nextgen_ads,
            customer_to_cluster_table=nextgen_assignments_table,
            df_cust=cells.select("AccountNumber"),
            n_ads=3,
        )
        if _uses_nextgen(cell_map)
        else _empty_v2_assignment(cells)
    )
    spine = (
        basic.select("AccountNumber", "Rank")
        .unionByName(best.select("AccountNumber", "Rank"))
        .unionByName(challenger.select("AccountNumber", "Rank"))
        .unionByName(nextgen.select("AccountNumber", "Rank"))
        .distinct()
    )
    joined = (
        spine.join(
            basic.select(
                "AccountNumber",
                F.col("UniqueAdID").alias("UniqueAdIDBasic"),
                F.col("TriggerScore").alias("TriggerScoreBasic"),
                "Rank",
            ),
            ["AccountNumber", "Rank"],
            "left",
        )
        .join(
            best.select(
                "AccountNumber",
                F.col("UniqueAdID").alias("UniqueAdIDBest"),
                F.col("TriggerScore").alias("TriggerScoreBest"),
                "Rank",
            ),
            ["AccountNumber", "Rank"],
            "left",
        )
        .join(
            challenger.select(
                "AccountNumber",
                F.col("UniqueAdID").alias("UniqueAdIDBestChallenger"),
                F.col("TriggerScore").alias("TriggerScoreBestChallenger"),
                "Rank",
            ),
            ["AccountNumber", "Rank"],
            "left",
        )
        .join(
            nextgen.select(
                "AccountNumber",
                F.col("UniqueAdID").alias("UniqueAdIDNextGenAds"),
                F.col("TriggerScore").alias("TriggerScoreNextGenAds"),
                "Rank",
            ),
            ["AccountNumber", "Rank"],
            "left",
        )
        .join(
            cells.withColumn("AdSuppressed", F.lit("AdSuppressed")),
            "AccountNumber",
            "left",
        )
    )
    assigned = (
        joined.withColumn(
            "UniqueAdIDMeasurement", chain_when_thens(cell_map["map"])
        )
        .join(
            ads.select("UniqueAdID", "UniqueAdIDPremium").withColumnRenamed(
                "UniqueAdID", "UniqueAdIDMeasurement"
            ),
            "UniqueAdIDMeasurement",
            "left",
        )
        .withColumn(
            "UniqueAdIDMeasurement",
            F.when(
                (F.col("IsPremium") == 1)
                & F.col("UniqueAdIDPremium").isNotNull(),
                F.col("UniqueAdIDPremium"),
            ).otherwise(F.col("UniqueAdIDMeasurement")),
        )
        .fillna("NoAdFound", subset=["UniqueAdIDMeasurement"])
        .withColumn(
            "UniqueAdIDAssigned",
            F.when(
                F.col("FallowControl") == cfg["fallow_control"]["true_label"],
                F.lit("NoAd"),
            ).otherwise(F.col("UniqueAdIDMeasurement")),
        )
    )
    treatments = (
        joined.drop(
            "AdSuppressed",
            "UniqueAdIDBasic",
            "UniqueAdIDBest",
            "UniqueAdIDBestChallenger",
            "UniqueAdIDNextGenAds",
        )
        .withColumns(
            {
                "AdSuppressed": F.lit("AdSuppressed"),
                "UniqueAdIDBasic": F.lit("Basic"),
                "UniqueAdIDBest": F.lit("Best"),
                "UniqueAdIDBestChallenger": F.lit("BestChallenger"),
                "UniqueAdIDNextGenAds": F.lit("NextGenAds"),
            }
        )
        .withColumn("Treatment", chain_when_thens(cell_map["map"]))
        .select("AccountNumber", "Rank", "Treatment")
    )
    assigned = assigned.join(
        treatments, ["AccountNumber", "Rank"], "left"
    ).withColumn(
        "Treatment",
        F.when(
            (F.col("IsPremium") == 1) & F.col("UniqueAdIDPremium").isNotNull(),
            F.concat(F.col("Treatment"), F.lit("Prem")),
        ).otherwise(F.col("Treatment")),
    )
    assigned = assigned.withColumn(
        "TriggerScore",
        F.when(
            F.col("Treatment").isin("Best", "BestPrem"),
            F.col("TriggerScoreBest"),
        )
        .when(
            F.col("Treatment").isin("BestChallenger", "BestChallengerPrem"),
            F.col("TriggerScoreBestChallenger"),
        )
        .when(
            F.col("Treatment").isin("Basic", "BasicPrem"),
            F.col("TriggerScoreBasic"),
        )
        .when(F.col("Treatment") == "AdSuppressed", F.col("TriggerScoreBest"))
        .when(
            F.col("Treatment").isin("NextGenAds", "NextGenAdsPrem"),
            F.col("TriggerScoreNextGenAds"),
        ),
    ).withColumn(
        "TriggerScore",
        F.when(
            F.col("UniqueAdIDAssigned") == "NoAdFound",
            F.lit(None).cast("float"),
        ).otherwise(F.col("TriggerScore")),
    )
    isolation = cfg["page_type_isolation"]
    if isolation["enabled"]:
        allowed = [
            group
            for group, pages in isolation.get("page_type_map_v2", {}).items()
            if page_type in pages
        ] + ["AllPages"]
        assigned = assigned.withColumn(
            "UniqueAdIDAssigned",
            F.when(
                F.col("PageTypeIsolation").isNotNull()
                & ~F.col("PageTypeIsolation").isin(allowed),
                F.lit("NoAd"),
            ).otherwise(F.col("UniqueAdIDAssigned")),
        )
    return (
        assigned.where(
            F.col("Treatment").isNotNull()
            & F.col("UniqueAdIDMeasurement").isNotNull()
        )
        .withColumn("PageType", F.lit(page_type))
        .withColumn("TriggerScore", F.col("TriggerScore").cast("float"))
        .select(*V2_OUTPUT_COLUMNS)
        .crossJoin(assignable)
        .drop("_assignable")
    )


def build_v2_assignments(
    spark: Any,
    *,
    cfg: Mapping[str, Any],
    page_types: Sequence[str],
    control: DataFrame,
    customer_cells: DataFrame,
    candidate_inputs: Any,
    nextgen_assignments_table: str,
) -> DataFrame:
    return _union(
        [
            _build_v2_scope(
                spark,
                page_type=page_type,
                cfg=cfg,
                control=control,
                cells=customer_cells,
                candidate_inputs=candidate_inputs,
                nextgen_assignments_table=nextgen_assignments_table,
            )
            for page_type in page_types
        ]
    ).select(*V2_OUTPUT_COLUMNS)


__all__ = [
    "V1_OUTPUT_COLUMNS",
    "V2_OUTPUT_COLUMNS",
    "build_v1_assignments",
    "build_v2_assignments",
]

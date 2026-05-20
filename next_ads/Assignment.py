from datetime import date, timedelta
from collections import defaultdict
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from collections.abc import Callable
from dsutils.dbc import get_spark
from dsutils.logtools import get_logger
from dsutils.etl import assert_pk, build_spark_schema
from dsutils.columnscalers import subtract_mean
from dsutils.timing import timer


logger = get_logger(__name__)


def assign_random_ads_v2(
        df_ads: DataFrame,
        df_cust_grp: DataFrame,
        grp_col: str = 'AlgoDivision',
        n_ads: int = 20,
        seed: int = 42) -> DataFrame:
    """
    Assigns N random ads per customer from their preferred group
    (e.g. AlgoDivision), ensuring uniform ad coverage across all customers.

    Uses a cyclic rotation approach:
      1. Ads are assigned a stable slot number (1..K) within each group.
      2. Each customer is assigned a random integer offset (0..K-1).
      3. The customer's N ads are the K slots starting at their offset
         (wrapping cyclically). This guarantees every ad is served to
         approximately the same number of customers.

    This avoids any per-customer cross-join or AccountNumber-partitioned
    window function: the only join is customers × within-group ads
    (~12M × ~29 rows per division), which Spark handles efficiently.

    Arguments:
        df_ads - PySpark dataframe with cols ("UniqueAdID", grp_col)
        df_cust_grp - PySpark dataframe with cols ("AccountNumber", grp_col)
                      where grp_col is the customer's preferred group
        grp_col - column representing the grouping (e.g. "AlgoDivision")
        n_ads - number of ads to return per customer (default 20)
        seed - random seed for reproducibility

    Returns:
        DataFrame with columns: AccountNumber, UniqueAdID, Rank
    """
    logger.info(
        f'Assigning {n_ads} random ads per customer within group: {grp_col} '
        f'using cyclic rotation (seed={seed})'
    )

    # 1. Assign a randomised slot number to each ad within its group.
    #    Ordering by rand() rather than UniqueAdID ensures slot assignments
    #    are not influenced by the source table row order or ad ID ordering.
    df_ads_slotted = (
        df_ads.select('UniqueAdID', grp_col)
        .withColumn(
            'AdSlot',
            F.row_number().over(
                Window.partitionBy(grp_col).orderBy(F.rand(seed=seed))
            )
        )
    )

    # Count of ads per group (small collect — one row per division)
    df_ad_counts = (
        df_ads_slotted
        .groupBy(grp_col)
        .agg(F.max('AdSlot').alias('nAds'))
    )

    # 2. Assign each customer a unique random offset in [0, nAds-1]
    df_cust_offset = (
        df_cust_grp.select('AccountNumber', grp_col)
        .join(df_ad_counts, on=grp_col, how='inner')
        .withColumn(
            'Offset',
            (F.rand(seed=seed) * F.col('nAds')).cast('int')
        )
    )

    # 3. Join each customer to all ads in their group (~29 ads per customer)
    #    Compute each ad's cyclic rank relative to this customer's offset:
    #      Rank = ((AdSlot - 1 - Offset + nAds) % nAds) + 1
    #    Then keep only the top n_ads ranks.
    df_result = (
        df_cust_offset
        .join(df_ads_slotted, on=grp_col, how='inner')
        .withColumn(
            'Rank',
            ((F.col('AdSlot') - 1 - F.col('Offset') + F.col('nAds'))
             % F.col('nAds')) + 1
        )
        .where(F.col('Rank') <= n_ads)
        .select('AccountNumber', 'UniqueAdID', 'Rank')
    )

    return df_result


def assign_random_ads(
        df_ads: DataFrame,
        df_cust_grp: DataFrame,
        grp_col: str = None) -> DataFrame:
    """
    Function assigns Ads randomly (and uniformly) within group.
    Arguments:
        df_ads - PySpark dataframe with cols ("UniqueAdID", grp_col)
        df_cust_grp - PySpark dataframe with cols ("AccountNumber", grp_col)
        grp_col - column reference to group (partition) by (e.g. "Division")
    Returns:
        Dataframe - Ads assigned randomly (uniform) to customers within-group
    """
    # TODO: Generalise function to assign_random_entity?

    # TODO: Remove the need for dummy column when assigning without grp_col
    if grp_col is None:
        df_ads = df_ads.withColumn('global', F.lit(1))
        df_cust_grp = df_cust_grp.withColumn('global', F.lit(1))
        grp_col = 'global'
        logger.info('Assigning ads randomly')
    else:
        logger.info(f'Assigning ads randomly within group: {grp_col}')

    w = Window.partitionBy(grp_col).orderBy("UniqueAdID")
    df_ads = df_ads.withColumn("RandomKey", F.row_number().over(w))

    # Dictionary of Ads per group (max RandomKey)
    # Number of Ads within group is arg for ntile
    df_ad_counts = (
        df_ads
        .groupBy(grp_col)
        .agg(F.max("RandomKey").alias("nAds"))
    )
    grp_ads = {row[grp_col]: row["nAds"] for row in df_ad_counts.collect()}

    # ntile customers into nAds groups by group to create RandomKey
    # orderBy first for deterministic output
    # TODO: Avoid for loop by passing nAds as arg to F.ntile()
    grp_cust_rdm_list = []
    w = Window.partitionBy(F.lit(1)).orderBy("RandomValue")
    for grp_k in grp_ads:
        df_cust_rdm_grp = (
            df_cust_grp
            .where(F.col(grp_col) == grp_k)
            .orderBy("AccountNumber")
            .withColumn("RandomValue", F.rand(seed=42))
            .withColumn("RandomKey", F.ntile(grp_ads[grp_k]).over(w))
            .drop("RandomValue")
        )
        grp_cust_rdm_list.append(df_cust_rdm_grp)

    df_cust_rdm = grp_cust_rdm_list.pop()
    for df_n in grp_cust_rdm_list:
        df_cust_rdm = df_cust_rdm.unionByName(df_n)

    df_cust_rdm_ads = (
        df_cust_rdm
        .join(df_ads, on=["RandomKey", grp_col])
        .drop("RandomKey")
    )

    return df_cust_rdm_ads.select('AccountNumber', 'UniqueAdID')


def assign_random_ads_with_exclusions(
        df_ads: DataFrame,
        df_cust_grp: DataFrame,
        grp_col: str = None) -> DataFrame:
    """
    Assigns Ads randomly (and uniformly) within group, excluding specific ads
    per customer.
    
    Arguments:
        df_ads - PySpark dataframe with cols ("UniqueAdID", grp_col)
        df_cust_grp - PySpark dataframe with cols ("AccountNumber",
                      grp_col, "ExcludedAdID")
        grp_col - column reference to group (partition) by
                  (e.g. "AlgoDivision")
    
    Returns:
        Dataframe - Ads assigned randomly (uniform) to customers
                    within-group, excluding the ExcludedAdID for each
                    customer
    """

    if grp_col is None:
        df_ads = df_ads.withColumn('global', F.lit(1))
        df_cust_grp = df_cust_grp.withColumn('global', F.lit(1))
        grp_col = 'global'
        logger.info('Assigning ads randomly with exclusions')
    else:
        logger.info(
            f'Assigning ads randomly within group: {grp_col} '
            'with exclusions'
        )

    w = Window.partitionBy(grp_col).orderBy("UniqueAdID")
    df_ads = df_ads.withColumn("RandomKey", F.row_number().over(w))

    # Dictionary of Ads per group (max RandomKey)
    df_ad_counts = (
        df_ads
        .groupBy(grp_col)
        .agg(F.max("RandomKey").alias("nAds"))
    )
    grp_ads = {row[grp_col]: row["nAds"] for row in df_ad_counts.collect()}

    # Assign random ads per customer, excluding their ExcludedAdID
    grp_cust_rdm_list = []
    
    for grp_k in grp_ads:
        logger.debug(f'Assigning for {grp_col}: {grp_k}')

        # Get eligible ads for this group
        df_ads_grp = (
            df_ads
            .where(F.col(grp_col) == grp_k)
        )

        # Get customers for this group
        df_cust_grp_filtered = (
            df_cust_grp
            .where(F.col(grp_col) == grp_k)
        )

        # Cross join customers with eligible ads, excluding ExcludedAdID
        df_cust_ads = (
            df_cust_grp_filtered
            .select("AccountNumber", grp_col, "ExcludedAdID")
            .crossJoin(
                df_ads_grp.select("UniqueAdID", "RandomKey", grp_col)
            )
            .where(
                (F.col("ExcludedAdID").isNull()) |
                (F.col("UniqueAdID") != F.col("ExcludedAdID"))
            )
        )
        
        # Assign random selection within eligible ads per customer
        w_customer = (
            Window
            .partitionBy("AccountNumber")
            .orderBy("RandomValue")
        )
        df_cust_rdm_grp = (
            df_cust_ads
            .withColumn("RandomValue", F.rand(seed=42))
            .withColumn(
                "SelectionRank",
                F.row_number().over(w_customer)
            )
            .where(F.col("SelectionRank") == 1)
            .select("AccountNumber", "UniqueAdID")
        )

        grp_cust_rdm_list.append(df_cust_rdm_grp)
    
    # Union all groups
    df_assigned = grp_cust_rdm_list.pop()
    for df_grp in grp_cust_rdm_list:
        df_assigned = df_assigned.unionByName(df_grp)

    assert_pk(df_assigned, ["AccountNumber"])

    return df_assigned


def assign_best_ads(
        df_ads: DataFrame,
        targeting_scores_table: str,
        df_cust: DataFrame = None,
        score_scale_fn: Callable = None,
        score_scale_partition: list[str] = ["TargetingCriteria"],
        return_ranks: list = [1],
        apply_ad_feedback: bool = False,
        ad_results_table: str = '',
        control_sheet_latest_table: str = '',
        ad_feedback_weight: float = 0.5
        ) -> DataFrame:
    """
    Assigns "best" Ad to each customer based on scores provided.

    Arguments:
        df_ads - Dataframe with columns: UniqueAdID, TargetingCriteria (unique
        in combination)
        targeting_scores_table - Name of table containing TargetingScores
        df_cust - Filter customers (Dataframe with col: AccountNumber)
        score_scale_fn - Function for scaling the score
        score_scale_partition - Partition for scaling
        return_ranks - Rankings to return (e.g. for 'second best ad' use [2])
    """
    logger.debug(f'Assigning {return_ranks} ranked ad(s) ' +
                 f'using scores from {targeting_scores_table}')

    ts_tbl_cols = [
        'AccountNumber', 'TargetingCriteria', 'TargetingScores'
        ]
    df_adscores = (
        df_ads
        .select("UniqueAdID", "TargetingCriteria")
        .join(get_spark().table(targeting_scores_table).select(ts_tbl_cols),
              on="TargetingCriteria",
              how="inner")
    )

    if df_cust:
        logger.debug('Filtering customers for assignment')
        df_adscores = df_adscores.join(df_cust,
                                       on="AccountNumber",
                                       how="inner")

    if score_scale_fn:
        logger.debug(
            f'Applying score scaling function {score_scale_fn.__name__}' +
            f' over {score_scale_partition}')
        df_adscores = (
            df_adscores
            .withColumn("TargetingScoreScaled",
                        score_scale_fn(F.col("TargetingScore"),
                                       partition_by=score_scale_partition))
            )
    else:
        logger.debug(
            'No scaling function provided, TargetingScoreScaled not scaled')
        df_adscores = (
            df_adscores
            .withColumn("TargetingScoreScaled", F.col("TargetingScore"))
            )

    if apply_ad_feedback:
        logger.debug('Applying ad feedback loop ' +
                     f'using results from {ad_results_table}')
        msg = ' not supplied for ad feedback loop'
        assert ad_results_table, 'Ad Results table' + msg
        assert control_sheet_latest_table, 'Control Sheet Latest table' + msg

        # The following step ensures scores are postive before applying the ad
        # feedback loop. This relies on the assumption that the minimum scaled
        # targeting score is >= -1.
        # An initial implementation of dynamically finding the minimum score
        # and adjusting this to zero was found to be too computationally
        # expensive (it resulted in lots of repetitive sorting of large
        # dataframes).
        # Not doing this dynamically creates an additional requirement that the
        # targeting/recommender scores provided to the engine are in the range
        # [0,1]. This allows for the established approach of rebasing the
        # scores to the average score within-group.
        # TODO: Find computationally efficient way to dynamically rebase
        # minimum overall score in df_adscores to zero.

        df_adscores = (
            df_adscores
            .withColumn(
                'TargetingScoreScaled',
                F.col('TargetingScoreScaled') + F.lit(1))
        )

        df_ad_feedback = get_ad_feedback_scores(
            ad_results_table=ad_results_table,
            control_sheet_latest_table=control_sheet_latest_table,
            ad_feedback_weight=ad_feedback_weight
        )
        if df_ad_feedback:
            df_adscores = (
                df_adscores
                .join(df_ad_feedback, on='UniqueAdID', how='left')
                .fillna(1, subset=['AdFeedbackScore'])
                .withColumn(
                    'TargetingScoreScaled',
                    F.col('TargetingScoreScaled')*F.col('AdFeedbackScore'))
            )

    assert_pk(df_adscores,
              ["AccountNumber", "UniqueAdID", "TargetingCriteria"])

    w_ad = (
        Window
        .partitionBy([F.col("AccountNumber")])
        .orderBy(F.col("TargetingScoreScaled").desc())
    )

    w_ad_tb = (
        Window
        .partitionBy([F.col("AccountNumber"), F.col("AdRank")])
        .orderBy(F.col("TieBreaker").desc())
    )
    # TieBreaker column creates a random split when multiple ads
    # are targeted using the same TargetingCriteria
    # Only one ad of those with matching TargetingCriteria will
    # be returned
    df_return = (
        df_adscores
        .withColumn('TieBreaker', F.rand(seed=99))
        .withColumn("AdRank", F.dense_rank().over(w_ad))
        .withColumn("AdRankTB", F.dense_rank().over(w_ad_tb))
        .where(F.col("AdRankTB") == 1)
        .where(F.col("AdRank").isin(return_ranks))
        .select("AccountNumber",
                "TargetingCriteria",
                "TargetingScoreScaled",
                "AdRank",
                "UniqueAdID")
    )

    return df_return


def assign_best_ads_with_constraints(
        df_ads: DataFrame,
        df_cust: DataFrame = None,
        constraints: dict = {},
        best_kwargs: dict = {}) -> DataFrame:

    if "targeting_within_division" in constraints:
        div_type = constraints["targeting_within_division"]
        logger.debug(
            f'Applying targeting_within_division constraint by {div_type}')
        divs = [row[0] for row in (df_cust
                                   .select(div_type)
                                   .distinct()).collect()]
        df_ads_best_div_list = []

        for div in divs:
            logger.debug(f'Assigning where {div_type}: {div}')
            df_ads_d = (
                df_ads
                .where(F.col(div_type) == div)
                .where(F.col("TargetingCriteria").isNotNull())
                .select("UniqueAdID", "TargetingCriteria")
            )
            df_cust_d = (
                df_cust
                .where(F.col(div_type) == div)
                .select("AccountNumber")
            )
            df_ads_best_d = (
                assign_best_ads(
                    df_ads=df_ads_d,
                    df_cust=df_cust_d,
                    **best_kwargs
                    )
            )
            df_ads_best_div_list.append(df_ads_best_d)

        df_assigned_best = df_ads_best_div_list.pop()
        for df_ads_best_div in df_ads_best_div_list:
            df_assigned_best = df_assigned_best.unionByName(df_ads_best_div)

        return df_assigned_best

    elif "filter_ads" in constraints:
        logger.debug('Applying filter_ads constraint')
        for k in constraints["filter_ads"].keys():
            logger.debug(
                f'Filtering where {k} == {constraints["filter_ads"][k]}')
            df_ads = (
                df_ads
                .where(F.col(k) == constraints["filter_ads"][k])
            )

        df_assigned_best = assign_best_ads(
                    df_ads=df_ads,
                    df_cust=df_cust,
                    **best_kwargs
                    )

        return df_assigned_best

    else:
        raise Exception("Constraint not understood")


def assign_best_ads_rec(
        df_ads: DataFrame,
        recommender_scores_table: str,
        df_cust: DataFrame = None,
        score_scale_fn: Callable = None,
        score_scale_partition: list[str] = ["UniqueAdID"],
        return_ranks: list = [1],
        apply_ad_feedback: bool = False,
        ad_results_table: str = '',
        control_sheet_latest_table: str = '',
        ad_feedback_weight: float = 0.5
        ) -> DataFrame:
    """
    Assigns "best" Ad to each customer based on RECOMMENDER scores provided.

    Arguments:
        df_ads - Dataframe with column: UniqueAdID (unique values)
        recommender_scores_table - Name of table containing RecommenderScores
        df_cust - Filter customers (Dataframe with col: AccountNumber)
        score_scale_fn - Function for scaling the score
        score_scale_partition - Partition for scaling
        return_ranks - Rankings to return (e.g. for 'second best ad' use [2])
    """
    logger.debug(f'Assigning {return_ranks} ranked ad(s) ' +
                 f'using scores from {recommender_scores_table}')

    rec_tbl_cols = ['AccountNumber', 'UniqueAdID', 'RecommenderScore']
    df_adscores = (
        df_ads
        .select("UniqueAdID")
        .join(get_spark().table(recommender_scores_table).select(rec_tbl_cols),
              on="UniqueAdID",
              how="inner")
    )

    if df_cust:
        logger.debug('Filtering customers for assignment')
        df_adscores = df_adscores.join(df_cust,
                                       on="AccountNumber",
                                       how="inner")

    if score_scale_fn:
        logger.debug(
            f'Applying score scaling function {score_scale_fn.__name__}' +
            f'over {score_scale_partition}')
        df_adscores = (
            df_adscores
            .withColumn("RecommenderScoreScaled",
                        score_scale_fn(F.col("RecommenderScore"),
                                       partition_by=score_scale_partition))
            )
    else:
        logger.debug(
            'No scaling function provided, RecommenderScoreScaled not scaled')
        df_adscores = (
            df_adscores
            .withColumn("RecommenderScoreScaled", F.col("RecommenderScore"))
            )

    if apply_ad_feedback:
        logger.debug('Applying ad feedback loop ' +
                     f'using results from {ad_results_table}')
        msg = ' not supplied for ad feedback loop'
        assert ad_results_table, 'Ad Results table' + msg
        assert control_sheet_latest_table, 'Control Sheet Latest table' + msg

        # The following step ensures scores are postive before applying the ad
        # feedback loop. This relies on the assumption that the minimum scaled
        # targeting score is >= -1.
        # An initial implementation of dynamically finding the minimum score
        # and adjusting this to zero was found to be too computationally
        # expensive (it resulted in lots of repetitive sorting of large
        # dataframes).
        # Not doing this dynamically creates an additional requirement that the
        # targeting/recommender scores provided to the engine are in the range
        # [0,1]. This allows for the established approach of rebasing the
        # scores to the average score within-group.
        # TODO: Find computationally efficient way to dynamically rebase
        # minimum overall score in df_adscores to zero.

        df_adscores = (
            df_adscores
            .withColumn(
                'RecommenderScoreScaled',
                F.col('RecommenderScoreScaled') + F.lit(1))
        )

        df_ad_feedback = get_ad_feedback_scores(
            ad_results_table=ad_results_table,
            control_sheet_latest_table=control_sheet_latest_table,
            ad_feedback_weight=ad_feedback_weight
        )
        if df_ad_feedback:
            df_adscores = (
                df_adscores
                .join(df_ad_feedback, on='UniqueAdID', how='left')
                .fillna(1, subset=['AdFeedbackScore'])
                .withColumn(
                    'RecommenderScoreScaled',
                    F.col('RecommenderScoreScaled')*F.col('AdFeedbackScore'))
            )

    assert_pk(df_adscores,
              ["AccountNumber", "UniqueAdID"])

    w_ad = (
        Window
        .partitionBy([F.col("AccountNumber")])
        .orderBy(F.col("RecommenderScoreScaled").desc())
    )

    w_ad_tb = (
        Window
        .partitionBy([F.col("AccountNumber"), F.col("AdRank")])
        .orderBy(F.col("TieBreaker").desc())
    )
    # TieBreaker column creates a random split when multiple ads
    # are have the same RecommenderScoreScaled
    # Random ad from each tie will be returned
    df_return = (
        df_adscores
        .withColumn('TieBreaker', F.rand(seed=99))
        .withColumn("AdRank", F.dense_rank().over(w_ad))
        .withColumn("AdRankTB", F.dense_rank().over(w_ad_tb))
        .where(F.col("AdRankTB") == 1)
        .where(F.col("AdRank").isin(return_ranks))
        .select("AccountNumber",
                "RecommenderScoreScaled",
                "AdRank",
                "UniqueAdID")
    )

    return df_return


def assign_best_ads_with_constraints_rec(
        df_ads: DataFrame,
        df_cust: DataFrame = None,
        constraints: dict = {},
        best_kwargs: dict = {}) -> DataFrame:

    if "targeting_within_division" in constraints:
        div_type = constraints["targeting_within_division"]
        logger.debug(
            f'Applying targeting_within_division constraint by {div_type}')
        divs = [row[0] for row in (df_cust
                                   .select(div_type)
                                   .distinct()).collect()]
        df_ads_best_div_list = []

        for div in divs:
            logger.debug(f'Assigning where {div_type}: {div}')
            df_ads_d = (
                df_ads
                .where(F.col(div_type) == div)
                .select("UniqueAdID")
            )
            df_cust_d = (
                df_cust
                .where(F.col(div_type) == div)
                .select("AccountNumber")
            )
            df_ads_best_d = (
                assign_best_ads_rec(
                    df_ads=df_ads_d,
                    df_cust=df_cust_d,
                    **best_kwargs
                    )
            )
            df_ads_best_div_list.append(df_ads_best_d)

        df_assigned_best = df_ads_best_div_list.pop()
        for df_ads_best_div in df_ads_best_div_list:
            df_assigned_best = df_assigned_best.unionByName(df_ads_best_div)

        return df_assigned_best

    elif "filter_ads" in constraints:
        logger.debug('Applying filter_ads constraint')
        for k in constraints["filter_ads"].keys():
            logger.debug(
                f'Filtering where {k} == {constraints["filter_ads"][k]}')
            df_ads = (
                df_ads
                .where(F.col(k) == constraints["filter_ads"][k])
            )

        df_assigned_best = assign_best_ads_rec(
                    df_ads=df_ads,
                    df_cust=df_cust,
                    **best_kwargs
                    )

        return df_assigned_best

    else:
        raise Exception("Constraint not understood")


def get_ad_feedback_scores(
        ad_results_table: str,
        control_sheet_latest_table: str,
        sessions_threshold: int = 10000,
        ad_feedback_weight: float = 0.5,
        lookback_period_days: int = 7,
        lookback_offset_days: int = 2,
        ad_id_col: str = 'UniqueAdID',
        sessions_col: str = 'Sessions',
        apportioned_revenue_col: str = 'ApportionedRevenue',
        ctrl_sessions_col: str = 'C_Sessions',
        ctrl_apportioned_revenue_col: str = 'C_ApportionedRevenue',
        session_overlap_ratio_col: str = 'SessionOverlapRatio',
        ) -> DataFrame | None:
    """
    Generates scaled ad performance scores designed for boosting/penalising
    targeting score of ads during assignment. If no suitable ad scores can be
    found, the function will return None.
    """
    start_delta_days = (lookback_period_days - 1) + lookback_offset_days
    date_start = date.today() - timedelta(days=start_delta_days)
    date_end = date.today() - timedelta(days=lookback_offset_days)
    logger.debug(
        f'Retrieving results from {date_start} to {date_end}' +
        f' for ads that are currently in {control_sheet_latest_table}')

    active_ads = (
        get_spark()
        .table(control_sheet_latest_table)
        .select(ad_id_col)
        .distinct()
    )

    df_ad_results_raw = (
        get_spark()
        .table(ad_results_table)
        .join(active_ads, how='inner', on=ad_id_col)
        .where(F.col('SessionDate') >= date_start)
        .where(F.col('SessionDate') <= date_end)
    )

    df_ad_results = (
        df_ad_results_raw
        .groupBy(ad_id_col)
        .agg(
            F.sum(sessions_col).alias(sessions_col),
            F.sum(apportioned_revenue_col).alias(apportioned_revenue_col),
            F.sum(ctrl_sessions_col).alias(ctrl_sessions_col),
            F.sum(ctrl_apportioned_revenue_col).alias(
                ctrl_apportioned_revenue_col),
            F.mean(session_overlap_ratio_col).alias(session_overlap_ratio_col)
            )
        .where(F.col(ctrl_sessions_col) >= sessions_threshold)
        .withColumn('ARPS',
                    (F.col(apportioned_revenue_col)
                     / F.col(sessions_col)))
        .withColumn('C_ARPS',
                    (F.col(ctrl_apportioned_revenue_col)
                     / F.col(ctrl_sessions_col)))
        .withColumn('IncARPS', F.col('ARPS')-F.col('C_ARPS'))
        .withColumn('IncARPSAdj',
                    F.col('IncARPS')/F.col(session_overlap_ratio_col))
        .withColumn('IncARPSAdjPct', F.col('IncARPSAdj')/F.col('C_ARPS'))
    )

    if df_ad_results.count() == 0:
        return None

    logger.debug('Scaling ad incremental performance')
    minIncPct = df_ad_results.agg(F.min('IncARPSAdjPct')).collect()[0][0]
    maxIncPct = df_ad_results.agg(F.max('IncARPSAdjPct')).collect()[0][0]

    if abs(minIncPct) > abs(maxIncPct):
        scaleFactorIncPct = abs(minIncPct)
    else:
        scaleFactorIncPct = abs(maxIncPct)

    df_ad_results_scaled = (
        df_ad_results
        .withColumn('IncARPSAdjPctScaled',
                    F.col('IncARPSAdjPct')/F.lit(scaleFactorIncPct))
    )

    logger.debug(f'Applying ad_feedback_weight of {ad_feedback_weight}')
    df_ad_results_scaled_stand = (
        df_ad_results_scaled
        .withColumn(
            'AdFeedbackScore',
            (F.col('IncARPSAdjPctScaled')*F.lit(ad_feedback_weight))+F.lit(1))
    )

    assert_pk(df_ad_results_scaled_stand, pk_cols=[ad_id_col])

    return df_ad_results_scaled_stand.select(ad_id_col, 'AdFeedbackScore')


def assign_preranked_ads_v2(
        df_ads: DataFrame,
        preranked_ads_table: str,
        page_type: str,
        df_cust: DataFrame = None,
        n_ads: int = 20,
) -> DataFrame:
    """
    Assigns pre-ranked ads to customers for a given PageType.

    Reads the preranked ads table (schema: AccountNumber, UniqueAdID, Score,
    TriggerScore, Rank, PageType), filters to the specified PageType, then
    inner-joins to df_ads to restrict to currently eligible ads. Assumes dense
    rank is used in preranked_ads to handle ties.

    The PageType filter is required even though df_ads is already scoped to a
    single page type, because the same UniqueAdID can appear in the preranked
    table for multiple page types (with different ranks). Filtering only on
    UniqueAdID would produce duplicate AccountNumber/UniqueAdID pairs and fail
    the PK assertion.

    Arguments:
        df_ads - DataFrame with column: UniqueAdID (eligible ads for this
                 PageType, already filtered from the control sheet)
        preranked_ads_table - Name of table containing preranked ads
        page_type - PageType to filter on (e.g. "ShoppingBag")
        df_cust - Optional customer filter (DataFrame with col: AccountNumber)
        n_ads - Maximum number of ranked ads to return per customer (default 20)

    Returns:
        DataFrame with columns: AccountNumber, UniqueAdID, Rank, TriggerScore
    """
    logger.info(
        f'Assigning preranked ads for PageType: {page_type} '
        f'using scores from {preranked_ads_table}'
    )

    df_adscores = (
        get_spark()
        .table(preranked_ads_table)
        .where(F.col('PageType') == page_type)
        .select('AccountNumber', 'UniqueAdID', 'Rank', 'TriggerScore')
        .join(df_ads.select('UniqueAdID'), on='UniqueAdID', how='inner')
        .where(F.col('Rank') <= n_ads)
    )

    if df_cust is not None:
        logger.debug('Filtering customers for assignment')
        df_adscores = df_adscores.join(df_cust, on='AccountNumber', how='inner')

    assert_pk(df_adscores, ['AccountNumber', 'UniqueAdID'])

    return df_adscores.select(
        'AccountNumber',
        'UniqueAdID',
        'Rank',
        'TriggerScore')


def assign_preranked_ads(
        df_ads: DataFrame,
        preranked_ads_table: str,
        location: str = '',
        df_cust: DataFrame = None,
        return_ranks: list = [1],
        inherit_rank_from_location: str = ''
        ) -> DataFrame:
    """
    Assigns "best" Ad to each customer based on pre-ranked ads provided.

    Arguments:
        df_ads - Dataframe with column: UniqueAdID (unique values)
        preranked_ads_table - Name of table containing AccountNumber,
        UniqueAdID, Rank
        df_cust - Filter customers (Dataframe with col: AccountNumber)
        return_ranks - Rankings to return (e.g. for 'second best ad' use [2])
    """

    if inherit_rank_from_location:
        logger.info(f'Inheriting rank for {location} from location: '
                    + f'{inherit_rank_from_location}')
        location = inherit_rank_from_location

    logger.info(f'Assigning {return_ranks} ranked ad(s) ' +
                f'using scores from {preranked_ads_table}')

    rank_tbl_cols = ['AccountNumber', 'UniqueAdID', 'Score', 'Rank']

    if location:
        logger.debug(f'Filtering preranked ads for location: {location}')
        df_adscores = (
            get_spark()
            .table(preranked_ads_table)
            .where(F.col('Location') == location)
            .select(rank_tbl_cols)
            .join(df_ads, on='UniqueAdID', how='inner')
        )
    else:
        logger.debug('No location provided, using global preranked ads')
        logger.debug(
            'Ads reranked to cover case where only subset of ads are eligible')
        df_adscores = (
            get_spark()
            .table(preranked_ads_table)
            .select(rank_tbl_cols)
            .join(df_ads, on='UniqueAdID', how='inner')
            .withColumn(
                'Rank',
                F.rank().over(
                    Window
                    .partitionBy(F.col('AccountNumber'))
                    .orderBy(F.col('Rank'))
                )
            )
        )

    if df_cust:
        logger.debug('Filtering customers for assignment')
        df_adscores = df_adscores.join(df_cust,
                                       on="AccountNumber",
                                       how="inner")

    assert_pk(df_adscores,
              ["AccountNumber", "UniqueAdID"])

    w_ad_tb = (
        Window
        .partitionBy([F.col("AccountNumber"), F.col("Rank")])
        .orderBy(F.col("TieBreaker").desc())
    )
    # TieBreaker column creates a random split when multiple ads
    # are have the same RecommenderScoreScaled
    # Random ad from each tie will be returned
    df_return = (
        df_adscores
        .withColumn('TieBreaker', F.rand(seed=99))
        .withColumn("RankTB", F.dense_rank().over(w_ad_tb))
        .where(F.col("RankTB") == 1)
        .where(F.col("Rank").isin(return_ranks))
        .select("AccountNumber",
                "Score",
                "Rank",
                "UniqueAdID")
    )

    return df_return


def assign_predetermined_audience(
        audiences: list[list[dict]],
        tables: dict
        ) -> DataFrame:
    """
    Assigns predefined audience, in order.
    First in list takes priority when customer in multiple audiences.

    Arguments:
        audiences - List of lists.
        First element of sublist if audience reference.
        Second element of sublist is dict containing column references
        e.g.
        ```[['Audience1',
             {'account_col': 'account_number',
              'label_col': 'segment'}],
            ['Audience2':
             {'account_col': 'account',
              'label_col': 'cluster_name'}]]
        ```
    Returns:
        DataFrame with columns `AccountNumber`, `Audience`.
        No `AccountNumber` will have multiple `Audiences`.
    """
    logger.debug('Assigning predetermined audiences')
    df_audience_list = []

    for (i, a) in enumerate(audiences):
        a_name = audiences[i][0]
        a_cols = audiences[i][1]
        logger.debug(f'Assigning audience: {a_name} ({a_cols}) - priority {i}')
        df_a = (
            get_spark()
            .table(tables[a_name])
            .withColumnsRenamed(
                {
                    a_cols["account_col"]: "AccountNumber",
                    a_cols["label_col"]: "Audience"
                }
            )
            .withColumn("AudiencePriority", F.lit(i))
            )
        df_audience_list.append(df_a)

    df_audiences = df_audience_list.pop()

    if len(df_audience_list) >= 1:
        for df_a_i in df_audience_list:
            df_audiences = df_audiences.unionByName(df_a_i)

    accW = Window.partitionBy("AccountNumber")

    df_audiences = (
        df_audiences
        .withColumn("MaxPriority",
                    F.min(F.col("AudiencePriority")).over(accW))
        .where(F.col("AudiencePriority") == F.col("MaxPriority"))
        .select("AccountNumber", "Audience")
    )

    assert_pk(df_audiences, ["AccountNumber"])

    return df_audiences


def melt_transient_cells(df: DataFrame) -> DataFrame:
    """
    Utility function for melting transient cells.
    """
    df_melted = df.unpivot(
        ids="AccountNumber",
        values=None,
        variableColumnName="Cell",
        valueColumnName="CellValue")
    return df_melted


def get_algo_divisions(model_scores_latest_table: str) -> DataFrame:
    """
    Returns AlgoDivison for all customers from the provided model scores table.
    The AlgoDivision returned is the Division for which the account has the
    highest propensity, once propensity scores have been expressed relative to
    the division's mean score. This yields a division per customer that is the
    division that they have the highest propensity to shop, relative to the
    average propensity to shop that division.

    Returns:
        DataFrame with columns `AccountNumber`, `AlgoDivision`
    """
    logger.debug(
        'Assigning customers to their preferred division (AlgoDivision)')
    division_scores = (
        get_spark()
        .table(model_scores_latest_table)
        .drop('rundate')
        .withColumnRenamed('account_number', 'AccountNumber')
        .select(
            'AccountNumber',
            F.col('div_womens').alias('Womens'),
            F.col('div_mens').alias('Mens'),
            F.col('div_boys').alias('Boys'),
            F.col('div_girls').alias('Girls'),
            F.col('div_beauty').alias('Beauty'),
            F.col('div_home').alias('Home'),
            F.col('div_baby').alias('Baby')
            )
    )

    w_acc_scaled_score_desc = (
        Window
        .partitionBy("AccountNumber")
        .orderBy(F.desc(F.col("ScoreScaled")))
        )

    division_assignments = (
        division_scores
        .unpivot(
            ids='AccountNumber',
            values=None,
            variableColumnName='AlgoDivision',
            valueColumnName='Score'
            )
        .withColumn('ScoreScaled',
                    subtract_mean(F.col('score'),
                                  partition_by=['AlgoDivision']))
        .withColumn('Rank', F.rank().over(w_acc_scaled_score_desc))
        .where(F.col('Rank') == 1)
        .select('AccountNumber', 'AlgoDivision')
    )

    assert_pk(division_assignments, ['AccountNumber', 'AlgoDivision'])

    return division_assignments


@timer
def greedy_assignment(
        df: DataFrame,
        item_quotas: dict,
        item_col: str = 'item',
        user_col: str = 'user',
        rank_col: str = 'rank',
        logging_interval: int = 100000) -> dict:
    """
    Make greedy assignmends of users to items, based on a sequence of
    item-user pairs, and associated item quotas.
    It is recommended that the supplied dataframe is filtered to items
    with quotas before passing it to the function for efficiency
    (if not all items have quotas).

    Args:
        `df`: PySpark DataFrame with columns [item, user, rank]
            where rank is ascending order of processing.
        `item_quotas`: Dictionary of `{item: quota}`. Items not in
            the dictionary are assumed to have zero quota (i.e. will
            be excluded from greedy assignment).
        `item_col`: Name of item column in df
        `user_col`: Name of user column in df
        `rank_col`: Name of rank column in df
        `logging_interval`: How many cycles between progress logs
    """

    logger.info('Starting greedy assignment')

    cmap = {
            'item': item_col,
            'user': user_col,
            'rank': rank_col
        }
    output_schema = build_spark_schema([
        [cmap['item'], 'string', 'not null'],
        [cmap['user'], 'string', 'not null']
    ])

    if not item_quotas:
        logger.error('No item quotas supplied, aborting greedy assignment')
        return get_spark().createDataFrame([], schema=output_schema)

    if all(cap <= 0 for cap in item_quotas.values()):
        logger.error('No positive item quotas supplied, aborting greedy assignment')  # noqa
        return get_spark().createDataFrame([], schema=output_schema)

    assignments = defaultdict(list)
    assigned_users = set()
    processed_count = 0

    assert set([item_col, user_col, rank_col]).issubset(set(df.columns)), f"Supplied col names not in supplied df"  # noqa
    cmap = {
        'item': item_col,
        'user': user_col,
        'rank': rank_col
    }

    df = df.withColumnsRenamed({v: k for k, v in cmap.items()})

    for row in df.orderBy('rank').toLocalIterator():
        processed_count += 1

        if processed_count % logging_interval == 0:
            filled = sum(1 for cap in item_quotas.values() if cap <= 0)
            logger.info(
                f"Processed {processed_count:,} records"
                + f"    {filled}/{len(item_quotas.keys()):,} items filled"
            )

        # Skip if user already assigned
        if row.user in assigned_users:
            continue

        # Skip if item is at quota
        item_quota_remaining = item_quotas.get(row.item, 0)
        assert item_quota_remaining >= 0,  f"Negative quota encounterd for item {row.item}"  # noqa
        if item_quota_remaining <= 0:
            continue

        # Otherwise assign user to item and decrement quota
        assignments[row.item].append(row.user)
        assigned_users.add(row.user)
        item_quotas[row.item] -= 1

        # Early termination: all items filled
        if all(cap <= 0 for cap in item_quotas.values()):
            logger.info(
                f"All items full after processing {processed_count:,} records")
            break

    # Convert result back to DataFrame for consistency
    result_data = [
        (item, user) for item, users in assignments.items() for user in users]
    result = get_spark().createDataFrame(result_data, schema=output_schema)

    return result

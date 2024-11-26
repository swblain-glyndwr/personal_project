from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from next_ads.utils.dbc import get_spark
from next_ads.utils.etl import assert_pk
from collections.abc import Callable


def assign_random_ads(
        df_ads: DataFrame,
        df_cust_grp: DataFrame,
        grp_col: str) -> DataFrame:
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

    w = Window().partitionBy(grp_col).orderBy("UniqueAdID")
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
    w = Window().partitionBy(F.lit(1)).orderBy("RandomValue")
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

    return df_cust_rdm_ads


def assign_best_ads(
        df_ads: DataFrame,
        targeting_scores_table: str,
        df_cust: DataFrame = None,
        score_scale_fn: Callable = None,
        score_scale_partition: list[str] = ["TargetingCriteria"],
        return_ranks: list = [1],
        tie_breaker: Callable = None
        ) -> DataFrame:
    """
    Assigns "best" Ad to each customer based on scores provided.
    Dev - Tie Breaker needed for one-to-many TargetingCriteria:Ad

    Arguments:
        df_ads - Dataframe with columns (UniqueAdID, TargetingCriteria)
        targeting_scores_table - Name of table containing TargetingScores
        df_cust - Filter customers (Dataframe with col: AccountNumber)
        score_scale_fn - Function for scaling the score
        score_scale_within - Partition for scaling
        return_ranks - Rankings to return (e.g. for 'second best ad' use [2])
        tie_breaker - TODO - How to break tie when one Targeting has many Ads
    """

    df_adscores = (
        df_ads
        .select("UniqueAdID", "TargetingCriteria")
        .join(get_spark().table(targeting_scores_table),
              on="TargetingCriteria",
              how="inner")
    )

    if df_cust:
        df_adscores = df_adscores.join(df_cust,
                                       on="AccountNumber",
                                       how="inner")

    df_adscores = (
        df_adscores
        .withColumn("TargetingScoreScaled",
                    score_scale_fn(F.col("TargetingScore"),
                                   partition_by=score_scale_partition))
        )

    assert_pk(df_adscores,
              ["AccountNumber", "UniqueAdID", "TargetingCriteria"])

    w = (
        Window
        .partitionBy([F.col("AccountNumber")])
        .orderBy(F.col("TargetingScoreScaled").desc(),
                 F.col("UniqueAdID").desc())
    )
    # Will take last Ad ID alphabetically (proxy for newest) if scores are tied
    # TODO: Probably quite a rare occurence, but need a better method

    df_return = (
        df_adscores
        .withColumn("AdRank", F.rank().over(w))
        .where(F.col("AdRank").isin(return_ranks))
        .select("AccountNumber",
                "TargetingCriteria",
                "TargetingScoreScaled",
                "AdRank",
                "UniqueAdID")
    )

    if tie_breaker:
        # TODO: Tie breaker condition
        # e.g. ads with common TargetingCriteria
        # utilise assign_random_ads within TargetingCriteria
        pass

    return df_return


def assign_best_ads_with_constraints(
        df_ads: DataFrame,
        df_cust: DataFrame = None,
        constraints: dict = {},
        best_kwargs: dict = {}) -> DataFrame:

    if "targeting_within_division" in constraints:

        div_type = constraints["targeting_within_division"]
        divs = [row[0] for row in (df_cust
                                   .select(div_type)
                                   .distinct()).collect()]
        df_ads_best_div_list = []

        for div in divs:
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

        for k in constraints["filter_ads"].keys():
            df_ads = (
                df_ads
                .where(F.col(k) == constraints["filter"][k])
            )

        df_assigned_best = assign_best_ads(
                    df_ads=df_ads,
                    df_cust=df_cust,
                    **best_kwargs
                    )

        return df_assigned_best

    else:
        raise Exception("Constraint not understood")


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

    df_audience_list = []

    for (i, a) in enumerate(audiences):
        a_name = audiences[i][0]
        a_cols = audiences[i][1]
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


def get_algo_divisions_legacy() -> DataFrame:
    """
    Approach of assigning each customer their 'best' Division.
    Code ported across from legacy code due to time constraints.
    Small syntax changes made to bypass need for Spark SQL and
    to_pandas_on_spark().\n
    **WARNING: Table references and variables are hard-coded.**

    Returns:
        DataFrame with columns `UniqueAdID`, `AlgoDivision`
    """
    schema = 'marketingdata_prod.warehouse'
    df_base = (
        get_spark()
        .table(schema + '.adm_v2_customers_base')
        .where((F.col('country') == 'GB') & (F.col('shopped_104w') == 'Y'))
    )
    df_brands = get_spark().table(schema + '.adm_v2_transaction_brands')
    df_divisions = get_spark().table(schema + '.adm_v2_transaction_divisions')
    df_beauty = get_spark().table(schema + '.adm_v2_transaction_beauty')

    cats = [
        'beauty',
        'mens',
        'womens',
        'boys',
        'girls',
        'home',
        'newbornboys',
        'newborngirls'
    ]

    total = df_base.agg(F.countDistinct('account_number')).collect()[0][0]

    exprs = [(F.countDistinct(F.when(
        (F.col(i+'_spend_1w') > 0) |
        (F.col(i+'_spend_1_4w') > 0) |
        (F.col(i+'_spend_4_13w') > 0) |
        (F.col(i+'_spend_13_26w') > 0) |
        (F.col(i+'_spend_26_52w') > 0),
        F.col('account_number')))/total).alias(i) for i in cats]

    (
        df_base
        .join(df_beauty, 'account_number', 'inner')
        .join(df_brands, 'account_number', 'inner')
        .join(df_divisions, 'account_number', 'inner')
    ).agg(F.count('account_number'), F.countDistinct('account_number'))

    target_proportions = (
        df_base
        .join(df_beauty, 'account_number', 'inner')
        .join(df_brands, 'account_number', 'inner')
        .join(df_divisions, 'account_number', 'inner')
        .agg(*exprs)
        .toDF(
            'beauty',
            'mens',
            'womens',
            'boys',
            'girls',
            'home',
            'newbornboys',
            'newborngirs')
    )

    target_proportions = (
        target_proportions
        .withColumn('newborn', F.col('newbornboys') + F.col('newborngirs'))
    )

    target_proportions = target_proportions.drop('newbornboys', 'newborngirs')

    target_proportions.withColumn('row_sum',
                                  sum([F.col(c)
                                       for c in target_proportions.columns]))
    # Proportions add up to more than 1? (1.535707 on 21.11.2024)
    # Cats aren't mutually exclusive, so maybe this is right?

    # TODO: Change to collective model view
    all_scores = (
        get_spark()
        .table('warehouse.next_uk_division_model_latest')
        .drop('rundate')
        .select(
            'account_number',
            F.col('WW').alias('womens'),
            F.col('MW').alias('mens'),
            F.col('BW').alias('boys'),
            F.col('GW').alias('girls'),
            F.col('BL').alias('beauty'),
            F.col('HW').alias('home'),
            F.col('NB').alias('newborn')
            )
        )

    standard_individual = all_scores
    cols = standard_individual.drop('account_number').columns
    w = Window().partitionBy(F.lit(1))

    for i in cols:
        standard_individual = (
            standard_individual
            .withColumn(
                i,
                (((((F.col(i))-F.mean(i).over(w)))*0.25)/(F.stddev(i).over(w))
                 ) + (target_proportions.select(i).collect()[0][0])
            )
        )

    # This DF is written out as delta lake file in legacy code
    # Needed for LP apparently

    # Rewrote the below legacy code in pyspark
    # DivModels = (
    #     get_spark()
    #     .sql("""
    # select distinct account_number,
    # womens as WW,
    # mens as MW,
    # girls as GW,
    # boys as BW,
    # newborn as NB,
    # beauty as BL,
    # home as HW
    # from standard_individual
    #             """)
    # )

    DivModels = (
        standard_individual
        .withColumnsRenamed(
            {
                "womens": "WW",
                "mens": "MW",
                "girls": "GW",
                "boys": "BW",
                "newborn": "NB",
                "beauty": "BL",
                "home": "HW"
            }
        )
        .select("account_number",
                "WW",
                "MW",
                "GW",
                "BW",
                "NB",
                "BL",
                "HW")
        .drop_duplicates()
    )
    # Rewrote the initial unpivot melt to avoid using to_pandas_on_spark()
    # DivModels
    # .to_pandas_on_spark()
    # .melt(id_vars='account_number')
    # .to_spark()

    DivModels = (
        DivModels
        .unpivot(ids="account_number",
                 values=None,
                 variableColumnName="variable",
                 valueColumnName="propensity")
        .withColumn('rank',
                    F.rank().over(
                        Window.partitionBy(
                            "account_number").orderBy(
                                F.col("propensity").desc())))
        .filter(F.col('rank') <= 1)
        .groupby('account_number')
        .pivot('rank')
        .agg(F.first('variable'))
        .withColumnRenamed('1', 'Best_Orig')
        )

    df_return = (
        DivModels
        .withColumnRenamed('account_number', 'AccountNumber')
        .withColumn(
            'AlgoDivision',
            F.when(F.col('Best_Orig') == 'WW', F.lit('Womens'))
            .when(F.col('Best_Orig') == 'MW', F.lit('Mens'))
            .when(F.col('Best_Orig') == 'GW', F.lit('Girls'))
            .when(F.col('Best_Orig') == 'BW', F.lit('Boys'))
            .when(F.col('Best_Orig') == 'NB', F.lit('Baby'))
            .when(F.col('Best_Orig') == 'BL', F.lit('Beauty'))
            .when(F.col('Best_Orig') == 'HW', F.lit('Home'))
            .otherwise(F.lit(None))
            )
        .select('AccountNumber', 'AlgoDivision')
    )

    assert_pk(df_return, ['AccountNumber', 'AlgoDivision'])

    return df_return

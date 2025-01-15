import datetime
from statistics import mean
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from itertools import chain, combinations, permutations
from collections.abc import Callable


def check_for_missing_dates(
        date_start: datetime.date,
        date_end: datetime.date,
        data_dates: list[datetime.date]) -> list:

    if not data_dates:
        raise Exception('No dates found during period')

    date_i = date_start
    dates = [date_i]
    date_patches = []

    while date_i < date_end:
        dates.append(date_i + datetime.timedelta(days=1))
        date_i = date_i + datetime.timedelta(days=1)

    data_dates.sort()

    if dates[0] != data_dates[0]:
        raise Exception(
            'Data missing from first date of results period - ' +
            're-run results with an earlier start date')

    if max(dates) > max(data_dates):
        date_tail = [d for d in dates if d > max(data_dates)]
        for d in date_tail:
            date_patches.append((d, max(data_dates)))

    i, j = 0, 0
    while i < len(data_dates):
        if data_dates[i] == dates[j]:
            i += 1
            j += 1
        else:
            date_patches.append((dates[j], data_dates[i-1]))
            j += 1

    return date_patches


def patch_missing_dates(
        date_patch: list[tuple],
        df: DataFrame,
        date_col: str) -> DataFrame:

    df_patched = []
    for date_p in date_patch:
        date_missing = date_p[0]
        date_copy = date_p[1]
        df_patched.append(
            df
            .where(F.col(date_col) == date_copy)
            .withColumn(date_col, F.lit(date_missing))
        )
    df_rtn = df_patched.pop()
    for df_i in df_patched:
        df_rtn = df_rtn.unionByName(df_i)

    return df_rtn


def validate_assignments_match_pf(
        df_assignments_pf: DataFrame) -> dict:

    mismatch_msgs = dict()
    df_mismatch = df_assignments_pf.where(F.col('MASID') != F.col('MASIDPF'))

    if df_mismatch.count() > 0:

        session_dates = [
            s[0] for s in
            df_mismatch.select('SessionDate').distinct().collect()
        ]

        for s in session_dates:

            df_mismatch_s = df_mismatch.where(F.col('SessionDate') == s)

            mismatch_n = df_mismatch_s.count()
            mismatch_locs = [
                x[0] for x in
                df_mismatch_s.select('Location').distinct().collect()
                ]
            mismatch_ads_n = (
                df_mismatch_s.select('UniqueAdIDAssigned').distinct().count()
                )
            mismatch_accs_n = (
                df_mismatch_s.select('AccountNumber').distinct().count()
            )

            mismatch_msgs[s.strftime('%Y-%m-%d')] = [
                (f'{mismatch_n:,} cases found where PF does not match ' +
                 'Assigned MASID'),
                f'Affected Location(s): {", ".join(mismatch_locs)}',
                f'Number of affected Ads: {mismatch_ads_n:,}',
                f'Number of affected Accounts: {mismatch_accs_n:,}'
            ]

        return mismatch_msgs
    else:
        return dict()


def summarise_sessions(
        df: DataFrame,
        session_id_col: str,
        page_id_col: str,
        revenue_col: str = 'Revenue',
        impressions_col: str = 'Impressions',
        clicks_col: str = 'Clicks',
        group_cols: list[str] = []) -> DataFrame:

    df_summary = (
        df
        .withColumn('Converted',
                    F.when(F.col(revenue_col) > 0, 1).otherwise(0))
        .groupBy(*group_cols, session_id_col, page_id_col)
        .agg(
            F.first(revenue_col).alias(revenue_col),
            F.max('Converted').alias('Converted'),
            F.first(impressions_col).alias(impressions_col),
            F.sum(clicks_col).alias(clicks_col),
            )
        .groupBy(*group_cols, session_id_col)
        .agg(
            F.first(revenue_col).alias(revenue_col),
            F.max('Converted').alias('Converted'),
            F.sum(impressions_col).alias(impressions_col),
            F.sum(clicks_col).alias(clicks_col),
            )
        .groupBy(*group_cols)
        .agg(
            F.countDistinct(session_id_col).alias('Sessions'),
            F.sum(revenue_col).alias(revenue_col),
            F.sum('Converted').alias('Conversions'),
            F.sum(impressions_col).alias(impressions_col),
            F.sum(clicks_col).alias(clicks_col)
            )
    )

    return df_summary


def estimate_incremental_value(
        df: DataFrame,
        session_col: str,
        value_col: str,
        control_col: str,
        test_label: str,
        control_label: str,
        group_cols: list[str] = []) -> DataFrame:

    df_inc = (
        df
        .groupBy(*group_cols, control_col, session_col)
        .agg(F.first(value_col).alias('Value'))
        .replace({
            test_label: 'T',
            control_label: 'C'
        }, subset=[control_col])
        .groupBy(*group_cols, control_col)
        .agg(
            F.countDistinct(session_col).alias('Sessions'),
            F.sum('Value').alias('Value')
            )
        .withColumn('AvgValue', F.col('Value')/F.col('Sessions'))
        .groupBy(*group_cols)
        .pivot(control_col)
        .agg(
            F.first('Sessions').alias('Sessions'),
            F.first('Value').alias('Value'),
            F.first('AvgValue').alias('AvgValue'),
        )
        .withColumn('IncValue', F.col('T_AvgValue') - F.col('C_AvgValue'))
        .withColumn('IncValuePercent', F.col('IncValue') / F.col('C_AvgValue'))
        .withColumn('EstIncValue',
                    F.col('IncValue') * F.col('T_Sessions'))
    )

    return df_inc


def marginal_contributions(
        df: DataFrame,
        contributions_col: str,
        value_function: Callable,
        value_function_kwargs: dict = {},
        value_function_return_col: str = "",
        ) -> dict:

    grand_coalition = set(
        [x[0] for x in df.select(contributions_col).distinct().collect()]
        )

    combo_sizes = range(1, len(grand_coalition) + 1)
    powerset = chain.from_iterable(
        combinations(grand_coalition, x) for x in combo_sizes)

    contributions = list()
    for x in powerset:
        df_coalition = df.where(F.col(contributions_col).isin(list(x)))
        coalition_result = value_function(
            df_coalition,
            **value_function_kwargs)
        if value_function_return_col:
            coalition_result = (
                coalition_result
                .select(value_function_return_col)
                .collect()[0][0]
            )
        contributions.append((set(x), coalition_result))

    perms = permutations(grand_coalition)
    marginal_contributions = {k: [] for k in grand_coalition}

    for perm in perms:
        coalition = set()
        perm_total = 0
        for i in range(len(perm)):
            coalition.add(perm[i])
            contribution = [v for (k, v) in contributions if k == coalition][0]
            marginal_contribution = contribution - perm_total
            # if marginal_contribution < 0:
            #     marginal_contribution = 0
            marginal_contributions[perm[i]].append(marginal_contribution)
            perm_total += marginal_contribution

    return {k: mean(v) for (k, v) in marginal_contributions.items()}

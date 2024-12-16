from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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


def estimate_incremental_rps(
        df,
        group_cols: list[str],
        sessions_col: str = 'Sessions',
        revenue_col: str = 'Revenue',
        control_col: str = 'FallowControl',
        test_label: str = 'Ads',
        control_label: str = 'No Ads') -> DataFrame:

    df_inc = (
        df
        .replace({
            test_label: 'test',
            control_label: 'control'
        }, subset=[control_col])
        .groupBy(*group_cols, control_col)
        .agg(
            F.sum(sessions_col).alias('Sessions'),
            F.sum(revenue_col).alias('Revenue')
            )
        .withColumn('RPS', F.col('Revenue')/F.col('Sessions'))
        .groupBy(*group_cols)
        .pivot(control_col)
        .agg(
            F.first('Sessions').alias('Sessions'),
            F.first('Revenue').alias('Revenue'),
            F.first('RPS').alias('RPS'),
        )
        .withColumn('Inc_RPS', F.col('test_RPS') - F.col('control_RPS'))
        .withColumn('Inc_RPS_Percent', F.col('Inc_RPS') / F.col('control_RPS'))
        .withColumn('Estimated_Inc_Revenue',
                    F.col('Inc_RPS') * F.col('test_Sessions'))
    )

    return df_inc


# def marginal_contributions(
#         df: DataFrame,
#         contributions_col: str,
#         ):

from pyspark.sql import DataFrame, functions as F
from dsutils.dbc import get_spark
from dsutils.logtools import get_logger


logger = get_logger(__name__)

def generate_experimentID(
    df: DataFrame, 
    experiments: list, 
    audience_df: DataFrame =None, 
    audience_sample: list = None, 
    audience_split: list = None
    ) -> DataFrame:
    """
    Creates ExperimentID entry for NextAs json export

    Args:
      df (DataFrame): CUSTOMER_CELLS_FIXED_LATEST DataFrame. Must contain AccountNumber, FallowControl, ShoppingBagTest1 and all the AdHocABTest split cols.
      experiments (list): List of dictionaries containing experiment names and the column name to be used for the split
      audience_df (DataFrame): Only relavent is an audience test is live. DataFrame containing AccountNumber and the audience split column
      audience_sample (list): Only relavent is an audience test is live. List of which customers are eligable for the audience. Accepted values: ['Best'] or ['Basic','Best']
      audience_split (list): Only relavent is an audience test is live. List of audience splits values. Eg [0,1] or ['A','B','C]
    Returns:
      DataFrame: DataFrame with AccountNumber and ExperimentID
    
    """
    logger.info("Starting ExperimentID generation")
    logger.debug(f'Experiments received: {experiments}')

    # If audience test live, join audience split column onto master customer_cells df
    if audience_df is not None:
        logger.info('Joining audience dataframe')
        df = (df.join(
            audience_df, on='accountNumber', how='left')
              )

    # Defining audience experiment vs standard
    exp_map = {}
    is_audience_exp = {} 
    
    for item in experiments:
        for k, v in item.items():
            if k == 'Audience':
                for aud_k, aud_v in v.items():
                    exp_map[aud_k] = aud_v
                    is_audience_exp[aud_k] = True
            else:
                exp_map[k] = v
                is_audience_exp[k] = False

    new_cols = []
    
    # Define expressions for each experiment and customer pot
    for col_name, split_col in exp_map.items():
        if is_audience_exp.get(col_name):
            expr = F.when(F.col('FallowControl') == 'NoAds', F.lit(f'Aud{col_name}_CT'))
            
            # Audience targeting needs to be dynamic to apply to Best only customers, or Best and Basic
            if 'Best' in audience_sample and 'Basic' not in audience_sample:
                expr = expr.when(F.col('ShoppingBagTest1') == 'Basic', F.lit(f'Aud{col_name}_BA'))
            # Look for values defined in audience_split list
            if audience_split:
                for val in audience_split:
                    expr = expr.when(F.col(split_col) == val, F.lit(f'Aud{col_name}_{val}'))
            # If customer not in audience default to _Z
            expr = expr.otherwise(F.lit(f'Aud{col_name}_Z'))
                    
        else:
            # For standard experiment, the following structure is used
            expr = (F.when(F.col('FallowControl') == 'NoAds', F.lit(f'{col_name}_CT'))
                    .when(F.col('ShoppingBagTest1') == 'Basic', F.lit(f'{col_name}_BA'))
                    .when((F.col('ShoppingBagTest1') == 'Best') & (F.col(split_col) == 'A'), F.lit(f'{col_name}_BE0'))
                    .when((F.col('ShoppingBagTest1') == 'Best') & (F.col(split_col) == 'B'), F.lit(f'{col_name}_BE1'))
                    .otherwise(None))
        
        new_cols.append(expr.alias(col_name))

    df = df.select("*", *new_cols)
    
    experiment_names = list(exp_map.keys())
    df = (df.withColumn(
        'ExperimentID', 
        F.concat_ws(' | ', F.lit('NextAds'), *[F.col(c) for c in experiment_names])
    )
          .select(
              'AccountNumber',
              'ExperimentID'
          ))

    logger.info('ExperimentID generation complete')
    return df
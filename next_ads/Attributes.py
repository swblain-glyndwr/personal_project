from pyspark.sql import functions as F
from dsutils.logtools import get_logger


logger = get_logger(__name__)


def parse_ad_attributes(
        df,
        ad_id_col: str = 'UniqueAdID',
        attribute_col: str = 'TargetingAttributes',
        split_delimiter: str = ', ',
        key_value_delimiter: str = ':'):
    """
    Parse ad attributes from string column to long dataframe
    of `ad_id_col,attribute,value` (string column must be of the form
    "attribute1:value1, attribute2:value2").
    """

    if split_delimiter == key_value_delimiter:
        raise ValueError(
            "`split_delimiter` and `key_value_delimiter` must be different")
    if split_delimiter == '|':
        logger.warning(
            'Using "|" as a `split_delimiter` requires escaping in regex.')
        split_delimiter = '\\|'

    df = df.select(ad_id_col, attribute_col)

    df_exploded = df.withColumn(
        'attribute_pair',
        F.explode(F.split(F.col(attribute_col), split_delimiter))
    ).withColumn(
        'attribute_pair_split',
        F.split(F.col('attribute_pair'), key_value_delimiter)
    )

    df_parsed = df_exploded.withColumn(
        'attribute', F.trim(F.col('attribute_pair_split').getItem(0))
    ).withColumn(
        'value', F.trim(F.col('attribute_pair_split').getItem(1))
    )

    df_result = (
        df_parsed
        .select(ad_id_col, 'attribute', 'value')
        .filter(F.col('attribute').isNotNull() & F.col('value').isNotNull())
        .filter((F.col('attribute') != '') & (F.col('value') != ''))
    )

    return df_result

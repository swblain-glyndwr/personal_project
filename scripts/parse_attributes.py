import sys
from pathlib import Path
try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() # type: ignore # noqa
    if not notebook_path.startswith('/Workspace'):
        notebook_path = '/Workspace' + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

import json
from pyspark.sql import functions as F
from datetime import date
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import (build_spark_schema,
                         map_tbl,
                         delete_from_and_load,
                         truncate_and_load)
from dsutils.argparser import get_job_parser


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg('--job_env')
CLIENT = jobparser.get_arg('--client')
LOG_LEVEL = jobparser.get_arg('--log_level')
REFRESH_ATTRIBUTES_DATE = jobparser.get_arg('--refresh_attributes_date')
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert JOB_ENV.lower() == 'dev', \
        f'Client must be specified when running in {JOB_ENV}'
    CLIENT = 'next_uk'  # Client can be specified for interactive debugging
    logger.warning(f'Client not specified (defaulting to {CLIENT})')

logger.info(f"Configuring run for client: {CLIENT}")
with open(PROJECT_ROOT / f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

TODAY = date.today().strftime(format='%Y-%m-%d')
SET_ATTRIBUTES = REFRESH_ATTRIBUTES_DATE == TODAY or False
BQ_EXPORT = jobparser.has_arg('--bq') or False

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

# Get read only table name
PRODUCT_CATALOG = cfg["tables"]["read"]["product_catalog"]
PRODUCT_CATALOG_LATEST = cfg["tables"]["read"]["product_catalog_latest"]
BASKETS = cfg["tables"]["read"]["baskets"]
NOV_SCORES_CSV = cfg["attributes"]["nov_scores_csv"]

# BQ export parameters
BQ_OPTIONS = cfg['big_query']

# Map write schema to parameterised write table names
tbl_args = {'schema': SCHEMA, 'client': CLIENT}
ATTRIBUTE_SET = map_tbl(tbls["attribute_set"], **tbl_args)
ATTRIBUTE_SET_LATEST = map_tbl(tbls["attribute_set_latest"], **tbl_args)
ITEM_ATTRIBUTES_LATEST = map_tbl(tbls["item_attributes_latest"], **tbl_args)

logger.info(f'Parsing attributes with parameters: {cfg["attributes"]}')
ATTRIBUTES = cfg["attributes"]["active"]
LOOKBACK_DAYS = cfg["attributes"]["lookback_days"]
FREQ_CUTOFF_PC = cfg["attributes"]["frequency_cutoff_pc"]
PC_CUTOFF_COL = cfg["attributes"]["pc_cutoff_col"]

logger.info(f'Fetching item metadata from {PRODUCT_CATALOG}')
df_catalog_full = (
    spark
    .table(PRODUCT_CATALOG)
    .where(F.col('end_date') > F.date_sub(F.current_date(), LOOKBACK_DAYS))
    )

# Assumed that pid is not reused
logger.info('Parsing metadata into attributes')
df_catalog = (
    df_catalog_full
    .drop('gender', 'category')
    .withColumnRenamed('department', 'next_department')
    .withColumn(
        'gender',
        F.when(F.lower(F.col('next_gender')).contains('women'), 'women')
        .when(F.lower(F.col('next_gender')).contains('men'), 'men')
        .when(F.lower(F.col('next_gender')).contains('girls'), 'girls')
        .when(F.lower(F.col('next_gender')).contains('boys'), 'boys')
        .otherwise(F.lit(None))
    )
    .withColumn(
        'lifestage',
        F.when(F.lower(F.col('next_gender')).contains('newborn'), 'newborn')
        .when(F.lower(F.col('gender')).isin('women', 'men', 'unisex'), 'adult')
        .when(F.lower(F.col('next_gender')).contains('older'), 'kids_older')
        .when(
            F.lower(F.col('next_gender')).contains('younger'), 'kids_younger')
        .otherwise(F.lit(None))
    )
    .withColumn(
        'department',
        F.when(F.lower(F.col('next_department')).contains('wear'), 'fashion')
        .when(F.lower(F.col('next_department')).contains('home'), 'home')
        .when(F.lower(F.col('next_department')).contains('beauty'), 'beauty')
        .otherwise(F.lit(None))
    )
    .withColumn(
        'brand',
        F.when(
            (
                F.array_contains(F.split(F.lower(F.col('range')), '\\|'),
                                 'npremium')
            ) | (
                F.array_contains(F.split(F.lower(F.col('range')), '\\|'),
                                 'n premium the snuggle grand')
            ),
            'npremium')
        .when(
            (F.lower(F.col('title')).rlike('signature'))
            &
            (
                F.lower(F.col('range')).rlike('signature')
                | F.lower(F.col('range')).rlike('next signature')
            )
            &
            (F.lower(F.col('brand')) == 'next')
            &
            (F.col('next_department') == 'menswear'),
            'nextsignature'
        ).otherwise(F.col('brand'))
    )
    .withColumnsRenamed(
        {
            'next_category': 'category',
            'next_colour': 'colour'
        }
    )
    .select('pid', *ATTRIBUTES)
)
df_catalog.cache()

n_items_total = df_catalog.select('pid').distinct().count()


# Get baskets by item for quantifying label prevelance
logger.info(f'Fetching basket data from {BASKETS}')
n_baskets_total = (
    spark
    .table(BASKETS)
    .where(F.col('orderdate') > F.date_sub(F.current_date(), LOOKBACK_DAYS))
    .select('orderid')
    .distinct()
    .count()
)

df_baskets = (
    spark
    .table(BASKETS)
    .where(F.col('orderdate') > F.date_sub(F.current_date(), LOOKBACK_DAYS))
    .withColumnRenamed('itemno', 'pid')
    .select('pid', 'orderid')
    .distinct()
)
df_baskets.cache()

attribute_dfs = dict()
for attribute in ATTRIBUTES:

    logger.info(f'Processing attribute: {attribute}')

    df = (
        df_catalog
        .select('pid', attribute)
        .withColumn('value_raw', F.explode(F.split(F.col(attribute), r'\|')))
        .withColumn('value', F.lower(F.trim(F.col('value_raw'))))
        .filter(F.col('value') != '')
        .select('pid', 'value')
        .distinct()
    )

    n_items = df.select('pid').distinct().count()

    df_count_items = (
        df
        .groupBy('value')
        .agg(F.countDistinct('pid').alias('n_products'))
        .withColumn('pc_products',
                    (F.col('n_products') / n_items) * 100)
        .withColumn('pc_products_total',
                    (F.col('n_products') / n_items_total) * 100)
    )

    df_count_baskets = (
        df_baskets
        .join(df, on='pid', how='inner')
        .groupBy('value')
        .agg(F.countDistinct('orderid').alias('n_orders'))
        .withColumn('pc_orders_total',
                    (F.col('n_orders') / n_baskets_total) * 100)
    )

    if SET_ATTRIBUTES:
        logger.info(f'REFRESH_ATTRIBUTES_DATE matches today ({TODAY})')
        logger.info(f'Creating new attribute set for {attribute}')
        logger.info(f'Filtering where {PC_CUTOFF_COL} >= {FREQ_CUTOFF_PC}%')
        df_count = (
            df_count_items
            .join(df_count_baskets, on='value', how='inner')
            .filter(F.col(PC_CUTOFF_COL) >= FREQ_CUTOFF_PC)
        )
    else:
        logger.info(f'Mapping items to latest set values for: {attribute}')
        df_set_values = (
            spark
            .table(ATTRIBUTE_SET_LATEST)
            .filter(F.col('attribute') == attribute)
            .select('value')
            .distinct()
        )
        if df_set_values.isEmpty():
            logger.warning(
                f'Requested attribute {attribute}'
                + f'not found in {ATTRIBUTE_SET_LATEST}')
            logger.warning(f'Skipping {attribute}')
            continue
        df_count = (
            df_count_items
            .join(df_set_values, on='value', how='inner')
        )

    df_count.cache()
    attribute_dfs[attribute] = df.join(df_count, on='value', how='inner')
    df_count.unpersist()


# Concatenate all attribute-value pairs into single dataframe
attr_schema = build_spark_schema([
    ["pid", "string", "not null"],
    ["attribute", "string", "not null"],
    ["value", "string", "not null"]
])

df_attributes_master = spark.createDataFrame([], attr_schema)

logger.info(f'Combining {len(attribute_dfs)} attributes'
            + f' {list(attribute_dfs.keys())} into single dataframe')

for attribute, df in attribute_dfs.items():
    df_attr = (
        df
        .select('pid', F.lit(attribute).alias('attribute'), 'value')
        .distinct()
    )
    df_attributes_master = df_attributes_master.unionByName(df_attr)

if SET_ATTRIBUTES:

    df_attribute_set = (
        df_attributes_master
        .select('attribute', 'value')
        .distinct()
        .orderBy('attribute', 'value')
    )

    logger.info('Exporting new attribute set')

    delete_from_and_load(
        df_attribute_set,
        ATTRIBUTE_SET,
        pk_cols=['attribute', 'value'],
        del_where={'rundate': 'current_date()'}
    )

    truncate_and_load(
        df_attribute_set,
        ATTRIBUTE_SET_LATEST,
        pk_cols=['attribute', 'value']
    )

    logger.info('Refreshing latest item-attribute mapping (using new attribute set)')  # noqa
    truncate_and_load(
        df_attributes_master,
        ITEM_ATTRIBUTES_LATEST,
        pk_cols=['pid', 'attribute', 'value']
    )

else:
    logger.info('Refreshing latest item-attribute mapping')
    truncate_and_load(
        df_attributes_master,
        ITEM_ATTRIBUTES_LATEST,
        pk_cols=['pid', 'attribute', 'value']
    )

    if BQ_EXPORT and not SET_ATTRIBUTES:
        logger.info('Combining item attributes & NOV score for BQ export')
        nov_scores = (
            spark
            .read
            .csv(NOV_SCORES_CSV, header=True)
            .select('item_number', 'next_order_value')
            .withColumnRenamed('item_number', 'pid')
        )

        product_catalog_latest = (
            spark
            .table(PRODUCT_CATALOG_LATEST)
            .select('pid', 'title', 'URL', 'large_image')
            .withColumn("URL", F.regexp_replace("URL", "#", "/"))
        )

        product_catalog_with_nov = (
            product_catalog_latest
            .join(nov_scores, on='pid', how='left')
            .distinct()
        )

        attributes_pivot = (
            df_attributes_master
            .groupBy("pid")
            .pivot("attribute")
            .agg(F.collect_list("value"))
        )

        for attribute in ATTRIBUTES:
            attributes_pivot = (
                attributes_pivot
                .withColumn(attribute, F.explode_outer(attribute))
            )

        attributes_pivot = (
            attributes_pivot
            .select('pid', *ATTRIBUTES)
            .distinct()
        )

        bq_item_attributes = (
            product_catalog_with_nov
            .join(
                attributes_pivot,
                on="pid",
                how="inner"
            )
            .distinct()
            .fillna("Unknown")
        )

        logger.info('Exporting item attributes to Big Query')
        logger.info(
            'Target BQ table:'
            + f'{map_tbl(BQ_OPTIONS["item_attributes_dashboard"], **tbl_args)}'
            )
        (
            bq_item_attributes
            .write.format('bigquery')
            .mode('overwrite')
            .option('temporaryGcsBucket', BQ_OPTIONS['temporaryGcsBucket'])
            .option('parentProject', BQ_OPTIONS['parentProject'])
            .option('table',
                    map_tbl(BQ_OPTIONS['item_attributes_dashboard'],
                            **tbl_args))
            .save()
        )

logger.info('Run complete')

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
from pyspark.sql import Window
from datetime import date, timedelta

from dsutils.dbc import configure_spark
from dsutils.argparser import get_job_parser
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import delete_from_and_load, truncate_and_load, map_tbl

from next_ads.Plotting import DirectedGraphPlotter


jobparser = get_job_parser()
jobparser._parse_args()
JOB_ENV = jobparser.get_arg('--job_env')
CLIENT = jobparser.get_arg('--client')
LOG_LEVEL = jobparser.get_arg('--log_level')
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

PRODUCT_CATALOG = cfg['tables']['read']['product_catalog']
BASKETS = cfg['tables']['read']['baskets']

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}')

# Map write schema to parameterised write table names
tbl_args = {'schema': SCHEMA, 'client': CLIENT}
ITEM_THEMES = map_tbl(tbls["item_themes_latest"], **tbl_args)
THEME_TRANSITIONS_LATEST = map_tbl(tbls["theme_transitions_latest"], **tbl_args)  # noqa
THEME_TRANSITIONS = map_tbl(tbls["theme_transitions"], **tbl_args)
NEXT_THEME_SCORES_LATEST = map_tbl(tbls["next_theme_scores_latest"], **tbl_args)  # noqa
NEXT_THEME_SCORES = map_tbl(tbls["next_theme_scores"], **tbl_args)
THEME_SCORING_EVENTS_LATEST = map_tbl(tbls["theme_scoring_events_latest"], **tbl_args)  # noqa

SCORE_LAST_N_BASKETS = jobparser.get_arg('--score-last-n-baskets') or 10
BASKET_HISTORY_DAYS = jobparser.get_arg('--basket-history-days') or 364
yesterday = date.today() - timedelta(days=1)
ACTIONS_END = jobparser.get_arg('--actions-end') or yesterday
if isinstance(ACTIONS_END, str):
    ACTIONS_END = date.fromisoformat(ACTIONS_END)
ACTIONS_START = ACTIONS_END - timedelta(days=BASKET_HISTORY_DAYS)

REFRESH_MODEL_DATE = jobparser.get_arg('--refresh_model_date')
TODAY = date.today().strftime(format='%Y-%m-%d')
TRAIN = REFRESH_MODEL_DATE == TODAY or False

TEST_ACCOUNT = jobparser.get_arg('--test-account')
SHOW_TOP_N = jobparser.get_arg('--show-top-n') or 100
PLOT_GRAPH = jobparser.has_arg('--plot-graph')

spark = configure_spark()

w_item_by_modified = (
    Window
    .partitionBy('pid')
    .orderBy(F.desc('date_modified'), 'title')
)
item_titles = (
    spark
    .table(PRODUCT_CATALOG)
    .select('pid', 'title', 'date_modified')
    .withColumn('modified_rank', F.row_number().over(w_item_by_modified))
    .where(F.col('modified_rank') == 1)
    .select('pid', 'title')
)
msg = 'Duplicate PIDs found when retrieving item titles'
assert item_titles.count() == item_titles.select('pid').distinct().count(), msg

# Only consider highest-ranked theme for each item
item_themes = (
    spark.table(ITEM_THEMES)
    .where(F.col('theme_rank') == 1)
    .select('pid', 'theme')
)

logger.info(f'Retrieving baskets from {ACTIONS_START} to {ACTIONS_END}')
w_acc = (Window.partitionBy('account_number')
         .orderBy(F.desc(F.col('ordertakendate'))))
baskets_with_themes = (
    spark
    .table(BASKETS)
    .where(F.col('ordertakendate') >= ACTIONS_START)
    .where(F.col('ordertakendate') <= ACTIONS_END)
    .select('account_number', 'itemno', 'ordertakendate')
    .withColumnRenamed('itemno', 'pid')
    .join(item_themes, on='pid', how='inner')
    .withColumn('order_no', F.dense_rank().over(w_acc) - 1)
    .join(item_titles, on='pid', how='left')
    .select('account_number', 'order_no', 'ordertakendate',
            'pid', 'title', 'theme')
    .distinct()
)

baskets_with_themes_export = (
    baskets_with_themes
    .withColumn('EventType', F.lit('order'))
    .withColumn('EventWeight',
                F.when(F.col('order_no') < SCORE_LAST_N_BASKETS,
                       F.lit(1.0)
                       ).otherwise(None))
    .select(F.col('account_number').alias('AccountNumber'),
            F.col('ordertakendate').alias('EventDate'),
            'EventType',
            'EventWeight',
            F.col('pid').alias('PID'),
            F.col('title').alias('ItemTitle'),
            F.col('theme').alias('Theme'))
)
# Remove order date (not required for downstream processing)
baskets_with_themes = baskets_with_themes.drop('ordertakendate')

logger.info(
    f'Loading baskets to scoring events table {THEME_SCORING_EVENTS_LATEST}')
truncate_and_load(
    baskets_with_themes_export,
    THEME_SCORING_EVENTS_LATEST,
    pk_cols=['AccountNumber', 'EventDate', 'EventType', 'PID', 'Theme']
)


if TEST_ACCOUNT:
    logger.info('History with themes for test account:')
    (
        baskets_with_themes
        .where(F.col('account_number') == TEST_ACCOUNT)
        .groupBy('account_number', 'order_no', 'pid', 'title')
        .agg(F.collect_set('theme').alias('themes'))
        .orderBy('order_no')
        .show(SHOW_TOP_N, truncate=False)
    )

if TRAIN:
    baskets_with_themes.cache()

# Self join to get next theme in sequence
w_acc_order_theme = Window.partitionBy('account_number', 'order_no', 'theme')
w_theme = Window.partitionBy('theme')
baskets_with_themes_next = (
    baskets_with_themes
    .select('account_number', 'order_no', 'theme')
    .join(
        (
            baskets_with_themes
            .select('account_number', 'order_no', 'theme')
            .withColumn('order_no', F.col('order_no') + 1)
            .withColumnRenamed('theme', 'next_theme')
        ), on=['account_number', 'order_no'], how='inner'
    )
)

if TRAIN:
    logger.info(f'REFRESH_MODEL_DATE matches today ({TODAY})')
    logger.info('Refreshing theme transition probabilities')
    # Global theme frequencies will become node weights
    # Count should be performed after the self-join (last basket is dropped)
    theme_frequency = (
        baskets_with_themes_next
        .groupBy('theme')
        .agg(F.countDistinct('account_number', 'order_no')
             .alias('theme_total'))
    )

    # Frequency of next themes for baseline probabilities
    basket_count = (
        baskets_with_themes_next
        .select('account_number', 'order_no').distinct().count()
    )
    next_theme_base_probs = (
        baskets_with_themes_next
        .groupBy('next_theme')
        .agg(F.countDistinct('account_number', 'order_no').alias('count'))
        .withColumn('prob_base', F.col('count') / basket_count)
    )

    # Probabilities will become edge weights
    # Fractional counting avoids overcounting when multiple themes in a basket
    transition_probs = (
        baskets_with_themes_next
        .withColumn(
            'fractional_count',
            F.lit(1.0) / F.count('next_theme').over(w_acc_order_theme))
        .groupBy('theme', 'next_theme')
        .agg(F.sum('fractional_count').alias('transition_freq'))
        .join(theme_frequency, on='theme', how='inner')
        .withColumn('probability',
                    F.col('transition_freq') / F.col('theme_total'))
        .join(next_theme_base_probs.select('next_theme', 'prob_base'),
              on='next_theme', how='inner')
        .withColumn('prob_rebased', F.col('probability') - F.col('prob_base'))
        .withColumnRenamed('prob_base', 'base_probability')
        .withColumnRenamed('prob_rebased', 'probability_rebased')
        .withColumn("transition_freq",
                    F.col("transition_freq").cast("decimal(12,2)"))
        .withColumn("theme_total", F.col("theme_total").cast("integer"))
        .withColumn("probability", F.col("probability").cast("decimal(10,9)"))
        .withColumn("base_probability",
                    F.col("base_probability").cast("decimal(10,9)"))
        .withColumn("probability_rebased",
                    F.col("probability_rebased").cast("decimal(10,9)"))
        .select('theme', 'next_theme', 'transition_freq', 'theme_total',
                'probability', 'base_probability', 'probability_rebased')
    )

    # Tolerance to account for floating point precision
    bad_total_probs = (
        transition_probs
        .groupBy('theme')
        .agg(F.sum('probability').alias('total_probability'))
        .where(F.col('total_probability') > 1.00001)
        .where(F.col('total_probability') < 0.99999)
    )
    assert bad_total_probs.isEmpty(), 'Total probabilities found != 1.0'

    logger.info(
        f'Loading theme transition to {THEME_TRANSITIONS_LATEST}')
    truncate_and_load(
        transition_probs,
        THEME_TRANSITIONS_LATEST,
        pk_cols=['theme', 'next_theme']
    )

    logger.info(
        f'Loading theme transition to {THEME_TRANSITIONS}')
    delete_from_and_load(
        transition_probs,
        THEME_TRANSITIONS,
        pk_cols=['theme', 'next_theme'],
        del_where={"rundate": "current_date()"})


# Get recent themes for each account
# TODO: Consider weighting by recency or frequency
account_themes = (
    baskets_with_themes
    .where(F.col('order_no') < SCORE_LAST_N_BASKETS)
    .select('account_number', 'theme')
    .distinct()
)
if TEST_ACCOUNT:
    logger.info('Recent themes for test account:')
    (
        baskets_with_themes
        .where(F.col('account_number') == TEST_ACCOUNT)
        .where(F.col('order_no') < SCORE_LAST_N_BASKETS)
        .select('account_number', 'order_no', 'theme')
        .orderBy('account_number', 'order_no')
        .show(SHOW_TOP_N, truncate=False)
    )

if not TRAIN:
    logger.info(
        f'Reading transition probabilities from {THEME_TRANSITIONS_LATEST}')
    transition_probs = spark.table(THEME_TRANSITIONS_LATEST)

w_next_theme = Window.partitionBy('next_theme')
next_theme_probs = (
    account_themes
    .join(transition_probs.select('theme', 'next_theme', 'probability'),
          on='theme', how='inner')
    .groupBy('account_number', 'next_theme')
    .agg(F.mean('probability').alias('prob_agg'))
    .withColumn('prob_base', F.mean('prob_agg').over(w_next_theme))
    .withColumn('prob_agg_rebased',
                F.col('prob_agg') - F.col('prob_base'))
)
if TEST_ACCOUNT:
    logger.info('Next theme probabilities for test account:')
    (
        next_theme_probs
        .where(F.col('account_number') == TEST_ACCOUNT)
        .orderBy(F.desc('prob_agg_rebased'))
        .show(SHOW_TOP_N, truncate=False)
    )
else:
    next_theme_probs = (
        next_theme_probs
        .withColumnsRenamed(
            {
                'account_number': 'AccountNumber',
                'next_theme': 'NextTheme',
                'prob_agg': 'ProbAgg',
                'prob_base': 'ProbBase',
                'prob_agg_rebased': 'ProbAggRebased'
            }
        )
    )

    logger.info('Loading customer next-theme scores to'
                + f' {NEXT_THEME_SCORES_LATEST}')
    truncate_and_load(
        next_theme_probs,
        NEXT_THEME_SCORES_LATEST,
        pk_cols=['AccountNumber', 'NextTheme']
    )

    logger.info('Loading customer next-theme scores to'
                + f' {NEXT_THEME_SCORES}')
    delete_from_and_load(
        next_theme_probs,
        NEXT_THEME_SCORES,
        pk_cols=['AccountNumber', 'NextTheme'],
        del_where={"rundate": "current_date()"}
    )

if PLOT_GRAPH:
    logger.info('Creating theme transition graph')
    graph = DirectedGraphPlotter(
        df=transition_probs.select(
            F.col('theme').alias('node'),
            F.col('next_theme').alias('next_node'),
            F.col('theme_total').alias('node_weight'),
            F.col('probability').alias('edge_weight')
        ),
        min_edge_weight=jobparser.get_arg('--min-edge-weight') or 0.03,
        min_node_weight=jobparser.get_arg('--min-node-weight') or 1000,
        colorscale='matter'
    )
    graph.create_figure()
    graph_filename = f'scratch_graph_{CLIENT}_{ACTIONS_END}.html'
    logger.info(f'Writing graph to {graph_filename}')
    graph.fig.write_html(graph_filename)

logger.info('Run complete')

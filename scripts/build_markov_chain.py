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
spark.conf.set("spark.sql.shuffle.partitions", "auto")
spark.conf.set("spark.sql.adaptive.enabled", "true")
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

# View history tables (optional — set to None to disable view scoring)
SESSIONS = cfg['tables']['read']['bq_sessions']
SESSIONS_APP = cfg['tables']['read']['bq_sessions_app']
VIEWS = cfg['tables']['read']['bq_views']
VIEWS_APP = cfg['tables']['read']['bq_views_app']
VIEWS_ENABLED = all([SESSIONS, VIEWS])

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


ACTIONS_END = jobparser.get_arg('--actions-end') or (date.today() - timedelta(days=1))
if isinstance(ACTIONS_END, str):
    ACTIONS_END = date.fromisoformat(ACTIONS_END)
ACTIONS_START = ACTIONS_END - timedelta(days=364)

REFRESH_MODEL_DATE = jobparser.get_arg('--refresh_model_date')
TODAY = date.today().strftime(format='%Y-%m-%d')
TRAIN = REFRESH_MODEL_DATE == TODAY or False

TEST_ACCOUNT = jobparser.get_arg('--test-account')
PLOT_GRAPH = jobparser.has_arg('--plot-graph')

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

item_themes = (
    spark.table(ITEM_THEMES)
    .where(F.col('theme_rank') == 1)
    .select('pid', 'theme')
    .distinct()
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
    .cache()
)

baskets_with_themes_export = (
    baskets_with_themes
    .withColumn('EventType', F.lit('order'))
    .withColumn('EventWeight',
                F.when(F.col('order_no') < 10,
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
        .show(100, truncate=False)
    )

if TRAIN:
    baskets_with_themes

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


# Get recent purchase themes for each account (baseline interest)
account_themes = (
    baskets_with_themes
    .where(F.col('order_no') < 10)
    .select('account_number', 'theme')
    .distinct()
)
if TEST_ACCOUNT:
    logger.info('Recent themes for test account:')
    (
        baskets_with_themes
        .where(F.col('account_number') == TEST_ACCOUNT)
        .where(F.col('order_no') < 10)
        .select('account_number', 'order_no', 'theme')
        .orderBy('account_number', 'order_no')
        .show(100, truncate=False)
    )

# --- View history (immediate intent signal) ---
if VIEWS_ENABLED:
    logger.info('Loading view history for scoring')

    rpid_lookup = (
        spark.table(SESSIONS)
        .where(F.col('AccountNumber_RPID').isNotNull())
        .where(F.col('date').between(ACTIONS_START, ACTIONS_END))
        .select('UniqueVisitID',
                F.col('AccountNumber_RPID').alias('account_number'))
    )
    if SESSIONS_APP:
        rpid_lookup = rpid_lookup.unionByName(
            spark.table(SESSIONS_APP)
            .where(F.col('AccountNumber_RPID').isNotNull())
            .where(F.col('date').between(ACTIONS_START, ACTIONS_END))
            .select('UniqueVisitID',
                    F.col('AccountNumber_RPID').alias('account_number'))
        )
    rpid_lookup = rpid_lookup.distinct()

    w_view = (Window.partitionBy('account_number')
              .orderBy(F.desc('date')))

    views_raw = (
        spark.table(VIEWS)
        .where(F.col('date').between(ACTIONS_START, ACTIONS_END))
        .select('UniqueVisitID', 'date',
                F.col('ProductSKU').alias('pid'))
    )
    if VIEWS_APP:
        views_raw = views_raw.unionByName(
            spark.table(VIEWS_APP)
            .where(F.col('date').between(ACTIONS_START, ACTIONS_END))
            .select('UniqueVisitID', 'date',
                    F.col('ProductSKU').alias('pid'))
        )

    account_view_themes = (
        views_raw
        .join(rpid_lookup, on='UniqueVisitID', how='inner')
        .join(F.broadcast(item_themes), on='pid', how='inner')
        .select('account_number', 'theme', 'date')
        .withColumn('rank', F.row_number().over(w_view))
        .where(F.col('rank') <= 1)
        .select('account_number', 'theme')
    )

    if TEST_ACCOUNT:
        logger.info('Recent view themes for test account:')
        (
            account_view_themes
            .where(F.col('account_number') == TEST_ACCOUNT)
            .show()
        )
else:
    account_view_themes = None
    logger.info('View tables not configured — scoring from purchases only')


if not TRAIN:
    logger.info(
        f'Reading transition probabilities from {THEME_TRANSITIONS_LATEST}')
    transition_probs = spark.table(THEME_TRANSITIONS_LATEST)

# --- Blended scoring (purchase baseline + view boost) ---
logger.info('Scoring purchase history against transition matrix')
transition_probs_slim = (
    transition_probs.select('theme', 'next_theme', 'probability')
)

scores_buy = (
    account_themes
    .join(transition_probs_slim, on='theme', how='inner')
    .groupBy('account_number', 'next_theme')
    .agg(F.mean('probability').alias('score_buy'))
)

if VIEWS_ENABLED and account_view_themes is not None:
    logger.info('Scoring view history against transition matrix')
    scores_view = (
        account_view_themes
        .join(transition_probs_slim, on='theme', how='inner')
        .groupBy('account_number', 'next_theme')
        .agg(F.mean('probability').alias('score_view'))
    )

    combined = (
        scores_buy
        .join(scores_view, on=['account_number', 'next_theme'], how='outer')
        .na.fill(0)
        .withColumn(
            'prob_agg',
            F.col('score_buy')
            + (F.col('score_view') * F.lit(0.1))
        )
    )
else:
    combined = scores_buy.withColumnRenamed('score_buy', 'prob_agg')

# Dynamic batch normalisation (rebase against population mean)
w_next_theme = Window.partitionBy('next_theme')
next_theme_probs = (
    combined
    .withColumn('prob_base', F.mean('prob_agg').over(w_next_theme))
    .withColumn('prob_agg_rebased',
                F.col('prob_agg') - F.col('prob_base'))
    .select('account_number', 'next_theme',
            'prob_agg', 'prob_base', 'prob_agg_rebased')
)

# --- Safety net: backfill with global best sellers ---
logger.info(f'Building safety net from top 25 recent themes')
global_top_themes = (
    spark.table(BASKETS)
    .where(F.col('ordertakendate') >= F.date_sub(F.current_date(), 30))
    .withColumnRenamed('itemno', 'pid')
    .join(F.broadcast(item_themes), on='pid', how='inner')
    .groupBy('theme')
    .agg(F.count('*').alias('sales_count'))
    .orderBy(F.desc('sales_count'))
    .limit(25)
    .select(F.col('theme').alias('next_theme'))
    .withColumn('prob_agg', F.lit(0.0))
    .withColumn('prob_base', F.lit(0.0))
    .withColumn('prob_agg_rebased', F.lit(-999.0))
)

unique_users = next_theme_probs.select('account_number').distinct()
backfill_block = unique_users.crossJoin(F.broadcast(global_top_themes))

next_theme_probs = (
    next_theme_probs
    .unionByName(backfill_block)
    .withColumn(
        '_dedup_rank',
        F.row_number().over(
            Window.partitionBy('account_number', 'next_theme')
            .orderBy(F.desc('prob_agg_rebased'))
        )
    )
    .where(F.col('_dedup_rank') == 1)
    .drop('_dedup_rank')
)

# # --- Rank output ---
# next_theme_probs = (
#     next_theme_probs
#     .withColumn(
#         'rank',
#         F.row_number().over(
#             Window.partitionBy('account_number')
#             .orderBy(F.desc('prob_agg_rebased'))
#         )
#     )
#     .where(F.col('rank') <= 100)
#     .drop('rank')
# )

if TEST_ACCOUNT:
    logger.info('Next theme probabilities for test account:')
    (
        next_theme_probs
        .where(F.col('account_number') == TEST_ACCOUNT)
        .orderBy(F.desc('prob_agg_rebased'))
        .show(100, truncate=False)
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
    ).cache()

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

import json
import datetime
from pyspark.sql import functions as F
from pyspark.sql import Window
from dsutils.dbc import configure_spark
from dsutils.logtools import configure_logging, get_logger
from dsutils.argparser import get_job_parser
from dsutils.etl import (map_tbl,
                         truncate_and_load,
                         delete_from_and_load,
                         assert_pk)

jobparser = get_job_parser()
jobparser._parse_args()
JOBNAME = jobparser.get_arg('--jobname')
JOB_ENV = jobparser.get_arg('--job_env')
CLIENT = jobparser.get_arg('--client')
LOG_LEVEL = jobparser.get_arg('--log_level')
configure_logging(log_level=LOG_LEVEL) if LOG_LEVEL else configure_logging()
logger = get_logger(__name__)
spark = configure_spark()
logger.info(f"Running in job environment: {JOB_ENV}")

if not CLIENT:
    assert not JOBNAME, 'Client must be specified when running as a job'
    CLIENT = 'next_uk'  # Client can be specified for interactive debugging
    logger.warning(f'Client not specified (defaulting to {CLIENT})')

logger.info(f"Configuring run for client: {CLIENT}")
with open(f"config/{CLIENT}.json") as f:
    cfg = json.load(f)

tbls = cfg["tables"]["write"]
SCHEMA = cfg["schema"][JOB_ENV]
logger.info(f'Write schema set to {SCHEMA}\n')

tbl_args = {'schema': SCHEMA, 'client': CLIENT}
CONDITIONAL_PROBABILITY_SCORES = map_tbl(
    tbls["conditional_probability_scores"],
    **tbl_args)
CONDITIONAL_PROBABILITY_SCORES_LATEST = map_tbl(
    tbls["conditional_probability_scores_latest"],
    **tbl_args)
ITEM_THEME_MAPPING = map_tbl(tbls["item_themes_latest"], **tbl_args)
CONDITIONAL_PROBABILITY_ITEM_THEMES_LATEST = map_tbl(
    tbls["conditional_probability_item_themes_latest"], **tbl_args)
CONDITIONAL_PROBABILITY_CUSTOMER_ITEM_INTERACTIONS_LATEST = map_tbl(
    tbls["conditional_probability_customer_item_interactions_latest"],
    **tbl_args)
CONDITIONAL_PROBABILITY_CUSTOMER_THEME_INTERACTIONS_LATEST = map_tbl(
    tbls["conditional_probability_customer_theme_interactions_latest"],
    **tbl_args)
CONDITIONAL_PROBABILITY_THEME_ASSOCIATIONS_LATEST = map_tbl(
    tbls["conditional_probability_theme_associations_latest"], **tbl_args)
BASKETS = cfg["tables"]["read"]["baskets"]
BQ_SESSIONS = cfg["tables"]["read"]["bq_sessions_with_accounts"]

REFERENCE_DATE = F.to_date(F.lit(jobparser.get_arg('--reference_date') or
                                 str(datetime.date.today())), "yyyy-MM-dd")
BASKETS_LOOKBACK_DAYS = jobparser.get_arg('--baskets_lookback_days') or 365
VIEWS_LOOKBACK_DAYS = jobparser.get_arg('--views_lookback_days') or 28
PURCHASE_WEIGHT = jobparser.get_arg('--purchase_weight') or 10
VIEW_WEIGHT = jobparser.get_arg('--view_weight') or 1
TIME_DECAY_FACTOR = jobparser.get_arg('--time_decay_factor') or -0.1
THEME_AFFINITY_THRESHOLD = jobparser.get_arg('--affinity_threshold') or 0.0


logger.info("Run parameters:")
logger.info(f"--reference_date: {REFERENCE_DATE}")
logger.info(f"--baskets_lookback_days: {BASKETS_LOOKBACK_DAYS}")
logger.info(f"--views_lookback_days: {VIEWS_LOOKBACK_DAYS}")
logger.info(f"--purchase_weight: {PURCHASE_WEIGHT}")
logger.info(f"--view_weight: {VIEW_WEIGHT}")
logger.info(f"--time_decay_factor: {TIME_DECAY_FACTOR}")
logger.info(f"--affinity_threshold: {THEME_AFFINITY_THRESHOLD}\n")

logger.info("Building TABLE 1: Item-Themes mapping with weights")
window_theme_count = Window.partitionBy('pid')
window_theme_count = Window.partitionBy('pid')
df_item_themes = (
    spark.table(ITEM_THEME_MAPPING)
    .filter(F.col('theme_rank') == 1)
    .withColumn('num_rank1_themes', F.count('theme').over(window_theme_count))
    .select('pid', 'theme', 'num_rank1_themes')
    .withColumn('item_theme_weight', F.lit(1.0) / F.col('num_rank1_themes'))
    .drop('num_rank1_themes')
)
truncate_and_load(
    df_item_themes,
    CONDITIONAL_PROBABILITY_ITEM_THEMES_LATEST
)
distinct_items_with_themes = df_item_themes.select('pid').distinct().count()
logger.info(f"  ↳ TABLE 1: {distinct_items_with_themes:,} distinct items "
            f"with theme_rank=1")

logger.info("Building TABLE 2: Customer interactions from source data")

# get purchase interactions from baskets
df_purchases = (
    spark.table(BASKETS)
    .filter(
        (F.col('order_date') >= F.date_sub(REFERENCE_DATE,
                                           BASKETS_LOOKBACK_DAYS)) &
        (F.col('order_date') <= REFERENCE_DATE)
    )
    .select(
        'account_number',
        F.col('itemno').alias('itemnumber'),
        F.col('order_date').alias('date')
    )
    .distinct()
    .withColumn('interaction_type', F.lit('purchase'))
)

purchase_count = df_purchases.count()
distinct_purchase_accounts = (
    df_purchases.select('account_number').distinct().count()
)
logger.info(f"  ↳ Purchases loaded: {purchase_count:,} interactions")

# get view interactions from BigQuery
df_views = (
    spark.table(BQ_SESSIONS)
    .filter(
        (F.col('date') >= F.date_sub(REFERENCE_DATE, VIEWS_LOOKBACK_DAYS)) &
        (F.col('date') <= REFERENCE_DATE)
    )
    .select(
        'account_number',
        F.col('productSku').alias('itemnumber'),
        'date'
    )
    .distinct()
    .withColumn('interaction_type', F.lit('view'))
)

view_count = df_views.count()
logger.info(f"  ↳ Views loaded: {view_count:,} interactions")


df_interactions_all = df_purchases.unionByName(df_views)

# calculate interaction type + time decay weights
df_interactions_weighted = (
    df_interactions_all
    .withColumn('days_ago', F.datediff(REFERENCE_DATE, F.col('date')))
    .withColumn(
        'inttype_and_time_decay_weight',
        F.when(F.col('interaction_type') == 'purchase', PURCHASE_WEIGHT)
         .otherwise(VIEW_WEIGHT)
        * F.exp(TIME_DECAY_FACTOR * F.col('days_ago'))
    )
)

# keep only last 5 views per customer (filter noise)
window_recent_views = (
    Window.partitionBy('account_number', 'interaction_type')
    .orderBy(F.desc('date'), F.desc('inttype_and_time_decay_weight'))
)

df_interactions = (
    df_interactions_weighted
    .withColumn(
        'view_rank',
        F.when(
            F.col('interaction_type') == 'view',
            F.row_number().over(window_recent_views)
        ).otherwise(F.lit(1))
    )
    # keep all purchases, only last 5 views per customer
    .filter(
        (F.col('interaction_type') != 'view') |
        (F.col('view_rank') <= 5)
    )
    .drop('view_rank')
    .select(
        'account_number',
        'itemnumber',
        'date',
        'interaction_type',
        'days_ago',
        'inttype_and_time_decay_weight'
    )
)

truncate_and_load(
    df_interactions,
    CONDITIONAL_PROBABILITY_CUSTOMER_ITEM_INTERACTIONS_LATEST
)
logger.info(f"  ↳ TABLE 2 created: {df_interactions.count():,} interactions "
            f"({purchase_count:,} purchases + filtered views)")

logger.info("Building TABLE 3: Customer-Theme interactions")
df_customer_theme_interactions = (
    df_interactions
    .join(
        df_item_themes,
        df_interactions.itemnumber == df_item_themes.pid,
        'inner'
    )
    .withColumn(
        'final_weight',
        F.col('inttype_and_time_decay_weight') * F.col('item_theme_weight')
    )
    .groupBy('account_number', 'date', 'theme', 'interaction_type')
    .agg(
        F.sum('final_weight').alias('interaction_weight'),
        F.collect_set('itemnumber').alias('array_agg_itemnumber'),
        F.first('item_theme_weight').alias('item_theme_weight'),
        F.first('inttype_and_time_decay_weight'
                ).alias('inttype_and_time_decay_weight')
    )
    .withColumn('item_count', F.size('array_agg_itemnumber'))
)

truncate_and_load(
    df_customer_theme_interactions,
    CONDITIONAL_PROBABILITY_CUSTOMER_THEME_INTERACTIONS_LATEST
)
logger.info(f"  ↳ TABLE 3 created: {df_customer_theme_interactions.count():,}"
            f" interactions")


logger.info("Building TABLE 4: Theme associations")

# get baskets from last year
df_baskets = (
    spark.table(BASKETS)
    .filter(F.col('order_date') >= F.date_sub(REFERENCE_DATE, 365))
    .select('account_number', 'itemno', 'order_date')
)

# join baskets with themes
df_baskets_themes = (
    df_baskets
    .join(
        df_item_themes,
        df_baskets.itemno == df_item_themes.pid,
        'inner'
    )
    .select(
        'account_number',
        'theme',
        'order_date',
        'item_theme_weight'
    )
)

# calculate total weights
df_bought_count = (
    df_baskets_themes
    .agg(F.sum('item_theme_weight').alias('total_weight'))
)

# calculate theme-level weights
df_single_bought_count = (
    df_baskets_themes
    .groupBy('theme')
    .agg(F.sum('item_theme_weight').alias('theme_total_weight'))
)

# get first purchase date per customer-theme
df_customer_theme_first = (
    df_baskets_themes
    .groupBy('account_number', 'theme')
    .agg(
        F.min('order_date').alias('first_purchase_date'),
        F.sum('item_theme_weight').alias('customer_theme_purchased_weight')
    )
)

# self-join to find sequential theme purchases (theme1 THEN theme2)
df_pairs = (
    df_customer_theme_first.alias('t1')
    .join(
        df_customer_theme_first.alias('t2'),
        (F.col('t1.account_number') == F.col('t2.account_number')) &
        (F.col('t2.first_purchase_date') > F.col('t1.first_purchase_date')) &
        (F.col('t1.theme') != F.col('t2.theme')),
        'inner'
    )
    .select(
        F.col('t1.theme').alias('theme1'),
        F.col('t2.theme').alias('theme2'),
        F.least(
            F.col('t1.customer_theme_purchased_weight'),
            F.col('t2.customer_theme_purchased_weight')
        ).alias('sequential_pair_weight')
    )
)

# aggregate sequential pairs
df_pair_counts = (
    df_pairs
    .groupBy('theme1', 'theme2')
    .agg(F.sum('sequential_pair_weight').alias('freq12'))
)

# calculate base statistics
df_total_weight = df_bought_count.collect()[0]['total_weight']

df_stats_base = (
    df_pair_counts
    .join(
        df_single_bought_count
        .withColumnRenamed('theme', 'theme1')
        .withColumnRenamed('theme_total_weight', 'freq1'),
        'theme1',
        'left'
    )
    .join(
        df_single_bought_count
        .withColumnRenamed('theme', 'theme2')
        .withColumnRenamed('theme_total_weight', 'freq2'),
        'theme2',
        'left'
    )
    .withColumn('total_weight', F.lit(df_total_weight))
    .withColumn('support1', F.col('freq1') / F.col('total_weight'))
    .withColumn('support2', F.col('freq2') / F.col('total_weight'))
    .withColumn('support12', F.col('freq12') / F.col('total_weight'))
    .withColumn(
        'lift',
        F.col('support12') / (F.col('support1') * F.col('support2'))
    )
    .withColumn(
        'lift_adjusted',
        F.col('lift') * F.pow(F.col('support2'), 0.1)
    )
    .withColumn(
        'cosine_similarity',
        F.col('freq12') / (F.sqrt(F.col('freq1')) * F.sqrt(F.col('freq2')))
    )
    .withColumn(
        'confidence_theme2_given_theme1',
        F.col('support12') / F.col('support1')
    )
    .withColumn(
        'prob_theme1_precedes_theme2',
        F.col('freq12') / F.col('freq2')
    )
)

# apply business filters and rank
window_rank = Window.partitionBy('theme1').orderBy(F.desc('freq12'))

df_filtered_ranked = (
    df_stats_base
    .filter(
        (F.col('freq12') > 50) |  # lower volume threshold
        ((F.col('freq12') > 5) & (F.col('lift') > 1.2)) |  # more lenient lift
        ((F.col('freq12') > 1) &
         (F.col('confidence_theme2_given_theme1') > 0.03) &
         (F.col('lift') > 1.5))  # ultra-niche
    )
    .withColumn('rank_by_freq', F.row_number().over(window_rank))
    .filter(F.col('rank_by_freq') <= 50)
)

# calculate percentages AFTER filtering (FIXED)
window_theme2 = Window.partitionBy('theme2')

df_theme_associations = (
    df_filtered_ranked
    .withColumn(
        'pct_of_theme2_sequences',
        F.col('freq12') / F.sum('freq12').over(window_theme2)
    )
    .select(
        'theme1',
        'theme2',
        F.round('freq12', 2).alias('freq12'),
        F.round('freq1', 2).alias('theme1_total_weight'),
        F.round('freq2', 2).alias('theme2_total_weight'),
        F.round('lift', 3).alias('lift'),
        F.round('lift_adjusted', 4).alias('lift_adjusted'),
        F.round('cosine_similarity', 3).alias('cosine_similarity'),
        F.round('pct_of_theme2_sequences', 3).alias('pct_of_theme2_sequences'),
        F.round('prob_theme1_precedes_theme2', 3
                ).alias('prob_theme1_precedes_theme2'),
        F.round('support1', 7).alias('support_theme1'),
        F.round('support2', 7).alias('support_theme2'),
        F.round('support12', 7).alias('support_sequence'),
        F.round('confidence_theme2_given_theme1', 3
                ).alias('confidence_theme2_given_theme1')
    )
)

truncate_and_load(
    df_theme_associations,
    CONDITIONAL_PROBABILITY_THEME_ASSOCIATIONS_LATEST
)
logger.info(f"  ↳ TABLE 4 created: {df_theme_associations.count()}"
            f" theme associations")

logger.info("Building TABLE 5: Customer recommendations")

# aggregate customer theme affinities from TABLE 3
# Collapse date/interaction_type granularity to get total affinity per theme
df_customer_theme_affinity_unfiltered = (
    df_customer_theme_interactions
    .groupBy('account_number', 'theme')
    .agg(
        F.sum('interaction_weight').alias('theme_affinity_score'),
        F.max('date').alias('most_recent_interaction'),
        F.countDistinct('date').alias('interaction_days'),
        F.array_distinct(
            F.flatten(F.collect_list('array_agg_itemnumber'))
        ).alias('all_seed_items')
    )
    .filter(F.col('theme_affinity_score') >= THEME_AFFINITY_THRESHOLD)
)

# generate recommendations by joining affinities with associations
df_item_weights = (
    df_customer_theme_interactions
    .select(
        'account_number',
        'theme',
        F.explode('array_agg_itemnumber').alias('itemnumber'),
        'interaction_weight',
        'date',
        'interaction_type'
    )
    .groupBy('account_number', 'theme', 'itemnumber')
    .agg(
        F.sum('interaction_weight').alias('seed_item_weight')
    )
)

df_raw_recommendations_base = (
    df_customer_theme_affinity_unfiltered
    .join(
        df_theme_associations,
        df_customer_theme_affinity_unfiltered.theme ==
        df_theme_associations.theme1,
        'inner'
    )
    .withColumnRenamed('theme', 'seed_theme')
    .withColumnRenamed('theme2', 'recommended_theme')
    .withColumn(
        'score_freq_based',
        F.col('theme_affinity_score') * F.col('freq12')
    )
    .withColumn(
        'score_lift_based',
        F.col('theme_affinity_score') * F.col('lift_adjusted')
    )
    .withColumn(
        'score_confidence_based',
        F.col('theme_affinity_score') *
        F.col('confidence_theme2_given_theme1')
    )
    .withColumn(
        'score_hybrid',
        F.col('theme_affinity_score') *
        F.col('freq12') *
        F.col('lift_adjusted')
    )
    .select(
        'account_number',
        'seed_theme',
        'recommended_theme',
        'theme_affinity_score',
        'score_freq_based',
        'score_lift_based',
        'score_confidence_based',
        'score_hybrid',
        'freq12',
        'lift_adjusted',
        'cosine_similarity',
        'confidence_theme2_given_theme1',
        'most_recent_interaction',
        'all_seed_items'
    )
)

df_raw_recommendations = (
    df_raw_recommendations_base.alias('base')
    .join(
        df_item_weights.alias('items'),
        (F.col('base.account_number') == F.col('items.account_number')) &
        (F.col('base.seed_theme') == F.col('items.theme')),
        'left'
    )
    .withColumn(
        'item_with_weight',
        F.struct(
            F.col('items.itemnumber').alias('itemno'),
            F.round(F.col('items.seed_item_weight'), 2).alias('weight')
        )
    )
    .select(
        F.col('base.account_number').alias('account_number'),
        F.col('base.seed_theme').alias('seed_theme'),
        F.col('base.recommended_theme').alias('recommended_theme'),
        F.col('base.theme_affinity_score').alias('theme_affinity_score'),
        F.col('base.score_freq_based').alias('score_freq_based'),
        F.col('base.score_lift_based').alias('score_lift_based'),
        F.col('base.score_confidence_based').alias('score_confidence_based'),
        F.col('base.score_hybrid').alias('score_hybrid'),
        F.col('base.freq12').alias('freq12'),
        F.col('base.lift_adjusted').alias('lift_adjusted'),
        F.col('base.cosine_similarity').alias('cosine_similarity'),
        F.col('base.confidence_theme2_given_theme1'
              ).alias('confidence_theme2_given_theme1'),
        F.col('base.most_recent_interaction').alias('most_recent_interaction'),
        'item_with_weight'
    )
    .groupBy(
        'account_number',
        'seed_theme',
        'recommended_theme'
    )
    .agg(
        F.first('theme_affinity_score').alias('theme_affinity_score'),
        F.first('score_freq_based').alias('score_freq_based'),
        F.first('score_lift_based').alias('score_lift_based'),
        F.first('score_confidence_based').alias('score_confidence_based'),
        F.first('score_hybrid').alias('score_hybrid'),
        F.first('freq12').alias('freq12'),
        F.first('lift_adjusted').alias('lift_adjusted'),
        F.first('cosine_similarity').alias('cosine_similarity'),
        F.first('confidence_theme2_given_theme1'
                ).alias('confidence_theme2_given_theme1'),
        F.first('most_recent_interaction').alias('most_recent_interaction'),
        F.collect_set('item_with_weight').alias('seed_items_with_weights')
    )
)

logger.info(f"  ↳ Raw recommendations: {df_raw_recommendations.count()}"
            f" candidate pairs")

# agg scores when multiple seed themes point to same recommendation
df_aggregated_recommendations_base = (
    df_raw_recommendations
    .groupBy('account_number', 'recommended_theme')
    .agg(
        F.sum('score_freq_based').alias('total_score_freq'),
        F.sum('score_lift_based').alias('total_score_lift'),
        F.sum('score_confidence_based').alias('total_score_confidence'),
        F.sum('score_hybrid').alias('total_score_hybrid'),
        F.collect_set(
            F.struct(
                F.col('seed_theme').alias('theme'),
                F.round('theme_affinity_score', 2).alias('weight')
            )
        ).alias('contributing_seed_themes'),
        F.sum('theme_affinity_score').alias('total_seed_affinity'),
        F.avg('freq12').alias('avg_association_strength'),
        F.max('cosine_similarity').alias('max_cosine'),
        F.max('lift_adjusted').alias('max_lift'),
        F.max('most_recent_interaction').alias('latest_seed_interaction'),
        # Collect all item-weight structs from contributing seed themes
        F.flatten(F.collect_list('seed_items_with_weights')
                  ).alias('all_items_raw')
    )
)

# Explode items to deduplicate and sum weights per item
df_items_exploded = (
    df_aggregated_recommendations_base
    .filter(F.size('all_items_raw') > 0)  # Filter out empty arrays
    .select(
        'account_number',
        'recommended_theme',
        F.explode('all_items_raw').alias('item_struct')
    )
    .select(
        'account_number',
        'recommended_theme',
        F.col('item_struct.itemno').alias('itemno'),
        F.col('item_struct.weight').alias('weight')
    )
    .groupBy('account_number', 'recommended_theme', 'itemno')
    .agg(
        F.sum('weight').alias('total_item_weight')
    )
    .groupBy('account_number', 'recommended_theme')
    .agg(
        F.collect_list(
            F.struct(
                F.col('itemno'),
                F.round('total_item_weight', 2).alias('weight')
            )
        ).alias('contributing_seed_items')
    )
)

# Join deduplicated items back to main aggregation
df_aggregated_recommendations = (
    df_aggregated_recommendations_base
    .drop('all_items_raw')
    .join(
        df_items_exploded,
        ['account_number', 'recommended_theme'],
        'left'
    )
    .withColumn(
        'contributing_seed_items',
        F.coalesce(F.col('contributing_seed_items'), F.array())
    )
    .withColumn('num_seed_themes', F.size('contributing_seed_themes'))
    .withColumn('num_contributing_items', F.size('contributing_seed_items'))
)

logger.info(f"  ↳ Aggregated recommendations: "
            f"{df_aggregated_recommendations.count()} unique"
            f" customer-theme pairs")

df_customer_recommendations = (
    df_aggregated_recommendations
    .select(
        'account_number',
        'recommended_theme',
        F.round('total_score_freq', 2).alias('score_freq'),
        F.round('total_score_lift', 2).alias('score_lift'),
        F.round('total_score_confidence', 2).alias('score_confidence'),
        F.round('total_score_hybrid', 2).alias('score_hybrid'),
        'contributing_seed_themes',
        'num_seed_themes',
        F.round('total_seed_affinity', 2).alias('total_seed_affinity'),
        F.round('avg_association_strength', 2).alias('avg_freq12'),
        F.round('max_cosine', 3).alias('max_cosine_similarity'),
        F.round('max_lift', 3).alias('max_lift_adjusted'),
        'latest_seed_interaction',
        'num_contributing_items',
        'contributing_seed_items'
    )
)


df_all_themes = (
    df_item_themes
    .select(F.col('theme').alias('recommended_theme'))
    .distinct()
)

df_all_customers = (
    df_customer_recommendations
    .select('account_number')
    .distinct()
)

df_full_matrix = (
    df_all_customers
    .crossJoin(df_all_themes)
)

# Left join to backfill missing combinations with 0 scores
df_customer_recommendations_backfilled = (
    df_full_matrix
    .join(
        df_customer_recommendations,
        ['account_number', 'recommended_theme'],
        'left'
    )
    .fillna(
        {
            'score_freq': 0.0,
            'score_lift': 0.0,
            'score_confidence': 0.0,
            'score_hybrid': 0.0,
            'total_seed_affinity': 0.0,
            'avg_freq12': 0.0,
            'max_cosine_similarity': 0.0,
            'max_lift_adjusted': 0.0,
            'num_seed_themes': 0,
            'num_contributing_items': 0
        }
    )
    .withColumn('contributing_seed_themes',
                F.coalesce(F.col('contributing_seed_themes'), F.array()))
    .withColumn('contributing_seed_items',
                F.coalesce(F.col('contributing_seed_items'), F.array()))
    .dropDuplicates(['account_number', 'recommended_theme'])
)

backfill_count = df_customer_recommendations_backfilled.count()
logger.info(f"  ↳ Backfilled: {backfill_count} customer-theme combinations")

logger.info("Loading output to table")
delete_from_and_load(df_customer_recommendations_backfilled,
                     CONDITIONAL_PROBABILITY_SCORES,
                     del_where={"rundate": "current_date()"})

logger.info("Loading output to table (latest)")
truncate_and_load(df_customer_recommendations_backfilled,
                  CONDITIONAL_PROBABILITY_SCORES_LATEST)

logger.info("Run complete")

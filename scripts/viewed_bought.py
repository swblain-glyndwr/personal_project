import sys
from pathlib import Path
try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() # type: ignore # noqa
    if not notebook_path.startswith('/Workspace'):
        notebook_path = '/Workspace' + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import functions as F
from pyspark.sql import Window

from dsutils.dbc import configure_spark
from dsutils.argparser import get_job_parser
from dsutils.logtools import configure_logging, get_logger
from dsutils.etl import truncate_and_load
from next_ads.utils import config_manager
from next_ads.common.paths import load_client_config
from next_ads.utils import etl

import math


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

# load configuration
config = config_manager.load_config(JOB_ENV)
logger.info(f"Configuring run for client: {CLIENT}")
cfg = load_client_config(CLIENT)

PRODUCT_CATALOG = cfg['tables']['read']['product_catalog']
BASKETS = cfg['tables']['read']['baskets']
BQ_SESSIONS = cfg['tables']['read']['bq_sessions']
BQ_SESSIONS_APP = cfg["tables"]["read"]['bq_sessions_app']
BQ_VIEWS = cfg['tables']['read']['bq_views']
BQ_VIEWS_APP = cfg['tables']['read']['bq_views_app']

tbls = cfg["tables"]["write"]
SCHEMA = config.schema_write
logger.info(f'Write schema set to {SCHEMA}')

# Map write schema to parameterised write table names
tbl_args = {'catalog': config.catalog_write, 'schema': SCHEMA, 'client': CLIENT}
VB_TABLE_LATEST = etl.map_tbl(tbls["viewed_bought_latest"], **tbl_args)

vb_config = cfg["real_time_unknown"]["viewed_bought"]

TIME_WINDOW_DAYS = vb_config["time_window_days"]
REVENUE_PERCENTILE = vb_config["revenue_percentile"]
MIN_CO_OCCURRENCES = vb_config["min_co_occurrences"]
TOP_N_PER_ITEM = vb_config["top_n_per_item"]
MIN_CONVERSION_RATE = vb_config["min_conversion_rate"]
PURCHASE_WINDOW_DAYS = vb_config["purchase_window_days"]

DECAY_HALF_LIFE_RATIO = vb_config["decay_half_life_ratio"]
HALF_LIFE_DAYS = TIME_WINDOW_DAYS / DECAY_HALF_LIFE_RATIO
DECAY_LAMBDA = math.log(2) / HALF_LIFE_DAYS

logger.info("Configuration:")
logger.info(f"  Time window: {TIME_WINDOW_DAYS} days")
logger.info(f"  Decay half-life: {HALF_LIFE_DAYS:.1f} days")
logger.info(f"  Decay constant (λ): {DECAY_LAMBDA:.4f}")
logger.info(f"  Weight at end of window:"
            f" {math.exp(-DECAY_LAMBDA * TIME_WINDOW_DAYS):.1%}")

items_recent = (
    spark.table(BASKETS)
    .filter(
        (F.col('clientid').rlike('^N')) &
        (F.col('order_date') > F.date_sub(F.current_date(), TIME_WINDOW_DAYS))
    )
)

baskets_grouped = (
    items_recent
    .groupBy('itemno')
    .agg(F.sum('s740orderstakenvalue').alias('total_spend'))
    .orderBy(F.col('total_spend').desc())
)

window_spec = Window.orderBy(F.col('total_spend').desc()
                             ).rowsBetween(Window.unboundedPreceding, 0)
total_sum = baskets_grouped.agg(F.sum('total_spend')).first()[0]

baskets_cum_pct = (
    baskets_grouped
    .withColumn('cum_spend', F.sum('total_spend').over(window_spec))
    .withColumn('cum_pct_spend', F.col('cum_spend') / F.lit(total_sum))
)

items_filtered = (
    baskets_cum_pct
    .filter(F.col('cum_pct_spend') <= REVENUE_PERCENTILE)
    .select('itemno')
)

logger.info(f"Items in analysis: {items_filtered.count()}")

rpid_lookup = (
    spark.table(BQ_SESSIONS)
    .select('UniqueVisitID', 'AccountNumber_RPID', 'date')
    .unionByName(
        spark.table(BQ_SESSIONS_APP)
        .select('UniqueVisitID', 'AccountNumber_RPID', 'date'),
        allowMissingColumns=True
    )
    .filter(
        (F.col('date') >= F.current_date() - TIME_WINDOW_DAYS) &
        (F.col('AccountNumber_RPID').isNotNull())
    )
    .select('UniqueVisitID',
            F.col('AccountNumber_RPID').alias('account_number'))
    .distinct()
)

views_web_raw = (
    spark.table(BQ_VIEWS)
    .filter(
        (F.col("date") >= F.current_date() - TIME_WINDOW_DAYS) &
        (F.col("EventType").rlike("pdp_view")) &
        (F.col("ViewTimespentSecs") > 0)
    )
    .select(
        F.col("uniquevisitid"),
        F.col("date"),
        F.col("timestamp"),
        F.col("ProductSKU").alias("itemno"),
        F.col("Category")
    )
)

views_app_raw = (
    spark.table(BQ_VIEWS_APP)
    .filter(
        (F.col("date") >= F.current_date() - TIME_WINDOW_DAYS) &
        (F.col("ScreenName") == "PDP") &
        (F.col("ViewTimespentSecs") > 0)
    )
    .select(
        F.col("uniquevisitid"),
        F.col("date"),
        F.col("timestamp"),
        F.col("productsku").alias("itemno"),
        F.col("Category")
    )
)

views = (
    views_web_raw.unionByName(views_app_raw)
    .groupBy("uniquevisitid", "itemno", "Category")
    .agg(
        F.min("timestamp").alias("timestamp"),
        F.min("date").alias("date")
    )
    .join(items_filtered, 'itemno', 'inner')
    .join(rpid_lookup, on='uniquevisitid', how='inner')
    .select('account_number', 'itemno', 'Category', 'date', 'timestamp')
)

logger.info(f"Total views: {views.count()}")

purchases = (
    spark.table(BASKETS)
    .filter(
        (F.col('clientid').rlike('^N')) &
        (F.col('order_date') > F.date_sub(F.current_date(), TIME_WINDOW_DAYS))
    )
    .select(
        'account_number',
        'itemno',
        F.col('order_date').alias('date'),
        F.col('orderdate').alias('timestamp')
    )
    .join(items_filtered, 'itemno', 'inner')
    .select('account_number', 'itemno', 'date', 'timestamp')
    .distinct()
)

pairs_raw = (
    views.alias("t1")
    .join(
        purchases.alias("t2"),
        on=['account_number'],
        how="inner"
    )
    .filter(
        (F.col("t2.date") >= F.col("t1.date")) &
        (F.col("t2.date") <= F.date_add(F.col("t1.date"),
                                        PURCHASE_WINDOW_DAYS)) &
        (F.col("t1.itemno") != F.col("t2.itemno"))
    )
    .select(
        F.col("account_number"),
        F.col("t1.date").alias("view_date"),
        F.col("t2.date").alias("purchase_date"),
        F.col("t1.itemno").alias("itemno1"),
        F.col("t2.itemno").alias("itemno2"),
        F.col("t1.Category").alias("category1")
    )
    .distinct()
).cache()

total_customers = pairs_raw.select("account_number").distinct().count()

views_count = (
    pairs_raw
    .groupBy("itemno1")
    .agg(F.countDistinct("account_number").alias("views"))
)

purchases_count = (
    pairs_raw
    .groupBy("itemno2")
    .agg(F.countDistinct("account_number").alias("purchases"))
)

pair_count = (
    pairs_raw
    .groupBy("itemno1", "itemno2")
    .agg(
        F.countDistinct("account_number").alias("freq12"),
        F.sum(F.exp(-DECAY_LAMBDA * F.datediff(F.current_date(),
                                               F.col("view_date")))
              ).alias("freq12_decay")
    )
)

stats_raw = (
    pair_count.alias("t0")
    .join(views_count.alias("t1"),
          F.col("t0.itemno1") == F.col("t1.itemno1"), "left")
    .join(purchases_count.alias("t2"),
          F.col("t0.itemno2") == F.col("t2.itemno2"), "left")
    .select(
        F.col("t0.itemno1"),
        F.col("t0.itemno2"),
        F.col("t0.freq12"),
        F.col("t0.freq12_decay"),
        F.col("t1.views").alias("freq1"),
        F.col("t2.purchases").alias("freq2")
    )
)

stats_final = (
    stats_raw
    .withColumn("all_customers", F.lit(total_customers))
    .withColumn("support1", F.col("freq1") / F.col("all_customers"))
    .withColumn("support2", F.col("freq2") / F.col("all_customers"))
    .withColumn("support12", F.col("freq12") / F.col("all_customers"))
    .withColumn("lift",
                F.col("support12") / (F.col("support1") * F.col("support2")))
    .withColumn("lift_adjusted",
                F.col("lift") * F.pow(F.col("support2"), 0.25))
    .withColumn("cosine_similarity",
                F.col("freq12") / (F.sqrt(F.col("freq1")
                                          ) * F.sqrt(F.col("freq2"))))
    .withColumn("conversion_rate", F.col("freq12") / F.col("freq1"))
)

window_spec = Window.partitionBy("itemno1"
                                 ).orderBy(F.col("lift_adjusted").desc())

results = (
    stats_final
    .filter(
        (F.col("freq12") >= MIN_CO_OCCURRENCES) &
        (F.col("conversion_rate") >= MIN_CONVERSION_RATE)
    )
    .withColumn("rank", F.row_number().over(window_spec))
    .filter(F.col("rank") <= TOP_N_PER_ITEM)
    .select(
        "itemno1",
        "itemno2",
        "freq12",
        "freq1",
        "freq2",
        "all_customers",
        F.round("support12", 8).alias("support12"),
        F.round("support1", 8).alias("support1"),
        F.round("support2", 8).alias("support2"),
        F.round("lift", 3).alias("lift"),
        F.round("lift_adjusted", 4).alias("lift_adjusted"),
        F.round("cosine_similarity", 3).alias("cosine_similarity"),
        F.round("conversion_rate", 5).alias("conversion_rate"),
        "rank"
    )
    .orderBy(F.col("freq1").desc(), F.col("lift_adjusted").desc())
).cache()

pk_cols = ["itemno1", "itemno2"]
results_dedup = results.dropDuplicates(pk_cols)
duplicate_rows = results.count() - results_dedup.count()
if duplicate_rows > 0:
    logger.warning(
        f"Found {duplicate_rows} duplicate rows for PK {pk_cols}; "
        "dropping duplicates before write"
    )

truncate_and_load(
    df=results_dedup,
    table=VB_TABLE_LATEST,
    pk_cols=pk_cols,
)

logger.info("Run Complete.")

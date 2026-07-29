# IMPORTANT: This script is not designed to be run with Spark Connect due to
# long-running processes that can cause gRPC timeouts. It is recommended to run
# this script directly on a Databricks cluster.

import sys
import uuid
from pathlib import Path

try:
    PROJECT_ROOT = Path(__file__).resolve().parents[2]
except NameError:
    # __file__ is not defined when running as a Databricks notebook
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parents[2]
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    SRC_ROOT = PROJECT_ROOT / "src"
    sys.path.insert(0, str(SRC_ROOT))
    sys.path.insert(1, str(PROJECT_ROOT))

from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import date, timedelta

from dsutils.dbc import configure_spark
from dsutils.argparser import get_job_parser
from dsutils.logtools import configure_logging, get_logger
from next_ads.common import config_manager
from next_ads.common.delta_writes import quote_qualified_identifier
from next_ads.common.paths import load_client_config
from next_ads.common.snapshot_writes import (
    capture_run_date,
    publish_history_and_latest,
    replace_validated_snapshot,
    with_run_date,
)
from next_ads.common import etl
from next_ads.ranking.theme_score_generation import (
    merge_and_rank_theme_scores,
    select_global_top_themes,
    select_latest_view_themes,
)

from next_ads.reporting.plotting import DirectedGraphPlotter


def main(
    JOB_ENV,
    CLIENT,
    LOG_LEVEL,
    ACTIONS_END=None,
    REFRESH_MODEL_DATE=None,
    TEST_ACCOUNT=None,
    PLOT_GRAPH=False,
    MIN_EDGE_WEIGHT=0.03,
    MIN_NODE_WEIGHT=1000,
):
    configure_logging(
        log_level=LOG_LEVEL
    ) if LOG_LEVEL else configure_logging()
    logger = get_logger(__name__)
    spark = configure_spark()
    spark.conf.set("spark.sql.shuffle.partitions", "auto")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    run_date = capture_run_date(spark)
    logger.info(f"Running in job environment: {JOB_ENV}")

    if not CLIENT:
        assert JOB_ENV.lower() == "dev", (
            f"Client must be specified when running in {JOB_ENV}"
        )
        CLIENT = "next_uk"  # Client can be specified for interactive debugging
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    # load configuration
    config = config_manager.load_config(JOB_ENV, client=CLIENT)
    logger.info(f"Configuring run for client: {CLIENT}")
    cfg = load_client_config(CLIENT)

    PRODUCT_CATALOG = cfg["tables"]["read"]["product_catalog"]
    BASKETS = cfg["tables"]["read"]["baskets"]

    # View history tables (optional — set to None to disable view scoring)
    SESSIONS = cfg["tables"]["read"]["bq_sessions"]
    SESSIONS_APP = cfg["tables"]["read"]["bq_sessions_app"]
    VIEWS = cfg["tables"]["read"]["bq_views"]
    VIEWS_APP = cfg["tables"]["read"]["bq_views_app"]
    VIEWS_ENABLED = all([SESSIONS, VIEWS])

    tbls = cfg["tables"]["write"]
    SCHEMA = config.schema_write
    logger.info(f"Write schema set to {SCHEMA}")

    # Map write schema to parameterised write table names

    tbl_args = {
        "catalog": config.catalog_write,
        "schema": SCHEMA,
        "client": CLIENT,
    }
    ITEM_THEMES = etl.map_tbl(tbls["item_themes_latest"], **tbl_args)
    THEME_TRANSITIONS_LATEST = etl.map_tbl(
        tbls["theme_transitions_latest"], **tbl_args
    )  # noqa
    THEME_TRANSITIONS = etl.map_tbl(tbls["theme_transitions"], **tbl_args)
    NEXT_THEME_SCORES_LATEST = etl.map_tbl(
        tbls["next_theme_scores_latest"], **tbl_args
    )  # noqa
    NEXT_THEME_SCORES = etl.map_tbl(tbls["next_theme_scores"], **tbl_args)
    THEME_SCORING_EVENTS_LATEST = etl.map_tbl(
        tbls["theme_scoring_events_latest"], **tbl_args
    )  # noqa

    ACTIONS_END = ACTIONS_END or (run_date - timedelta(days=1))

    if isinstance(ACTIONS_END, str):
        ACTIONS_END = date.fromisoformat(ACTIONS_END)
    ACTIONS_START = ACTIONS_END - timedelta(days=364)

    TODAY = run_date.isoformat()
    TRAIN = REFRESH_MODEL_DATE == TODAY or False

    w_item_by_modified = Window.partitionBy("pid").orderBy(
        F.desc("date_modified"), "title"
    )
    item_titles = (
        spark.table(PRODUCT_CATALOG)
        .select("pid", "title", "date_modified")
        .withColumn("modified_rank", F.row_number().over(w_item_by_modified))
        .where(F.col("modified_rank") == 1)
        .select("pid", "title")
    )
    item_themes = (
        spark.table(ITEM_THEMES)
        .where(F.col("theme_rank") == 1)
        .select("pid", "theme")
        .distinct()
    )

    logger.info(f"Retrieving baskets from {ACTIONS_START} to {ACTIONS_END}")
    w_acc = Window.partitionBy("account_number").orderBy(
        F.desc(F.col("ordertakendate"))
    )
    baskets_with_themes = (
        spark.table(BASKETS)
        .where(F.col("ordertakendate") >= ACTIONS_START)
        .where(F.col("ordertakendate") <= ACTIONS_END)
        .select("account_number", "itemno", "ordertakendate")
        .withColumnRenamed("itemno", "pid")
        .join(item_themes, on="pid", how="inner")
        .withColumn("order_no", F.dense_rank().over(w_acc) - 1)
        .join(item_titles, on="pid", how="left")
        .select(
            "account_number",
            "order_no",
            "ordertakendate",
            "pid",
            "title",
            "theme",
        )
        .distinct()
        .cache()
    )

    baskets_with_themes_export = (
        baskets_with_themes.withColumn("EventType", F.lit("order"))
        .withColumn(
            "EventWeight",
            F.when(F.col("order_no") < 10, F.lit(1.0)).otherwise(None),
        )
        .select(
            F.col("account_number").alias("AccountNumber"),
            F.col("ordertakendate").alias("EventDate"),
            "EventType",
            "EventWeight",
            F.col("pid").alias("PID"),
            F.col("title").alias("ItemTitle"),
            F.col("theme").alias("Theme"),
        )
    )
    # Remove order date (not required for downstream processing)
    baskets_with_themes = baskets_with_themes.drop("ordertakendate")

    logger.info(
        f"Loading baskets to scoring events table {THEME_SCORING_EVENTS_LATEST}"
    )
    replace_validated_snapshot(
        spark,
        with_run_date(baskets_with_themes_export, run_date),
        table=THEME_SCORING_EVENTS_LATEST,
        key_columns=[
            "AccountNumber",
            "EventDate",
            "EventType",
            "PID",
            "Theme",
        ],
    )

    if TEST_ACCOUNT:
        logger.info("History with themes for test account:")
        (
            baskets_with_themes.where(F.col("account_number") == TEST_ACCOUNT)
            .groupBy("account_number", "order_no", "pid", "title")
            .agg(F.collect_set("theme").alias("themes"))
            .orderBy("order_no")
            .show(100, truncate=False)
        )

    if TRAIN:
        baskets_with_themes

    # Self join to get next theme in sequence
    w_acc_order_theme = Window.partitionBy(
        "account_number", "order_no", "theme"
    )
    baskets_with_themes_next = baskets_with_themes.select(
        "account_number", "order_no", "theme"
    ).join(
        (
            baskets_with_themes.select("account_number", "order_no", "theme")
            .withColumn("order_no", F.col("order_no") + 1)
            .withColumnRenamed("theme", "next_theme")
        ),
        on=["account_number", "order_no"],
        how="inner",
    )

    if TRAIN:
        logger.info(f"REFRESH_MODEL_DATE matches today ({TODAY})")
        logger.info("Refreshing theme transition probabilities")
        # Global theme frequencies will become node weights
        # Count should be performed after the self-join (last basket is dropped)
        theme_frequency = baskets_with_themes_next.groupBy("theme").agg(
            F.countDistinct("account_number", "order_no").alias("theme_total")
        )

        # Frequency of next themes for baseline probabilities
        basket_count = (
            baskets_with_themes_next.select("account_number", "order_no")
            .distinct()
            .count()
        )
        next_theme_base_probs = (
            baskets_with_themes_next.groupBy("next_theme")
            .agg(F.countDistinct("account_number", "order_no").alias("count"))
            .withColumn("prob_base", F.col("count") / basket_count)
        )

        # Probabilities will become edge weights
        # Fractional counting avoids overcounting when multiple themes in a basket
        transition_probs = (
            baskets_with_themes_next.withColumn(
                "fractional_count",
                F.lit(1.0) / F.count("next_theme").over(w_acc_order_theme),
            )
            .groupBy("theme", "next_theme")
            .agg(F.sum("fractional_count").alias("transition_freq"))
            .join(theme_frequency, on="theme", how="inner")
            .withColumn(
                "probability", F.col("transition_freq") / F.col("theme_total")
            )
            .join(
                next_theme_base_probs.select("next_theme", "prob_base"),
                on="next_theme",
                how="inner",
            )
            .withColumn(
                "prob_rebased", F.col("probability") - F.col("prob_base")
            )
            .withColumnRenamed("prob_base", "base_probability")
            .withColumnRenamed("prob_rebased", "probability_rebased")
            .withColumn(
                "transition_freq",
                F.col("transition_freq").cast("decimal(12,2)"),
            )
            .withColumn("theme_total", F.col("theme_total").cast("integer"))
            .withColumn(
                "probability", F.col("probability").cast("decimal(10,9)")
            )
            .withColumn(
                "base_probability",
                F.col("base_probability").cast("decimal(10,9)"),
            )
            .withColumn(
                "probability_rebased",
                F.col("probability_rebased").cast("decimal(10,9)"),
            )
            .select(
                "theme",
                "next_theme",
                "transition_freq",
                "theme_total",
                "probability",
                "base_probability",
                "probability_rebased",
            )
        )

        # Tolerance to account for floating point precision
        bad_total_probs = (
            transition_probs.groupBy("theme")
            .agg(F.sum("probability").alias("total_probability"))
            .where(F.col("total_probability") > 1.00001)
            .where(F.col("total_probability") < 0.99999)
        )
        assert bad_total_probs.isEmpty(), "Total probabilities found != 1.0"

        logger.info(f"Loading theme transition to {THEME_TRANSITIONS}")
        logger.info(f"Loading theme transition to {THEME_TRANSITIONS_LATEST}")
        publish_history_and_latest(
            spark,
            transition_probs,
            history_table=THEME_TRANSITIONS,
            latest_table=THEME_TRANSITIONS_LATEST,
            key_columns=["theme", "next_theme"],
            run_date=run_date,
        )

    # Read the materialised scoring events from this run. This keeps the
    # downstream scoring lineage independent from the cached basket build.
    account_themes = (
        spark.table(THEME_SCORING_EVENTS_LATEST)
        .where(F.col("EventType") == "order")
        .where(F.col("EventWeight") == 1.0)
        .select(
            F.col("AccountNumber").alias("account_number"),
            F.col("Theme").alias("theme"),
        )
        .distinct()
    )

    if TEST_ACCOUNT:
        logger.info("Recent themes for test account:")
        (
            account_themes.where(F.col("account_number") == TEST_ACCOUNT)
            .orderBy("account_number", "theme")
            .show(100, truncate=False)
        )

    baskets_with_themes.unpersist()

    # --- View history (immediate intent signal) ---
    if VIEWS_ENABLED:
        logger.info("Loading view history for scoring")

        rpid_lookup = (
            spark.table(SESSIONS)
            .where(F.col("AccountNumber_RPID").isNotNull())
            .where(F.col("date").between(ACTIONS_START, ACTIONS_END))
            .select(
                "UniqueVisitID",
                F.col("AccountNumber_RPID").alias("account_number"),
            )
        )
        if SESSIONS_APP:
            rpid_lookup = rpid_lookup.unionByName(
                spark.table(SESSIONS_APP)
                .where(F.col("AccountNumber_RPID").isNotNull())
                .where(F.col("date").between(ACTIONS_START, ACTIONS_END))
                .select(
                    "UniqueVisitID",
                    F.col("AccountNumber_RPID").alias("account_number"),
                )
            )
        rpid_lookup = rpid_lookup.distinct()

        views_raw = (
            spark.table(VIEWS)
            .where(F.col("date").between(ACTIONS_START, ACTIONS_END))
            .select("UniqueVisitID", "date", F.col("ProductSKU").alias("pid"))
        )
        if VIEWS_APP:
            views_raw = views_raw.unionByName(
                spark.table(VIEWS_APP)
                .where(F.col("date").between(ACTIONS_START, ACTIONS_END))
                .select(
                    "UniqueVisitID", "date", F.col("ProductSKU").alias("pid")
                )
            )

        account_view_themes = select_latest_view_themes(
            views_raw.join(rpid_lookup, on="UniqueVisitID", how="inner")
            .join(F.broadcast(item_themes), on="pid", how="inner")
            .select("account_number", "theme", "date")
        )

        if TEST_ACCOUNT:
            logger.info("Recent view themes for test account:")
            (
                account_view_themes.where(
                    F.col("account_number") == TEST_ACCOUNT
                ).show()
            )
    else:
        account_view_themes = None
        logger.info("View tables not configured — scoring from purchases only")

    if not TRAIN:
        logger.info(
            f"Reading transition probabilities from {THEME_TRANSITIONS_LATEST}"
        )
        transition_probs = spark.table(THEME_TRANSITIONS_LATEST)

    # --- Blended scoring (purchase baseline + view boost) ---
    logger.info("Scoring purchase history against transition matrix")
    transition_probs_slim = transition_probs.select(
        "theme", "next_theme", "probability"
    )

    scores_buy = (
        account_themes.join(transition_probs_slim, on="theme", how="inner")
        .groupBy("account_number", "next_theme")
        .agg(F.mean("probability").alias("score_buy"))
    )

    if VIEWS_ENABLED and account_view_themes is not None:
        logger.info("Scoring view history against transition matrix")
        scores_view = (
            account_view_themes.join(
                transition_probs_slim, on="theme", how="inner"
            )
            .groupBy("account_number", "next_theme")
            .agg(F.mean("probability").alias("score_view"))
        )

        combined = (
            scores_buy.join(
                scores_view, on=["account_number", "next_theme"], how="outer"
            )
            .na.fill(0)
            .withColumn(
                "prob_agg",
                F.col("score_buy") + (F.col("score_view") * F.lit(0.1)),
            )
        )
    else:
        combined = scores_buy.withColumnRenamed("score_buy", "prob_agg")

    # Dynamic batch normalisation (rebase against population mean)
    w_next_theme = Window.partitionBy("next_theme")
    next_theme_probs = (
        combined.withColumn("prob_base", F.mean("prob_agg").over(w_next_theme))
        .withColumn("prob_agg_rebased", F.col("prob_agg") - F.col("prob_base"))
        .select(
            "account_number",
            "next_theme",
            "prob_agg",
            "prob_base",
            "prob_agg_rebased",
        )
    )

    # --- Safety net: backfill with global best sellers ---
    logger.info("Building safety net from top 25 recent themes")
    global_top_themes = select_global_top_themes(
        spark.table(BASKETS)
        .where(F.col("ordertakendate") >= F.date_sub(F.lit(ACTIONS_END), 30))
        .where(F.col("ordertakendate") <= F.lit(ACTIONS_END))
        .withColumnRenamed("itemno", "pid")
        .join(F.broadcast(item_themes), on="pid", how="inner")
        .groupBy("theme")
        .agg(F.count("*").alias("sales_count"))
    )

    next_theme_probs = merge_and_rank_theme_scores(
        next_theme_probs,
        global_top_themes,
    )

    if TEST_ACCOUNT:
        logger.info("Next theme probabilities for test account:")
        (
            next_theme_probs.where(F.col("account_number") == TEST_ACCOUNT)
            .orderBy(F.desc("prob_agg_rebased"))
            .show(100, truncate=False)
        )
    else:
        next_theme_probs = next_theme_probs.withColumnsRenamed(
            {
                "account_number": "AccountNumber",
                "next_theme": "NextTheme",
                "prob_agg": "ProbAgg",
                "prob_base": "ProbBase",
                "prob_agg_rebased": "ProbAggRebased",
            }
        )

        logger.info("Materialising customer next-theme scores to temp table")
        temp_table_name = (
            f"{config.catalog_write}.{SCHEMA}."
            f"temp_next_theme_probs_{uuid.uuid4().hex}"
        )
        # Keep this managed materialisation until runtime 18.1 checkpointing is
        # available. A run-unique table prevents concurrent executions from
        # overwriting each other's lineage cut.
        try:
            (
                next_theme_probs.write.mode("errorifexists").saveAsTable(
                    temp_table_name
                )
            )
            temp_next_theme_probs = spark.table(temp_table_name)

            logger.info(
                "Loading customer next-theme scores to"
                + f" {NEXT_THEME_SCORES}"
            )
            logger.info(
                "Loading customer next-theme scores to"
                + f" {NEXT_THEME_SCORES_LATEST}"
            )
            publish_history_and_latest(
                spark,
                temp_next_theme_probs,
                history_table=NEXT_THEME_SCORES,
                latest_table=NEXT_THEME_SCORES_LATEST,
                key_columns=["AccountNumber", "NextTheme"],
                run_date=run_date,
            )
        finally:
            spark.sql(
                "DROP TABLE IF EXISTS "
                + quote_qualified_identifier(temp_table_name)
            )

    if PLOT_GRAPH:
        logger.info("Creating theme transition graph")
        graph = DirectedGraphPlotter(
            df=transition_probs.select(
                F.col("theme").alias("node"),
                F.col("next_theme").alias("next_node"),
                F.col("theme_total").alias("node_weight"),
                F.col("probability").alias("edge_weight"),
            ),
            min_edge_weight=MIN_EDGE_WEIGHT,
            min_node_weight=MIN_NODE_WEIGHT,
            colorscale="matter",
        )
        graph.create_figure()
        graph_filename = f"scratch_graph_{CLIENT}_{ACTIONS_END}.html"
        logger.info(f"Writing graph to {graph_filename}")
        graph.fig.write_html(graph_filename)

    logger.info("Run complete")


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    JOB_ENV = jobparser.get_arg("--job_env")
    CLIENT = jobparser.get_arg("--client")
    LOG_LEVEL = jobparser.get_arg("--log_level")
    main(
        JOB_ENV,
        CLIENT,
        LOG_LEVEL,
        ACTIONS_END=jobparser.get_arg("--actions-end"),
        REFRESH_MODEL_DATE=jobparser.get_arg("--refresh_model_date"),
        TEST_ACCOUNT=jobparser.get_arg("--test-account"),
        PLOT_GRAPH=jobparser.has_arg("--plot-graph"),
        MIN_EDGE_WEIGHT=jobparser.get_arg("--min-edge-weight") or 0.03,
        MIN_NODE_WEIGHT=jobparser.get_arg("--min-node-weight") or 1000,
    )

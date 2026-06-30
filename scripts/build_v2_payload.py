import sys
from pathlib import Path


try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
except NameError:
    from dsutils.dbc import get_dbutils

    dbutils = get_dbutils()
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore # noqa
    if not notebook_path.startswith("/Workspace"):
        notebook_path = "/Workspace" + notebook_path
    PROJECT_ROOT = Path(notebook_path).parent.parent
finally:
    print(f"Project root resolved to: {PROJECT_ROOT}")
    sys.path.insert(0, str(PROJECT_ROOT))

import os

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from dsutils.argparser import get_job_parser
from dsutils.dbc import configure_spark
from dsutils.etl import delete_from_and_load
from dsutils.logtools import configure_logging, get_logger

from next_ads.utils import config_manager
from next_ads.common.paths import load_client_config
from next_ads.Export import generate_experimentid


def get_input_dataframes(config, spark):
    assignments_v2_latest = spark.table(
        config.tables_write.assignments_v2_latest
    )
    customer_cells_fixed_latest = spark.table(
        config.tables_write.customer_cells_fixed_latest
    )
    customer_cells_latest = spark.table(
        config.tables_write.customer_cells_latest
    )
    control_sheet_latest_v2 = spark.table(
        config.tables_write.control_sheet_latest_v2
    )
    rpid_with_accounts = spark.table(config.tables_read.rpid_with_accounts)

    return (
        assignments_v2_latest,
        customer_cells_fixed_latest,
        customer_cells_latest,
        control_sheet_latest_v2,
        rpid_with_accounts,
    )


def get_experiments(
    payload_experiment_settings: dict | None = None,
) -> list[dict[str, object]] | None:
    """Build ExperimentID configuration entries from payload config.

    Supported input form under `payload_experiment_id.experiments`:
    - dict mapping experiment name -> fixed-cell split column

    If an enabled `audience_experiment` is present, appends an audience
    experiment entry in the format expected by `generate_experimentid`.

    Returns None when no experiments are configured.
    """
    settings = payload_experiment_settings or {}
    experiments: list[dict[str, object]] = []

    configured_experiments = settings.get("experiments")
    if configured_experiments:
        for exp_name, split_col in configured_experiments.items():
            experiments.append({exp_name: split_col})

    audience_experiment = settings.get("audience_experiment")
    if audience_experiment and audience_experiment.get("enabled"):
        exp_name = audience_experiment.get("name", "Audience")
        split_col = audience_experiment.get("split_col", "Audience")
        experiments.append({"Audience": {exp_name: split_col}})

    return experiments if experiments else None


def get_payload_experiment_settings(client: str) -> dict:
    cfg = load_client_config(client)
    return cfg.get("payload_experiment_id", {})


def get_fatigue_rotation_settings(spark):
    frd = [
        {
            "pageType": "ShoppingBagPage",
            "enableAdFatigueRotation": True,
            "rotation": 2,
        },
        {
            "pageType": "ProductListingPage",
            "enableAdFatigueRotation": True,
            "rotation": 2,
        },
        {
            "pageType": "HomePage",
            "enableAdFatigueRotation": False,
            "rotation": 0,
        },
        {
            "pageType": "CheckoutPage",
            "enableAdFatigueRotation": True,
            "rotation": 2,
        },
        {
            "pageType": "ForYouPage",
            "enableAdFatigueRotation": False,
            "rotation": 0,
        },
    ]

    fr = spark.createDataFrame(frd)
    return fr


def assign_experiments(
    customer_cells_fixed_latest,
    customer_cells_latest,
    payload_experiment_settings,
    logger,
):
    """Generate per-customer ExperimentID values for payload export.

    Validates fixed-cell experiment split columns, optionally joins audience
    splits from `customer_cells_latest`, auto-derives audience split values
    when not configured, and delegates token generation to
    `generate_experimentid`.

    Raises:
        ValueError: If payload experiments are missing or invalid.
    """
    experiments = get_experiments(payload_experiment_settings)

    if experiments is None:
        raise ValueError(
            "Invalid payload experiment configuration. "
            "`payload_experiment_id.experiments` is mandatory."
        )

    fixed_cell_columns = set(customer_cells_fixed_latest.columns)
    missing_experiment_columns: list[str] = []
    for experiment in experiments:
        for exp_name, split_col in experiment.items():
            if exp_name == "Audience":
                continue
            if split_col not in fixed_cell_columns:
                missing_experiment_columns.append(f"{exp_name}:{split_col}")

    if missing_experiment_columns:
        missing_cols_str = ", ".join(missing_experiment_columns)
        raise ValueError(
            "Invalid payload experiment configuration. Missing split columns "
            f"in customer_cells_fixed_latest: {missing_cols_str}"
        )

    audience_experiment = (payload_experiment_settings or {}).get(
        "audience_experiment"
    )
    audience_df = None
    audience_sample = None
    audience_split = None

    if audience_experiment and audience_experiment.get("enabled"):
        split_col = audience_experiment.get("split_col", "Audience")
        audience_sample = audience_experiment.get("sample", ["Best"])
        audience_split = audience_experiment.get("split")

        if split_col in customer_cells_latest.columns:
            audience_df = customer_cells_latest.select(
                "AccountNumber", split_col
            )
            if audience_split is None:
                audience_split = [
                    r[split_col]
                    for r in audience_df.select(split_col)
                    .where(F.col(split_col).isNotNull())
                    .distinct()
                    .collect()
                ]
                logger.info(
                    "Audience experiment split values auto-derived from `%s`: %s",
                    split_col,
                    audience_split,
                )
        else:
            logger.warning(
                "Audience experiment enabled but split column `%s` not found "
                "in customer cells latest table; disabling audience experiment",
                split_col,
            )

    return generate_experimentid(
        customer_cells_fixed_latest,
        experiments,
        audience_df=audience_df,
        audience_sample=audience_sample,
        audience_split=audience_split,
    )


def combine_tables(
    assignments_v2_latest,
    control_sheet_latest_v2,
    customer_cells_fixed_latest,
    customer_cells_latest,
    spark,
    logger,
    payload_experiment_settings: dict | None = None,
):
    assn = assignments_v2_latest
    csfl_ctrl = customer_cells_fixed_latest.withColumn(
        "control",
        F.when(F.col("FallowControl") == "NoAds", True).otherwise(False),
    ).select("AccountNumber", "control")

    csfl_exp = assign_experiments(
        customer_cells_fixed_latest=customer_cells_fixed_latest,
        customer_cells_latest=customer_cells_latest,
        payload_experiment_settings=payload_experiment_settings,
        logger=logger,
    )
    csfl = csfl_exp.join(csfl_ctrl, ["AccountNumber"])

    cs = control_sheet_latest_v2.select(
        "UniqueAdID", "PotNumber", "CampaignNumber", "TemplateName"
    )

    fr = get_fatigue_rotation_settings(spark)

    window_spec = Window.partitionBy("AccountNumber", "UniqueAdID")

    comb = (
        assn.join(cs, assn.UniqueAdIDMeasurement == cs.UniqueAdID, "inner")
        .join(csfl, ["AccountNumber"])
        .join(fr, ["pageType"])
        .withColumn(
            "type",
            F.when(F.col("TemplateName").like("%Standard%"), "s").otherwise(
                "d"
            ),
        )  # standard or dynamic (video) content
        .withColumn(
            "fragmentId",
            F.concat_ws(
                "_", F.col("PotNumber"), F.col("CampaignNumber"), F.col("type")
            ),
        )
        .withColumn("adFatigueImpressionThreshold", F.lit(2))
        # Hard-coded to false for now as we haven't determined when this will go live and where etc.
        # TODO: ad rotation deployment plan
        .withColumn("enableAdFatigueRotation", F.lit(False))
        .withColumn(
            "max_TriggerScore", F.max("TriggerScore").over(window_spec)
        )
    )
    return comb


def get_rpid_mapping(df_roaming_profile):
    roaming_profile = (
        df_roaming_profile.filter(
            "account_number not in ('420356A449144ED854830CF2ABB970629FB0D3620798632BA5F1F0A659DC9070')"
        )
        .select("account_number", "roamingprofileid")
        .distinct()
    )

    Dupes1 = (
        roaming_profile.groupBy("account_number")
        .count()
        .filter("count>1")
        .select("account_number")
    )
    Dupes2 = (
        roaming_profile.groupBy("roamingprofileid")
        .count()
        .filter("count>1")
        .select("roamingprofileid")
    )

    roaming_profile = roaming_profile.join(
        Dupes1, on="account_number", how="leftanti"
    ).join(Dupes2, on="roamingprofileid", how="leftanti")

    return roaming_profile


def make_payload(df_combined):
    triggers_window = Window.partitionBy("AccountNumber")

    # create a column that lists the top ads for that customer as a whole,
    # and the raw model score for that ad
    agg_comb1 = (
        df_combined.withColumn("rank_int", F.col("Rank").cast("int"))
        # list top triggers for ads for this customer
        .withColumn(
            "fulltriggers",
            F.sort_array(  # sort by the struct with Max_TriggerScore first, sort descending so the most important triggers are at the front of the list
                F.collect_list(  # list the max TriggerScore, for each fragmentId, for that customer
                    F.struct(
                        F.col("Max_TriggerScore").alias("t"),
                        F.col("fragmentId").alias("id"),
                    )
                ).over(triggers_window),
                asc=False,
            ),
        )
    )

    # limit to the top 5 triggers to avoid payload bloat, we can adjust this number as needed
    # collect a list of ads per customer and pagetype, sorted by their rank ascending
    agg_comb2 = (
        agg_comb1.withColumn("triggers", F.expr("slice(fulltriggers, 1, 5)"))
        .groupBy(
            "AccountNumber",
            "pageType",
            "control",
            "adFatigueImpressionThreshold",
            "experimentId",
            "enableAdFatigueRotation",
            "triggers",
        )
        .agg(
            F.sort_array(
                F.array_sort(
                    F.collect_list(
                        F.struct(F.col("rank_int"), F.col("fragmentId"))
                    )
                ),
                asc=True,
            ).alias("fragments"),
        )
    )
    # convert the fragments into just the fragment ids
    # group by the lists of fragment ids, to get a list of the pagetypes that share these ads
    # and rotation settings in common
    agg_comb3 = (
        agg_comb2.withColumn(
            "fragmentIds", F.expr("transform(fragments, x -> x.fragmentId)")
        )
        .select(
            "AccountNumber",
            "adFatigueImpressionThreshold",
            "experimentId",
            "triggers",
            "control",
            "enableAdFatigueRotation",
            "pageType",
            "enableAdFatigueRotation",
            "fragmentIds",
        )
        .groupBy(
            "AccountNumber",
            "adFatigueImpressionThreshold",
            "experimentId",
            "triggers",
            "control",
            "fragmentIds",
            "enableAdFatigueRotation",
        )
        .agg(F.collect_list("pageType").alias("pageTypes"))
    )

    # build structs that associate the list of page types with
    # the list of ads and the rotation setting for those pagetype(s)
    agg_comb4 = agg_comb3.groupBy(
        "AccountNumber",
        "adFatigueImpressionThreshold",
        "experimentId",
        "triggers",
        "control",
    ).agg(
        F.collect_list(
            F.struct(
                F.col("pageTypes"),
                F.col("enableAdFatigueRotation"),
                F.col("fragmentIds"),
            )
        ).alias("frag_pagetype")
    )

    # collate the list of fragments data per customer
    # and collate the next_ads payload struct
    agg_comb5 = (
        agg_comb4.groupBy(
            "AccountNumber",
            "adFatigueImpressionThreshold",
            "experimentId",
            "triggers",
            "control",
        )
        .agg(F.collect_list("frag_pagetype").alias("fragments"))
        .selectExpr(
            "AccountNumber as account_number",
            "struct(AccountNumber, adFatigueImpressionThreshold, experimentId, triggers, control , fragments) as next_ads",
        )
    )

    # Add a hash of the next_ads struct, include it in the struct
    agg_comb6 = (
        agg_comb5.withColumn(
            "ads_hash", F.sha2(F.to_json(F.col("next_ads")), 256)
        )
        .withColumn(
            "next_ads",
            F.col("next_ads").withField("adsHash", F.col("ads_hash")),
        )
        .drop("ads_hash")
    )

    return agg_comb6


def set_rpid(df_payload, df_roaming_profile):
    next_ads = df_payload.join(
        df_roaming_profile, on="account_number", how="inner"
    ).select(
        "roamingprofileid",
        "next_ads",
    )
    return next_ads


def write_payload_tables(
    df_output,
    payload_table: str,
    payload_latest_table: str,
    logger,
):
    logger.info(f"Loading payload output to {payload_table}")
    delete_from_and_load(
        df_output,
        payload_table,
        pk_cols=["roamingprofileid"],
        del_where={"rundate": "current_date()"},
    )

    logger.info(f"Loading payload output to {payload_latest_table}")
    delete_from_and_load(
        df_output,
        payload_latest_table,
        pk_cols=["roamingprofileid"],
        del_where={"rundate": "current_date()"},
    )


def write_output_to_csv(df_output, pii_exponea_next_uk_path, logger, process="next_ads"):
    payload_path = os.path.join(
        pii_exponea_next_uk_path, "outbound", "customer_attributes", process
    )

    logger.info(f"Writing output dataframe to CSV at: {payload_path}")
    (
        df_output.coalesce(1)
        .write.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .mode("overwrite")
        .csv(payload_path)
    )
    logger.info("CSV write complete")


def setup_run_context(JOB_ENV: str, CLIENT: str, LOG_LEVEL: str):
    if LOG_LEVEL:
        configure_logging(log_level=LOG_LEVEL)
    else:
        configure_logging()

    logger = get_logger(__name__)
    spark = configure_spark()
    logger.info(f"Running in job environment: {JOB_ENV}")

    if not CLIENT:
        assert JOB_ENV.lower() == "dev", (
            f"Client must be specified when running in {JOB_ENV}"
        )
        CLIENT = "next_uk"
        logger.warning(f"Client not specified (defaulting to {CLIENT})")

    config = config_manager.load_config(JOB_ENV)
    logger.info(f"Configuring run for client: {CLIENT}")

    return logger, spark, CLIENT, config


def main(JOB_ENV: str, CLIENT: str, LOG_LEVEL: str, DO_EXPORT: bool):
    # load configuration
    logger, spark, CLIENT, config = setup_run_context(
        JOB_ENV, CLIENT, LOG_LEVEL
    )

    logger.info("Loading data...")
    payload_experiment_settings = get_payload_experiment_settings(CLIENT)

    # get the data
    (
        assignments_v2_latest,
        customer_cells_fixed_latest,
        customer_cells_latest,
        control_sheet_latest_v2,
        rpid_with_accounts,
    ) = get_input_dataframes(config, spark)

    logger.info("Combining data...")
    combined = combine_tables(
        assignments_v2_latest,
        control_sheet_latest_v2,
        customer_cells_fixed_latest,
        customer_cells_latest,
        spark,
        logger,
        payload_experiment_settings,
    )

    logger.info("Constructing payload...")
    payload = make_payload(combined)

    logger.info("Key on RPID...")
    roaming_profile = get_rpid_mapping(rpid_with_accounts)
    rpid_keyed_output = set_rpid(payload, roaming_profile)

    logger.info("Writing to Tables...")
    # write the output to the payload tables
    write_payload_tables(
        rpid_keyed_output,
        config.tables_write.nextads_payload,
        config.tables_write.nextads_payload_latest,
        logger,
    )

    df_latest_payload = spark.table(config.tables_write.nextads_payload_latest)

    if DO_EXPORT:
        logger.info("Writing to csv...")

        # convert the next_ads column to JSON
        df_latest_payload = df_latest_payload.select(
            "roamingprofileid",
            F.to_json(
                "next_ads", {"ignoreNullFields": "false", "pretty": "false"}
            ).alias("next_ads"),
        )

        write_output_to_csv(df_latest_payload, config.pii_exponea_next_uk_path, logger)

        # exponea_cust = spark.sql("""select distinct trim(roamingprofileid) as roamingprofileid from pii.next_uk_exponea_customers
        #                  where roamingprofileid is not null
        #                  and trim(roamingprofileid)!=''
        #                  and (next_ads is not null)""")
        # in_exp_notin_source=exponea_cust.join(df_latest_payload, ["roamingprofileid"], "left_anti") #GET RECORDS IN EXPONEA NOT IN MASID AND BLANK THEM
        # distinct_nextads_blank=in_exp_notin_source.withColumn("next_ads", lit(""))

        # write_output_to_csv(distinct_nextads_blank, config.pii_exponea_next_uk_path, logger, process="next_ads_blanking")

    logger.info("Output complete")
    logger.info(f"Output row count: {df_latest_payload.count()}")
    df_latest_payload.show(20, truncate=False)


if __name__ == "__main__":
    jobparser = get_job_parser()
    jobparser._parse_args()
    JOB_ENV = jobparser.get_arg("--job_env")
    CLIENT = jobparser.get_arg("--client")
    LOG_LEVEL = jobparser.get_arg("--log_level")
    DO_EXPORT = jobparser.get_typed_arg("--do_export", bool)
    main(JOB_ENV, CLIENT, LOG_LEVEL, DO_EXPORT)

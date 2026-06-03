# Databricks notebook source
# MAGIC %md
# MAGIC # Build Multi-Snapshot pCTR Training Data
# MAGIC
# MAGIC Runs the pCTR feature/training notebooks for a sequence of monthly
# MAGIC `reference_date` values and appends each point-in-time training table into
# MAGIC snapshot tables.

# COMMAND ----------

from datetime import datetime

from dateutil.relativedelta import relativedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC `latest_reference_date` is the final point-in-time anchor in the backfill.
# MAGIC `snapshot_months` controls how many monthly anchors are generated ending on
# MAGIC that date. For example, `snapshot_months=1` runs only the latest reference
# MAGIC date, while `snapshot_months=18` runs the latest date and the previous 17
# MAGIC monthly reference dates.
# MAGIC
# MAGIC Each child notebook receives one generated `reference_date` and treats it as
# MAGIC the day of run. All recency windows, 7-day/30-day demand windows,
# MAGIC same-month-last-year windows, and labelled exposure windows are calculated
# MAGIC relative to that generated date.
# MAGIC
# MAGIC The builder always passes `write_mode=append_snapshot` to child notebooks so
# MAGIC each generated month is written as a replaceable snapshot partition.
# MAGIC
# MAGIC The purpose of this notebook is to create training history, not just today's
# MAGIC latest feature tables. A pCTR model trained on one reference date only sees
# MAGIC one season/campaign mix. A snapshot table gives the model many point-in-time
# MAGIC examples, so the model training notebook can use older months for training
# MAGIC and newer months for validation/testing.
# MAGIC
# MAGIC Use `snapshot_table_suffix=snapshots` for the real backfill tables. Use a
# MAGIC throwaway suffix such as `smoke` when testing a one-month or two-month run,
# MAGIC so the test does not overwrite latest tables or the real snapshot history.

# COMMAND ----------

def get_widget_value(name, default):
    try:
        dbutils.widgets.text(name, str(default))
        value = dbutils.widgets.get(name)
        return value if value not in (None, "") else default
    except NameError:
        return default


def validate_positive_int(name, value):
    parsed_value = int(value)
    if parsed_value <= 0:
        raise ValueError(f"Invalid widget value for {name}: {value}. Value must be a positive integer.")
    return parsed_value


latest_reference_date = get_widget_value("latest_reference_date", "2026-04-15")
snapshot_months = validate_positive_int("snapshot_months", get_widget_value("snapshot_months", "18"))
snapshot_table_suffix = get_widget_value("snapshot_table_suffix", "snapshots")
notebook_timeout_seconds = int(get_widget_value("notebook_timeout_seconds", "0"))

print(
    "pCTR snapshot builder run config: "
    f"latest_reference_date={latest_reference_date}, "
    f"snapshot_months={snapshot_months}, "
    f"snapshot_table_suffix={snapshot_table_suffix}, "
    f"notebook_timeout_seconds={notebook_timeout_seconds}. "
    "Widget options: latest_reference_date='YYYY-MM-DD'; snapshot_months is a positive integer; "
    "snapshot_table_suffix is free text, for example 'snapshots' or 'smoke'; "
    "notebook_timeout_seconds is 0 for no timeout or a positive integer. "
    "Meaning: snapshot_months=1 runs only latest_reference_date; snapshot_months=18 "
    "runs monthly reference dates ending at latest_reference_date and going back "
    "17 months."
)

notebook_sequence = [
    "./customer_behaviour_features",
    "./pctr_advert_metadata_attribute_profile",
    "./pctr_advert_semantic_embeddings",
    "./pctr_product_embedding_features",
    "./pctr_seasonal_product_features",
    "./pctr_tagged_click_training",
]

# COMMAND ----------

def monthly_reference_dates(latest_date, month_count):
    latest = datetime.strptime(latest_date, "%Y-%m-%d").date()
    first = latest - relativedelta(months=month_count - 1)
    return [
        (first + relativedelta(months=offset)).isoformat()
        for offset in range(month_count)
    ]


reference_dates = monthly_reference_dates(latest_reference_date, snapshot_months)

display(spark.createDataFrame([(value,) for value in reference_dates], ["reference_date"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Snapshot Builds

# COMMAND ----------

for snapshot_reference_date in reference_dates:
    print(f"Starting pCTR snapshot build for {snapshot_reference_date}")
    for notebook_path in notebook_sequence:
        print(f"Running {notebook_path} for {snapshot_reference_date}")
        dbutils.notebook.run(
            notebook_path,
            timeout_seconds=notebook_timeout_seconds,
            arguments={
                "reference_date": snapshot_reference_date,
                "write_mode": "append_snapshot",
                "snapshot_table_suffix": snapshot_table_suffix,
            },
        )
    print(f"Completed pCTR snapshot build for {snapshot_reference_date}")

print(f"Completed {len(reference_dates)} pCTR monthly snapshots ending {latest_reference_date}")

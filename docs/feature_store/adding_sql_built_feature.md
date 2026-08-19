# Adding a SQL-Built Feature to the Feature Store

Azure Boards story: 5111869
Feature: 5111595 - Reusable feature layer (Databricks Feature Store)

## When To Use This Route

This note explains how a data scientist should add a reusable feature when the feature logic already exists in SQL.

Add the feature through the feature-store contract route. Do not write directly to the shared feature-store schema, and do not hide reusable feature creation inside a model training or scoring job.

## SQL-Built Feature Eligibility

Before adding the feature, confirm that it belongs in the feature store:

- The output is reusable by more than one model route, or it is a stable input to a named model-ready feature table.
- The grain, primary keys and snapshot/date key are clear.
- The feature can be built point-in-time without leaking future data into training snapshots.
- The output is a model input, label or quality event, not a final model score, ranking decision or assignment output.

If the feature is only a model run result, keep it in a model output table. If it changes ranking, assignment or production delivery behaviour, treat that as a separate model or decisioning change.

## Contract-To-Publication Steps

1. Choose the target contract. Reuse an existing feature-store table when the grain and ownership already match; otherwise add a new physical table entry to `configs/features/nextads_feature_store.yaml`.
2. Define the table metadata in the registry, including `name`, `entity`, `grain`, `primary_keys`, optional `timestamp_key` and `snapshot_date_key`, `source_job`, `owner`, `freshness`, `training_safe` and `consumers`.
3. Add or update the SQL DDL contract under `sql/features/nextads/`. New table files must be named `create_table_<table_name>.sql` so the setup script can resolve them from the registry.
4. Move the feature-building SQL into repo-owned feature code, usually as a Spark builder function under `src/next_ads/features/` that returns a DataFrame. Keep source catalog/schema/reference-date values parameterised.
5. Wire the builder into the relevant entrypoint under `jobs/features/nextads/` and write through `next_ads.features.materialization.write_feature_table`. That helper aligns to the SQL contract, validates primary keys, replaces the requested date partition where applicable, and writes through the Databricks Feature Engineering client.
6. Extend `jobs/features/nextads/preflight_checks.py` and the quality-check route when the table is part of the scheduled feature-store refresh.
7. Update tests for registry validity, SQL contract presence, job wiring and any new compatibility view or consumer behaviour.
8. Validate in personal DEV or the shared `DEV_FEATURE_STORE` route before any model job consumes the table. Production feature-store publication remains a separate curated change with explicit consumer need and release sign-off.

The expected write path is:

```text
SQL logic -> Spark DataFrame builder -> feature job entrypoint
-> write_feature_table -> Databricks Feature Engineering table
```

For a date-partitioned READY snapshot, `snapshot_date_key` defines which rows
belong to the requested snapshot date. It defaults to `timestamp_key`. Declare
the two separately when the daily business date differs from the exact event
time used for point-in-time lookups. Shopping Bag click labels are scoped by
`session_date`, while `exposure_timestamp` remains their Feature Store
time-series key.

## Review Evidence

For a feature-store PR, include:

- the table grain, keys and date/snapshot behaviour;
- the model consumers that need the feature;
- the source tables used by the SQL;
- DEV validation evidence for table creation and materialisation;
- row-count, key-uniqueness and null-key checks where applicable;
- confirmation that production scoring, ranking, assignment and delivery outputs are unchanged unless the PR explicitly changes them.

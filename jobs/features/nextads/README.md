# Next Ads Feature Store Jobs

These entrypoints are the first opt-in home for Next Ads feature-store
materialisation.

The initial slice is Databricks Feature Engineering first: the registry and
SQL contracts define table metadata and schemas, while the setup job creates
feature tables through `FeatureEngineeringClient.create_table`.

The build entrypoints currently resolve and log the registry tables they own
without changing existing production outputs.

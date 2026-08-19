# Next Ads Feature Store Jobs

These entrypoints are the first opt-in home for Next Ads feature-store materialisation.

The initial slice is Databricks Feature Engineering first: the registry and SQL contracts define table metadata and schemas, while the setup job creates feature tables through `FeatureEngineeringClient.create_table`.

All registered physical tables now have repository builders. A builder does not count as live evidence until its DEV run has populated the table and passed the declared key, schema, freshness and row checks.

The Analytics pCTR source and publication responsibilities are deliberately separated:

1. `mktg_next_uk_nextads_analytics_pctr_feature_source` runs the retained Analytics SQL and records the exact output version.
2. The centrally owned `mktg_next_uk_nextads_feature_store` job consumes that exact receipt and publishes account-advert affinity, session context and the pCTR model input with the other declared feature contracts.

Publication records the exact input and output Delta versions, then marks all three tables READY together. It never falls back to the newest physical table.

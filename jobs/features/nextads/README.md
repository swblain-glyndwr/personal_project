# Next Ads Feature Store Jobs

These entrypoints are the first opt-in home for Next Ads feature-store materialisation.

The initial slice is Databricks Feature Engineering first: the registry and SQL contracts define table metadata and schemas, while the setup job creates feature tables through `FeatureEngineeringClient.create_table`.

All registered physical tables now have repository builders. A builder does not count as live evidence until its DEV run has populated the table and passed the declared key, schema, freshness and row checks.

The Analytics pCTR source-building and Feature Store publication responsibilities are deliberately separated inside the centrally owned `mktg_next_uk_nextads_feature_store` job:

1. The internal `analytics_pctr_*` notebook tasks run the retained source SQL for the Feature Store job's resolved reference date and combine the source features.
2. `receipt_analytics_pctr_feature_source` validates the combined source and records its exact Delta version, schema, date and producing Feature Store run.
3. The downstream Feature Store builders consume that exact receipt and publish account-advert affinity, session context and the pCTR model input with the other declared feature contracts.

There is no standalone Analytics pCTR source saved job. Publication records the exact input and output Delta versions, then marks all three tables READY together. It never falls back to the newest physical table.

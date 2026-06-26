# Next Ads Feature Store Jobs

These entrypoints are the first opt-in home for Next Ads feature-store
materialisation.

The initial slice is Databricks Feature Engineering first: the registry and
SQL contracts define table metadata and schemas, while the setup job creates
feature tables through `FeatureEngineeringClient.create_table`.

The first materialised slice now populates account features, web activity,
advert metadata, item attributes, advert attribute rollups, Theme Affinity
features/model input, Theme response labels and Shopping Bag click labels from
stable source tables. Embedding-derived tables, pCTR model input and candidate
similarity remain scaffolded until their source/model contracts are migrated.

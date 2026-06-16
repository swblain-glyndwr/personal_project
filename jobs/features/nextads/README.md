# Next Ads Feature Store Jobs

These entrypoints are the first opt-in home for Next Ads feature-store
materialisation.

The initial slice is Databricks Feature Engineering first: the registry and
SQL contracts define table metadata and schemas, while the setup job creates
feature tables through `FeatureEngineeringClient.create_table`.

The Theme Affinity/LTR entrypoints now populate the first feature-store slice
from existing hackathon/Theme Affinity outputs, using the Databricks Feature
Engineering client for writes. Account, advert and CWB pCTR entrypoints remain
scaffold/dependency-only until their source contracts are migrated.

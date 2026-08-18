# Feature Store Job Flow

This page shows the dependency order inside
`mktg_next_uk_nextads_feature_store`. For the inclusive inputs-and-outputs guide
covering all 48 NextAds jobs declared in this checkout, start with
[`nextads_job_table_flow.md`](nextads_job_table_flow.md).

The shared route runs in the `DEV_FEATURE_STORE` target and writes to
`marketingdata_dev.nextads_feature_store`. It is a model-building layer, not
part of live production delivery.

```mermaid
flowchart TD
  subgraph sources["Source data and artifacts"]
    operational["Operational warehouse sources<br/>customer, control, item, web, actions and assignments"]
    theme_source["Theme Affinity outputs"]
    analytics_source["Analytics pCTR source tables"]
    embedding_model["Registered product-embedding model"]
    historical["Historical Theme Affinity preparation<br/>explicit training date only"]
  end

  subgraph job["mktg_next_uk_nextads_feature_store"]
    resolve["resolve_feature_store_reference_date"]
    create["create_feature_store_tables"]
    analytics["refresh_analytics_pctr_feature_source<br/>child job"]
    preflight["preflight_feature_store_sources"]
    account["build_account_features"]
    advert["build_advert_features"]
    embeddings["build_product_embeddings_latest"]
    advert_product["build_advert_product_profile_daily"]
    advert_semantic["build_advert_semantic_profile_daily"]
    seasonal["build_seasonal_product_demand_daily"]
    theme["build_theme_affinity_features"]
    pctr["build_pctr_affinity_features"]
    training["build_theme_affinity_training_input<br/>skips unless a historical date is supplied"]
    inputs["build_model_inputs"]
    quality["quality_checks"]
  end

  subgraph outputs["Output groups"]
    reusable["Account, advert, item, embedding,<br/>semantic and seasonal features"]
    affinities["Theme and account-advert affinities<br/>plus session context"]
    model_inputs["Theme Affinity and Analytics pCTR<br/>model inputs and compatibility views"]
    labels["Click and theme-response labels"]
    training_output["Historical Theme Affinity training input"]
    metadata["Feature builds, exact source/output versions<br/>and READY snapshot bindings"]
    events["Feature quality events"]
  end

  operational --> resolve
  resolve --> create
  create --> analytics
  analytics_source --> analytics
  create --> preflight
  analytics --> preflight
  operational --> preflight
  theme_source --> preflight

  preflight --> account
  preflight --> advert
  preflight --> embeddings
  embedding_model --> embeddings

  embeddings --> advert_product
  advert --> advert_semantic
  embeddings --> advert_semantic
  advert --> seasonal
  embeddings --> seasonal

  account --> theme
  advert --> theme
  account --> pctr
  advert --> pctr

  create --> training
  historical --> training
  theme --> inputs
  pctr --> inputs

  inputs --> quality
  training --> quality
  advert_product --> quality
  advert_semantic --> quality
  seasonal --> quality

  account --> reusable
  advert --> reusable
  embeddings --> reusable
  advert_product --> reusable
  advert_semantic --> reusable
  seasonal --> reusable
  theme --> affinities
  pctr --> affinities
  pctr --> model_inputs
  inputs --> model_inputs
  theme --> labels
  inputs --> labels
  training --> training_output
  account --> metadata
  advert --> metadata
  embeddings --> metadata
  theme --> metadata
  pctr --> metadata
  inputs --> metadata
  advert_product --> metadata
  advert_semantic --> metadata
  seasonal --> metadata
  training --> metadata
  quality --> events
```

The job keeps reusable feature creation separate from final scoring and
decisioning. It does not create production rankings, assignments or delivery
payloads.

Use the following documents for detail rather than repeating it here:

- [Feature Store README](../feature_store/README.md) for delivery gates and
  current evidence.
- [`feature_store_table_design.md`](../feature_store/feature_store_table_design.md) for
  table grain, keys, dates, ownership and refresh expectations.
- [`migration_backlog.md`](../feature_store/migration_backlog.md) for remaining
  migration and environment gates.
- [`building_a_challenger_model.md`](../feature_store/building_a_challenger_model.md)
  for the model-author route that consumes READY snapshots.

# Feature Store Job Flow

This page shows the dependency order inside `mktg_next_uk_nextads_feature_store`. For the inclusive inputs-and-outputs guide covering all 40 NextAds jobs declared in this checkout, start with [`nextads_job_table_flow.md`](nextads_job_table_flow.md).

The shared route runs in the `DEV_FEATURE_STORE` target and writes to `marketingdata_dev.nextads_feature_store`. It is a model-building layer, not part of live production delivery.

```mermaid
flowchart TD
  subgraph sources["Source data and artifacts"]
    operational["Operational warehouse sources<br/>customer, control, item, web, actions and assignments"]
    theme_source["Theme Affinity outputs"]
    embedding_model["Registered product-embedding model"]
    historical["Historical Theme Affinity preparation<br/>explicit training date only"]
  end

  subgraph job["mktg_next_uk_nextads_feature_store"]
    resolve["resolve_feature_store_reference_date"]
    create["create_feature_store_tables"]
    analytics_base["analytics_pctr_base_sessions"]
    analytics_core["analytics_pctr_core_datasets<br/>and customer-advert base"]
    analytics_parallel["session, CTR, page-view, purchase,<br/>exposure and affinity branches"]
    analytics_combine["analytics_pctr_combine_features"]
    analytics_receipt["receipt_analytics_pctr_feature_source<br/>exact Delta version"]
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
  create --> analytics_base
  operational --> analytics_base
  analytics_base --> analytics_core --> analytics_parallel --> analytics_combine --> analytics_receipt
  create --> preflight
  analytics_receipt --> preflight
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
  analytics_receipt --> metadata
  quality --> events
```

The retained Analytics pCTR SQL now runs as source-building tasks inside this job. The receipt task pins the combined source before preflight and publication, so there is no separate Analytics pCTR source saved job or child-job dependency.

The job keeps reusable source and feature creation separate from final model scoring and decisioning. It does not create production rankings, assignments or delivery payloads.

## Declared Model Consumption And Research Boundary

Models supported by the shared lifecycle use the same declaration and accepted-snapshot contract. Shopping Bag is currently the only model supported through that full route:

```mermaid
flowchart LR
  declaration["Model and research declaration<br/>nextads_models.yaml"]
  builders["Accepted reusable or on-demand<br/>Feature Store builders"]
  snapshots["READY Feature Store snapshots"]
  lifecycle["Centrally owned model-development job<br/>build, research, select or evaluate"]
  receipt["Point-in-time TrainingSetReceipt"]
  model["Registered DEV model"]
  frame["Immutable PII-reduced research frame<br/>at one exact Delta version"]
  parent["MLflow parent research run"]
  candidates_run["Declared nested candidate runs"]
  evidence["Comparable validation evidence<br/>and readable explanations"]
  recommendation["Automatic recommendation"]
  selection["AUTO or durable reviewed selection"]
  selected_test["Selected candidate only<br/>untouched test evidence"]
  selected_model["Selected registered DEV model"]
  automl["Optional bounded AutoML discovery"]
  candidates["Accepted SB1 and SB2 candidate build"]
  evaluation["Isolated evaluation scores"]
  serving["Serving portfolios, assignments and payloads"]

  declaration --> lifecycle
  builders --> snapshots --> lifecycle
  lifecycle --> receipt
  receipt --> model
  receipt --> frame --> parent --> candidates_run --> evidence --> recommendation
  recommendation --> selection --> selected_test --> selected_model
  frame -. "disabled by default" .-> automl
  model --> evaluation
  selected_model --> evaluation
  snapshots --> evaluation
  candidates --> evaluation
  evaluation -. "no write" .-> serving
```

The lifecycle job reads the exact Delta versions recorded by READY Feature Store snapshots and saves those bindings before training. Point-in-time joins prevent later information from leaking into earlier observations. Shopping Bag's account-activity and label builders remain on-demand Python entry points; the scheduled Feature Store job does not rebuild those two inputs.

`BUILD` trains the existing declared route. `RESEARCH` compares model options on the same fixed data and keeps the final test period hidden. `REVIEW_SELECT` records the chosen model and tests only that choice. `EVALUATE` pins an exact registered model, READY features and an accepted advert-candidate attempt, then writes isolated comparison scores. None of these operations changes serving score selection, assignments or payloads.

AutoML remains a separate, manually enabled DEV discovery route. It cannot register or activate a winner. Analytics pCTR is declared for compatibility but is not currently supported end to end through these shared operations.

Use the following documents for detail rather than repeating it here:

- [Feature Store README](../feature_store/README.md) for delivery gates and current evidence.
- [`feature_store_table_design.md`](../feature_store/feature_store_table_design.md) for table grain, keys, dates, ownership and refresh expectations.
- [`migration_backlog.md`](../feature_store/migration_backlog.md) for remaining migration and environment gates.
- [Model research: data scientist guide](../model_research_walkthrough.md) for model declarations, comparison rules, evidence, retries and the worked Shopping Bag example.

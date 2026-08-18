# Feature Store Flow

The shared Feature Store route runs in the `DEV_FEATURE_STORE` target and writes
to `marketingdata_dev.nextads_feature_store`. It is a model-building layer, not
part of live production delivery.

```mermaid
flowchart TD
  subgraph sources["Source tables"]
    theme_prod["PROD Theme Affinity outputs<br/>marketingdata_prod.warehouse"]
    prod_core["Existing PROD source tables<br/>customer, control, item, web, actions"]
    analytics["Versioned Analytics pCTR feature output<br/>exact Delta receipt"]
    historical["Historical Theme Affinity prep<br/>explicit training date only"]
  end

  subgraph job["mktg_next_uk_nextads_feature_store"]
    create["create_feature_store_tables"]
    preflight["preflight_feature_store_sources"]
    account["build_account_features"]
    advert["build_advert_features"]
    theme["build_theme_affinity_features"]
    pctr["build_pctr_affinity_features<br/>Analytics model input plus reusable affinity/session features"]
    training["build_theme_affinity_training_input<br/>skips unless historical date supplied"]
    inputs["build_model_inputs"]
    quality["quality_checks"]
  end

  subgraph tables["Feature Store outputs"]
    base["Reusable base features<br/>account, web, advert, item, theme"]
    latest["next_uk_nextads_fs_theme_affinity_model_input<br/>daily/latest model-input contract"]
    labelled["next_uk_nextads_fs_theme_affinity_training_input<br/>historical labelled training data"]
    analytics_input["next_uk_nextads_fs_pctr_model_input<br/>Analytics pCTR model columns"]
    views["Compatibility views<br/>model-shaped reads"]
    events["next_uk_nextads_fs_feature_quality_events"]
  end

  theme_prod --> preflight
  prod_core --> preflight
  historical --> training
  analytics --> pctr

  create --> preflight
  create --> training
  preflight --> account
  preflight --> advert
  account --> theme
  advert --> theme
  account --> pctr
  advert --> pctr
  theme --> inputs
  pctr --> inputs
  inputs --> quality

  account --> base
  advert --> base
  theme --> base
  inputs --> latest
  training --> labelled
  pctr --> analytics_input
  latest --> views
  analytics_input --> views
  quality --> events
```

`next_uk_nextads_fs_theme_affinity_model_input` is the daily/latest model-input
contract. `next_uk_nextads_fs_theme_affinity_training_input` is historical
labelled training data and requires an explicit historical training reference
date.

Feature Store creates reusable features and model inputs. It does not create
final assignment, ranking, delivery or production scoring decisions.

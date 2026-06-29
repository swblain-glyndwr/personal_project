# Next Ads Feature Store Reusable Feature Inventory

Azure Boards story: 5111856
Feature: 5111595 - Reusable feature layer (Databricks Feature Store)

## Purpose

This inventory maps reusable signals from the existing Theme Affinity/LTR-style pipeline, hackathon jobs, response-model/pCTR jobs, and CWB analytics pCTR work into the first Next Ads feature-store contracts.

The target shape is model-neutral. The feature store is for Next Ads-wide reusable batch features, not a pCTR-specific store. Current pCTR and LTR/theme affinity work should consume these governed feature tables instead of rebuilding source-specific feature joins inline.

## Existing Outputs and Reusable Signals

| Source area | Current reusable signals | Current grain | Feature-store target |
| --- | --- | --- | --- |
| Theme Affinity/hackathon model | Month, basket/view behaviour, recency/frequency, theme retrieval counts, repurchase stage, user totals, theme seasonal demand, simple rules rank | Account, theme, model reference date | `next_uk_nextads_fs_account_theme_affinity_daily`, `next_uk_nextads_fs_theme_affinity_model_input` |
| Theme interaction SQL | Views, baskets, add-to-bag, repurchase and derived theme interactions | Account, theme, reference date | `next_uk_nextads_fs_account_theme_interactions_daily` |
| Theme/global popularity SQL | Theme demand, trend, yearly comparison and global popularity signals | Theme, reference date | `next_uk_nextads_fs_theme_popularity_daily` |
| Analytics pCTR | Viewed/purchased advert category affinity, customer-ad impressions, rule-based affinity features | Account, advert, location, reference date | `next_uk_nextads_fs_account_advert_affinity_daily`, `next_uk_nextads_fs_pctr_model_input` |
| Response-model/pCTR customer behaviour | Account descriptors, lifecycle, order/spend/return history, browse sessions, web recency, add-to-bag and shopping-bag activity | Account, reference date | `next_uk_nextads_fs_account_profile`, `next_uk_nextads_fs_account_web_activity_90d` |
| Response-model/pCTR advert metadata | Control-sheet advert metadata, placement, creative text, campaign/category/theme/brand fields and linked item counts | Advert, location, feature date | `next_uk_nextads_fs_advert_core_daily` |
| Response-model/pCTR advert attributes | Rolled-up linked-item attributes, top brand/category/colour/use/style/department/gender and coverage metrics | Advert, feature date | `next_uk_nextads_fs_advert_attribute_profile_daily` |
| Response-model/pCTR semantic features | Advert text corpus, text embedding dimensions, linked item semantic fields and semantic coverage | Advert, feature date, embedding model/version | `next_uk_nextads_fs_advert_semantic_profile_daily` |
| Response-model/pCTR product embeddings | Item/product embeddings, advert product vectors and embedding coverage inputs | Item/product or advert, feature date, embedding model/version | `next_uk_nextads_fs_product_embeddings_latest`, `next_uk_nextads_fs_advert_product_profile_daily` |
| Response-model/pCTR seasonal features | Same-month-last-year, 7-day, 30-day and trend product demand features | Product/ad/account entity, item, feature date | `next_uk_nextads_fs_seasonal_product_demand_daily` |
| Response-model/pCTR labels | Observed Shopping Bag exposures, tagged-click labels and attribution windows | Account, advert, location, session date, label horizon | `next_uk_nextads_fs_labels_clicks` |
| Theme Affinity labels | Theme response labels and targets produced by the current theme pipeline | Account, theme, reference date, label name | `next_uk_nextads_fs_labels_theme_response` |

## Embedding Outputs and Version Requirements

Embedding-derived feature tables must preserve:

- `embedding_model_name`
- `embedding_model_version`
- `embedding_model_uri` where available
- vector columns or vector arrays used by model training
- scalar dimensions where Spark ML pipelines require fixed numeric columns
- coverage metrics showing whether both sides of a comparison had usable vectors

The initial feature-store contracts include these requirements in:

- `next_uk_nextads_fs_product_embeddings_latest`
- `next_uk_nextads_fs_advert_semantic_profile_daily`
- `next_uk_nextads_fs_advert_product_profile_daily`
- `next_uk_nextads_fs_seasonal_product_demand_daily`
- `next_uk_nextads_fs_pctr_model_input`


## Initial Feature-Store Candidates

The first registration/population candidates should be small enough to validate permissions and table lifecycle in DEV without changing production behaviour:

| Priority | Table | Why first |
| --- | --- | --- |
| 1 | `next_uk_nextads_fs_account_profile` | Stable customer/account-grain table with clear primary key and broad model reuse. |
| 2 | `next_uk_nextads_fs_account_web_activity_90d` | Reusable customer activity features needed by pCTR and LTR. |
| 3 | `next_uk_nextads_fs_advert_core_daily` | Stable advert/date metadata contract for ad-level models and compatibility views. |
| 4 | `next_uk_nextads_fs_product_embeddings_latest` | Establishes embedding metadata/version contract for pCTR feature work and future offline analysis. |
| 5 | `next_uk_nextads_fs_account_theme_affinity_daily` | Allows current Theme Affinity/LTR-style model to move behind a compatibility view. |
| 6 | `next_uk_nextads_fs_pctr_model_input` | Allows pCTR work to consume a governed model-ready input. |

## Initial Exclusions

The following stay out of the first feature-store slice:

- Existing production output tables, including `next_uk_next_ads_hackathon_model_latest`, `next_uk_next_ads_hackathon_model_full`, hackathon prerank/theme score outputs, and current pCTR score outputs.
- Full online serving store support.
- Candidate similarity diagnostics. That work is a separate offline follow-up and is not part of the first production model contracts.


## Acceptance Criteria Mapping

| Acceptance criterion | Evidence in this document/branch |
| --- | --- |
| Existing feature outputs identified | Inventory table above. |
| Grain/key/date fields recorded | Registry in `configs/features/nextads_feature_store.yaml`; table design doc for story 5111861. |
| Embedding outputs/model/version requirements identified | Embedding section above. |
| Initial Feature Store candidates agreed | Initial candidate table above. |

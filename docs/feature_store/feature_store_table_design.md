# Next Ads Feature Store Table Design

Azure Boards story: 5111861
Feature: 5111595 - Reusable feature layer (Databricks Feature Store)

## Contract Scope And Ownership

This document is the human-readable design for all 22 physical Feature Store
contracts and both compatibility views currently declared for Next Ads. It owns
their grain, keys, date boundary, refresh expectation and training-safety state;
the Feature Store README owns delivery status and runtime evidence.

The executable contract lives in
[`configs/features/nextads_feature_store.yaml`](../../configs/features/nextads_feature_store.yaml)
and the matching SQL schema files under
[`sql/features/nextads/`](../../sql/features/nextads/). If this page and the
registry disagree, change the registry and this page together. The Databricks
setup job uses `FeatureEngineeringClient.create_table`, so the physical tables
are created through the Databricks Feature Engineering route rather than plain
SQL `CREATE TABLE`.

## Feature Store Environment Bindings

| Environment phase | Catalog | Schema |
| --- | --- | --- |
| Branch/SANDBOX smoke | `marketingdata_dev` | `${workspace.current_user.short_name}` via `feature_store_schema` |
| DEV pipeline | `marketingdata_dev` | Normalised `${var.git_last_commit_user_name}` via `feature_store_schema`, e.g. `Stephen_Blain` becomes `stephen_blain` |
| DEV integration | `marketingdata_dev` | `nextads_integration` via `feature_store_schema` |
| Shared DEV feature store | `marketingdata_dev` | `nextads_feature_store` via `DEV_FEATURE_STORE` |
| Future production publication | `marketingdata_prod` | Dedicated feature-store schema only after specific feature contracts need production runtime or monitoring use |

The branch includes `feature_store_schema` as an explicit bundle variable per target so development runs follow the repo pattern and shared environments use governed schemas. Feature-store paths normalise user schema values to the repo's lower-case Databricks schema convention before validation or writes. The registry fallback is `nextads_feature_store` for manual use, but DAB jobs should always pass the target-specific schema value.

The shared DEV feature-store job is scheduled daily at 21:00 Europe/London. It defaults to stable production Theme Affinity outputs in `marketingdata_prod.warehouse` as the first source and writes reusable model-building features to `marketingdata_dev.nextads_feature_store`. The deployed job also exposes source catalog/schema/prefix job parameters, so manual DEV validation can temporarily point reads at `marketingdata_dev.<schema>` Theme Affinity outputs while production materialized-view access for the DEV service principal is agreed.

Every physical contract is owned by `marketing_data`. Per-table differences in
cadence, state and training safety are listed below.

## Account Feature Tables

| State | Table | Grain | Primary keys | Date key | Refresh | Training safe |
| --- | --- | --- | --- | --- | --- | --- |
| `ACTIVE` | `next_uk_nextads_fs_account_profile` | Account/reference date | `account_number`, `reference_date` | `reference_date` | Daily | Yes |
| `ACTIVE` | `next_uk_nextads_fs_account_web_activity_90d` | Account/reference date | `account_number`, `reference_date` | `reference_date` | Daily | Yes |
| `ACTIVE` | `next_uk_nextads_fs_shopping_bag_account_activity_90d` | Active web account/reference date | `account_number`, `reference_date` | `reference_date` | On demand | Yes |

These tables provide reusable account descriptors, lifecycle fields, recency,
browse activity, page views, add-to-bag activity and bounded Shopping Bag
context.

## Item, Advert And Product Feature Tables

| State | Table | Grain | Primary keys | Date key | Refresh | Training safe |
| --- | --- | --- | --- | --- | --- | --- |
| `ACTIVE` | `next_uk_nextads_fs_item_attributes_latest` | Item | `item_id` | None; latest lookup | Daily | Yes |
| `ACTIVE` | `next_uk_nextads_fs_product_embeddings_latest` | Item/embedding model version | `item_id`, `embedding_model_name`, `embedding_model_version` | None; latest lookup | Daily | Yes |
| `ACTIVE` | `next_uk_nextads_fs_advert_core_daily` | Advert/location/feature date | `advert_id`, `location`, `feature_date` | `feature_date` | Daily | Yes |
| `ACTIVE` | `next_uk_nextads_fs_advert_attribute_profile_daily` | Advert/feature date | `advert_id`, `feature_date` | `feature_date` | Daily | Yes |
| `ACTIVE` | `next_uk_nextads_fs_advert_semantic_profile_daily` | Advert/feature date/embedding model version | `advert_id`, `feature_date`, `embedding_model_name`, `embedding_model_version` | `feature_date` | Daily | Yes |
| `ACTIVE` | `next_uk_nextads_fs_advert_product_profile_daily` | Advert/feature date/embedding model version | `advert_id`, `feature_date`, `embedding_model_name`, `embedding_model_version` | `feature_date` | Daily | Yes |
| `ACTIVE` | `next_uk_nextads_fs_seasonal_product_demand_daily` | Entity/product/feature date | `entity_type`, `entity_id`, `item_id`, `feature_date` | `feature_date` | Daily | Yes |

These tables separate stable advert metadata, rolled-up item attributes,
semantic and product profiles, reusable embeddings and seasonal demand. Model
jobs can bind their exact snapshots without copying notebook-owned shapes.

## Theme And Account-Advert Feature Tables

| State | Table | Grain | Primary keys | Date key | Refresh | Training safe |
| --- | --- | --- | --- | --- | --- | --- |
| `ACTIVE` | `next_uk_nextads_fs_account_theme_interactions_daily` | Account/theme/reference date | `account_number`, `theme`, `reference_date` | `reference_date` | Daily | Yes |
| `ACTIVE` | `next_uk_nextads_fs_account_theme_affinity_daily` | Account/theme/reference date | `account_number`, `theme`, `reference_date` | `reference_date` | Daily | Yes |
| `ACTIVE` | `next_uk_nextads_fs_theme_popularity_daily` | Theme/reference date | `theme`, `reference_date` | `reference_date` | Daily | Yes |
| `ACTIVE` | `next_uk_nextads_fs_account_advert_affinity_daily` | Account/advert/reference date | `account_number`, `advert_id`, `reference_date` | `reference_date` | Daily | Yes |
| `ACTIVE` | `next_uk_nextads_fs_session_context_daily` | Account/session/session date | `account_number`, `session_id`, `session_date` | `session_date` | Daily | Yes |

## Model Assembly And Labels

| State | Table | Grain | Primary keys | Date key | Refresh | Training safe |
| --- | --- | --- | --- | --- | --- | --- |
| `COMPATIBILITY` | `next_uk_nextads_fs_theme_affinity_model_input` | Account/theme/reference date | `account_number`, `theme`, `reference_date` | `reference_date` | Daily | Yes |
| `COMPATIBILITY` | `next_uk_nextads_fs_theme_affinity_training_input` | Labelled account/theme/reference date | `account_number`, `theme`, `reference_date` | `reference_date` | On demand | Yes |
| `COMPATIBILITY` | `next_uk_nextads_fs_pctr_model_input` | Analytics pCTR account/advert/reference date | `account_number`, `advert_id`, `reference_date` | `reference_date` | Daily | Yes |
| `COMPATIBILITY` | `next_uk_nextads_fs_labels_clicks` | Account/advert/location/session date/horizon | `account_number`, `advert_id`, `location`, `session_date`, `label_horizon_days` | `session_date` | Daily | **No: legacy inferred label** |
| `ACTIVE` | `next_uk_nextads_fs_shopping_bag_click_labels` | Observed Shopping Bag advert impression/mature horizon | `exposure_id`, `label_horizon_days`, `exposure_timestamp` | `exposure_timestamp` | Daily | Yes |
| `ACTIVE` | `next_uk_nextads_fs_labels_theme_response` | Account/theme/reference date/label | `account_number`, `theme`, `reference_date`, `label_name` | `reference_date` | Daily | Yes |

Model assembly tables are intentionally separated from base feature tables.
The inferred `next_uk_nextads_fs_labels_clicks` contract is retained only for
compatibility and must not be presented as an observed training label. The
worked Shopping Bag route uses
`next_uk_nextads_fs_shopping_bag_click_labels`.

## Feature Quality Event Contract

| State | Table | Grain | Primary keys | Date key | Refresh | Training safe |
| --- | --- | --- | --- | --- | --- | --- |
| `ACTIVE` | `next_uk_nextads_fs_feature_quality_events` | Feature table/check/run timestamp | `table_name`, `check_name`, `run_timestamp` | `run_timestamp` | Per run | No |

## Compatibility Views

| View | Physical source | Consumers |
| --- | --- | --- |
| `next_uk_nextads_theme_affinity_features_latest` | `next_uk_nextads_fs_theme_affinity_model_input` | Theme Affinity and LTR compatibility |
| `next_uk_nextads_pctr_features_latest` | `next_uk_nextads_fs_pctr_model_input` | Analytics pCTR compatibility |

These views are read-only compatibility names. They are not additional physical
feature contracts and are not counted in the 22-table total.

Every feature table contract should carry build metadata columns where relevant:

- `reference_date`, `feature_date` or `session_date`
- `created_at` or `updated_at`
- source/build identifiers where available
- embedding model metadata for vector-backed features

## Feature Store Runtime Permissions

DEV validation requires:

- Ability to create schemas/tables in the target DEV catalog/schema.
- Ability to call `FeatureEngineeringClient.create_table` from the Databricks runtime.
- Ability for the job cluster service principal/user to read source tables and write Delta feature tables.
- No writes to PROD targets or existing operational Next Ads output tables.

The bundle route deploys a manual personal copy to `DEV`, writing to the normal commit-author schema, and a scheduled shared copy to `DEV_FEATURE_STORE`, writing to `marketingdata_dev.nextads_feature_store`. The personal copy is limited to one concurrent run and has no schedule.

## Feature Table Design Acceptance Criteria

| Acceptance criterion | Evidence in this document/branch |
| --- | --- |
| Complete physical feature catalogue defined | The four table sections above cover all 22 registry contracts. |
| Compatibility views identified | Compatibility Views section. |
| Primary keys and snapshot dates defined | Table design sections and registry. |
| Refresh frequency and ownership recorded | Environment binding and table contract sections above. |
| Location and permission requirements documented | Target location and permissions sections. |

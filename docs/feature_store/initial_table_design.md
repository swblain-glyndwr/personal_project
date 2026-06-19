# Next Ads Feature Store Initial Table Design

Azure Boards story: 5111861
Feature: 5111595 - Reusable feature layer (Databricks Feature Store)

## Purpose

This document defines the first batch Databricks Feature Engineering table design for reusable Next Ads features.

The executable contract lives in `configs/features/nextads_feature_store.yaml` and the matching SQL schema files under `sql/features/nextads/`. The Databricks setup job uses `databricks.feature_engineering.FeatureEngineeringClient.create_table` so the physical tables are created through the Databricks feature-engineering route rather than plain SQL `CREATE TABLE`.

## Target Location

| Environment phase | Catalog | Schema |
| --- | --- | --- |
| Branch/SANDBOX smoke | `marketingdata_dev` | `${workspace.current_user.short_name}` via `feature_store_schema` |
| DEV pipeline | `marketingdata_dev` | Normalised `${var.git_last_commit_user_name}` via `feature_store_schema`, e.g. `Stephen_Blain` becomes `stephen_blain` |
| DEV integration | `marketingdata_dev` | `nextads_integration` via `feature_store_schema` |
| Future PREPROD/PROD | `marketingdata_prod` | `nextads_feature_store`, after permission and migration sign-off |

The branch includes `feature_store_schema` as an explicit bundle variable per target so development runs follow the repo pattern and shared environments use governed schemas. Feature-store paths normalise user schema values to the repo's lower-case Databricks schema convention before validation or writes. The registry fallback is `nextads_feature_store` for manual use, but DAB jobs should always pass the target-specific schema value.

## Initial Customer Feature Tables

| Table | Grain | Primary keys | Snapshot/date key | Refresh | Owner |
| --- | --- | --- | --- | --- | --- |
| `next_uk_nextads_fs_account_profile` | One row per account and reference date | `account_number`, `reference_date` | `reference_date` | Daily | `marketing_data` |
| `next_uk_nextads_fs_account_web_activity_90d` | One row per account and reference date | `account_number`, `reference_date` | `reference_date` | Daily | `marketing_data` |

These are the preferred first customer tables to register and populate. They provide reusable account descriptors, account lifecycle fields, recency, browse activity, page views, add-to-bag activity and shopping-bag context needed by current models (LTR/theme affinity, pCTR, etc.) and any future models.

## Initial Advert and Embedding Feature Tables

| Table | Grain | Primary keys | Snapshot/date key | Refresh | Owner |
| --- | --- | --- | --- | --- | --- |
| `next_uk_nextads_fs_advert_core_daily` | One row per advert, location and feature date | `advert_id`, `location`, `feature_date` | `feature_date` | Daily | `marketing_data` |
| `next_uk_nextads_fs_advert_attribute_profile_daily` | One row per advert and feature date | `advert_id`, `feature_date` | `feature_date` | Daily | `marketing_data` |
| `next_uk_nextads_fs_advert_semantic_profile_daily` | One row per advert, feature date and embedding model/version | `advert_id`, `feature_date`, `embedding_model_name`, `embedding_model_version` | `feature_date` | Daily | `marketing_data` |
| `next_uk_nextads_fs_product_embeddings_latest` | One row per item and embedding model/version | `item_id`, `embedding_model_name`, `embedding_model_version` | None; latest lookup | Weekly | `marketing_data` |

These tables separate stable advert metadata, rolled-up product attributes, semantic text/image/product embedding features, and reusable product embeddings. That lets work reuse the same advert-side contracts without copying notebook outputs directly into model jobs.

## Model Assembly and Labels

| Table | Grain | Primary keys | Snapshot/date key | Consumer |
| --- | --- | --- | --- | --- |
| `next_uk_nextads_fs_theme_affinity_model_input` | Account, theme, reference date | `account_number`, `theme`, `reference_date` | `reference_date` | Theme Affinity/LTR |
| `next_uk_nextads_fs_pctr_model_input` | Account, advert, location, session date, reference date | `account_number`, `advert_id`, `location`, `session_date`, `reference_date` | `reference_date` | pCTR |
| `next_uk_nextads_fs_labels_clicks` | Account, advert, location, session date and label horizon | `account_number`, `advert_id`, `location`, `session_date`, `label_horizon_days` | `session_date` | pCTR/LTR |
| `next_uk_nextads_fs_labels_theme_response` | Account, theme, reference date and label name | `account_number`, `theme`, `reference_date`, `label_name` | `reference_date` | Theme Affinity/LTR |

Model assembly tables are intentionally separated from base feature tables. They can join reusable feature groups into current model-ready shapes while preserving compatibility for existing model consumers.

## Quality and Metadata

| Table | Grain | Primary keys | Purpose |
| --- | --- | --- | --- |
| `next_uk_nextads_fs_feature_quality_events` | Feature table, check and run timestamp | `table_name`, `check_name`, `run_timestamp` | Records row counts, key uniqueness, null-rate, freshness and build metadata per feature table/run. |

Every feature table contract should carry build metadata columns where relevant:

- `reference_date`, `feature_date` or `session_date`
- `created_at` or `updated_at`
- source/build identifiers where available
- embedding model metadata for vector-backed features

## Permissions and Setup Requirements

DEV validation requires:

- Ability to create schemas/tables in the target DEV catalog/schema.
- Ability to call `FeatureEngineeringClient.create_table` from the Databricks runtime.
- Ability for the job cluster service principal/user to read source tables and write Delta feature tables.
- No writes to PROD targets or existing operational Next Ads output tables.

The branch creates a paused, opt-in DAB job only for `SANDBOX`, `DEV` and `DEV_INTEGRATION`.

## Acceptance Criteria Mapping

| Acceptance criterion | Evidence in this document/branch |
| --- | --- |
| Initial customer feature table defined | Customer feature tables section. |
| Initial advert or embedding feature table defined | Advert and embedding feature tables section. |
| Primary keys and snapshot dates defined | Table design sections and registry. |
| Refresh frequency and ownership recorded | Customer/advert table sections and registry. |
| Location and permission requirements documented | Target location and permissions sections. |

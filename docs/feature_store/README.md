# Next Ads Feature Store Documentation

Feature: 5111595 - Reusable feature layer (Databricks Feature Store)  
Documentation/backlog story: 5111881

## Scope

This folder documents the first repo-owned Next Ads Databricks Feature Store route.

The implementation is intentionally batch/offline first. It creates governed Databricks Feature Engineering table contracts and a paused development-only DAB job so existing production Theme Affinity, hackathon, response-model and pCTR outputs are not renamed or replaced in this slice.

## Documents

| Document | Story | Purpose |
| --- | --- | --- |
| `reusable_feature_inventory.md` | 5111856 | Existing reusable signals and first migration candidates. |
| `initial_table_design.md` | 5111861 | Initial customer, advert, embedding, model-input and quality table design. |
| `candidate_similarity.md` | Follow-up | Offline candidate similarity diagnostics concept; not part of current production model inputs. |
| `migration_backlog.md` | 5111881 | Prioritised migration backlog and dependencies. |

## Executable Contracts

The repo-owned executable contract is split across:

- `configs/features/nextads_feature_store.yaml` for table names, grain, primary keys, owner, freshness and consumers.
- `sql/features/nextads/` for table schemas consumed by the setup script.
- `scripts/table_operations/create_feature_store_tables.py` for Databricks Feature Engineering table creation.
- `resources/jobs/mktg_next_uk_nextads_feature_store.yml` for the paused development-only DAB job.
- `jobs/features/nextads/` for build-entrypoint scaffolds.

The docs should explain intent and migration order. The registry and SQL contracts remain the source of truth for physical table shape.

The Theme Affinity/LTR entrypoints now materialise the first populated feature-store slice from existing hackathon/Theme Affinity outputs through the Databricks Feature Engineering client. Account, advert and CWB pCTR jobs remain scaffold/dependency-only until their source contracts are migrated.

## Feature Catalogue

| Feature group | Physical table/view | Entity/grain | Primary consumers |
| --- | --- | --- | --- |
| Account profile | `next_uk_nextads_fs_account_profile` | Account/reference date | Theme Affinity, pCTR, LTR |
| Account web activity | `next_uk_nextads_fs_account_web_activity_90d` | Account/reference date | pCTR, LTR |
| Item attributes | `next_uk_nextads_fs_item_attributes_latest` | Item | pCTR, LTR |
| Product embeddings | `next_uk_nextads_fs_product_embeddings_latest` | Item/model version | pCTR |
| Advert core | `next_uk_nextads_fs_advert_core_daily` | Advert/location/feature date | pCTR, LTR |
| Advert attribute profile | `next_uk_nextads_fs_advert_attribute_profile_daily` | Advert/feature date | pCTR, LTR |
| Advert semantic profile | `next_uk_nextads_fs_advert_semantic_profile_daily` | Advert/feature date/model version | pCTR |
| Advert product profile | `next_uk_nextads_fs_advert_product_profile_daily` | Advert/feature date | pCTR |
| Seasonal product demand | `next_uk_nextads_fs_seasonal_product_demand_daily` | Entity/product/feature date | pCTR |
| Account theme interactions | `next_uk_nextads_fs_account_theme_interactions_daily` | Account/theme/reference date | Theme Affinity, LTR |
| Account theme affinity | `next_uk_nextads_fs_account_theme_affinity_daily` | Account/theme/reference date | Theme Affinity, LTR |
| Theme popularity | `next_uk_nextads_fs_theme_popularity_daily` | Theme/reference date | Theme Affinity, LTR |
| Account advert affinity | `next_uk_nextads_fs_account_advert_affinity_daily` | Account/advert/location/reference date | pCTR, LTR |
| Session context | `next_uk_nextads_fs_session_context_daily` | Account/session/session date | pCTR |
| Theme model input | `next_uk_nextads_fs_theme_affinity_model_input` | Account/theme/reference date | Theme Affinity, LTR |
| pCTR model input | `next_uk_nextads_fs_pctr_model_input` | Account/advert/location/session/reference date | pCTR |
| Click labels | `next_uk_nextads_fs_labels_clicks` | Account/advert/location/session/horizon | pCTR, LTR |
| Theme labels | `next_uk_nextads_fs_labels_theme_response` | Account/theme/reference date/label | Theme Affinity, LTR |
| Quality events | `next_uk_nextads_fs_feature_quality_events` | Table/check/run timestamp | Feature-store operations |
| Theme compatibility view | `next_uk_nextads_theme_affinity_features_latest` | Current Theme Affinity model shape | Theme Affinity, LTR |
| pCTR compatibility view | `next_uk_nextads_pctr_features_latest` | Current pCTR model shape | pCTR |

## Ownership and Refresh

Initial owner is `marketing_data` for all feature tables. Most feature groups are daily refreshes keyed by `reference_date`, `feature_date` or `session_date`; product embeddings are weekly/latest until a source-change-driven refresh is introduced; quality events are per run.

The first development deployments should target `marketingdata_dev` with explicit target-specific schemas: SANDBOX uses the current user's schema, DEV uses the last commit author's schema normalised to the repo's lower-case user schema convention, and DEV_INTEGRATION uses `nextads_integration`. Future production setup should use `marketingdata_prod.nextads_feature_store` after write permissions and migration ownership are agreed.

## Dependencies

The feature-store route depends on:

- DEV Feature Engineering Client availability and write permissions.
- Existing source jobs remaining stable while compatibility views are proven.
- Existing Theme Affinity outputs being built before the feature-store materialisation job runs for `reference_date=predict`.
- CWB analytics pCTR source contracts being brought into the branch before pCTR feature tables are populated.
- Challenger testing before feature-store model inputs affect production ranking.
- Separate offline diagnostics stories before candidate-similarity work is added to the repo.

## Acceptance Criteria Mapping

| Acceptance criterion | Evidence |
| --- | --- |
| Feature catalogue created or updated | Feature catalogue section. |
| Initial table ownership and refresh approach recorded | Ownership and refresh section plus registry. |
| Remaining migrations listed and prioritised | `migration_backlog.md`. |
| Dependency on challenger testing and future decisioning work linked | Dependencies section and migration backlog. |

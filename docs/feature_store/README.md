# Next Ads Feature Store Documentation

Feature: 5111595 - Reusable feature layer (Databricks Feature Store)
Documentation/backlog story: 5111881

## Scope

This folder documents the first repo-owned Next Ads Databricks Feature Store route.

The implementation is intentionally batch/offline first. It creates governed Databricks Feature Engineering table contracts and a shared DEV feature-store job in `marketingdata_dev.nextads_feature_store` so model-building work has a stable reusable feature layer without changing production scoring or delivery outputs in this slice.

## Documents

| Document | Story | Purpose |
| --- | --- | --- |
| `reusable_feature_inventory.md` | 5111856 | Existing reusable signals and first migration candidates. |
| `initial_table_design.md` | 5111861 | Initial customer, advert, embedding, model-input and quality table design. |
| `candidate_similarity.md` | Follow-up | Offline candidate similarity diagnostics concept; not part of current production model inputs. |
| `migration_backlog.md` | 5111881 | Prioritised migration backlog and dependencies. |
| [`../architecture/feature_store_flow.md`](../architecture/feature_store_flow.md) | Architecture | Mermaid view of the shared DEV Feature Store flow and model-building boundaries. |
| [`../architecture/nextads_model_feature_overview.md`](../architecture/nextads_model_feature_overview.md) | Architecture | Wider NextAds model, Feature Store and MLflow overview. |

## Executable Contracts

The repo-owned executable contract is split across:

- `configs/features/nextads_feature_store.yaml` for logical definitions, delivery state, keys, ownership, freshness, consumers and DEV/PREPROD/PROD bindings.
- `sql/features/nextads/` for table schemas consumed by the setup script.
- `jobs/table_operations/create_feature_store_tables.py` for Databricks Feature Engineering table creation.
- `pipelines/databricks/jobs/mktg_next_uk_nextads_feature_store.yml` for personal, integration and shared DEV feature-store DAB jobs.
- `jobs/features/nextads/` for build-entrypoint scaffolds.

The docs should explain intent and migration order. The registry and SQL contracts remain the source of truth for physical table shape.

The first populated feature-store slice now materialises customer/account features, web activity, advert metadata, item attributes, advert attribute rollups, Theme Affinity latest model-input features, Theme Affinity labelled historical training input, Theme response labels and Shopping Bag click labels from stable production source tables through the Databricks Feature Engineering client. Embedding-derived advert/product tables, pCTR model input and candidate-similarity diagnostics remain scaffolded until their model/source contracts are promoted into this route.

The registry makes that distinction executable. `ACTIVE` identifies reusable or support tables with an implemented builder, `COMPATIBILITY` identifies implemented model-specific inputs retained during migration, and `SCAFFOLD` identifies table shells that must not be presented as implemented. Every scaffold lists its missing source or materialisation contracts.

Physical locations are separate `OfflineStoreBinding` records. DEV currently resolves to `marketingdata_dev.nextads_feature_store`, and its job target is declared in the repository. PREPROD resolves to release-isolated tables in `marketingdata_prod.ds_sandbox`, while PROD resolves to `marketingdata_prod.nextads_feature_store`; both remain plans until their deployment PRs land. Repository declaration is not evidence of a live job, populated table or READY snapshot. These bindings do not add PREPROD or PROD jobs in this contract-only change.

Use the local read-only plan to inspect the same logical graph across all three environments, including builders, current task dependencies, resolved locations or release templates, and missing contracts:

```powershell
.\.venv\Scripts\python.exe jobs\features\nextads\plan_offline_feature_store.py `
  --environment ALL --format text
```

To resolve exact PREPROD table names, add a release identifier:

```powershell
.\.venv\Scripts\python.exe jobs\features\nextads\plan_offline_feature_store.py `
  --environment PREPROD --release-id release/2026.08.12 --format text
```

The planner reports `CONTRACT_READY` only when the repository has an
implemented source contract for a compatibility view. It does not claim that a
table is deployed, populated or backed by a READY immutable snapshot.

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
| Theme latest model input | `next_uk_nextads_fs_theme_affinity_model_input` | Account/theme/reference date | Theme Affinity, LTR |
| Theme labelled training input | `next_uk_nextads_fs_theme_affinity_training_input` | Account/theme/reference date | Theme Affinity |
| pCTR model input | `next_uk_nextads_fs_pctr_model_input` | Account/advert/location/session/reference date | pCTR |
| Click labels | `next_uk_nextads_fs_labels_clicks` | Account/advert/location/session/horizon | pCTR, LTR |
| Theme labels | `next_uk_nextads_fs_labels_theme_response` | Account/theme/reference date/label | Theme Affinity, LTR |
| Quality events | `next_uk_nextads_fs_feature_quality_events` | Table/check/run timestamp | Feature-store operations |
| Theme compatibility view | `next_uk_nextads_theme_affinity_features_latest` | Current Theme Affinity model shape | Theme Affinity, LTR |
| pCTR compatibility view | `next_uk_nextads_pctr_features_latest` | Current pCTR model shape | pCTR |

## Ownership and Refresh

Initial owner is `marketing_data` for all feature tables. Most feature groups are daily refreshes keyed by `reference_date`, `feature_date` or `session_date`; product embeddings are weekly/latest until a source-change-driven refresh is introduced; quality events are per run.

The first development deployments target `marketingdata_dev` with explicit target-specific schemas: SANDBOX uses the current user's schema, DEV uses the last commit author's schema normalised to the repo's lower-case user schema convention, DEV_INTEGRATION uses `nextads_integration`, and DEV_FEATURE_STORE uses the shared `nextads_feature_store` schema.

`DEV_FEATURE_STORE` is scheduled daily at 21:00 Europe/London and reads latest Theme Affinity source tables from `marketingdata_prod.warehouse`. It writes reusable latest features to `marketingdata_dev.nextads_feature_store`. Theme Affinity training jobs read `marketingdata_dev.nextads_feature_store.next_uk_nextads_fs_theme_affinity_training_input`, which is populated only from an explicit historical run where the existing Theme Affinity prep builds a 31-day future-window basket target via `2_target.sql` and joins it back through `6_master_assoc.sql`.

The feature-store job exposes source-table job parameters so DEV runs can be pointed at DEV-owned Theme Affinity outputs while production source access is being agreed. For example, a manual DEV run can override `theme_source_catalog=marketingdata_dev`, `theme_source_schema=<user_schema_or_nextads_integration>`, and `theme_table_prefix=next_uk_nextads_account_theme_foundation`. This keeps the feature-store table creation and write path testable against ordinary Delta tables without requiring the DEV service principal to read Lakeflow-managed storage.

The labelled training-input build is controlled by `feature_store_theme_training_reference_date`. The default is `skip` so the daily latest-feature refresh does not silently build or overwrite training data. To create or refresh training data, run the feature-store route with a historical date at least 28 days old; the task stages historical Theme Affinity prep tables with `feature_store_theme_training_table_prefix`, validates that positives and negatives exist, then writes `next_uk_nextads_fs_theme_affinity_training_input`. Production feature-store publication is intentionally deferred to a later curated PR for specific stable feature contracts that need production runtime or monitoring use.

## Dependencies

The feature-store route depends on:

- DEV Feature Engineering Client availability and write permissions.
- Existing source jobs remaining stable while compatibility views are proven.
- Existing production Theme Affinity outputs being available before the shared feature-store materialisation job runs for `reference_date=predict`.
- Historical Theme Affinity prep sources having enough future-window basket data for the requested `feature_store_theme_training_reference_date`.
- Existing production customer, control-sheet, item-attribute, assignment and BigQuery web/action tables being available for the same resolved feature-store reference date.
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

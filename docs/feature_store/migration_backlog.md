# Next Ads Feature Store Migration Backlog

Azure Boards story: 5111881
Feature: 5111595 - Reusable feature layer (Databricks Feature Store)

## Migration Principles

- Keep existing operational outputs unchanged until output equivalence is proven.
- Register and populate small, reusable feature tables in the shared DEV feature store before wiring model jobs to them.
- Move consumers through compatibility views first, then native feature-store reads.
- Keep feature materialisation separate from model training, scoring and assignment jobs.
- Keep candidate similarity out of production model contracts until a separate offline diagnostics story is agreed.

## Prioritised Backlog

| Priority | Migration | Target tables/views | Notes |
| --- | --- | --- | --- |
| 1 | Confirm shared DEV schema, permissions and Feature Engineering Client registration | All setup tables | Run the shared DEV feature-store job against `marketingdata_dev.nextads_feature_store` and confirm table registration. |
| 2 | Populate Theme Affinity feature-store slice | Theme affinity feature tables, labels and model input | First populated slice; reads the operationalised Theme Affinity DEV Integration runtime tables and leaves operational outputs unchanged. |
| 3 | Validate Theme Affinity compatibility view | `next_uk_nextads_theme_affinity_features_latest` | Preserve current `hackathon_model/config.py` feature list while proving output equivalence. |
| 4 | Populate first customer feature table | `next_uk_nextads_fs_account_profile` | Start with account descriptors and reference-date metadata. Validate key uniqueness and row counts. |
| 5 | Populate first advert/embedding table | `next_uk_nextads_fs_advert_core_daily` or `next_uk_nextads_fs_product_embeddings_latest` | Choose advert core first for lower risk, or embeddings first if the DEV validation focuses on vector metadata. |
| 6 | Wire CWB analytics pCTR source contract | `next_uk_nextads_fs_account_advert_affinity_daily`, `next_uk_nextads_fs_pctr_model_input`, `next_uk_nextads_pctr_features_latest` | CWB analytics pCTR remains an external dependency until its source tables/notebooks are migrated into this route. |
| 7 | Add offline candidate similarity diagnostics | Separate diagnostics table | New follow-up story only; no production model or scoring job reads this output. |
| 8 | Add labels and backfill training snapshots | `next_uk_nextads_fs_labels_clicks`, `next_uk_nextads_fs_labels_theme_response` | Preserve point-in-time correctness for repeatable training and validation. |
| 9 | Keep shared DEV feature store refreshed | `marketingdata_dev.nextads_feature_store` | Scheduled daily from DEV Integration Theme Affinity outputs for model-building use. |
| 10 | Prepare production feature publication | Curated production feature tables only | Separate PR; requires explicit consumer need, permissions, ownership and release sign-off. |

## Remaining Feature Migrations

| Area | Remaining work | Priority |
| --- | --- | --- |
| Account/customer features | Replace source-specific customer behaviour outputs with materialised account/profile and web activity feature tables. | High |
| Advert metadata | Promote advert core and attribute profile from pCTR notebooks into the advert feature jobs. | High |
| Product embeddings | Register product embedding lookup with explicit model/version metadata and coverage checks. | High |
| Semantic advert features | Promote advert semantic embeddings and neighbour signals after embedding cache behaviour is stable. | Medium |
| Seasonal demand | Move same-month-last-year, 7-day, 30-day and trend features into seasonal feature tables. | Medium |
| Theme Affinity features | Move current Theme Affinity runtime outputs behind the compatibility view and then into native feature-store tables. | High |
| CWB pCTR affinity | Map CWB analytics pCTR affinity features into account-advert and pCTR model-input contracts. | High |
| Labels | Standardise click/impression and theme response labels with horizons and point-in-time metadata. | Medium |
| Quality checks | Extend scaffolded checks into row-count, key uniqueness, null-rate and freshness writes to quality events. | High |
| Candidate similarity diagnostics | Define a bounded offline candidate source and vector dependencies before creating any diagnostics output. | Later |

## Challenger and Decisioning Dependencies

The feature-store setup should not directly change production ranking or assignment.

Before any feature-store-driven model input affects current production decisioning:

1. Run DEV smoke on feature branch tables.
2. Prove table registration and schema/key contracts.
3. Compare compatibility-view outputs with current Theme Affinity and pCTR model inputs.
4. Run challenger model tests using feature-store inputs.
5. Agree where any challenger model score would join the existing ranking and assignment path.
6. Capture an explicit release/rollback path before production writes are enabled.

Candidate similarity is offline diagnostics only until a separate model experiment proves value and receives explicit approval to affect production decisioning.

## Acceptance Criteria Mapping

| Acceptance criterion | Evidence in this document |
| --- | --- |
| Feature catalogue created or updated | `README.md` in this folder. |
| Initial table ownership and refresh approach recorded | `README.md` and `initial_table_design.md`. |
| Remaining migrations listed and prioritised | Prioritised backlog and remaining migrations sections. |
| Dependency on challenger testing and future decisioning work linked | Challenger and decisioning dependencies section. |

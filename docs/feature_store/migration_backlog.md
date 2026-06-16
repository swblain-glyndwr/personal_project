# Next Ads Feature Store Migration Backlog

Azure Boards story: 5111881  
Feature: 5111595 - Reusable feature layer (Databricks Feature Store)

## Migration Principles

- Keep existing operational outputs unchanged until output equivalence is proven.
- Register and populate small, reusable feature tables in DEV before wiring model jobs to them.
- Move consumers through compatibility views first, then native feature-store reads.
- Keep feature materialisation separate from model training, scoring and assignment jobs.
- Calculate similarity after candidate creation, not from a full customer-by-ad cross join.

## Prioritised Backlog

| Priority | Migration | Target tables/views | Notes |
| --- | --- | --- | --- |
| 1 | Confirm DEV schema, permissions and Feature Engineering Client registration | All setup tables | Run the paused DEV/SANDBOX setup job against `${var.feature_store_schema}` and confirm table registration. |
| 2 | Populate first customer feature table | `next_uk_nextads_fs_account_profile` | Start with account descriptors and reference-date metadata. Validate key uniqueness and row counts. |
| 3 | Populate first advert/embedding table | `next_uk_nextads_fs_advert_core_daily` or `next_uk_nextads_fs_product_embeddings_latest` | Choose advert core first for lower risk, or embeddings first if the DEV validation focuses on vector metadata. |
| 4 | Wire Theme Affinity compatibility view | `next_uk_nextads_theme_affinity_features_latest` | Preserve current `hackathon_model/config.py` feature list while proving output equivalence. |
| 5 | Migrate Theme Affinity native reads | Theme affinity feature tables and model input | Only after compatibility-view output equivalence is signed off. |
| 6 | Wire CWB analytics pCTR model input | `next_uk_nextads_fs_pctr_model_input`, `next_uk_nextads_pctr_features_latest` | Replace notebook-local joins with governed model input after DEV smoke and row/key checks. |
| 7 | Add candidate-level similarity features | pCTR model input and two-tower pairs | Use documented candidate-pair route; avoid full cross join. |
| 8 | Add labels and backfill training snapshots | `next_uk_nextads_fs_labels_clicks`, `next_uk_nextads_fs_labels_theme_response` | Preserve point-in-time correctness for repeatable training and validation. |
| 9 | Introduce shared integration schema | `marketingdata_dev.nextads_integration` | Use after feature branch validation, before production promotion. |
| 10 | Prepare PROD governed schema | `marketingdata_prod.nextads_feature_store` | Requires explicit permission, ownership and release sign-off. |

## Remaining Feature Migrations

| Area | Remaining work | Priority |
| --- | --- | --- |
| Account/customer features | Replace source-specific customer behaviour outputs with materialised account/profile and web activity feature tables. | High |
| Advert metadata | Promote advert core and attribute profile from pCTR notebooks into the advert feature jobs. | High |
| Product embeddings | Register product embedding lookup with explicit model/version metadata and coverage checks. | High |
| Semantic advert features | Promote advert semantic embeddings and neighbour signals after embedding cache behaviour is stable. | Medium |
| Seasonal demand | Move same-month-last-year, 7-day, 30-day and trend features into seasonal feature tables. | Medium |
| Theme Affinity features | Move current hackathon SQL outputs behind the compatibility view and then into native feature-store tables. | High |
| CWB pCTR affinity | Map CWB analytics pCTR affinity features into account-advert and pCTR model-input contracts. | High |
| Labels | Standardise click/impression and theme response labels with horizons and point-in-time metadata. | Medium |
| Quality checks | Extend scaffolded checks into row-count, key uniqueness, null-rate and freshness writes to quality events. | High |
| Two-tower retrieval | Define anchor/candidate generation and negative sampling before populating training pairs. | Later |

## Challenger and Decisioning Dependencies

The feature-store setup should not directly change production ranking or assignment.

Before any feature-store-driven model input affects current production decisioning:

1. Run DEV smoke on feature branch tables.
2. Prove table registration and schema/key contracts.
3. Compare compatibility-view outputs with current Theme Affinity and pCTR model inputs.
4. Run challenger model tests using feature-store inputs.
5. Agree where pCTR/LTR/two-tower scores join the existing ranking and assignment path.
6. Capture an explicit release/rollback path before production writes are enabled.

Future decisioning work must define candidate generation before candidate similarity and two-tower retrieval can become production inputs.

## Acceptance Criteria Mapping

| Acceptance criterion | Evidence in this document |
| --- | --- |
| Feature catalogue created or updated | `README.md` in this folder. |
| Initial table ownership and refresh approach recorded | `README.md` and `initial_table_design.md`. |
| Remaining migrations listed and prioritised | Prioritised backlog and remaining migrations sections. |
| Dependency on challenger testing and future decisioning work linked | Challenger and decisioning dependencies section. |

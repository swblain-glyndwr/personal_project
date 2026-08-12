# Next Ads Feature Store Migration Backlog

| Work item | Purpose |
| --- | --- |
| 5111595 | Reusable feature layer (Databricks Feature Store). |
| 5111881 | Documentation and migration backlog. |

## Migration Principles

- Keep existing operational outputs unchanged until output equivalence is proven.
- Complete every physical table and compatibility view in the declared offline inventory in the shared DEV store before immutable publication work or environment promotion starts.
- A SQL contract, registered empty shell or planner entry is not DEV completion; the table must have an implemented source and materializer, data at its declared cadence, and passing table-appropriate quality evidence.
- PREPROD and PROD work is blocked until both `DEV_COMPLETE` and `DEV_SNAPSHOT_SAFE` are evidenced.
- Register and populate small, reusable feature tables in the shared DEV feature store before wiring model jobs to them.
- Move consumers through compatibility views first, then native feature-store reads.
- Keep feature materialisation separate from model training, scoring and assignment jobs.
- Keep candidate similarity out of production model contracts until a separate offline diagnostics story is agreed.

## Delivery Gates

| Order | Gate | Scope | Exit condition |
| ---: | --- | --- | --- |
| 0 | `CONTRACT_BASELINE` | Current contracts PR | All intended definitions are classified; the planner makes no runtime-readiness claim. |
| 1 | `DEV_COMPLETE` | Populate the complete shared DEV inventory | All 20 intended physical tables are materially populated according to their cadence; both compatibility views resolve implemented sources; `SCAFFOLD=0`; no missing contracts remain; table-appropriate schema, key, freshness and row evidence passes. |
| 2 | `DEV_SNAPSHOT_SAFE` | Add immutable DEV publication | Build-scoped staging, validated atomic reference-date publication, exact source/output Delta versions, READY only after every required group passes, and the previous READY snapshot remains available after failure. |
| 3 | `DEV_OPERATION_PROVEN` | Prove the complete shared route | DBR 15.4 dependency smoke, registry-driven graph and checks, a complete run, same-date retry and injected-failure proof in shared DEV. |
| 4 | `ENVIRONMENT_PARITY` | Add matching PREPROD and PROD batch stores | The same definitions and graph are used; only physical bindings, identities, secrets, sizing and schedule state differ; destructive setup is blocked; the tagged build passes manual PREPROD then PROD proof. |
| 5 | `OFFLINE_ACTIVATED` | Activation-only change | Enable PROD at 21:00 only after parity, freshness and quality pass; alerts are enabled; rollback disables the schedule and retains the previous READY snapshot. |

## Shared DEV Completion Slices

| Order | Reviewable slice | Contract set and exit evidence |
| ---: | --- | --- |
| 1 | Audit current implemented contracts and make checks registry-driven | Prove population of the 11 `ACTIVE` and 2 `COMPATIBILITY` definitions rather than trusting `implemented=true`; cover `item_attributes_latest`, the on-demand Theme Affinity training input and quality events explicitly. |
| 2 | Product embedding source and product-profile materializers | Populate `next_uk_nextads_fs_product_embeddings_latest` and `next_uk_nextads_fs_advert_product_profile_daily` from a pinned promoted model and approved advert-item source; prove model version, vector dimension, coverage and retry-cache behaviour. |
| 3 | Advert semantic materializer | Populate `next_uk_nextads_fs_advert_semantic_profile_daily` from the governed embedding source; prove active-ad coverage, stable keys and no row multiplication. |
| 4 | Seasonal demand contract and materializer | Reconcile the declared entity/item grain with the existing experiment, populate `next_uk_nextads_fs_seasonal_product_demand_daily`, and prove that no future events enter any feature window. |
| 5 | Analytics pCTR external adapter | Populate `next_uk_nextads_fs_account_advert_affinity_daily` with explicit source provenance and documented mappings for affinity, impression and rule-based fields. |
| 6 | Shopping Bag/BQ session builder | Populate `next_uk_nextads_fs_session_context_daily` with an explicit stable session ID, device/channel/geo semantics and page-count rules. |
| 7 | pCTR model assembly | Decide and document the compatibility shape, populate `next_uk_nextads_fs_pctr_model_input` using declared feature cutoffs and labels, then unblock `next_uk_nextads_pctr_features_latest`. |
| 8 | Shared DEV closure evidence | Record a table-by-table manifest for all 20 physical contracts and both views, with fully qualified location, build/reference date, row count, key/null/duplicate result, schema hash, source/output Delta versions and build ID; require `SCAFFOLD=0`. |

- `SCAFFOLD=0` must be reached by implementing the named contracts, not by relabelling empty shells.
- If a registered table is no longer intended, remove it only through an explicit reviewed inventory decision; otherwise it continues to block `DEV_COMPLETE`.

## Remaining Feature Migrations

| Area | Remaining work | Priority |
| --- | --- | --- |
| Account/customer features | Replace source-specific customer behaviour outputs with materialised account/profile and web activity feature tables. | High |
| Advert metadata | Promote advert core and attribute profile from pCTR notebooks into the advert feature jobs. | High |
| Product embeddings | Register product embedding lookup with explicit model/version metadata and coverage checks. | High |
| Semantic advert features | Promote advert semantic embeddings and neighbour signals after embedding cache behaviour is stable. | Medium |
| Seasonal demand | Move same-month-last-year, 7-day, 30-day and trend features into seasonal feature tables. | Medium |
| Theme Affinity features | Move current Theme Affinity runtime outputs behind the compatibility view and then into native feature-store tables. | High |
| pCTR source adapters | Keep provenance explicit: bind approved CWB analytics outputs as versioned external sources for account-advert affinity and CTR semantics, build session context through the repo-owned Shopping Bag/BQ route, and assemble the compatibility input only after both contracts are proven. | High |
| Labels | Standardise click/impression and theme response labels with horizons and point-in-time metadata. | Medium |
| Quality checks | Extend scaffolded checks into row-count, key uniqueness, null-rate and freshness writes to quality events. | High |
| Candidate similarity diagnostics | Define a bounded offline candidate source and vector dependencies before creating any diagnostics output. | Later; non-gating |

## Challenger and Decisioning Dependencies

The feature-store setup should not directly change production ranking or assignment.

Before any feature-store-driven model input affects current production decisioning:

1. Run DEV smoke on feature branch tables.
2. Prove table registration and schema/key contracts.
3. Compare compatibility-view outputs with current Theme Affinity and Shopping Bag pCTR model inputs.
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

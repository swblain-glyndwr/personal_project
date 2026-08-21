# Next Ads Feature Store Migration Backlog

| Work item | Purpose |
| --- | --- |
| 5111595 | Reusable feature layer (Databricks Feature Store). |
| 5111881 | Documentation and migration backlog. |

## Migration Safety Principles

- Keep existing operational outputs unchanged until output equivalence is proven.
- Complete every physical table and compatibility view in the declared offline inventory in the shared DEV store before immutable publication work or environment promotion starts.
- A SQL contract, registered empty shell or planner entry is not DEV completion; the table must have an implemented source and materializer, data at its declared cadence, and passing table-appropriate quality evidence.
- PREPROD and PROD work is blocked until both `DEV_COMPLETE` and `DEV_SNAPSHOT_SAFE` are evidenced.
- Register and populate small, reusable feature tables in the shared DEV feature store before wiring model jobs to them.
- Move consumers through compatibility views first, then native feature-store reads.
- Keep feature materialisation separate from model training, scoring and assignment jobs.
- Keep candidate similarity out of production model contracts until a separate offline diagnostics story is agreed.

## Environment Delivery Gates

| Order | Gate | Scope | Exit condition |
| ---: | --- | --- | --- |
| 0 | `CONTRACT_BASELINE` | Current contracts PR | All intended definitions are classified; the planner makes no runtime-readiness claim. |
| 1 | `DEV_COMPLETE` | Populate the complete shared DEV inventory | All 22 physical tables are materially populated according to their cadence; both compatibility views resolve implemented sources; `SCAFFOLD=0`; no missing contracts remain; table-appropriate schema, key, freshness and row evidence passes. |
| 2 | `DEV_SNAPSHOT_SAFE` | Add immutable DEV publication | Build-scoped staging, validated atomic reference-date publication, exact source/output Delta versions, READY only after every required group passes, and the previous READY snapshot remains available after failure. |
| 3 | `DEV_OPERATION_PROVEN` | Prove the complete shared route | DBR 15.4 dependency smoke, registry-driven graph and checks, a complete run, same-date retry and injected-failure proof in shared DEV. |
| 4 | `ENVIRONMENT_PARITY` | Add matching PREPROD and PROD batch stores | The same definitions and graph are used; only physical bindings, identities, secrets, sizing and schedule state differ; destructive setup is blocked; the tagged build passes manual PREPROD then PROD proof. |
| 5 | `OFFLINE_ACTIVATED` | Activation-only change | Enable PROD at 21:00 only after parity, freshness and quality pass; alerts are enabled; rollback disables the schedule and retains the previous READY snapshot. |

## Remaining Shared DEV Evidence Slices

| Order | Reviewable slice | Contract set and exit evidence |
| ---: | --- | --- |
| 1 | Complete shared DEV run | Run the registry-driven graph in `DEV_FEATURE_STORE` and prove all 18 `ACTIVE` plus four `COMPATIBILITY` tables at their declared cadence; both compatibility views must resolve. |
| 2 | Immutable-publication proof | Capture exact source/output Delta versions, READY bindings, same-date reuse and injected-failure retention for every required daily group. Prove the earlier READY snapshot remains selectable after failure. |
| 3 | Operational proof | Capture the DBR 15.4 dependency smoke, one complete shared run, one identical retry and the quality result for every in-scope physical contract and view. |
| 4 | Shared DEV closure evidence | Record a table-by-table manifest for all 22 physical contracts and both views, with fully qualified location, build/reference date, row count, key/null/duplicate result, schema hash, source/output Delta versions and build ID; require `SCAFFOLD=0`. |

- `SCAFFOLD=0` must be reached by implementing the named contracts, not by relabelling empty shells.
- If a registered table is no longer intended, remove it only through an explicit reviewed inventory decision; otherwise it continues to block `DEV_COMPLETE`.

## Remaining Migrations And Evidence

| Area | Remaining work | Priority |
| --- | --- | --- |
| Shared DEV evidence | Prove the complete 22-table graph and both views in the shared Feature Store target; personal Shopping Bag evidence is not a substitute for this gate. | High |
| Theme Affinity consumers | Compare the compatibility view to the current Theme Affinity input, then move reviewed consumers to native READY feature reads. | High |
| Analytics pCTR consumers | Retain exact external-source provenance, prove the pCTR compatibility view and move reviewed consumers to native READY feature reads. | High |
| Labels | Retire the non-training-safe inferred click-label contract only after every consumer uses observed, mature labels such as `next_uk_nextads_fs_shopping_bag_click_labels`. | High |
| Environment parity | Add release-bound PREPROD and governed PROD bindings only after the shared DEV completion, snapshot and operation gates pass. | High |
| Monitoring | Agree freshness, failure and drift alert ownership before enabling any additional scheduled target. | Medium |
| Candidate similarity diagnostics | Define a bounded offline candidate source and vector dependencies before creating any diagnostics output. | Later; non-gating |

## Challenger Activation Dependencies

The feature-store setup should not directly change production ranking or assignment.

Before any feature-store-driven model input affects current production decisioning:

1. Run DEV smoke on feature branch tables.
2. Prove table registration and schema/key contracts.
3. Compare compatibility-view outputs with the current Theme Affinity and Analytics pCTR model inputs.
4. Run challenger model tests using feature-store inputs.
5. Agree where any challenger model score would join the existing ranking and assignment path.
6. Capture an explicit release/rollback path before production writes are enabled.

Candidate similarity is offline diagnostics only until a separate model experiment proves value and receives explicit approval to affect production decisioning.

## Migration Backlog Acceptance Criteria

| Acceptance criterion | Evidence in this document |
| --- | --- |
| Feature catalogue created or updated | [`feature_store_table_design.md`](feature_store_table_design.md) and the executable registry. |
| Table ownership and refresh approach recorded | [`feature_store_table_design.md`](feature_store_table_design.md). |
| Remaining migrations listed and prioritised | Prioritised backlog and remaining migrations sections. |
| Dependency on challenger testing and future decisioning work linked | Challenger and decisioning dependencies section. |

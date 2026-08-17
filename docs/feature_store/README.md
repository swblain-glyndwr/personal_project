# Next Ads Feature Store Documentation

| Work item | Purpose |
| --- | --- |
| 5111595 | Reusable feature layer (Databricks Feature Store). |
| 5111881 | Documentation and migration backlog. |

## Scope

| Area | Current scope |
| --- | --- |
| Ownership | Feature contracts, job definitions and promotion controls remain in this repository. |
| Serving mode | Batch/offline first. Realtime and online publication are separate later phases. |
| Shared store | DEV writes to `marketingdata_dev.nextads_feature_store`. |
| Customer impact | No production scoring, assignment, payload or delivery change in the contract-only slice. |

## Delivery Order

| Order | Gate | Exit condition |
| ---: | --- | --- |
| 0 | `CONTRACT_BASELINE` | All intended definitions are classified and the environment-neutral graph is inspectable without claiming runtime readiness. |
| 1 | `DEV_COMPLETE` | All 20 intended physical tables are implemented and populated in shared DEV according to their cadence, both compatibility views resolve implemented sources, `SCAFFOLD=0`, and table-level key, schema, freshness and row evidence passes. |
| 2 | `DEV_SNAPSHOT_SAFE` | Build-scoped staging and atomic reference-date publication record exact Delta versions and retain the previous READY snapshot after failure. |
| 3 | `DEV_OPERATION_PROVEN` | The complete DBR 15.4 route passes dependency smoke, full run, same-date retry and injected-failure evidence. |
| 4 | `ENVIRONMENT_PARITY` | The completed logical graph is added to release-isolated PREPROD and matching PROD batch jobs. |
| 5 | `OFFLINE_ACTIVATED` | PROD scheduling and alerts are enabled in an activation-only change after manual parity proof. |

- PREPROD and PROD work is blocked until `DEV_COMPLETE` and `DEV_SNAPSHOT_SAFE` are evidenced.
- A registered empty shell does not count as a populated DEV feature.
- The on-demand Theme Affinity training input and feature-quality events are included in the table-by-table DEV evidence; they are not exempt because they do not share the daily cadence.

## Documents

| Document | Story | Purpose |
| --- | --- | --- |
| `reusable_feature_inventory.md` | 5111856 | Existing reusable signals and first migration candidates. |
| `initial_table_design.md` | 5111861 | Initial customer, advert, embedding, model-input and quality table design. |
| `candidate_similarity.md` | Follow-up | Offline candidate similarity diagnostics concept; not part of current production model inputs. |
| `migration_backlog.md` | 5111881 | Prioritised migration backlog and dependencies. |
| [`../architecture/feature_store_flow.md`](../architecture/feature_store_flow.md) | Architecture | Mermaid view of the shared DEV Feature Store flow and model-building boundaries. |
| [`../architecture/nextads_model_feature_overview.md`](../architecture/nextads_model_feature_overview.md) | Architecture | Wider NextAds model, Feature Store and MLflow overview. |
| [`building_a_challenger_model.md`](building_a_challenger_model.md) | Author route | The decisions, contracts and evidence a model author follows from a problem statement to an evaluation-only challenger. |

## Executable Contracts

### Artifacts

| Artifact | Responsibility |
| --- | --- |
| `configs/features/nextads_feature_store.yaml` | Logical definitions, delivery states, keys, ownership, freshness, consumers and environment bindings. |
| `sql/features/nextads/` | Physical table and compatibility-view schemas. |
| `jobs/table_operations/create_feature_store_tables.py` | Databricks Feature Engineering table creation. |
| `pipelines/databricks/jobs/mktg_next_uk_nextads_feature_store.yml` | Personal, integration and shared DEV Feature Store jobs. |
| `jobs/features/nextads/` | Feature builders, checks and the read-only plan command. |
| `configs/features/README.md` | Contract-state, binding and planner terminology. |

### Current Contract Status

| State | Count | Current coverage |
| --- | ---: | --- |
| `ACTIVE` | 17 | Account, web activity, advert, item, embedding, affinity, session, Theme Affinity, labels and quality tables with implemented builders. |
| `COMPATIBILITY` | 2 | Theme Affinity model and training inputs retained during migration. |
| `SCAFFOLD` | 1 | The assembled pCTR model input still needs its approved session-level training source and materialiser. |

- These counts describe repository contracts, not live DEV completion. `DEV_COMPLETE` requires every intended physical contract and both compatibility views to meet the migration-backlog exit gate.

### Environment Bindings

| Environment | Location | Job target | Current state |
| --- | --- | --- | --- |
| DEV | `marketingdata_dev.nextads_feature_store` | `DEV_FEATURE_STORE` | Repository-declared. |
| PREPROD | Release-isolated tables in `marketingdata_prod.ds_sandbox` | `PREPROD` | Planned; blocked by `DEV_COMPLETE` and `DEV_SNAPSHOT_SAFE`. |
| PROD | `marketingdata_prod.nextads_feature_store` | `PROD` | Planned; blocked by `DEV_COMPLETE` and `DEV_SNAPSHOT_SAFE`. |

### Read-only Plan

All environments:

```powershell
.\.venv\Scripts\python.exe jobs\features\nextads\plan_offline_feature_store.py `
  --environment ALL --format text
```

One release-isolated PREPROD plan:

```powershell
.\.venv\Scripts\python.exe jobs\features\nextads\plan_offline_feature_store.py `
  --environment PREPROD --release-id release/2026.08.12 --format text
```

| Planner term | Meaning |
| --- | --- |
| `REPO_DECLARED` | The job target is present in repository configuration. |
| `PLANNED` | The binding exists but its job target has not been added. |
| `CONTRACT_READY` | A compatibility view has an implemented repository source contract. |
| `BLOCKED` | The source feature still has named missing contracts. |
| `RELEASE_ID_REQUIRED` | An exact PREPROD location needs `--release-id`. |

- Planner states do not prove that a job is deployed, a table is populated or an immutable snapshot is READY.

### Runtime Audit

The final quality task derives its coverage from the same registry and writes one event for every implemented physical contract. It also logs one deterministic line beginning `FEATURE_STORE_DEV_AUDIT_MANIFEST=` with physical paths, audit scope, row/key results, ordered schema hashes, live Feature Engineering keys, table/reference-date commit tags, exact final Delta versions and compatibility-view status.

| Audit field | Meaning |
| --- | --- |
| `CURRENT_IMPLEMENTED_PASS` | Every non-skipped implemented contract passed physical schema, Feature Engineering key, scoped commit-recency and row/key checks, and every implemented-source compatibility view matched its declared source and row/key evidence. |
| `current_implemented_complete` | `true` only when none of the 13 implemented contracts was skipped and every implemented-source view is ready. |
| `dev_complete` | `true` only when current implemented coverage is complete, every intended feature is implemented, no scaffolds remain and both views are ready. |
| `BLOCKED` | The compatibility view resolves a source that is still a scaffold; the job does not present it as operational. |

The normal daily job keeps the on-demand Theme Affinity training build at `skip`, so its manifest must report that table in `skipped_current_contracts` and keep `current_implemented_complete=false`. Supply an explicit historical `theme_training_reference_date` to build and audit that exact partition.

The quality table's own persisted event uses `MANIFEST_ONLY` because a row cannot truthfully contain the Delta version created by writing itself. The deterministic manifest is emitted after that final merge and contains the exact resulting quality-table version.

The registry declares `next_uk_nextads_fs_item_attributes_latest` with `write_mode: overwrite`. Its builder uses one atomic whole-table replacement. Dated tables use one atomic reference-date replacement rather than a delete followed by a merge. Exact Delta receipts can therefore be attached to a build without a temporary missing partition.

#### Personal DEV runtime evidence

| Evidence | Result |
| --- | --- |
| Revision and deployment | Revision `e96485e931576695205787227eaa20297c76d0d5` deployed through pipeline `2071583`; the pipeline succeeded and selected only the bounded personal DEV deployment. The job used DBR 15.4 and Feature Engineering 0.12.1. |
| Failure and fix sequence | Run `64488519593329` exposed the first live timestamp-key metadata shape. Run `439768216959422` then completed every build but showed that Feature Engineering appends the timestamp key after entity keys for the two composite label contracts. Revisions `55408db` and `e96485e` accept those exact live shapes while retaining strict entity-key order, timestamp metadata and missing/extra-key checks. |
| Exact-revision proof | Run `373370623960025` completed `SUCCESS` in 152.8 minutes from revision `e96485e931576695205787227eaa20297c76d0d5`. One worker was lost to a cloud communication health check during the click-label shuffle; Databricks restored the cluster to four workers and the task completed without a code retry. |
| Audit manifest | `CURRENT_IMPLEMENTED_PASS`; 13 implemented contracts, two compatibility views, seven scaffolds, no failed current contracts and one intentionally skipped Theme Affinity training-input contract. `current_implemented_complete=false` and `dev_complete=false` remain truthful. |
| Label evidence | Click labels: 407,436 rows and distinct keys, zero null or duplicate keys, schema/key/commit checks passed, Delta version 17. Theme response labels: 3,971,236,814 rows and distinct keys, zero null or duplicate keys, schema/key/commit checks passed, Delta version 16. |
| Compatibility views | `next_uk_nextads_theme_affinity_features_latest` is `READY` with 1,264,725,100 rows. `next_uk_nextads_pctr_features_latest` remains `BLOCKED` until its approved Shopping Bag pCTR source and materializer exist. |
| Environment boundary | All writes were limited to `marketingdata_dev.stephen_blain`. This evidence does not activate shared DEV, DEV Integration, PREPROD, PROD or realtime resources. |

| Remaining boundary | Evidence and required follow-up |
| --- | --- |
| Optional training input | The normal route keeps `next_uk_nextads_fs_theme_affinity_training_input` at `skip`; an explicit historical date is still required to prove that contract. |
| Scaffold coverage | Seven registered feature groups are still scaffolds and are not presented as operational. They must be implemented and proven in DEV before the shared DEV/PREPROD/PROD sequence. |
| Source reproducibility | The latest item source changed from 639,124 to 639,388 rows between attempts for the same reference date. The immutable-snapshot work must record and consume exact source Delta versions rather than relying on a moving latest source. |
| Failure-safe publication | Dated tables still remove the requested partition before their Feature Engineering merge. This run proves the current contracts but does not prove atomic multi-table snapshots; the `FeatureBuild` and `FeatureSnapshot` work must remove that exposure gap. |

#### Product embedding foundation evidence

| Evidence | Result |
| --- | --- |
| Revision and deployment | Revision `74db4f1149a488d67a0f98e884014fa50af5f50a` deployed through pipeline `2071889`; 1,044 tests passed and 13 skipped. Only the personal DEV route deployed. Destroy, DEV Integration, shared Feature Store, PREPROD and PROD stages were skipped. |
| Failure and repair sequence | Run `1096436449658921` passed the bridge but exposed an incorrect `model_data` assumption in the registered model's MLflow flavour. Revision `44e8c92` read the actual pyfunc data path and run `1089941215576212` passed, allowing the exact artifact digest to be recorded. Revision `800ae62` pinned that digest and run `533832210189819` proved the approved and downloaded artifacts matched. |
| Final compatibility fix | Run `533832210189819` also exposed a suppressed callback traceback from the base image's thread-pool inspection library. Revision `74db4f1` pins `threadpoolctl==3.6.0` and checks the runtime library controllers explicitly. |
| Exact-revision proof | Parent run `334271287452983` completed `SUCCESS` from revision `74db4f1149a488d67a0f98e884014fa50af5f50a`. Bridge task `661018763219592` and embedding task `446396556326604` both completed `SUCCESS`; both outputs were complete rather than truncated. |
| Bridge manifest | Nine fixture rows across five adverts; zero duplicate keys and zero future rows consumed. Source precedence, weighting and conflicting-position rejection passed; `writes_performed=false`. |
| Embedding manifest | DBR 15.4, Python 3.11 and Standard CPU execution passed with MLflow 3.11.1, NumPy 1.26.4, protobuf 4.24.1, Sentence Transformers 2.4.0, threadpoolctl 3.6.0, CPU-only PyTorch 2.10.0 and Transformers 4.41.2. The three-module graph and safetensor artifact passed validation, and the output contained 384 values with norm `0.999999976604877`. |
| Artifact provenance | The fixed personal DEV binding resolves registered model version 11 and source run `95be978bd9e24783afe4e68def0c9845`. The approved and observed artifact SHA-256 values both equal `bc4daec2a2647ce42ad35df49181c762187cd4e1fed008915ce4f76ac89ca384`; custom model code and runtime registration are disabled. |
| Clean-log gate | All 1,381 embedding log lines were checked. `Exception ignored`, `threadpoolctl.py`, the earlier `NoneType.split` callback error and `Traceback` each occurred zero times. |
| Environment boundary | The manual job is unscheduled and read-only. It did not create a model run, register a model, move an alias, write a feature table or alter shared data. Model artifacts were downloaded only to ephemeral task storage. |

| Remaining boundary | Required follow-up |
| --- | --- |
| Registry state | Product embeddings, advert semantic profiles and advert product profiles remain `SCAFFOLD`. This foundation does not present any of those feature tables as operational. |
| Materialisation | The personal binding proves one fixed artifact is compatible with DBR 15.4; it is not a shared feature dependency or a production materialiser. A separate change must define the reusable product source, build semantics, keys, freshness and publication evidence. |
| Live advert inputs | The bridge run used in-memory fixtures. Live source metadata matches the expected shapes, but live row quality, historical coverage and full-volume scan cost remain unproven. |
| Higher environments | No shared DEV, DEV Integration, PREPROD, PROD or realtime resource was changed or exercised. |

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
| Account advert affinity | `next_uk_nextads_fs_account_advert_affinity_daily` | Account/advert/reference date | pCTR, LTR |
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

The first development deployments target `marketingdata_dev` with explicit target-specific schemas: SANDBOX uses the current user's schema, DEV uses the last commit author's schema normalised to the repo's lower-case user schema convention, DEV_INTEGRATION uses `nextads_integration`, and DEV_FEATURE_STORE uses the shared `nextads_feature_store` schema. The DEV target includes a manual Feature Store copy for branch validation; it has no schedule and permits one run at a time.

`DEV_FEATURE_STORE` is scheduled daily at 21:00 Europe/London and reads latest Theme Affinity source tables from `marketingdata_prod.warehouse`. It writes reusable latest features to `marketingdata_dev.nextads_feature_store`. Theme Affinity training jobs read `marketingdata_dev.nextads_feature_store.next_uk_nextads_fs_theme_affinity_training_input`, which is populated only from an explicit historical run where the existing Theme Affinity prep builds a 31-day future-window basket target via `2_target.sql` and joins it back through `6_master_assoc.sql`.

The feature-store job exposes source-table job parameters so DEV runs can be pointed at DEV-owned Theme Affinity outputs while production source access is being agreed. For example, a manual DEV run can override `theme_source_catalog=marketingdata_dev`, `theme_source_schema=<user_schema_or_nextads_integration>`, and `theme_table_prefix=next_uk_nextads_account_theme_foundation`. This keeps the feature-store table creation and write path testable against ordinary Delta tables without requiring the DEV service principal to read Lakeflow-managed storage.

The labelled training-input build is controlled by `feature_store_theme_training_reference_date`. The default is `skip` so the daily latest-feature refresh does not silently build or overwrite training data. To create or refresh training data, run the feature-store route with a historical date at least 28 days old; the task stages historical Theme Affinity prep tables with `feature_store_theme_training_table_prefix`, validates that positives and negatives exist, then writes `next_uk_nextads_fs_theme_affinity_training_input`. Matching PREPROD and PROD publication is blocked until the complete shared-DEV inventory passes `DEV_COMPLETE` and immutable publication passes `DEV_SNAPSHOT_SAFE`.

## Dependencies

The feature-store route depends on:

- DEV Feature Engineering Client availability and write permissions.
- Existing source jobs remaining stable while compatibility views are proven.
- Existing production Theme Affinity outputs being available before the shared feature-store materialisation job runs for `reference_date=predict`.
- Historical Theme Affinity prep sources having enough future-window basket data for the requested `feature_store_theme_training_reference_date`.
- Existing production customer, control-sheet, item-attribute, assignment and BigQuery web/action tables being available for the same resolved feature-store reference date.
- Repo-owned Shopping Bag/BQ source contracts being implemented for session and label features, with explicitly approved CWB analytics inputs bound as versioned external sources for affinity and CTR semantics rather than moving the analytics notebooks wholesale.
- Challenger testing before feature-store model inputs affect production ranking.
- Separate offline diagnostics stories before candidate-similarity work is added to the repo.

## Acceptance Criteria Mapping

| Acceptance criterion | Evidence |
| --- | --- |
| Feature catalogue created or updated | Feature catalogue section. |
| Initial table ownership and refresh approach recorded | Ownership and refresh section plus registry. |
| Remaining migrations listed and prioritised | `migration_backlog.md`. |
| Dependency on challenger testing and future decisioning work linked | Dependencies section and migration backlog. |

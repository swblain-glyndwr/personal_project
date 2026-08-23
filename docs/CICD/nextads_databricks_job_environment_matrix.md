# NextAds Databricks Job Environment Matrix

Status: Working agreement

This page records the bundle targets in which each of the 40 NextAds jobs is declared in this checkout. It describes repository availability, not proof that a job is deployed, enabled or successfully run in an environment. For job inputs and outputs, see [`nextads_job_table_flow.md`](../architecture/nextads_job_table_flow.md); for schedules and child-job relationships, see [`nextads_databricks_runtime_map.md`](nextads_databricks_runtime_map.md).

## Bundle Target And Release Policy

| Target | Purpose | Job availability rule |
| --- | --- | --- |
| `SANDBOX` | Personal isolated bundle testing in the DEV workspace. | Operational routes and the manual table-operations job. |
| `DEV` | Developer-specific feature-branch validation in the DEV workspace. | Operational routes plus personal Feature Store, generic model lifecycle, generic model discovery, smoke, setup and manual service-principal-owned object-access reconciliation. |
| `DEV_INTEGRATION` | Shared integration validation from `develop`. | Operational routes, Theme Affinity training, reviewed model import and integration table setup/migration jobs. |
| `DEV_FEATURE_STORE` | Shared scheduled DEV Feature Store refresh. | Only the Feature Store job; its Analytics pCTR source building and receipt are internal tasks. |
| `PREPROD` | Release-candidate validation in the PROD workspace, writing to the release validation namespace. | Operational routes plus PREPROD setup, dependency smoke and exact model-import jobs. |
| `PROD` | Tagged production deployment. | Operational routes plus explicit model-promotion, model-monitoring, read-only contract controls and manual service-principal-owned object-access reconciliation. |

## Job Availability By Bundle Target

Jobs are grouped only when their target set and release boundary are identical. Every declared job name appears once below.

| Job group | Declared jobs | Targets | Boundary |
| --- | --- | --- | --- |
| Operational inputs and scoring | `mktg_next_uk_nextads_model_scoring`<br>`mktg_next_uk_nextads_markov_scoring`<br>`mktg_next_uk_nextads_candidate_foundation`<br>`mktg_next_uk_nextads_candidate_build`<br>`mktg_next_uk_nextads_data_pull` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | The generic scoring job currently supports `model_name=theme_affinity` and calls the main NextAds job's same-date `PREPARE_SCORING_INPUTS` operation before scoring. The main job's schedule retains `CANDIDATE_BUILD` as its default. Pipeline-backed jobs travel with their referenced pipeline resources. |
| Assignment, delivery and validation | `mktg_next_uk_nextads_page_build`<br>`mktg_next_uk_nextads_page_build_v2`<br>`mktg_next_uk_nextads_masid_handoff`<br>`mktg_next_uk_nextads_plp_gs_delivery`<br>`mktg_next_uk_nextads_payload_export`<br>`mktg_next_uk_nextads_candidate_compatibility`<br>`mktg_next_uk_nextads_assignment_validation` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | Child jobs and monitoring resources must exist beside their caller; validation does not create a data table. |
| Reporting, realtime and retention | `mktg_next_uk_nextads_results_cicd`<br>`mktg_next_uk_nextads_realtime_results_cicd`<br>`mktg_next_uk_nextads_realtime_inputs`<br>`mktg_next_uk_nextads_realtime_data`<br>`mktg_next_uk_nextads_table_maintenance` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | These can write reporting, realtime or retained-state changes; schedule and target must be checked before a manual run. |
| Manual table operations | `mktg_next_uk_nextads_table_operations` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | Dry-run by default. Mutating and destructive operations require their explicit confirmations. |
| Manual service-principal-owned object access | `mktg_next_uk_nextads_sp_owned_table_access` | `DEV`, `PROD` | Unscheduled and dry-run by default. Source fixes the recipients, execution identity and allowed scope; applying relation-level grants requires `confirm_mutating=true` and `dry_run=false`. |
| Shared Feature Store route | `mktg_next_uk_nextads_feature_store` | `DEV`, `DEV_FEATURE_STORE` | The DEV copy is manual and personal-schema scoped. The `DEV_FEATURE_STORE` copy is the scheduled shared refresh; its Analytics pCTR source tasks and receipt run inside the same job, and no operational delivery job belongs there. |
| Personal DEV declared model lifecycle | `mktg_next_uk_nextads_model_development`<br>`mktg_next_uk_nextads_model_discovery` | `DEV` | Centrally owned manual jobs parameterised by a repository model declaration. The lifecycle job supports `BUILD`, `RESEARCH`, `REVIEW_SELECT` and `EVALUATE`; the separate-runtime AutoML discovery job is disabled unless explicitly enabled. Neither selects a serving provider or changes assignments. |
| Personal DEV retained model proof | `mktg_next_uk_nextads_analytics_pctr`<br>`mktg_next_uk_nextads_product_embedding_runtime_smoke` | `DEV` | Existing bounded proof routes retained for their current implementation contracts; neither is the pattern for adding a new declared model. |
| Personal DEV support | `mktg_next_uk_nextads_dev_setup`<br>`mktg_next_uk_nextads_table_monitoring` | `DEV` | Personal setup and current DEV-only table-size monitoring. |
| Theme Affinity training | `mktg_next_uk_nextads_theme_affinity_model_train`<br>`mktg_next_uk_nextads_theme_affinity_model_train_spark` | `DEV`, `DEV_INTEGRATION` | Development training only; neither job selects the production model URI. |
| DEV Integration model and table preparation | `mktg_next_uk_nextads_model_import_dev_integration`<br>`mktg_next_uk_nextads_dev_integration_setup`<br>`mktg_next_uk_nextads_dev_integration_alter`<br>`mktg_next_uk_nextads_dev_integration_migrate` | `DEV_INTEGRATION` | Exact reviewed model movement plus deliberate shared-schema setup or migration. The migrate job is destructive. |
| PREPROD release preparation | `mktg_next_uk_nextads_model_import_preprod`<br>`mktg_next_uk_nextads_theme_affinity_model_import_dev`<br>`mktg_next_uk_nextads_preprod_setup`<br>`mktg_next_uk_nextads_preprod_dependency_smoke` | `PREPROD` | Release-owner-controlled table setup, read-only dependency proof and exact reviewed model import. |
| PROD model and contract controls | `mktg_next_uk_nextads_theme_affinity_model_promote`<br>`mktg_next_uk_nextads_theme_affinity_model_monitor`<br>`mktg_next_uk_nextads_theme_affinity_quality_monitor_setup`<br>`mktg_next_uk_nextads_prod_table_contract_smoke` | `PROD` | Explicit model movement and monitoring controls. Promotion does not automatically change the scoring selection; the contract smoke remains read-only. |

## Bundle Resource Declaration Rules

- `DEV_FEATURE_STORE` contains exactly `mktg_next_uk_nextads_feature_store`; the Analytics pCTR source build is an internal task chain rather than another job resource.
- A job with a `pipeline_task` is declared only where the referenced pipeline resource also exists.
- Jobs called through `${resources.jobs.<job_key>.id}` are declared in the same target as their caller.
- Personal DEV Feature Store and model jobs remain manual, author-schema scoped and limited to their declared concurrency.
- PREPROD and PROD model movement uses exact reviewed versions; registering or copying a version does not select it for live scoring.
- PREPROD Theme Affinity Lakeflow relations use the `_pp_` table prefix; PROD retains the unqualified stage prefix even though both pipelines use `marketingdata_prod.ds_sandbox`.
- New job files use explicit target blocks unless a review agrees that the job belongs in every target.

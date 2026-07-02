# NextAds Databricks Job Environment Matrix

Status: Working agreement

This page defines which Databricks Asset Bundle jobs should exist in each
target. The rule is that a target should only receive jobs that belong to that
route. `DEV_FEATURE_STORE` is deliberately single-purpose and should not receive
normal operational jobs.

## Target Policy

| Target | Purpose | Job availability rule |
| --- | --- | --- |
| `SANDBOX` | Personal isolated bundle testing in the DEV workspace. | Normal operational jobs only. |
| `DEV` | Developer-specific feature branch validation in the DEV workspace. | Normal operational jobs plus DEV-only setup where required. |
| `DEV_INTEGRATION` | Shared integration validation from `develop`. | Normal operational jobs plus DEV integration setup/migration support. |
| `DEV_FEATURE_STORE` | Shared scheduled DEV feature-store refresh. | Only `mktg_next_uk_nextads_feature_store`. |
| `PREPROD` | Release-candidate validation in the PROD workspace writing to `ds_sandbox`. | Normal operational jobs plus PREPROD setup/smoke and import-to-preprod model movement. |
| `PROD` | Tagged production deployment writing to `warehouse`. | Normal operational jobs plus PROD-only model/quality controls. |

## Job Matrix

| Job or group | Targets | What it affects | Why it belongs there | Notes and risks |
| --- | --- | --- | --- | --- |
| `mktg_next_uk_nextads_candidate_build` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | Main NextAds generation route: customer cells, control-sheet load, theme scoring, ad mapping and page-build trigger. | It is the core operational route and needs the same bundle shape through development, release validation and production. | Schedules remain target-controlled by bundle presets; do not deploy this into `DEV_FEATURE_STORE`. |
| `mktg_next_uk_nextads_page_build` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | Page assignment/build outputs and asynchronous downstream job triggers. | It is part of normal NextAds delivery and must be available wherever the candidate build can trigger it. | Requires QA, MASID handoff, payload export and PLP Google Sheets delivery jobs in the same targets. |
| `mktg_next_uk_nextads_qa` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | Operational QA checks after page outputs exist. | Page-build references this job by resource id, so it must travel with page-build. | This is operational QA, not the PROD-only Databricks quality monitor setup. |
| `mktg_next_uk_nextads_masid_handoff` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | MASID handoff checks after page outputs. | Page-build triggers this downstream route in the normal flow. | Keep scoped with page-build to avoid broken resource references. |
| `mktg_next_uk_nextads_payload_export` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | Bloomreach/global-solution payload export outputs. | It is a delivery route triggered from page-build. | Output location follows target runtime config; not a feature-store job. |
| `mktg_next_uk_nextads_plp_gs_delivery` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | PLP Google Sheets delivery. | It is part of the normal downstream delivery bundle. | Keep deployment separate from feature-store refreshes. |
| `mktg_next_uk_nextads_results_cicd` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | Results, performance checks, BigQuery output and Theme Affinity inference-log enrichment. | Reporting must be validated through the same branch/release route as the operational output. | Can write external/reporting outputs; check target and release route before running. |
| `mktg_next_uk_nextads_realtime_inputs` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | Realtime input preparation. | It supports the normal realtime route across environments. | Not part of feature-store deployment. |
| `mktg_next_uk_nextads_realtime_results_cicd` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | Realtime result outputs. | Realtime reporting needs the normal environment route. | Production schedules should remain controlled by release/tag deployment. |
| `mktg_next_uk_nextads_theme_affinity` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | Theme Affinity Lakeflow/DLT prep, publish, prediction, clean output and sense checks. | It is a normal operational model route, not the shared DEV feature-store route. | Must only exist where `nextads_theme_affinity_predict_data_prep` pipeline also exists. |
| `mktg_next_uk_nextads_data_pull` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | Sort-order data-pull pipeline and archive task. | It is an operational data route that can be validated through normal targets. | Must only exist where `mktg_next_uk_nextads_data_pull` pipeline also exists. |
| `mktg_next_uk_nextads_feature_store` | `DEV_FEATURE_STORE` | Shared DEV model-building feature tables in `marketingdata_dev.nextads_feature_store`. | This target exists only to keep reusable model-building features refreshed from stable sources. | No other jobs should be deployed into `DEV_FEATURE_STORE`. |
| `mktg_next_uk_nextads_theme_affinity_model_train` and `mktg_next_uk_nextads_theme_affinity_model_train_spark` | `DEV`, `DEV_INTEGRATION` | Theme Affinity challenger training jobs. | Training belongs in development/integration until a promoted model is selected. | Not scheduled production controls. |
| `mktg_next_uk_nextads_theme_affinity_model_import_dev` | `PREPROD` | Imports reviewed DEV model version into PREPROD model namespace. | It is a release-validation movement step, not a production promotion. | Requires explicit version parameters and release evidence. |
| `mktg_next_uk_nextads_theme_affinity_model_promote` | `PROD` | Promotes reviewed model from PREPROD namespace to production namespace. | Production model movement should only happen on the PROD route. | Manual/explicit control; do not deploy to DEV feature-store. |
| `mktg_next_uk_nextads_theme_affinity_model_monitor` | `PROD` | MLflow drift evidence for Theme Affinity model tables. | Production monitoring evidence belongs to the production route. | Separate from operational QA. |
| `mktg_next_uk_nextads_theme_affinity_quality_monitor_setup` | `PROD` | Databricks quality monitor setup for Theme Affinity ranked outputs. | Native Databricks quality-monitor setup is production-control work. | Sensitive to table ownership and workspace identity. |
| `mktg_next_uk_nextads_table_operations` | `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD`, `PROD` | Manual table create/alter/recreate/drop support. | It is an operational support tool with inert dry-run defaults. | Mutating/destructive runs require explicit runtime confirmations. |
| DEV integration setup/migrate/alter jobs | `DEV_INTEGRATION` | Shared DEV integration table setup and schema migration. | These jobs only manage `marketingdata_dev.nextads_integration`. | Do not run as routine smoke if destructive recreation is enabled. |
| `mktg_next_uk_nextads_preprod_setup` | `PREPROD` | Creates missing PREPROD validation tables in `marketingdata_prod.ds_sandbox`. | PREPROD setup is release-owner controlled. | Metadata-changing but non-destructive by default. |
| `mktg_next_uk_nextads_preprod_dependency_smoke` | `PREPROD` | Metadata-only release dependency smoke. | It validates release-candidate routing without altering data. | Must remain read-only. |
| `mktg_next_uk_nextads_prod_table_contract_smoke` | `PROD` | Read-only production table-contract smoke. | Production contract checks belong to the tagged PROD route. | Must remain read-only. |
| `mktg_next_uk_nextads_dev_setup` | `DEV` | Personal DEV table setup. | It exists only for developer-specific DEV setup. | Should not create shared integration, PREPROD or PROD tables. |
| `mktg_next_uk_nextads_table_monitoring` | `DEV` | Table size monitoring support route. | Current bundle scope is DEV-only support. | Re-scope separately if this becomes a formal production monitor. |

## Bundle Resource Rules

- Normal operational jobs must be target-scoped to `SANDBOX`, `DEV`, `DEV_INTEGRATION`, `PREPROD` and `PROD`.
- `DEV_FEATURE_STORE` must contain exactly one job: `mktg_next_uk_nextads_feature_store`.
- A job with a `pipeline_task` must only be present in targets where the referenced pipeline resource is also declared.
- Jobs triggered by another job through `${resources.jobs.<job_key>.id}` must exist in the same target as the triggering job.
- New bundle job files should not declare top-level `resources.jobs` unless a review explicitly agrees that the job is truly global across every target.

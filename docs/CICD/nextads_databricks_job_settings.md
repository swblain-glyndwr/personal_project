# NextAds Databricks Job Settings

Status: Working reference

This page explains the runtime settings declared in [`pipelines/databricks/jobs/`](../../pipelines/databricks/jobs/). For the data consumed and produced by every job, see [`nextads_job_table_flow.md`](../architecture/nextads_job_table_flow.md). For target availability and release-route rules, see [`nextads_databricks_job_environment_matrix.md`](nextads_databricks_job_environment_matrix.md).

## How To Read The Scoring And Assignment Settings

The configuration uses several short technical names. In this page, a **score source** (`provider` in task and table identifiers) is a method such as Theme Affinity or Markov that publishes account/theme scores in the shared table shape. A **score-selection list** (`portfolio`) chooses the exact ready score output for each serving or comparison role. **Shared customer inputs** (`Candidate Foundation`) are accepted customer cells, repeat-ad exposure and advert feedback recorded together. Both routes use the first two inputs, while only V1 applies advert feedback. An **advert option** (`candidate`) is an eligible scored advert, not a final assignment. A **build** is a recorded result for fixed inputs; the surrounding words say whether it is a scoring result, advert-option result, model-training result or assignment result.

Task names, operation values, parameters, table names and manifest fields retain their exact repository spelling below.

## Settings Shared By Multiple Jobs

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `client` | Client configuration key used to resolve client tables and settings. | Usually `next_uk`; only use another configured client after checking `configs/clients/`. |
| `job_env` | Runtime environment passed to scripts and config loading. | `${var.job_parameter_environment_name}` from the bundle target: normally `dev`, `preprod`, or `prod`. |
| `log_level` | Python logging verbosity. | Standard logging levels such as `INFO`, `WARNING`, `ERROR`, `DEBUG`. |
| `reference_date` | Feature or model data reference date. | `current` where supported, or `YYYY-MM-DD`. |
| Refresh date flags | Date-gated refresh controls such as `refresh_model_date`. | `YYYY-MM-DD`; refresh happens only when the supplied date matches the run date. |
| Table names / namespaces | Unity Catalog objects used by table, feature, model, or monitor jobs. | Fully qualified `catalog.schema.table` or `catalog.schema` unless the script documents otherwise. |
| Boolean settings | String booleans passed through DAB/job parameters. | Prefer `true` or `false`. Some scripts also accept `1`/`0`. |

## Job-Specific Settings

### `mktg_next_uk_nextads_scoring_inputs`

Unscheduled shared-input job. It accepts only `run_date`, which defaults to the job start date. Shared model scoring calls it synchronously for the same logical date. It prepares and accepts reusable scoring inputs; it does not score a model, build advert options or publish assignments.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `land_authoritative_theme_mapping` | `client`, `job_env`, `run_date`, Git/task identity | Lands the configured theme mapping and returns its exact landing ID and Delta version. |
| `refresh_item_attributes` | `client`, `job_env`, date-gated attribute refresh, `run_date` | Refreshes item attributes independently of theme-mapping landing. |
| `build_authoritative_item_themes` | `client`, `job_env`, date-gated theme refresh, mapping config and exact landing values | Runs after both input branches and creates the item-theme source available to score sources. |
| `accept_scoring_inputs` | `client`, `job_env`, `run_date`, exact landing values and Git/task identity | Records the accepted scoring-input snapshot and its exact source bindings. |

### `mktg_next_uk_nextads_model_scoring`

Scheduled at 12:15 Europe/London and parameterised by `model_name`. The current supported value is `theme_affinity`. The job validates that name against the repository scoring declaration, calls the separate shared scoring-inputs job with the same `run_date`, then runs the resolved scoring implementation and both compatibility branches. A future implementation extends this shared route instead of adding another saved scoring job.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `validate_model_scoring_request` | `model_name` | Requires an exact declared score source (`provider`) and supported implementation before any operational write; currently resolves only `theme_affinity`. |
| `prepare_scoring_inputs` | Native child job with the same `run_date` | Calls `mktg_next_uk_nextads_scoring_inputs`, waits for its accepted snapshot and does not generate advert options. |
| `use_theme_affinity_scoring` | Implementation returned by request validation | Selects the current Theme Affinity implementation; an unsupported implementation fails validation rather than silently using this branch. |
| `prepare_foundation_context` | `run_date`, `input_snapshot_id`, publication namespaces/prefixes and run identity | Pins the accepted scoring-input snapshot and opens the work record called a foundation context. |
| `predict_data_prep` | Theme Affinity Lakeflow pipeline | Builds the complete, ranked and feature relations for the pinned context. |
| `publish_and_score` | `model_uri`, exact context/pipeline/run identity and publication namespaces/prefixes | Publishes the prepared ranked account-theme data once, scores from that exact version, writes the shared score-source signals and records `READY_FOR_NEXTADS` last. |
| `publish_provider_compatibility` and `publish_feature_compatibility` | Exact `run_date`, score-source id (`provider` id), source/target namespaces and feature suffixes | Publish legacy model-output and four feature table shapes in parallel after the shared score output is accepted. |
| `sense_check_model_outputs` and `sense_check_foundation` | Exact compatibility outputs and configured baselines | Independently validate both compatibility branches; a later failure cannot revoke the already accepted score-source manifest. |

### `mktg_next_uk_nextads_candidate_foundation`

Scheduled at 16:00 Europe/London. It prepares the shared customer information available when advert options are built. Customer-cell assignment/combine, repeat-ad exposure and advert feedback remain separate calculations; publication records their exact table versions in one accepted `Candidate Foundation` record only after all three branches succeed. Both routes select that record. V1 applies cells, exposure and feedback; V2 applies cells and exposure but explicitly disables feedback.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `assign_customer_cells` | `client`, `job_env`, date-gated control refresh and `run_date` | Builds the fixed and transient cell assignments. |
| `combine_customer_cells` | `client`, `job_env`, `run_date`, Git commit | Runs after cell assignment and atomically replaces the accepted combined customer-cell table. |
| `build_repeat_ad_exposure` | `client`, `job_env`, `run_date`, shared-input identity (`foundation` identity) and Git commit | Calculates repeat-ad exposure independently of the customer-cell branch. |
| `build_ad_feedback` | `client`, `job_env`, `run_date`, shared-input identity (`foundation` identity) and Git commit | Calculates advert feedback independently of the customer-cell branch. |
| `publish_candidate_foundation` | Exact table/version/receipt values from all three outputs plus run/task identity | Records the accepted shared-customer-input snapshot last; it does not recalculate the source data. |

### `mktg_next_uk_nextads_candidate_build`

Scheduled at 18:00 Europe/London. It selects the accepted shared customer inputs produced by the separate 16:00 `Candidate Foundation` job, loads and audits the independent v1/v2 control sheets, resolves the configured score selection for each route, maps the selected scores to eligible adverts, and waits for the route-specific page-build jobs. Shared scoring inputs and model scoring remain separate upstream jobs, and Markov remains an independently runnable shadow score source that advert-option publication does not wait for.

`run_date` defaults to `{{job.start_time.iso_date}}` and is forwarded to both page-build jobs. The `v1_portfolio_policy_id` and `v2_portfolio_policy_id` parameters default to the declared score-selection policies. The parameters cannot name an undeclared policy or override a higher-precedence matching policy. A v1 control or required score-source failure cannot block the v2 route, and the reverse is also true. Business coverage findings remain warning-only; technical inability to run an audit or read the pinned score output fails only that route.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `select_candidate_foundation` | `client`, `job_env`, `run_date`, shared-customer-input snapshot (`foundation` snapshot) selection and task attempt | Selects one accepted shared-customer-input snapshot for the run and passes its exact table/version bindings to both routes. |
| `load_control_sheet_v1` | `client`, `job_env`, `run_date` | Loads v1 location control-sheet data and writes `control_sheet_latest`. Home Page remains on this route. |
| `audit_control_sheet_v1` | `route`, `client`, `job_env`, `run_date`, `warn-only` | Reports business findings as warnings. A technical audit failure stops v1 before mapping. |
| `quality_audit_ads_v1` | `route=v1`, `client`, `job_env`, `run_date`, `item-num-threshold=10`, `item-coverage=0.75`, `theme-coverage=0.5`, `image-item-coverage=0.7` | Runs after `load_control_sheet_v1` and publishes v1 advert-quality measurements. |
| `load_control_sheet_v2` | `client`, `job_env`, `run_date`, `phase=land` | Reads the current v2 Google Sheet and exclusions, then replaces their dated raw and latest tables before any CMS request is made. |
| `write_exclusions` | `client`, `job_env` | Runs after `load_control_sheet_v2` and publishes the landed exclusions to the configured Cosmos container. It does not gate V2 control processing or advert-option mapping. |
| `trigger_data_pull_for_CMS_pull` | Native child job with `run_date` | Runs after the raw v2 control sheet is landed, so CMS and sort-order acquisition use the advert IDs from that exact sheet. |
| `process_control_sheet_v2` | `client`, `job_env`, `run_date`, `phase=process` | Runs after CMS acquisition, reads the same-date landed inputs, checks them against the refreshed CMS and sort-order data, then writes `control_sheet_latest_v2`. |
| `audit_control_sheet_v2` | `route`, `client`, `job_env`, `run_date`, `warn-only` | Runs after v2 processing and reports business findings as warnings. A technical audit failure stops v2 before mapping. |
| `quality_audit_ads_v2` | `route=v2`, `client`, `job_env`, `run_date`, `item-num-threshold=10`, `item-coverage=0.75`, `theme-coverage=0.5`, `image-item-coverage=0.7` | Runs after `process_control_sheet_v2` and `quality_audit_ads_v1`. The extra v1 dependency serialises the two writers to their shared quality tables. |
| `resolve_scoring_portfolio_v1` and `resolve_scoring_portfolio_v2` | policy id, capability, use case, route, run date, task attempt | Resolve each route's score-selection list (`portfolio`) by priority and then stable policy-ID precedence. Required serving score sources wait until the fixed 18:30 Europe/London deadline and select same-day readiness or an accepted fallback no more than 24 hours old. Shadow score sources never block the route. Each entry pins the exact score-source attempt, table, Delta version, input snapshot, experiment and variant; entries publish before the ready selection-list header. |
| `validate_score_provider_theme_coverage_v1` and `validate_score_provider_theme_coverage_v2` | route plus serving score-selection entry (`portfolio` entry), score-source/current input snapshots and `warn-only` | Compare active advert themes with the exact serving score output. When fallback uses an older input snapshot, themes whose accepted definition changed are excluded. Missing business coverage warns; an unreadable or invalid score-source version fails the route. |
| `map_theme_scores_to_ads_v1` | run date, exact score-selection attempt (`portfolio` attempt), current input snapshot, customer-cell, exposure and feedback bindings, task attempt and compatibility limit | Captures the control-table Delta version once, calculates each unique serving score source once, applies V1 advert feedback, writes the standard advert sets and up to 20 advert-option rows per account/ad set/selection entry, then marks the advert-option result ready. |
| `map_theme_scores_to_ads_v2` | run date, exact score-selection attempt (`portfolio` attempt), current input snapshot, customer-cell and exposure bindings, task attempt and compatibility limit | Applies the same accepted advert-option contract at page-type grain with advert feedback disabled, and marks the v2 advert-option result ready only after its standard tables are written. |
| `run_page_build_v1` | Native child job plus accepted advert-option (`candidate`) attempt and existing input/output identifiers | Waits for the complete v1 assignment calculation, publication, validation and delivery result. |
| `run_page_build_v2` | Native child job plus accepted advert-option (`candidate`) attempt and existing input/output identifiers | Waits for the complete v2 assignment calculation, publication and payload result. |

Advert-option publication uses three internal tables whose names retain `candidate`. `candidate_ad_sets` records content-stable advert-set membership and route scopes. `candidate_scores` records the compact top-20 account/ad-set rows for every serving score-selection entry. `candidate_builds` is written last and is the only readiness signal. Rows from a failed or interrupted attempt are therefore not selectable. Shadow entries are not materialised on the nightly advert-option path. The separate 21:00 compatibility job reads the exact accepted v1/v2 attempts and publishes the existing `preranked_ads_from_themes_latest` and `preranked_ads_from_themes_v2_latest` table shapes.

The page-build jobs read only that accepted advert-option attempt. They resolve `best` and `best_challenger` from separate score-selection entries, even when both entries bind to the same score source today. Candidate, portfolio and candidate-foundation IDs retain those exact technical names when copied into assignment staging and completion events; the public assignment tables retain their existing columns.

### `mktg_next_uk_nextads_candidate_compatibility`

Independent 21:00 compatibility and validation job. Its v1 and v2 branches select the exact same-date READY advert-option result for their route and publish the existing preranked table shapes. After both compatibility branches succeed, it starts the assignment-validation job for the same run date. Failure here alerts separately and does not revoke an accepted advert-option result or live assignment snapshot.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `publish_v1_compatibility` | `client`, `job_env`, `run_date`, `route=v1` | Publishes `preranked_ads_from_themes_latest` from the exact accepted v1 attempt for that date. |
| `publish_v2_compatibility` | `client`, `job_env`, `run_date`, `route=v2` | Publishes `preranked_ads_from_themes_v2_latest` from the exact accepted v2 attempt for that date. |
| `assignment_quality_monitor` | Child job with `run_date` | Starts only after both compatibility tasks succeed and waits for the assignment-validation result. |

### `mktg_next_uk_nextads_markov_scoring`

Independent Markov score-source graph. It starts at 13:00 Europe/London and waits for the accepted daily scoring input for up to 90 minutes. That input carries the item-theme mapping produced by the shared scoring-inputs job invoked by the 12:15 model-scoring job; Markov does not refresh the mapping itself. It has its own failure alert and a 26,100-second timeout measured from the actual run start. An on-time 13:00 run therefore reaches that limit at 20:15, while a delayed start moves the cutoff later. A Markov failure remains outside the advert-option job's failure domain because Markov is registered as a shadow score source, not selected for serving.

Before a non-training run starts, Markov resolves the existing transition matrix from the production read catalog, rejects an empty model, and records its exact Delta table and version in the score-source work record and scoring-result identity. DEV scoring therefore uses the same fixed transition model as the current route while every scoring event, score-source signal, receipt and compatibility output continues to write only to the named DEV schema.

`build_and_publish_markov` pins the scoring input and transition-model versions, calculates the model output, converts it to the shared account/theme score shape, writes the score-source signals once, and records `READY_FOR_NEXTADS` last. It closes the score-source work record within the same task. The following compatibility task reads that exact accepted scoring result and updates the legacy Markov table shapes without changing its ready status.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `build_and_publish_markov` | `client`, `job_env`, `refresh_model_date`, `run_date`, `input_snapshot_id`, context/orchestration/task identity and Git commit | Waits up to 90 minutes for the accepted scoring input, pins that input and the transition-model version, calculates and publishes the shared score-source output, writes readiness last and closes the work record. |
| `publish_markov_compatibility` | `client`, `job_env`, `run_date`, `provider_id=markov` | Reads the exact same-date READY Markov scoring result and publishes the legacy compatibility tables. A compatibility failure does not revoke that READY result. |

### Adding A Score Source

This page records job parameters only. For the full connection sequence, supported score shapes, current role selection and promotion checks, see [Adding another scoring model](../architecture/future_model_adoption.md).

### `mktg_next_uk_nextads_dev_setup`

Personal DEV table bootstrap. This job prepares tables only; it does not score advert options.

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `setup_mode` | Job run mode shown in the Databricks job parameters UI. | `create_only` by default; use `seed_latest` only when a personal DEV schema needs seed data. |
| `--create-only` | Create missing personal DEV tables from terminal/manual CLI use. | Deprecated job flag alias for `setup_mode=create_only`. |
| `--seed-latest` | Create missing tables and seed the small latest/reference table set from terminal/manual CLI use. | Deprecated job flag alias for `setup_mode=seed_latest`. |
| `--sample` | Deprecated alias for `--seed-latest`. | Kept for old Databricks terminal commands. |
| `--standard` | Deprecated alias for `--create-only`. | Kept to avoid abruptly breaking old job parameters. |
| `job_env` | Environment guard. | Must be `dev`. Non-DEV values fail. |

### `mktg_next_uk_nextads_table_operations`

Manual table maintenance. Defaults are inert: every `run_*` action defaults to `false`, and `dry_run` defaults to `true`. Select exactly one `run_*` action for each run.

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `run_create_missing_tables` | Create configured tables that do not already exist. | Set to `true` for this action only. Requires `confirm_mutating=true` when `dry_run=false`. |
| `run_alter_tables` | Repair configured tables to match their SQL contracts. | Set to `true` for this action only. It adds safe trailing nullable columns directly and rebuilds drifted DEV/PREPROD tables by column name when order, type, nullability, or required defaulted columns need repair. PROD rebuild repair is blocked. Requires `confirm_mutating=true` when `dry_run=false`. |
| `run_recreate_tables` | Drop and recreate configured tables. | Set to `true` for this action only. Requires `confirm_destructive=true` when `dry_run=false`. |
| `run_drop_tables` | Drop explicit tables listed in `tables`. | Set to `true` for this action only. Requires `confirm_destructive=true` when `dry_run=false`. |
| `run_copy_prod_tables_to_dev` | Copy configured PROD read/source tables into the selected DEV schema. | Set to `true` for this action only. Requires `job_env=dev` and `confirm_mutating=true` when `dry_run=false`. |
| `client` | Client config key. | Usually `next_uk`. |
| `job_env` | Environment config to use. | Target-provided `dev`, `preprod`, or `prod`. |
| `catalog`, `schema` | Namespace for explicit table operations. | Required for `drop_tables`; defaults come from target variables. |
| `tables` | Optional comma-separated table list. | Blank means all configured tables for create/alter/recreate. For `drop_tables`, explicit names are required when `dry_run=false`. Unqualified names resolve under `catalog.schema`; fully qualified names must match `catalog.schema`. Wildcards are rejected. |
| `history_days` | Number of days copied by `run_copy_prod_tables_to_dev`. | Defaults to `1`. |
| `input_tables_only` | Skips generated ranking output tables during PROD-to-DEV copy. | Defaults to `true`. |
| `confirm_mutating` | Allows non-destructive mutation. | Must be `true` with `dry_run=false` for `run_create_missing_tables`, `run_alter_tables`, and `run_copy_prod_tables_to_dev`. |
| `confirm_destructive` | Allows destructive mutation. | Must be `true` with `dry_run=false` for `recreate_tables` and `drop_tables`. |
| `dry_run` | Preview without executing. | Defaults to `true`; set `false` only with the relevant confirmation. |

To copy PROD source tables into a personal DEV schema, run `mktg_next_uk_nextads_table_operations` with `run_copy_prod_tables_to_dev=true`, `job_env=dev`, `client=next_uk`, `history_days=1`, `input_tables_only=true`, `confirm_mutating=true`, and `dry_run=false`. Leave `dry_run=true` first when you only want to check the selected action.

To repair stale DEV table layouts before running advert-option or page-build jobs, run the same job with `run_alter_tables=true`, `job_env=dev`, `client=next_uk`, `tables` blank, `confirm_mutating=true`, and `dry_run=false`. This checks all configured write tables against the repo SQL contracts. For the known control-sheet drift, it rebuilds the stale table from a backup using column names rather than positional writes, so `IsUnderperforming` sits before `rundate` as expected. For `customer_cells_latest`, missing `Audience` is repaired with the literal string value `"false"`.

For a clean end-to-end run in a disposable personal DEV schema, recreate all feature-owned derived tables in one operation. Set `run_recreate_tables=true`, `confirm_destructive=true`, `dry_run=false`, and set `tables` to this comma-separated list:

```text
scoring_input_theme_mapping_raw,scoring_input_snapshots,scoring_input_snapshot_sources,scoring_input_item_themes,scoring_foundation_builds,scoring_foundation_outputs,scoring_foundation_run_contexts,account_theme_foundation_ranked,score_provider_builds,score_provider_signals,score_provider_run_contexts,scoring_portfolios,scoring_portfolio_entries,candidate_foundation_builds,candidate_foundation_sources,candidate_repeat_ad_exposure,candidate_ad_feedback,candidate_builds,candidate_scores,candidate_ad_sets,assignments_build_staging,assignments_v2_build_staging,assignment_build_events
```

This deliberately starts the prepared-input, score-source, score-selection, advert-option and internal assignment tables empty, including the technical `foundation`, `provider`, `portfolio` and `candidate` tables named above. The acceptance job sequence fills them in dependency order. The operation drops and recreates only these named tables and does not make backup copies. Follow it with `run_create_missing_tables=true`, `tables` blank, `confirm_mutating=true`, and `dry_run=false` to create any other configured table that is absent without changing an existing table.

Do not run a blank `run_alter_tables` pass as part of this clean personal-schema bootstrap. None of the preserved public assignment, delivery, control-sheet or customer-cell tables requires alteration for the split-table migration. Those tables retain useful history and input context and are updated by their normal jobs. Diagnose and target any unrelated legacy drift separately rather than allowing a broad repair to backup-copy large tables during this acceptance run.

In a non-disposable environment where the wider split-table state must be retained, the minimum mandatory migration is still to recreate `assignments_build_staging`, `assignments_v2_build_staging`, and `assignment_build_events` when they lack the accepted advert-option, score-selection and shared-customer-input identifiers. Broad `alter_tables` intentionally refuses to backup-copy this transient data because doing so can exceed the one-hour table-operations timeout.

### `mktg_next_uk_nextads_sp_owned_table_access`

Manual DEV/PROD object-access reconciliation for relations owned by the service principal that runs the bundle. It is unscheduled, has one task and defaults to an inert dry run. The code fixes the recipient list, expected execution identities and allowed scopes; job parameters cannot broaden them.

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `dry_run` | Discovers qualifying relations and logs the proposed grants without changing access. | Defaults to `true`. |
| `confirm_mutating` | Explicitly permits the fixed grant plan to be applied. | Defaults to `false`; both `confirm_mutating=true` and `dry_run=false` are required to write grants. |
| Target-owned scope | Limits relation discovery to service-principal-owned objects. | DEV covers the approved `marketingdata_dev` owner scope; PROD covers only `marketingdata_prod.warehouse` and `marketingdata_prod.ds_sandbox`. |

Before applying, the task confirms that it is running as the expected target service principal, rejects an empty or unexpectedly large relation set and accepts only managed/external tables, views and materialized views. It grants the maximum supported object-level access (`ALL PRIVILEGES` and `MANAGE`) to the fixed recipients, then reads the grants back. It does not grant `USE CATALOG` or `USE SCHEMA`; those namespace permissions remain a separate access prerequisite.

### DEV Integration And PREPROD Table Setup Job Settings

These are fixed-parameter wrappers around `table_operations.py`.

| Job | Settings | Notes / options |
| --- | --- | --- |
| `mktg_next_uk_nextads_dev_integration_setup` | `operation=create_missing_tables`, `confirm_mutating=true`, `dry_run=false` | Creates missing shared DEV Integration tables. |
| `mktg_next_uk_nextads_dev_integration_alter` | `operation=alter_tables`, `confirm_mutating=true`, `dry_run=false` | Adds supported missing columns in shared DEV Integration. |
| `mktg_next_uk_nextads_dev_integration_migrate` | `operation=recreate_tables`, `confirm_destructive=true`, `dry_run=false` | Destructive table recreation for shared DEV Integration; run deliberately. |
| `mktg_next_uk_nextads_preprod_setup` | `operation=create_missing_tables`, `confirm_mutating=true`, `dry_run=false` | Creates missing PREPROD validation tables only. |

### `mktg_next_uk_nextads_feature_store`

Shared DEV feature-store build. The retained Analytics pCTR source SQL, source validation and exact receipt now run as internal tasks before Feature Store preflight; there is no standalone source saved job.

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `reference_date` | Feature snapshot date. | `current` or `YYYY-MM-DD`. |
| `source_catalog`, `source_schema` | Primary source namespace. | Existing Unity Catalog catalog/schema. |
| `theme_source_catalog`, `theme_source_schema` | Theme source namespace. | Existing Unity Catalog catalog/schema. |
| `theme_table_prefix` | Prefix for theme source tables. | Physical Delta prefix, for example `next_uk_nextads_account_theme_foundation`. |
| `theme_training_reference_date` | Reference date for theme-affinity training input. | `current` or `YYYY-MM-DD`. |
| `analytics_pctr_source_binding` | Repository declaration for the expected Analytics pCTR source table and schema. | Target-provided config path. |
| `analytics_pctr_source_schema` | Schema where the internal Analytics pCTR notebook chain builds its source tables. | Target-provided schema; the source catalog is the bundle marketing-data catalog. |
| `recreate_feature_tables` | Recreate feature-store tables before building. | `false` by default; use `true` only for intentional table rebuilds. |
| Fixed task settings | `catalog`, `schema`, `manage_principal`, `all_privileges_principal`, `replace_reference_date`, `log_level` | Set by bundle variables/job definition; only change with feature-store ownership review. |

### Shared Feature And Model Job Settings

These are the centrally owned feature and model routes around the shared Feature Store. A data scientist declares a model in `configs/models/nextads_models.yaml` and selects an operation on the shared job; a model, use case or experiment does not get its own saved job.

| Job | Operator-selected settings | Notes / options |
| --- | --- | --- |
| `mktg_next_uk_nextads_model_development` | `operation`, declared `model_name`, then only that operation's fields | `BUILD` requires observation dates, feature dates and `label_end`; `RESEARCH` requires `label_end` and takes split dates from the declaration; `REVIEW_SELECT` requires the exact research result, selected model option (`candidate`), reviewer and reason; `EVALUATE` requires the exact model-training result and run date and accepts bounded evaluation overrides. The job rejects irrelevant fields before starting the operation. |
| `mktg_next_uk_nextads_model_discovery` | `enabled`, declared `model_name`, exact `research_build_id` and `timeout_minutes` | Centrally owned separate-runtime discovery. It is disabled by default; when enabled, the timeout must be 1-120 minutes (30 by default), discovery uses the exact receipted research frame and no model is registered or activated. |
| `mktg_next_uk_nextads_product_embedding_runtime_smoke` | `log_level` | Read-only advert-item bridge and registered embedding runtime proof. |

### Theme Affinity Model Lifecycle Job Settings

| Job | Settings | Notes / options |
| --- | --- | --- |
| `mktg_next_uk_nextads_theme_affinity_model_train` | `client`, `job_env`, `input_table`, `alias_suffix=gpu_xgboost`, `log_level` | GPU XGBoost training. `input_table` must be a readable training table. |
| `mktg_next_uk_nextads_theme_affinity_model_train_spark` | `client`, `job_env`, `input_table`, `log_level` | Spark XGBoost training. |
| `mktg_next_uk_nextads_model_import_dev_integration` | `source_model_name`, `source_model_version`, `source_alias`, `target_model_name`, `target_alias`, `model_family` | Shared lifecycle copy from a reviewed personal DEV model namespace into `marketingdata_dev.nextads_integration` after the PR is completed. Provide the reviewed `source_model_version` where possible. |
| `mktg_next_uk_nextads_model_import_preprod` | `source_model_name`, `source_model_version`, `source_alias`, `target_model_name`, `target_alias`, `model_family` | Shared lifecycle copy from a reviewed DEV Integration version into PREPROD. Provide the reviewed exact version where possible. |
| `mktg_next_uk_nextads_theme_affinity_model_import_dev` | `source_model_name`, `source_model_version`, `source_alias`, `target_model_name`, `target_alias` | Imports reviewed DEV Integration model into PREPROD namespace. Provide the reviewed `source_model_version` where possible. If it is blank, `source_alias` must resolve to the reviewed source version. |
| `mktg_next_uk_nextads_theme_affinity_model_promote` | `source_model_name`, `source_model_version`, `source_alias`, `target_model_name`, `target_alias` | Promotes reviewed PREPROD model into PROD namespace. Provide the reviewed `source_model_version` where possible. If it is blank, `source_alias` must resolve to the reviewed source version. |
| `mktg_next_uk_nextads_theme_affinity_model_monitor` | `baseline_table`, `candidate_table`, `sample_limit`, `log_level` | Compares two model output tables. `sample_limit` is an integer row cap. |

For the data-science operating sequence, evidence to capture and stop conditions, see [`model_lifecycle_runbook.md`](../model_lifecycle_runbook.md).

### `mktg_next_uk_nextads_theme_affinity_quality_monitor_setup`

Databricks quality monitor configuration for Theme Affinity ranked outputs.

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `action` | Monitor action. | Currently `setup`; use other values only if supported by `setup_quality_monitor.py`. |
| `monitor_type` | Databricks monitor type. | Currently `time_series`. |
| `table_name` | Table to monitor. | Fully qualified table name. |
| `output_schema_name` | Schema for monitor assets/output. | Fully qualified `catalog.schema`. |
| `assets_dir` | Workspace path for monitor assets. | Workspace path. |
| `warehouse_id` | SQL warehouse used by monitor setup. | Databricks warehouse id. |
| `timestamp_col` | Time-series timestamp/date column. | Existing column in `table_name`. |
| `granularities` | Time windows. | Comma-separated Databricks monitor granularities, for example `1 day`. |
| `slicing_exprs` | Segment columns/expressions. | Comma-separated expressions. |
| `custom_metrics_profile` | Custom metric profile to use. | Profile name supported by the setup script. |
| `problem_type` | ML problem type. | Currently `classification`. |
| `prediction_col`, `model_id_col`, `label_col`, `prediction_proba_col` | Monitor column mapping. | Existing column names; `prediction_proba_col` may be empty when not used. |

### Page Build And Delivery Job Settings

| Job | Settings | Notes / options |
| --- | --- | --- |
| `mktg_next_uk_nextads_page_build` | `run_date`, `build_run_id`, accepted advert-option attempt, score-source/shared-customer-input identifiers and pinned customer cells | `build_and_publish_v1` calculates all 77 primary locations plus SB2/OC2 in one Spark graph, validates the complete 79-scope output, writes history and then live latest. MASID and PLP child jobs start only after that task succeeds. |
| `mktg_next_uk_nextads_page_build_v2` | `run_date`, `build_run_id`, accepted advert-option attempt, score-source/shared-customer-input identifiers and pinned customer cells | `build_and_publish_v2` calculates and validates all five page types in one Spark graph, writes history and then live latest. Payload export starts only after that task succeeds. |
| `mktg_next_uk_nextads_assignment_validation` | The saved job declares `run_date` and older optional result/source/input parameters, but its task passes only fixed `client` and `job_env`; the compatibility caller forwards only `run_date` | Runs operational assignment and input-quality checks without writing a data table. The current task does not consume the caller's accepted-result identifiers. |
| `mktg_next_uk_nextads_masid_handoff` | `client`, `job_env` | Runs MASID handoff checks. |
| `mktg_next_uk_nextads_payload_export` | `client`, `job_env`, `do_export` | `do_export=1` enables export. |
| `mktg_next_uk_nextads_plp_gs_delivery` | `client`, `job_env`, `territory` | Iterates configured client/territory inputs. |

### Results, Realtime And Data Pull Job Settings

| Job | Settings | Notes / options |
| --- | --- | --- |
| `mktg_next_uk_nextads_results_cicd` | `client`, `job_env`, plus `label_window_days=28` for inference-log enrichment | Results tasks run in sequence; `label_window_days` is an integer day window. |
| `mktg_next_uk_nextads_realtime_data` | `client`, `job_env`, `reference-date`, `history-data-weighting`, `lift-threshold`, `ad-coverage-threshold`, `advert-matching-threshold` | Builds advert-to-advert affinity data and known realtime reranking features. |
| `mktg_next_uk_nextads_realtime_inputs` | `client`, `job_env` | Builds realtime viewed/bought inputs. |
| `mktg_next_uk_nextads_realtime_results_cicd` | `client`, `job_env` | Builds realtime result outputs. |
| `mktg_next_uk_nextads_data_pull` | `client`, `job_env`, `log_level` | Pulls and archives sort-order data through the configured pipeline/task graph. |
| `mktg_next_uk_nextads_analytics_pctr` | `catalog_schema_prefix`, `start_date`, `end_date`, `lookback_period`, `year_lookback_period`, `table_prefix`, model URIs | DEV-only analytics PCTR notebook graph. Dates default to `{{job.start_time.iso_date}}`; lookback values are integer day windows; model URIs are MLflow model references. |

### Smoke, Contract, Monitoring And Retention Job Settings

| Job | Settings | Notes / options |
| --- | --- | --- |
| `mktg_next_uk_nextads_preprod_dependency_smoke` | `job_env`, `sample_read_count`, `log_level` | `sample_read_count=0` keeps the smoke metadata-only. Use positive integers only when sample reads are deliberately required. |
| `mktg_next_uk_nextads_prod_table_contract_smoke` | `client`, `job_env`, `log_level` | Read-only production table-contract check. |
| `mktg_next_uk_nextads_table_monitoring` | No explicit task parameters in the bundle. | Runs `calculate_table_sizes.py` using script defaults/current runtime context. |
| `mktg_next_uk_nextads_table_maintenance` | `run_date`; fixed `client`, `job_env` and `log_level` | Applies the allowlisted retention and vacuum plan for the logical run date. |

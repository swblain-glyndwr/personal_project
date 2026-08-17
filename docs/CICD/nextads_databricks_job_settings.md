# NextAds Databricks Job Settings

Status: Working reference

This page explains the runtime settings declared in `pipelines/databricks/jobs/*.yml`. For target availability and release-route rules, see `docs/CICD/nextads_databricks_job_environment_matrix.md`.

## Common Settings

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `client` | Client configuration key used to resolve client tables and settings. | Usually `next_uk`; only use another configured client after checking `configs/clients/`. |
| `job_env` | Runtime environment passed to scripts and config loading. | `${var.job_parameter_environment_name}` from the bundle target: normally `dev`, `preprod`, or `prod`. |
| `log_level` | Python logging verbosity. | Standard logging levels such as `INFO`, `WARNING`, `ERROR`, `DEBUG`. |
| `reference_date` | Feature or model data reference date. | `current` where supported, or `YYYY-MM-DD`. |
| Refresh date flags | Date-gated refresh controls such as `refresh_model_date`. | `YYYY-MM-DD`; refresh happens only when the supplied date matches the run date. |
| Table names / namespaces | Unity Catalog objects used by table, feature, model, or monitor jobs. | Fully qualified `catalog.schema.table` or `catalog.schema` unless the script documents otherwise. |
| Boolean settings | String booleans passed through DAB/job parameters. | Prefer `true` or `false`. Some scripts also accept `1`/`0`. |

## Job Settings

### `mktg_next_uk_nextads_theme_inputs`

Scheduled at 12:15 Europe/London. It lands the agreed theme mapping and refreshes item attributes in parallel, builds the authoritative item-theme table, then accepts one scoring-input snapshot after the physical inputs have succeeded. `run_date` defaults to `{{job.start_time.iso_date}}`; use an explicit `YYYY-MM-DD` only for a controlled historical run.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `land_authoritative_theme_mapping` | `client`, `job_env`, `run_date`, Git/task identity | Lands the configured theme mapping and returns its exact landing ID and Delta version. |
| `refresh_item_attributes` | `client`, `job_env`, date-gated attribute refresh, `run_date` | Refreshes item attributes independently of theme-mapping landing. |
| `build_authoritative_item_themes` | `client`, `job_env`, date-gated theme refresh, mapping config and exact landing values | Runs only after both input branches and builds the item-theme source used by providers. |
| `accept_scoring_inputs` | `client`, `job_env`, `run_date`, exact landing values and Git/task identity | Writes the accepted scoring-input snapshot after its source bindings are available. |

### `mktg_next_uk_nextads_candidate_foundation`

Scheduled at 16:00 Europe/London. It prepares the shared customer inputs before candidate build. Customer-cell assignment/combine, repeat-ad exposure and advert feedback remain separate calculations; publication records their exact table versions in one accepted foundation only after all three branches succeed.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `assign_customer_cells` | `client`, `job_env`, date-gated control refresh and `run_date` | Builds the fixed and transient cell assignments. |
| `combine_customer_cells` | `client`, `job_env`, `run_date`, Git commit | Runs after cell assignment and atomically replaces the accepted combined customer-cell table. |
| `build_repeat_ad_exposure` | `client`, `job_env`, `run_date`, foundation identity and Git commit | Builds repeat-ad exposure independently of the customer-cell branch. |
| `build_ad_feedback` | `client`, `job_env`, `run_date`, foundation identity and Git commit | Builds advert feedback independently of the customer-cell branch. |
| `publish_candidate_foundation` | Exact table/version/receipt values from all three outputs plus run/task identity | Records the accepted foundation last; it does not recalculate the source data. |

### `mktg_next_uk_nextads_candidate_build`

Main NextAds candidate-generation graph. It selects the accepted Candidate Foundation produced by the separate 16:00 job, loads and audits the independent v1/v2 control sheets, resolves the configured provider selection for each route, maps the selected scores to adverts, and waits for the route-specific page-build jobs. Candidate rows are accepted through an internal manifest before the page job receives their exact attempt ID. Theme Inputs and Theme Affinity are separate upstream jobs. Markov is an independently runnable shadow provider and candidate publication does not wait for it.

The candidate job parameter `run_date` defaults to `{{job.start_time.iso_date}}` and is forwarded to both page-build jobs. The `v1_portfolio_policy_id` and `v2_portfolio_policy_id` parameters default to the declared route policies. The parameters cannot name an undeclared policy or override a higher-precedence matching policy. A v1 control or required-provider failure cannot block the v2 route, and the reverse is also true. Business coverage findings remain warning-only; technical inability to run an audit or read the pinned provider output fails only that route.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `select_candidate_foundation` | `client`, `job_env`, `run_date`, foundation snapshot selection and task attempt | Selects one accepted Candidate Foundation for the run and passes its exact table/version bindings to both routes. |
| `load_control_sheet_v1` | `client`, `job_env`, `run_date` | Loads v1 location control-sheet data and writes `control_sheet_latest`. Home Page remains on this route. |
| `audit_control_sheet_v1` | `route`, `client`, `job_env`, `run_date`, `warn-only` | Reports business findings as warnings. A technical audit failure stops v1 before mapping. |
| `load_control_sheet_v2` | `client`, `job_env`, `run_date`, `phase=land` | Reads the current v2 Google Sheet and exclusions, then replaces their dated raw and latest tables before any CMS request is made. |
| `trigger_data_pull_for_CMS_pull` | Native child job with `run_date` | Runs after the raw v2 control sheet is landed, so CMS and sort-order acquisition use the advert IDs from that exact sheet. |
| `process_control_sheet_v2` | `client`, `job_env`, `run_date`, `phase=process` | Runs after CMS acquisition, reads the same-date landed inputs, checks them against the refreshed CMS and sort-order data, then writes `control_sheet_latest_v2`. |
| `audit_control_sheet_v2` | `route`, `client`, `job_env`, `run_date`, `warn-only` | Runs after v2 processing and reports business findings as warnings. A technical audit failure stops v2 before mapping. |
| `resolve_scoring_portfolio_v1/v2` | policy id, capability, use case, route, run date, task attempt | Applies priority then stable policy-ID precedence. Required serving providers wait until the fixed 18:30 Europe/London deadline and select same-day readiness or an accepted fallback no more than 24 hours old. Shadow providers never block the route. Each entry pins the exact provider attempt, table, Delta version, input snapshot, experiment and variant; entries publish before the ready portfolio header. |
| `validate_score_provider_theme_coverage_v1/v2` | route plus serving portfolio entry, provider/current input snapshots, `warn-only` | Compares active ad themes with the exact serving output. When fallback uses an older input snapshot, themes whose accepted definition changed are excluded. Missing business coverage warns; an unreadable or invalid provider version fails the route. |
| `map_theme_scores_to_ads_v1` | run date, exact portfolio attempt, current input snapshot, candidate-foundation bindings, task attempt, compatibility limit | Captures the control-table Delta version once, calculates each unique serving provider once, writes the canonical ad sets and up to 20 candidate rows per account/ad set/provider entry, then marks the candidate build ready. |
| `map_theme_scores_to_ads_v2` | run date, exact portfolio attempt, current input snapshot, candidate-foundation bindings, task attempt, compatibility limit | Applies the same accepted-candidate contract at page-type grain and marks the v2 candidate build ready only after its canonical tables are written. |
| `run_page_build_v1` | Native child job plus accepted candidate attempt and existing provenance | Waits for the complete v1 page build, publication, validation and delivery result. |
| `run_page_build_v2` | Native child job plus accepted candidate attempt and existing provenance | Waits for the complete v2 page build, publication and payload result. |

Candidate publication uses three internal tables. `candidate_ad_sets` records content-stable ad-set membership and route scopes. `candidate_scores` records the compact top-20 account/ad-set rows for every serving portfolio entry. `candidate_builds` is written last and is the only readiness signal. Rows from a failed or interrupted attempt are therefore not selectable. Shadow entries are not materialised on the nightly candidate path. The separate 21:00 compatibility job reads the exact accepted v1/v2 attempts and publishes the existing `preranked_ads_from_themes_latest` and `preranked_ads_from_themes_v2_latest` table shapes.

The page-build jobs read only that accepted attempt. They resolve `best` and `best_challenger` from separate portfolio entries, even when both entries bind to the same provider today. Candidate, portfolio and candidate-foundation IDs are copied into assignment staging and completion events; the public assignment tables retain their existing columns.

### `mktg_next_uk_nextads_candidate_compatibility`

Independent 21:00 compatibility and monitoring job. Its v1 and v2 branches select the exact same-date READY candidate build for their route and publish the existing preranked table shapes. After both compatibility branches succeed, it starts the assignment-quality job for the same run date. Failure here alerts separately and does not revoke an accepted candidate build or live assignment snapshot.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `publish_v1_compatibility` | `client`, `job_env`, `run_date`, `route=v1` | Publishes `preranked_ads_from_themes_latest` from the exact accepted v1 attempt for that date. |
| `publish_v2_compatibility` | `client`, `job_env`, `run_date`, `route=v2` | Publishes `preranked_ads_from_themes_v2_latest` from the exact accepted v2 attempt for that date. |
| `assignment_quality_monitor` | Child job with `run_date` | Starts only after both compatibility tasks succeed and waits for the assignment-quality result. |

### `mktg_next_uk_nextads_markov_scoring`

Independent Markov score-provider graph. It starts at 13:00 Europe/London and waits for the same accepted daily scoring input for up to 90 minutes. That accepted input carries the item-theme mapping produced by the separate theme input job; Markov does not refresh the mapping itself. It has its own failure alert and a 26,100-second job deadline, so a delayed run cannot continue beyond 20:15. A Markov failure remains outside the candidate-build failure domain because Markov is registered as a shadow provider, not selected for serving.

Before a non-training run starts, Markov resolves the existing transition matrix from the production read catalog, rejects an empty model, and records its exact Delta table and version in the provider context and build identity. DEV scoring therefore uses the same immutable transition model as the current route while every scoring event, provider signal, receipt and compatibility output continues to write only to the named DEV schema.

`build_and_publish_markov` pins the scoring input and transition-model versions, calculates the model output, converts it to the shared account/theme score shape, writes the provider signals once, and records `READY_FOR_NEXTADS` last. It closes the provider context within the same task. The following compatibility task reads that exact accepted build and updates the legacy Markov table shapes without changing whether the provider build is ready.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `build_and_publish_markov` | `client`, `job_env`, `refresh_model_date`, `run_date`, `input_snapshot_id`, context/orchestration/task identity and Git commit | Waits up to 90 minutes for the accepted scoring input, pins that input and the transition-model version, calculates and publishes the canonical provider output, writes readiness last and closes the context. |
| `publish_markov_compatibility` | `client`, `job_env`, `run_date`, `provider_id=markov` | Reads the exact same-date READY Markov provider build and publishes the legacy compatibility tables. A compatibility failure does not revoke the canonical READY build. |

### Adding another score provider

A new challenger follows the same route whether it is theme-based, ad-based, or uses another registered account/entity capability:

1. Register the provider, capability, entity type and source-column mapping in `configs/scoring/scoring_settings.yaml`.
2. Build the model and emit one row per account/entity with its raw and final score.
3. Use `adapt_configured_provider_scores` to convert those configured columns to the canonical contract, then use `stage_provider_signals` to write the exact build.
4. Complete the build through the shared provider publication contract, which records readiness last.
5. Add the provider to a portfolio as `SHADOW`/`EVALUATE` first. Promotion to a serving challenger or champion is a separate reviewed configuration change.

The portfolio entry is the plug-in point: it declares the capability and serving or evaluation slot, then binds the exact validated model build. The candidate route depends on that contract, not on how the model produced its scores. The current default keeps Theme Affinity in both `best` and `best_challenger` and records Markov as non-blocking shadow evidence.

The full job and table hand-off is shown in [`nextads_databricks_runtime_map.md`](nextads_databricks_runtime_map.md).

When two serving entries bind the same provider build, candidate scoring is computed once and published under both entry identities. A different compatible provider uses the same canonical adapter without a provider-specific branch.

No model-specific code belongs in the shared adapter or publisher. A compatibility publisher is configured only where an existing consumer still needs a legacy table shape. The consuming route must already support the provider capability; the current theme-ranking route consumes `account_theme`, while `account_ad` is accepted by the contract for a route that supports ad scores.

### `mktg_next_uk_nextads_dev_setup`

Personal DEV table bootstrap. This job prepares tables only; it does not run candidate scoring.

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

To repair stale DEV table layouts before running candidate/page-build jobs, run the same job with `run_alter_tables=true`, `job_env=dev`, `client=next_uk`, `tables` blank, `confirm_mutating=true`, and `dry_run=false`. This checks all configured write tables against the repo SQL contracts. For the known control-sheet drift, it rebuilds the stale table from a backup using column names rather than positional writes, so `IsUnderperforming` sits before `rundate` as expected. For `customer_cells_latest`, missing `Audience` is repaired with the literal string value `"false"`.

For a clean end-to-end run in a disposable personal DEV schema, recreate all feature-owned derived tables in one operation. Set `run_recreate_tables=true`, `confirm_destructive=true`, `dry_run=false`, and set `tables` to this comma-separated list:

```text
scoring_input_theme_mapping_raw,scoring_input_snapshots,scoring_input_snapshot_sources,scoring_input_item_themes,scoring_foundation_builds,scoring_foundation_outputs,scoring_foundation_run_contexts,account_theme_foundation_ranked,score_provider_builds,score_provider_signals,score_provider_run_contexts,scoring_portfolios,scoring_portfolio_entries,candidate_foundation_builds,candidate_foundation_sources,candidate_repeat_ad_exposure,candidate_ad_feedback,candidate_builds,candidate_scores,candidate_ad_sets,assignments_build_staging,assignments_v2_build_staging,assignment_build_events
```

This deliberately starts the modular input, foundation, provider, portfolio, candidate and internal assignment state empty. The acceptance job sequence fills them in dependency order. The operation drops and recreates only these named tables and does not make backup copies. Follow it with `run_create_missing_tables=true`, `tables` blank, `confirm_mutating=true`, and `dry_run=false` to create any other configured table that is absent without changing an existing table.

Do not run a blank `run_alter_tables` pass as part of this clean personal-schema bootstrap. None of the preserved public assignment, delivery, control-sheet or customer-cell tables requires alteration for the modular migration. Those tables retain useful history and input context and are updated by their normal jobs. Diagnose and target any unrelated legacy drift separately rather than allowing a broad repair to backup-copy large tables during this acceptance run.

In a non-disposable environment where the wider modular state must be retained, the minimum mandatory migration is still to recreate `assignments_build_staging`, `assignments_v2_build_staging`, and `assignment_build_events` when they lack candidate, portfolio and foundation provenance. Broad `alter_tables` intentionally refuses to backup-copy this transient data because doing so can exceed the one-hour table-operations timeout.

### DEV Integration And PREPROD Table Setup Jobs

These are fixed-parameter wrappers around `table_operations.py`.

| Job | Settings | Notes / options |
| --- | --- | --- |
| `mktg_next_uk_nextads_dev_integration_setup` | `operation=create_missing_tables`, `confirm_mutating=true`, `dry_run=false` | Creates missing shared DEV Integration tables. |
| `mktg_next_uk_nextads_dev_integration_alter` | `operation=alter_tables`, `confirm_mutating=true`, `dry_run=false` | Adds supported missing columns in shared DEV Integration. |
| `mktg_next_uk_nextads_dev_integration_migrate` | `operation=recreate_tables`, `confirm_destructive=true`, `dry_run=false` | Destructive table recreation for shared DEV Integration; run deliberately. |
| `mktg_next_uk_nextads_preprod_setup` | `operation=create_missing_tables`, `confirm_mutating=true`, `dry_run=false` | Creates missing PREPROD validation tables only. |

### `mktg_next_uk_nextads_feature_store`

Shared DEV feature-store build.

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `reference_date` | Feature snapshot date. | `current` or `YYYY-MM-DD`. |
| `source_catalog`, `source_schema` | Primary source namespace. | Existing Unity Catalog catalog/schema. |
| `theme_source_catalog`, `theme_source_schema` | Theme source namespace. | Existing Unity Catalog catalog/schema. |
| `theme_table_prefix` | Prefix for theme source tables. | Physical Delta prefix, for example `next_uk_nextads_account_theme_foundation`. |
| `theme_training_reference_date` | Reference date for theme-affinity training input. | `current` or `YYYY-MM-DD`. |
| `recreate_feature_tables` | Recreate feature-store tables before building. | `false` by default; use `true` only for intentional table rebuilds. |
| Fixed task settings | `catalog`, `schema`, `manage_principal`, `all_privileges_principal`, `replace_reference_date`, `log_level` | Set by bundle variables/job definition; only change with feature-store ownership review. |

### `mktg_next_uk_nextads_theme_affinity`

Operational Theme Affinity preparation and scoring graph. It contains three tasks: prepare the pinned foundation context, run the Lakeflow preparation, then publish the ranked foundation and provider signals on one Spark task. The accepted provider build is recorded READY last. This job is a shared upstream producer for candidate mapping and does not depend on either v1 or v2 control sheets; the route split happens later inside `mktg_next_uk_nextads_candidate_build`.

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `run_date` | Logical date for the foundation and provider build. | Defaults to `{{job.start_time.iso_date}}`; use `YYYY-MM-DD` for a controlled rerun. |
| `input_snapshot_id` | Accepted Theme Inputs snapshot to use. | `same_day` by default, or an explicit accepted snapshot ID for a pinned rerun. |
| `publish_source_namespace`, `publish_target_namespace` | Source Lakeflow and target canonical namespaces. | `catalog.schema`. |
| `publish_source_table_prefix`, `publish_target_table_prefix` | Source staging and target canonical table prefixes. | Prefix string without suffix. |
| `model_uri` | Model used for prediction. | Job parameter defaulting to `${var.theme_affinity_model_uri}`; override per validation run with the reviewed MLflow model URI or alias. |

### `mktg_next_uk_nextads_theme_feature_compatibility`

Independent 17:00 compatibility and monitoring graph. One branch reads the exact READY Theme Affinity provider build for the requested date and publishes the existing model-output table shapes before its model-output sense check. The other branch publishes the four Lakeflow feature table shapes before its foundation sense check. Failures alert independently and do not revoke or delay the accepted provider build.

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `run_date` | Exact provider and feature date to publish. | Defaults to `{{job.start_time.iso_date}}`; use the original build date for a controlled repair. |
| `source_namespace`, `target_namespace` | Lakeflow source and compatibility target namespaces. | `catalog.schema`. |
| `source_table_prefix`, `target_table_prefix` | Staging source and compatibility target prefixes. | Prefix string without suffix. |
| `table_suffixes` | Lakeflow feature outputs copied by the feature branch. | Bundle variable containing the four required suffixes. |

### Theme Affinity Model Lifecycle Jobs

| Job | Settings | Notes / options |
| --- | --- | --- |
| `mktg_next_uk_nextads_theme_affinity_model_train` | `client`, `job_env`, `input_table`, `alias_suffix=gpu_xgboost`, `log_level` | GPU XGBoost training. `input_table` must be a readable training table. |
| `mktg_next_uk_nextads_theme_affinity_model_train_spark` | `client`, `job_env`, `input_table`, `log_level` | Spark XGBoost training. |
| `mktg_next_uk_nextads_model_import_dev_integration` | `source_model_name`, `source_model_version`, `source_alias`, `target_model_name`, `target_alias`, `model_family` | Generic lifecycle copy from a reviewed personal DEV model namespace into `marketingdata_dev.nextads_integration` after the PR is completed. Provide the reviewed `source_model_version` where possible. |
| `mktg_next_uk_nextads_theme_affinity_model_import_dev` | `source_model_name`, `source_model_version`, `source_alias`, `target_model_name`, `target_alias` | Imports reviewed DEV Integration model into PREPROD namespace. Provide the reviewed `source_model_version` where possible. If it is blank, `source_alias` must resolve to the reviewed source version. |
| `mktg_next_uk_nextads_theme_affinity_model_promote` | `source_model_name`, `source_model_version`, `source_alias`, `target_model_name`, `target_alias` | Promotes reviewed PREPROD model into PROD namespace. Provide the reviewed `source_model_version` where possible. If it is blank, `source_alias` must resolve to the reviewed source version. |
| `mktg_next_uk_nextads_theme_affinity_model_monitor` | `baseline_table`, `candidate_table`, `sample_limit`, `log_level` | Compares two model output tables. `sample_limit` is an integer row cap. |

For the DS operating sequence, evidence to capture and stop conditions, see `docs/model_lifecycle_runbook.md`.

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

### Page Build And Delivery Jobs

| Job | Settings | Notes / options |
| --- | --- | --- |
| `mktg_next_uk_nextads_page_build` | `run_date`, `build_run_id`, accepted candidate attempt, provider/foundation provenance and pinned customer cells | `build_and_publish_v1` calculates all 77 primary locations plus SB2/OC2 in one Spark graph, validates the complete 79-scope output, writes history and then live latest. MASID and PLP child jobs start only after that task succeeds. |
| `mktg_next_uk_nextads_page_build_v2` | `run_date`, `build_run_id`, accepted candidate attempt, provider/foundation provenance and pinned customer cells | `build_and_publish_v2` calculates and validates all five page types in one Spark graph, writes history and then live latest. Payload export starts only after that task succeeds. |
| `mktg_next_uk_nextads_qa` | `client`, `job_env` | Runs operational QA in the target environment. |
| `mktg_next_uk_nextads_masid_handoff` | `client`, `job_env` | Runs MASID handoff checks. |
| `mktg_next_uk_nextads_payload_export` | `client`, `job_env`, `do_export` | `do_export=1` enables export. |
| `mktg_next_uk_nextads_plp_gs_delivery` | `client`, `job_env`, `territory` | Iterates configured client/territory inputs. |

### Results, Realtime, And Data Pull Jobs

| Job | Settings | Notes / options |
| --- | --- | --- |
| `mktg_next_uk_nextads_results_cicd` | `client`, `job_env`, plus `label_window_days=28` for inference-log enrichment | Results tasks run in sequence; `label_window_days` is an integer day window. |
|`mktg_next_uk_nextads_realtime_data` | `client`, `job_env`, `reference-date`, `history-data-weighting`,`lift-threshold`, `ad-coverage-threshold`| Builds realtime  advert: advert affinity inputs. |
| `mktg_next_uk_nextads_realtime_inputs` | `client`, `job_env` | Builds realtime viewed/bought inputs. |
| `mktg_next_uk_nextads_realtime_results_cicd` | `client`, `job_env` | Builds realtime result outputs. |
| `mktg_next_uk_nextads_data_pull` | `client`, `job_env`, `log_level` | Pulls and archives sort-order data through the configured pipeline/task graph. |
| `mktg_next_uk_nextads_analytics_pctr` | `catalog_schema_prefix`, `start_date`, `end_date`, `lookback_period`, `year_lookback_period`, `table_prefix`, model URIs | DEV-only analytics PCTR notebook graph. Dates default to `{{job.start_time.iso_date}}`; lookback values are integer day windows; model URIs are MLflow model references. |

### Smoke, Contract, And Monitoring Jobs

| Job | Settings | Notes / options |
| --- | --- | --- |
| `mktg_next_uk_nextads_preprod_dependency_smoke` | `job_env`, `sample_read_count`, `log_level` | `sample_read_count=0` keeps the smoke metadata-only. Use positive integers only when sample reads are deliberately required. |
| `mktg_next_uk_nextads_prod_table_contract_smoke` | `client`, `job_env`, `log_level` | Read-only production table-contract check. |
| `mktg_next_uk_nextads_table_monitoring` | No explicit task parameters in the bundle. | Runs `calculate_table_sizes.py` using script defaults/current runtime context. |

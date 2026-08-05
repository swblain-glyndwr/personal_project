# NextAds Databricks Job Settings

Status: Working reference

This page explains the runtime settings declared in `pipelines/databricks/jobs/*.yml`.
For target availability and release-route rules, see
`docs/CICD/nextads_databricks_job_environment_matrix.md`.

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

### `mktg_next_uk_nextads_candidate_build`

Main NextAds candidate-generation graph. It builds shared customer cells, loads
the independent v1/v2 control sheets, resolves an immutable scoring portfolio
for each route, maps the serving output version, and waits for the route-specific
page-build jobs. Legacy product Theme
Mapping and Markov scoring run independently in
`mktg_next_uk_nextads_markov_scoring`; candidate publication does not wait for
that job.

The candidate job parameter `run_date` defaults to
`{{job.start_time.iso_date}}` and is forwarded to both page-build jobs. The
`v1_portfolio_policy_id` and `v2_portfolio_policy_id` parameters default to the
declared route policies. The parameters cannot name an undeclared policy or
override a higher-precedence matching policy. A v1 control or required-provider
failure cannot block the v2 route,
and the reverse is also true. Business coverage findings remain warning-only;
technical inability to run an audit or read the pinned provider output fails only
that route.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `assign_customer_cells` | `client`, `job_env`, `refresh_control_date`, `run_date` | `refresh_control_date` is date-gated; use a current `YYYY-MM-DD` only when deliberately refreshing control assignments. |
| `combine_customer_cells` | `client`, `job_env`, `run_date` | Combines the accepted cell outputs for the logical run date. |
| `load_control_sheet_v1` | `client`, `job_env`, `run_date` | Loads v1 location control-sheet data and writes `control_sheet_latest`. Home Page remains on this route. |
| `audit_control_sheet_v1` | `route`, `client`, `job_env`, `run_date`, `warn-only` | Reports business findings as warnings. A technical audit failure stops v1 before mapping. |
| `trigger_data_pull_for_CMS_pull` | Native child job with `run_date` | Waits for the CMS acquisition job used by the v2 snapshot. |
| `load_control_sheet_v2` | `client`, `job_env`, `run_date` | Runs after CMS acquisition and writes `control_sheet_latest_v2`. |
| `audit_control_sheet_v2` | `route`, `client`, `job_env`, `run_date`, `warn-only` | Reports business findings as warnings. A technical audit failure stops v2 before mapping. |
| `resolve_scoring_portfolio_v1/v2` | policy id, capability, use case, route, run date, task attempt | Applies priority then stable policy-ID precedence. Required serving providers wait until the fixed 18:30 Europe/London deadline and select same-day readiness or an accepted fallback no more than 24 hours old. Shadow providers never block the route. Each entry pins the exact provider attempt, table, Delta version, input snapshot, experiment and variant; entries publish before the ready portfolio header. |
| `validate_score_provider_theme_coverage_v1/v2` | route plus serving portfolio entry, provider/current input snapshots, `warn-only` | Compares active ad themes with the exact serving output. When fallback uses an older input snapshot, themes whose accepted definition changed are excluded. Missing business coverage warns; an unreadable or invalid provider version fails the route. |
| `map_theme_scores_to_ads_v1` | run date, serving provider build/table/version/source date, provider/current input snapshots, `apply-ad-feedback`, `top-ads-per-location` | Reads the immutable canonical score version, quarantines changed fallback themes, joins `EntityID` to ad `Themes`, ranks by `Location`, and writes `preranked_ads_from_themes_latest`. |
| `map_theme_scores_to_ads_v2` | run date, serving provider build/table/version/source date, provider/current input snapshots, `top-ads-per-page-type` | Applies the same immutable and fallback rules at page-type grain and writes `preranked_ads_from_themes_v2_latest`. |
| `run_page_build_v1` | Native child job plus run/build/provider identities | Waits for the complete v1 page build, publication, validation and delivery result. |
| `run_page_build_v2` | Native child job plus run/build/provider identities | Waits for the complete v2 page build, publication and payload result. |

### `mktg_next_uk_nextads_markov_scoring`

Independent Markov score-provider graph. It starts at 13:00 Europe/London and
waits for the same accepted daily scoring input for up to 90 minutes. That
accepted input carries the item-theme mapping produced by the separate theme
input job; Markov does not refresh the mapping itself. It has its own failure
alert and a 26,100-second job deadline, so a delayed run cannot continue beyond
20:15. A Markov failure remains outside the candidate-build failure domain
because Markov is registered as a shadow provider, not selected for serving.

`build_markov_scores` materialises the model result once, converts that exact
frame to the canonical account/entity score contract, and stages it. The shared
`publish_provider_build` task then reads the exact staged Delta version,
validates keys, scores, ranks and metadata, publishes the legacy Markov tables
from the same frame, and records `READY_FOR_NEXTADS` last. The provider context
is consumed only after publication succeeds.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `prepare_provider_context` | provider, run date, input snapshot selection and orchestration identity | Waits up to 90 minutes for the accepted daily scoring input and pins its exact identity. |
| `build_markov_scores` | `client`, `job_env`, `refresh_model_date`, pinned input/build/context values | Uses the pinned item-theme input, produces one canonical Markov account-theme output and preserves the transition output. |
| `publish_provider_build` | pinned input/build/context values plus the staged Delta version | Uses the model-neutral publisher to validate the exact output, publish legacy next-theme compatibility tables, and mark the build ready last. |
| `finalize_provider_context` | context slot and orchestration run | Marks an unconsumed context failed after an unsuccessful build or publication; an already-consumed context is left unchanged. |

### Adding another score provider

A new challenger follows the same route whether it is theme-based, ad-based,
or uses another registered account/entity capability:

1. Register the provider, capability, entity type and source-column mapping in
   `configs/scoring/scoring_settings.yaml`.
2. Build the model and emit one row per account/entity with its raw and final
   score.
3. Use `adapt_configured_provider_scores` to convert those configured columns
   to the canonical contract, then use `stage_provider_signals` to stage the
   exact build.
4. Run the shared `publish_score_provider_build.py` task. It validates and
   versions the output, then records readiness last.
5. Add the provider to a portfolio as `SHADOW`/`EVALUATE` first. Promotion to a
   serving challenger or champion is a separate reviewed configuration change.

The portfolio entry is the plug-in point: it declares the capability and
serving or evaluation slot, then binds the exact validated model build. The
candidate route depends on that contract, not on how the model produced its
scores. The current default keeps Theme Affinity in both `best` and
`best_challenger` and records Markov as non-blocking shadow evidence.

No model-specific code belongs in the shared adapter or publisher. A
compatibility publisher is configured only where an existing consumer still
needs a legacy table shape. The consuming route must already support the
provider capability; the current theme-ranking route consumes `account_theme`,
while `account_ad` is accepted by the contract for a route that supports ad
scores.

### `mktg_next_uk_nextads_dev_setup`

Personal DEV table bootstrap. This job prepares tables only; it does not run
candidate scoring.

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

Operational Theme Affinity publish, predict, clean, and sense-check graph.
This job is a shared upstream producer for candidate mapping. It does not depend
on either v1 or v2 control sheets; the route split happens later inside
`mktg_next_uk_nextads_candidate_build`.

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `publish_source_namespace`, `publish_target_namespace` | Source and target namespaces for Lakeflow/DLT outputs. | `catalog.schema`. |
| `publish_source_table_prefix`, `publish_target_table_prefix` | Table prefix for source/target publish. | Prefix string without suffix. |
| `publish_table_suffixes` | Tables to publish by suffix. | Comma-separated suffix list from bundle variable. |
| `model_uri` | Model used for prediction. | Job parameter defaulting to `${var.theme_affinity_model_uri}`; override per validation run with the reviewed MLflow model URI or alias. |
| `check_scope` | Sense-check scope. | `data` or `model_outputs`. |
| Baseline/candidate settings | Baseline namespaces, prefixes, final table, and summary table. | Fully qualified namespaces/tables used for comparison evidence. |

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
| `mktg_next_uk_nextads_page_build` | `run_date`, `build_run_id`, `location`, `inherit_basic_from`, downstream trigger job ids/names | V1 uses `build_run_id=v1_{{job.run_id}}` by default and passes both build values to every location iteration. |
| `mktg_next_uk_nextads_page_build_v2` | `run_date`, `build_run_id`, `page_type`, downstream trigger job ids/names | V2 uses `build_run_id=v2_{{job.run_id}}` by default and passes both build values to every page-type iteration. |
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

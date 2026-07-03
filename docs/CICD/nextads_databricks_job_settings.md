# NextAds Databricks Job Settings

Status: Working reference

This page explains the runtime settings declared in `resources/jobs/*.yml`.
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

Main NextAds candidate-generation graph.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `assign_customer_cells` | `client`, `job_env`, `refresh_control_date` | `refresh_control_date` is date-gated; use a current `YYYY-MM-DD` only when deliberately refreshing control assignments. |
| `combine_customer_cells` | `client`, `job_env` | Combines outputs from assignment. |
| `load_control_sheet`, `load_control_sheet_v2` | `client`, `job_env` | Loads current control-sheet data for the target environment. |
| `parse_attributes` | `client`, `job_env`, `refresh_attributes_date` | Refreshes the attribute set only when the date is today; otherwise remaps using latest attributes. |
| `parse_theme_mapping` | `client`, `job_env`, `refresh_themes_date` | Refreshes theme mapping only when the date is today; otherwise uses latest mapping. |
| `score_lightweight` | `client`, `job_env`, `refresh_model_date` | Runs Markov scoring. Refreshes transition probabilities only when the date is today. |
| `map_theme_scores_to_ads` | `client`, `job_env`, `apply-ad-feedback`, `top-ads-per-location` | `apply-ad-feedback` is a flag. `top-ads-per-location` is a positive integer. |
| `map_theme_scores_to_ads_v2` | `client`, `job_env`, `top-ads-per-page-type` | `top-ads-per-page-type` is a positive integer. |
| `trigger_page_build_job` | `job-id`, `job-name`, `fail-on-submit-error` | Uses the target-local page-build job id. `fail-on-submit-error` is a flag. |

### `mktg_next_uk_nextads_dev_setup`

Personal DEV table bootstrap. This job prepares tables only; it does not run
candidate scoring.

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `--create-only` | Create missing personal DEV tables. | Default DAB mode and recommended onboarding path. |
| `--seed-latest` | Create missing tables and seed the small latest/reference table set. | Use only when a personal DEV schema needs seed data. |
| `--sample` | Deprecated alias for `--seed-latest`. | Kept for old Databricks terminal commands. |
| `--standard` | Deprecated alias for `--create-only`. | Kept to avoid abruptly breaking old job parameters. |
| `job_env` | Environment guard. | Must be `dev`. Non-DEV values fail. |

### `mktg_next_uk_nextads_table_operations`

Manual table maintenance. Defaults are inert.

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `operation` | Operation to prepare or execute. | `create_missing_tables`, `alter_tables`, `recreate_tables`, `drop_tables`. |
| `client` | Client config key. | Usually `next_uk`. |
| `job_env` | Environment config to use. | Target-provided `dev`, `preprod`, or `prod`. |
| `catalog`, `schema` | Namespace for explicit table operations. | Required for `drop_tables`; defaults come from target variables. |
| `tables` | Comma-separated table list for `drop_tables`. | Unqualified names resolve under `catalog.schema`; fully qualified names must match `catalog.schema`. Wildcards are rejected. |
| `confirm_mutating` | Allows non-destructive mutation. | Must be `true` with `dry_run=false` for `create_missing_tables` and `alter_tables`. |
| `confirm_destructive` | Allows destructive mutation. | Must be `true` with `dry_run=false` for `recreate_tables` and `drop_tables`. |
| `dry_run` | Preview without executing. | Defaults to `true`; set `false` only with the relevant confirmation. |

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
| `theme_table_prefix` | Prefix for theme source tables. | Table-name prefix, for example `next_uk_nextads_theme_affinity_predict`. |
| `theme_training_reference_date` | Reference date for theme-affinity training input. | `current` or `YYYY-MM-DD`. |
| `recreate_feature_tables` | Recreate feature-store tables before building. | `false` by default; use `true` only for intentional table rebuilds. |
| Fixed task settings | `catalog`, `schema`, `manage_principal`, `all_privileges_principal`, `replace_reference_date`, `log_level` | Set by bundle variables/job definition; only change with feature-store ownership review. |

### `mktg_next_uk_nextads_theme_affinity`

Operational Theme Affinity publish, predict, clean, and sense-check graph.

| Setting | Meaning | Options / format |
| --- | --- | --- |
| `publish_source_namespace`, `publish_target_namespace` | Source and target namespaces for Lakeflow/DLT outputs. | `catalog.schema`. |
| `publish_source_table_prefix`, `publish_target_table_prefix` | Table prefix for source/target publish. | Prefix string without suffix. |
| `publish_table_suffixes` | Tables to publish by suffix. | Comma-separated suffix list from bundle variable. |
| `model_uri` | Model used for prediction. | `${var.theme_affinity_model_uri}`; usually MLflow model URI or alias. |
| `check_scope` | Sense-check scope. | `data` or `model_outputs`. |
| Baseline/candidate settings | Baseline namespaces, prefixes, final table, and summary table. | Fully qualified namespaces/tables used for comparison evidence. |

### Theme Affinity Model Lifecycle Jobs

| Job | Settings | Notes / options |
| --- | --- | --- |
| `mktg_next_uk_nextads_theme_affinity_model_train` | `client`, `job_env`, `input_table`, `alias_suffix=gpu_xgboost`, `log_level` | GPU XGBoost training. `input_table` must be a readable training table. |
| `mktg_next_uk_nextads_theme_affinity_model_train_spark` | `client`, `job_env`, `input_table`, `log_level` | Spark XGBoost training. |
| `mktg_next_uk_nextads_theme_affinity_model_import_dev` | `source_model_name`, `source_model_version`, `source_alias`, `target_model_name`, `target_alias` | Imports reviewed DEV Integration model into PREPROD namespace. Provide either a version or resolvable source alias. |
| `mktg_next_uk_nextads_theme_affinity_model_promote` | `source_model_name`, `source_model_version`, `source_alias`, `target_model_name`, `target_alias` | Promotes reviewed PREPROD model into PROD namespace. Provide either a version or resolvable source alias. |
| `mktg_next_uk_nextads_theme_affinity_model_monitor` | `baseline_table`, `candidate_table`, `sample_limit`, `log_level` | Compares two model output tables. `sample_limit` is an integer row cap. |

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
| `mktg_next_uk_nextads_page_build` | `page_type`, `location`, `inherit_basic_from`, downstream trigger job ids/names | Iterates over configured page types and locations. `inherit_basic_from` is optional inheritance for secondary locations. |
| `mktg_next_uk_nextads_qa` | `client`, `job_env` | Runs operational QA in the target environment. |
| `mktg_next_uk_nextads_masid_handoff` | `client`, `job_env` | Runs MASID handoff checks. |
| `mktg_next_uk_nextads_payload_export` | `client`, `job_env`, `do_export` | `do_export=1` enables export. |
| `mktg_next_uk_nextads_plp_gs_delivery` | `client`, `job_env`, `territory` | Iterates configured client/territory inputs. |

### Results, Realtime, And Data Pull Jobs

| Job | Settings | Notes / options |
| --- | --- | --- |
| `mktg_next_uk_nextads_results_cicd` | `client`, `job_env`, plus `label_window_days=28` for inference-log enrichment | Results tasks run in sequence; `label_window_days` is an integer day window. |
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

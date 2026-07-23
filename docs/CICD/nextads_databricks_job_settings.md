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

Main NextAds candidate-generation graph. Customer cells and item attributes are
shared, then v1 and v2 split for Theme Mapping, lightweight theme scoring, and
the control-sheet join. Theme Affinity is not a task in this graph; its 09:00
scheduled job still writes the existing v1 assignment source.

| Task | Settings | Notes / options |
| --- | --- | --- |
| `assign_customer_cells` | `client`, `job_env`, `refresh_control_date` | `refresh_control_date` is date-gated; use a current `YYYY-MM-DD` only when deliberately refreshing control assignments. |
| `combine_customer_cells` | `client`, `job_env` | Combines outputs from assignment. |
| `load_control_sheet_v1` | `client`, `job_env` | Loads v1 location control-sheet data and writes `control_sheet_latest`. Home Page remains on this route. |
| `load_control_sheet_v2` | `client`, `job_env` | Loads v2 page-type control-sheet data and writes `control_sheet_latest_v2`. |
| `parse_attributes` | `client`, `job_env`, `refresh_attributes_date` | Refreshes the attribute set only when the date is today; otherwise remaps using latest attributes. |
| `parse_theme_mapping_v1` | `client`, `job_env`, `refresh_themes_date`, `route=v1` | Reads the existing workbook Theme Mapping tab and writes v1 `theme_mapping_latest` / `item_themes_latest`. |
| `compare_theme_mappings` | `client`, `job_env`, optional `fail-on-differences` | Reads both workbook Theme Mapping tabs and logs row-level differences so Trade can review them; posts the warning only in PROD. |
| `parse_theme_mapping_v2` | `client`, `job_env`, `refresh_themes_date`, `route=v2` | Reads the v2 workbook Theme Mapping tab and writes v2 `theme_mapping_latest_v2` / `item_themes_latest_v2`. |
| `score_lightweight_v1` | `client`, `job_env`, `refresh_model_date`, `route=v1` | Runs v1 Markov scoring. Refreshes transition probabilities only when the date is today. |
| `score_lightweight_v2` | `client`, `job_env`, `refresh_model_date`, `route=v2` | Runs v2 Markov scoring from `item_themes_latest_v2` and writes `next_theme_scores_latest_v2`. |
| `map_theme_scores_to_ads_v1` | `client`, `job_env`, `apply-ad-feedback`, `top-ads-per-location` | Reads `control_sheet_latest` plus shared Theme Affinity scores and writes `preranked_ads_from_themes_latest`. `apply-ad-feedback` is a flag. |
| `map_theme_scores_to_ads_v2` | `client`, `job_env`, `top-ads-per-page-type` | Reads `control_sheet_latest_v2` plus `next_theme_scores_latest_v2` and writes `preranked_ads_from_themes_v2_latest`; it does not read v1 preranked output. |
| `trigger_page_build_v1_job` | `job-id`, `job-name`, `fail-on-submit-error` | Uses the target-local `mktg_next_uk_nextads_page_build` job id. `fail-on-submit-error` is a flag. |
| `trigger_page_build_v2_job` | `job-id`, `job-name`, `fail-on-submit-error` | Uses the target-local `mktg_next_uk_nextads_page_build_v2` job id. `fail-on-submit-error` is a flag. |

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
| `theme_table_prefix` | Prefix for theme source tables. | Table-name prefix, for example `next_uk_nextads_theme_affinity_predict`. |
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
| `mktg_next_uk_nextads_page_build` | `page_type`, `location`, `inherit_basic_from`, downstream trigger job ids/names | Iterates over configured page types and locations. `inherit_basic_from` is optional inheritance for secondary locations. |
| `mktg_next_uk_nextads_page_build_v2` | `page_type`, `location`, `inherit_basic_from`, downstream trigger job ids/names | Iterates over configured page types and locations. `inherit_basic_from` is optional inheritance for secondary locations. |
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

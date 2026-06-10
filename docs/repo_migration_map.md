# NextAds Repo Migration Map

This document is the control map for moving the current NextAds repo into the
target production package structure. It is intentionally explicit so future PRs
can point to a known destination, risk level, and validation expectation before
moving code.

The map supports story 5111778: map current repo components to target
locations.

## Target Structure

```text
next-ads/
  src/          # reusable production package code
  pipelines/    # Databricks/process-flow definitions
  jobs/         # Databricks Python entrypoints
  configs/      # settings and policies
  sql/          # table/view/reporting SQL
  experiments/  # exploration and operational-transition model work
  docs/         # team and AI context
  tests/        # confidence checks
  deployment/   # release setup
```

The target production package is:

```text
src/
  next_ads/
    common/       # shared utilities used across the repo
    data/         # data contracts, features, labels and datasets
    control/      # control sheet, ad metadata and eligibility
    retrieval/    # creates the pool of ads that could be considered
    ranking/      # scores or orders candidate ads
    decisioning/  # applies rules and selects the final ad
    delivery/     # prepares outputs for downstream systems
    reporting/    # reusable reporting and diagnostics logic
    realtime/     # real-time adjustment logic and contracts
```

## Status Labels

| Status | Meaning |
|---|---|
| Production | Actively used by current scheduled or release-controlled NextAds operation. |
| Operational-transition | Used or intended for use, but not yet cleanly absorbed into the production package/job structure. |
| Experiment | Exploration or analysis that should not be on the production path without a later operationalisation story. |
| Deployment | CI/CD, Databricks Asset Bundle, permissions, release, or dependency setup. |
| Documentation | Team, user, engineering, or AI context. |
| Deprecated candidate | Looks unused or superseded, but must be confirmed before removal. |

## Risk Labels

| Risk | Meaning | Minimum validation before move |
|---|---|---|
| Low | Structure, docs, pure imports, or compatibility wrappers. | Import test, Ruff on changed Python, focused unit tests, DAB validate where relevant. |
| Medium | Job entrypoints, config layout, or SQL location changes without intended output change. | Unit tests, job-path tests, DAB validate, DEV Integration deploy or smoke where relevant. |
| High | Assignment, ranking, model, output, write-route, or table-contract changes. | Output equivalence, DEV Integration run, PREPROD validation, release evidence. |

## Top-Level Folder Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `src/` | New target package root introduced for reusable production code. | `src/` | Production | Low | Keep as the target home; expand package contents through controlled stories. | Import tests, package discovery tests, Ruff on changed Python. | This is already the destination, not a legacy folder to migrate away from. |
| `next_ads/` | Existing importable package with reusable production logic. | `src/next_ads/` | Production | High | Move module-by-module with compatibility wrappers. | Old and new imports, unit tests, output comparison for decisioning/ranking. | Do not big-bang move. |
| `scripts/` | Current Databricks Python job entrypoints and operational utilities. | `jobs/` | Production | High | After package imports are stable. | Job-path tests, DAB validate, DEV Integration deploy, PREPROD smoke. | Current jobs point here directly. |
| `resources/jobs/` | Databricks Asset Bundle job definitions. | `pipelines/databricks/jobs/` | Deployment | Medium | With DAB include-path update. | DAB validate for DEV Integration, PREPROD, PROD. | Keep `databricks.yml` at root unless agreed otherwise. |
| `resources/variables/` | Databricks cluster/library variables. | `pipelines/databricks/variables/` | Deployment | Medium | With DAB include-path update. | DAB validate. | Move with resources/jobs or immediately after. |
| `config/` | Current Dynaconf and JSON/table settings. | `configs/` | Production | Medium | After config loader supports target path. | Config manager tests, table config tests, DAB validate. | Use compatibility fallback during transition. |
| `sql/` | Production table/view/reporting SQL. | `sql/` | Production | Medium | Stay as canonical SQL folder; add grouping docs. | Table setup tests, DAB validate, table creation smoke where relevant. | Do not move unless grouping is agreed. |
| `response_model/` | pCTR/response model feature and model scripts. | `experiments/pctr/` initially, then `src/next_ads/ranking/` and `jobs/model/pctr/` where productised. | Operational-transition | High | Item-by-item after ownership and route agreed. | Model/table contract checks, Databricks run evidence. | Contains model-building code that may become operational. |
| `hackathon_model/` | Legacy Theme Affinity model work with currently used outputs. It scores/ranks account-to-theme affinity from behaviour, repurchase, popularity, and theme interaction features. | `experiments/theme_affinity/` for retained notebooks/assets, then `src/next_ads/ranking/theme_affinity/`, `src/next_ads/data/theme_affinity/`, `src/next_ads/delivery/theme_affinity/`, and `jobs/model/theme_affinity/` as the MLflow route matures. | Operational-transition | High | Only after current output consumers and contracts are documented. | Model output contract check, MLflow load/run evidence, Databricks run link. | Do not preserve "hackathon" as the target domain name. Current legacy outputs are used and must remain compatible during the move. |
| `real_time/` | Real-time adjustment work and config. | `src/next_ads/realtime/` plus `jobs/realtime/` | Operational-transition | High | After current realtime route is documented. | Realtime output/contract checks, DAB validate if job-backed. | Split reusable logic from entrypoints/config. |
| `adsv2/` | NextAds v2 control/output-contract work. This is a parallel-run route for a fundamental change in how NextAds outputs interact with downstream systems, not an experiment. | `jobs/nextads_v2/` for Databricks entrypoints, `src/next_ads/control/adsv2/` for reusable control-sheet parsing/loading logic, `src/next_ads/delivery/adsv2/` for v2 output shaping, and `configs/adsv2/` for v2-specific settings. | Operational-transition | High | After v2 output contracts and downstream consumers are documented. | Config tests, output contract checks, DEV Integration run, PREPROD validation before production adoption. | Keep isolated from the current v1 production path until the parallel-run output comparison is accepted. |
| `QA/` | QA notebooks and exploratory checks. | `experiments/qa/` or `docs/qa/` | Experiment | Low | When experiments folder is introduced. | None beyond docs/file move review. | Notebook SQL may not lint as Python. |
| `docs/` | Team docs, workflow docs, release docs, and maintained LLM context for tools such as GitHub Copilot, Claude, Codex, and other assistants. | `docs/` | Documentation | Low | Keep canonical. | Markdown review. | Add migration docs and AI/LLM operating context here. |
| `tests/` | Unit and integration tests. | `tests/` | Production | Medium | Keep canonical; update imports as modules move. | Tests must continue to run. | Do not move without pytest config update. |
| `devops/` | Azure DevOps scripts/templates/variables. | `deployment/azure_devops/` | Deployment | Medium | After pipeline paths are updated. | PR validation and CI/CD validation. | Branch policies reference pipelines, not this folder directly. |
| `.azuredevops/` | Azure Repos PR template. | `deployment/azure_devops/` or keep `.azuredevops/` if Azure requires it. | Deployment | Low | Confirm Azure template discovery before moving. | PR template still loads in Azure DevOps. | Likely must remain for Azure DevOps convention. |
| `.databricks/` | Local/generated Databricks bundle state. | Do not migrate into target structure. | Deployment | Low | Do not intentionally move. | N/A. | Usually local/tool state. |
| `.pytest_cache/`, `.ruff_cache/`, `.venv/` | Local generated tooling state. | Do not migrate or commit. | Deprecated candidate | Low | Keep ignored/local. | N/A. | These are machine-local caches/environments, not repo structure. |
| `wheels/` | Local wheel dependencies. | `deployment/wheels/` | Deployment | Medium | After dependency strategy agreed. | Poetry install/export, CI install. | Keep path stable while pyproject references it. |
| `.devcontainer/`, `.vscode/` | Developer environment settings. | `deployment/dev_environment/` or keep as tool-conventional folders. | Deployment | Low | Only if team agrees. | Local dev sanity check. | Tool convention may favour keeping at root. |

## Root Tooling And Dependency Map

These files do not belong under the production package, but they still need an
explicit migration decision because they control local development, CI
behaviour, dependency installation, and Databricks bundle packaging.

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `pyproject.toml` | Python package, dependency, Ruff, pytest, and tooling configuration. | Keep at root. | Deployment | High | Do not move. Update only when package discovery or dependency strategy changes. | Local unit tests, Ruff, CI validation, package import checks. | Moving this would change how Python discovers the repo package. |
| `poetry.lock` | Locked Python dependencies. | Keep at root. | Deployment | Medium | Keep with `pyproject.toml`. | `poetry install`/CI dependency install. | Update intentionally when dependencies change. |
| `requirements.txt` | Alternative/pipeline dependency input if used by Databricks or CI tooling. | Keep at root unless dependency strategy is simplified. | Deployment | Medium | Only after confirming whether any pipeline or Databricks path uses it. | CI install and DAB validate. | Do not delete just because Poetry exists. |
| `.pre-commit-config.yaml` | Local pre-commit hook configuration. | Keep at root. | Deployment | Low | Keep unless the team changes local dev tooling. | `pre-commit run` if hooks are used. | Supports local quality checks. |
| `.gitignore` | Git ignore rules for local/generated files. | Keep at root. | Deployment | Low | Keep. | Git status sanity check. | Important while `.venv`, caches, and Databricks local state exist. |
| `.dockerignore` | Docker build ignore rules. | Keep at root unless Docker support is removed. | Deployment | Low | Confirm Docker/devcontainer usage before changing. | Devcontainer/Docker build if used. | Tool-conventional root file. |
| `dsutils-*.whl` at repo root | Legacy/local wheel artifact. | Prefer `wheels/` or external package source after dependency strategy is agreed. | Deployment | Medium | Only after confirming all install paths. | Poetry/CI install and Databricks library validation. | There is also a `wheels/` copy; rationalise deliberately rather than deleting casually. |

## Current Package Module Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `next_ads/__init__.py` | Existing package init; now also transitional bridge to `src/next_ads`. | `src/next_ads/__init__.py` eventually. | Production | Low | Retire bridge last. | Old and new import tests. | Keep until all callers move. |
| `next_ads/Assignment.py` | Assignment, greedy allocation, preranked ads, NextGenAds, algorithm division logic. | `src/next_ads/decisioning/assignment.py` | Production | High | After output equivalence harness exists. | Unit tests plus representative assignment output comparison. | Decision-affecting. Move late. |
| `next_ads/Attributes.py` | Compatibility wrapper for attribute parsing and theme/control attribute helpers. | `src/next_ads/control/attributes.py` | Production | Medium | Moved; keep wrapper until legacy imports are retired. | Attribute unit tests and old/new import compatibility. | Affects input/control interpretation. |
| `next_ads/Scoring.py` | Model score retrieval and aggregation. | `src/next_ads/ranking/scoring.py` | Production | High | After ranking output comparison exists. | Unit tests plus representative score output comparison. | Ranking-affecting. |
| `next_ads/Results.py` | Result aggregation, checks, reporting helpers. | `src/next_ads/reporting/results.py` | Production | Medium | After reporting tests are stable. | Results tests and reporting output sanity checks. | May affect dashboards/reporting. |
| `next_ads/Plotting.py` | Graph plotting helpers. | `src/next_ads/reporting/plotting.py` | Operational-transition | Low | Early utility move. | Import test. | Low production risk unless used in job. |
| `next_ads/Export.py` | Export helpers. | `src/next_ads/delivery/export.py` | Operational-transition | Medium | After usage confirmed. | Import test, export smoke if used. | Confirm downstream route before moving. |
| `next_ads/utils/config_manager.py` | Dynaconf config loading and environment resolution. | `src/next_ads/common/config_manager.py` or `src/next_ads/common/config/manager.py` | Production | Medium | Early, with wrapper. | Config manager tests for dev/preprod/prod. | Central dependency for many scripts. |
| `next_ads/utils/etl.py` | Shared ETL/table helpers. | `src/next_ads/common/etl.py` | Production | Medium | Moved with compatibility wrapper. | Old/new import tests and existing behaviour checks. | Current helper is table-name formatting only; future table IO helpers still need separate write-route review. |
| `next_ads/utils/gs_helpers.py` | Google Sheets / PLP GS helper logic. | `src/next_ads/control/google_sheets.py` or `src/next_ads/delivery/google_sheets.py` | Production | Medium | After PLP/control sheet usage documented. | PLP GS tests and smoke. | Decide final target based on actual role. |
| `next_ads/utils/__init__.py` | Utils package init. | `src/next_ads/common/__init__.py` | Production | Low | With utils migration. | Import tests. | Keep compatibility wrapper. |
| `next_ads/data_validation/schemas.py` | Pandera schemas/data contracts. | `src/next_ads/data/validation/schemas.py` | Production | Medium | Early, after package skeleton. | Schema import and table setup tests. | Good candidate after pure utilities. |
| `next_ads/data_validation/custom_checks.py` | Pandera custom checks. | `src/next_ads/data/validation/custom_checks.py` | Production | Medium | With schemas. | Schema tests. | Keep old import wrapper. |
| `next_ads/data_validation/__init__.py` | Data validation package init. | `src/next_ads/data/validation/__init__.py` | Production | Low | With schemas. | Import tests. | Keep old import wrapper. |

## Current Main Job Entrypoint Map

| Current path | Current task/role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `scripts/load_control_sheet.py` | `load_control_sheet` task; reads control sheet and writes control-sheet raw/latest outputs. | `jobs/nextads_main/load_control_sheet.py` | Production | High | After package/config migration. | DEV Integration run, PREPROD smoke/full validation. | Writes critical control data. |
| `scripts/assign_customer_cells.py` | `assign_customer_cells` task. | `jobs/nextads_main/assign_customer_cells.py` | Production | High | After decisioning package move. | Assignment output comparison. | Decision/output-affecting. |
| `scripts/combine_customer_cells.py` | `combine_customer_cells` task. | `jobs/nextads_main/combine_customer_cells.py` | Production | High | After data/decisioning package move. | Customer cell output comparison. | Output-affecting. |
| `scripts/parse_attributes.py` | `parse_attributes` task. | `jobs/nextads_main/parse_attributes.py` | Production | Medium | After control package move. | Attribute table sanity check. | Control metadata. |
| `scripts/parse_theme_mapping.py` | `parse_theme_mapping` task. | `jobs/nextads_main/parse_theme_mapping.py` | Production | Medium | After control package move. | Theme mapping table sanity check. | Control metadata. |
| `scripts/build_markov_chain.py` | `score_lightweight` task. | `jobs/nextads_main/build_markov_chain.py` | Production | High | After ranking/retrieval map agreed. | Model score output comparison. | Ranking/scoring-affecting. |
| `scripts/map_theme_scores_to_ads.py` | `map_theme_scores_to_ads` task; maps scores to ads. | `jobs/nextads_main/map_theme_scores_to_ads.py` | Production | High | After ranking/decisioning package moves. | Representative ranking output comparison. | Uses legacy Theme Affinity assignments when configured through current `hackathon_assignments` config. |
| `scripts/build_page.py` | `build_page_primary` and `build_page_secondary` tasks. | `jobs/nextads_main/build_page.py` | Production | High | After decisioning package move. | Page output comparison. | Final assignment output-affecting. |
| `scripts/plp_gs.py` | `nextads_plp_gs` task. | `jobs/nextads_main/plp_gs.py` | Production | Medium | After control/delivery target decided. | PLP GS tests and run evidence. | External/sheet integration. |
| `scripts/qa.py` | `QA` task. | `jobs/nextads_main/qa.py` | Production | Medium | After package imports stable. | QA task run evidence. | Validation logic. |
| `scripts/viewed_bought.py` | `viewed_bought` task. | `jobs/nextads_main/viewed_bought.py` | Production | Medium | After retrieval/ranking map agreed. | Viewed-bought output sanity. | May feed realtime/recommendation logic. |
| `scripts/build_page_v2.py` | Alternative/v2 page build entrypoint. | `jobs/nextads_v2/build_page.py` if part of the v2 route; reusable output logic to `src/next_ads/delivery/adsv2/`. | Operational-transition | High | Confirm v2 route ownership and output contract before moving. | Import checks, v1/v2 output comparison, DEV Integration run, PREPROD validation if retained. | Not in active main DAB job today, but v2 output changes are production-transition work rather than experiment. |
| `scripts/map_theme_scores_to_ads_v2.py` | Alternative/v2 score mapping. | `jobs/nextads_v2/map_theme_scores_to_ads.py` if part of the v2 route; reusable ranking/mapping logic to `src/next_ads/ranking/adsv2/` or `src/next_ads/decisioning/adsv2/` depending on final role. | Operational-transition | High | Confirm v2 route ownership and output contract before moving. | Output checks, ranking comparison, DEV Integration run, PREPROD validation if retained. | Not in active main DAB job today, but score mapping can alter final outputs. |
| `scripts/build_targeting_scores.py` | Targeting score build utility. | `jobs/nextads_main/build_targeting_scores.py` or `src/next_ads/ranking/` | Operational-transition | High | Confirm route before moving. | Score output checks. | Uses `next_ads.Scoring`. |
| `scripts/conditional_probability_recs.py` | Conditional probability theme recommender/baseline candidate. | Reusable logic to `src/next_ads/retrieval/conditional_probability/`; optional job wrapper to `jobs/model/conditional_probability/` only if operationalised. | Dormant candidate / operational-transition | Medium | Confirm usage before moving. | Output checks if retained or reactivated. | No active DAB job reference found during mapping, but it writes recommender-style outputs and should not be deleted without a product/domain decision. |
| `scripts/get_ad_items.py` | Ad item retrieval utility. | `jobs/nextads_main/get_ad_items.py` or `src/next_ads/retrieval/` | Operational-transition | Medium | Confirm usage. | Retrieval output checks. | May become package logic plus entrypoint. |
| `scripts/truncate_assignments_latest.py` | Truncation utility. | `jobs/table_operations/truncate_assignments_latest.py` | Production/Deployment | High | Only with explicit operational need. | Manual approval and table safety evidence. | Destructive operation. |
| `scripts/start_stop_job.py` | Job utility. | `deployment/databricks/start_stop_job.py` or `jobs/admin/` | Deployment | Medium | Confirm usage. | Dry run or admin validation. | Operational admin script. |
| `scripts/__init__.py` | Scripts package marker. | Remove after scripts retired. | Deprecated candidate | Low | Last phase. | No imports depend on it. | Keep while scripts exist. |

## Table Operation And Smoke Entrypoint Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `scripts/table_operations/create_tables.py` | Creates configured tables; used by DEV Integration and PREPROD setup. | `jobs/table_operations/create_tables.py` | Deployment/Production | High | After job-path test coverage exists. | DAB validate, DEV Integration setup, PREPROD setup if selected. | Can create/write tables. |
| `scripts/table_operations/calculate_table_sizes.py` | Table size monitoring job. | `jobs/table_operations/calculate_table_sizes.py` | Production | Medium | With table operations move. | Table monitoring job validate/run. | Used by DAB job. |
| `scripts/table_operations/create_user_schemas.py` | User schema setup. | `jobs/table_operations/create_user_schemas.py` | Deployment | Medium | Confirm usage. | DEV schema setup smoke. | Environment setup. |
| `scripts/table_operations/init_starting_tables.py` | Starting table initialisation. | `jobs/table_operations/init_starting_tables.py` | Deployment | High | Confirm current usage and safeguards. | DEV-only run evidence. | Can write initial tables. |
| `scripts/table_operations/mirror_prod_tables_in_dev.py` | Mirrors prod tables in dev. | `jobs/table_operations/mirror_prod_tables_in_dev.py` | Deployment | High | Confirm permissions and route. | DEV-only smoke, no prod writes. | Sensitive due prod reads. |
| `scripts/table_operations/setup_dev_tables.py` | DEV table setup helper. | `jobs/table_operations/setup_dev_tables.py` | Deployment | Medium | With table operations move. | DEV setup smoke. | Local/dev support. |
| `scripts/table_operations/setup_dev_tables.sh` | Shell setup helper. | `jobs/table_operations/setup_dev_tables.sh` or `deployment/dev_setup/` | Deployment | Medium | Confirm usage. | DEV setup smoke. | Shell entrypoint. |
| `scripts/table_operations/truncate_tables_in_dev.py` | DEV truncation utility. | `jobs/table_operations/truncate_tables_in_dev.py` | Deployment | High | Confirm usage. | DEV-only safety evidence. | Destructive operation, DEV only. |
| `scripts/table_operations/__init__.py` | Package marker. | Remove after move or recreate under `jobs/table_operations`. | Deployment | Low | With move. | Import/path tests. | Keep while folder exists. |
| `scripts/smoke/preprod_dependency_smoke.py` | Read-only PREPROD dependency smoke. | `jobs/smoke/preprod_dependency_smoke.py` | Production/Deployment | Medium | With smoke job path move. | PREPROD smoke run. | Must remain metadata-only by default. |

## Results And Realtime Entrypoint Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `scripts/results_1.py` | Results job stage 1. | `jobs/results/results_1.py` | Production | Medium | After reporting package move. | Results job run or reporting output sanity. | Active DAB job. |
| `scripts/results_2.py` | Results job stage 2. | `jobs/results/results_2.py` | Production | Medium | With results job move. | Results job run or reporting output sanity. | Active DAB job. |
| `scripts/results_3.py` | Results job stage 3. | `jobs/results/results_3.py` | Production | Medium | With results job move. | Results job run or reporting output sanity. | Active DAB job. |
| `scripts/results_agg.py` | Aggregated results job. | `jobs/results/results_agg.py` | Production | Medium | With results job move. | Aggregated results sanity. | Active DAB job. |
| `scripts/results_performance_checks.py` | Results performance checks. | `jobs/results/results_performance_checks.py` | Production | Medium | With results job move. | Performance-check run evidence. | Active DAB job. |
| `scripts/results_to_bigquery.py` | Results export to BigQuery. | `jobs/results/results_to_bigquery.py` | Production | High | After downstream/export owner review. | Export smoke and downstream impact check. | Downstream activation/reporting risk. |
| `scripts/results_top_ads_by_location.py` | Top ads reporting output. | `jobs/results/results_top_ads_by_location.py` | Production | Medium | With results job move. | Reporting output sanity. | Active DAB job. |
| `scripts/realtime_results.py` | Realtime results job. | `jobs/realtime/realtime_results.py` | Production | High | After realtime package move. | Realtime output contract check. | Active DAB job. |
| `real_time/real_time_unknown.py` | Real-time unknown logic/prototype. | `src/next_ads/realtime/unknown.py` or `experiments/realtime/` | Operational-transition | High | Confirm current use before move. | Realtime output checks if operational. | Split reusable logic from entrypoint. |
| `real_time/config/next_uk.json` | Realtime config. | `configs/realtime/next_uk.json` | Operational-transition | Medium | With realtime move. | Config load test. | Do not change behaviour silently. |

## Databricks Asset Bundle Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `databricks.yml` | Root bundle manifest and target definitions. | `databricks.yml` | Deployment | Medium | Keep at repo root unless team agrees otherwise. | DAB validate all relevant targets. | Root location is expected by normal DAB workflows. |
| `resources/jobs/mktg_next_uk_nextads.yml` | Main NextAds Databricks job definition. | `pipelines/databricks/jobs/mktg_next_uk_nextads.yml` | Production | High | After job entrypoints move. | DAB validate, DEV Integration deploy, PREPROD deploy. | Active main job. |
| `resources/jobs/mktg_next_uk_nextads_results.yml` | Results job definition. | `pipelines/databricks/jobs/mktg_next_uk_nextads_results.yml` | Production | Medium | With results job move. | DAB validate and results job run/smoke. | Active results route. |
| `resources/jobs/mktg_next_uk_nextads_realtime_results.yml` | Realtime results job definition. | `pipelines/databricks/jobs/mktg_next_uk_nextads_realtime_results.yml` | Production | High | With realtime move. | DAB validate and realtime smoke. | Active realtime route. |
| `resources/jobs/table_size_monitoring.yml` | Table size monitoring job. | `pipelines/databricks/jobs/table_size_monitoring.yml` | Production | Medium | With table operation move. | DAB validate and job smoke. | Monitoring/support route. |
| `resources/jobs/dev_integration_setup.yml` | DEV Integration setup/migration job. | `pipelines/databricks/jobs/dev_integration_setup.yml` | Deployment | High | After table operation path update. | DEV Integration setup validation. | Can create/drop dev integration tables. |
| `resources/jobs/preprod_setup.yml` | PREPROD setup job. | `pipelines/databricks/jobs/preprod_setup.yml` | Deployment | High | After table operation path update. | PREPROD setup validation if run. | Creates missing PREPROD tables in `ds_sandbox`. |
| `resources/jobs/preprod_dependency_smoke.yml` | PREPROD dependency smoke job. | `pipelines/databricks/jobs/preprod_dependency_smoke.yml` | Deployment | Medium | With smoke path update. | PREPROD smoke run. | Must stay read-only by default. |
| `resources/variables/clusters.yml` | DAB cluster config. | `pipelines/databricks/variables/clusters.yml` | Deployment | Medium | With DAB include update. | DAB validate. | Controls compute. |
| `resources/variables/libraries.yml` | DAB shared libraries. | `pipelines/databricks/variables/libraries.yml` | Deployment | Medium | With DAB include update. | DAB validate and job cluster library check. | Controls runtime deps. |

## Config Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `config/settings.yaml` | Environment/catalog/schema config. | `configs/settings.yaml` | Production | Medium | After config loader target-path support. | Config manager tests for dev/preprod/prod. | Central config. |
| `config/tables_settings.yaml` | Table read/write settings. | `configs/tables_settings.yaml` | Production | High | With table config tests. | Table config tests and output route checks. | Affects table names and writes. |
| `config/load_control_sheet_settings.yaml` | Control sheet settings. | `configs/load_control_sheet_settings.yaml` | Production | High | With control sheet test coverage. | Load control sheet config tests. | Affects control sheet ingestion. |
| `config/global_solution_settings.yaml` | Global solution settings. | `configs/global_solution_settings.yaml` | Production | Medium | With config migration. | Config manager tests. | Confirm consumers. |
| `config/next_uk.json` | Client config. | `configs/clients/next_uk.json` | Production | High | With config loader support. | Client config tests, DAB validate. | Client-specific operational settings. |
| `config/next_gb.json` | Client config. | `configs/clients/next_gb.json` | Production | High | With config loader support. | Client config tests. | Client-specific operational settings. |
| `config/users.yaml` | User/schema config. | `configs/users.yaml` | Deployment | Medium | With config migration. | DEV/user schema tests. | Affects dev deployment/schema. |

## SQL Map

The `sql/` folder remains the target home, but SQL should be grouped and owned
by functional area before any further restructuring.

| SQL family | Current examples | Target area | Status | Risk | Validation required |
|---|---|---|---|---|---|
| Control sheet tables | `create_table_control_sheet*.sql`, `create_table_control_sheet_raw*.sql`, `create_table_control_sheet_plp_raw*.sql` | `sql/control/` if grouping is introduced | Production | High | Table creation smoke, control sheet output check. |
| Attribute and theme tables | `create_table_attribute_set*.sql`, `create_table_item_attributes*.sql`, `create_table_theme_mapping*.sql`, `create_table_item_themes*.sql` | `sql/control/` or `sql/data/` | Production | Medium | Parse attributes/theme mapping checks. |
| Customer cell tables | `create_table_customer_cells*.sql`, `create_table_exclusions*.sql` | `sql/decisioning/` | Production | High | Customer cell output comparison. |
| Assignment tables | `create_table_assignments*.sql`, `create_table_preranked_ads*.sql` | `sql/decisioning/` | Production | High | Assignment/page output comparison. |
| Model score and theme score tables | `create_table_next_theme_scores*.sql`, `create_view_next_uk_nextads_model_scores*.sql`, `create_table_theme_score_components*.sql`, `create_table_theme_scoring_events_latest.sql` | `sql/ranking/` | Production | High | Ranking/model score validation. |
| Conditional probability tables | `create_table_conditional_probability*.sql` | `sql/retrieval/conditional_probability/` | Dormant candidate / operational-transition | Medium | Confirm operational use, output contract, and whether these tables remain part of the target recommender route. |
| Theme transition tables | `create_table_theme_transitions*.sql` | `sql/retrieval/` or `sql/ranking/` | Production | Medium | Markov/theme transition output check. |
| Results/reporting tables | `create_table_results*.sql`, `create_table_nextads_table_sizes.sql` | `sql/reporting/` | Production | Medium | Results/reporting output checks. |
| Realtime tables | `create_table_realtime_results*.sql`, `create_table_viewed_bought_latest.sql` | `sql/realtime/` | Production | High | Realtime output contract check. |
| PLP/GS tables | `create_table_nextads_plp_gs*.sql` | `sql/delivery/` or `sql/control/` | Production | Medium | PLP GS smoke. |
| Ad item tables | `create_table_ad_items.sql` | `sql/retrieval/` | Production | Medium | Retrieval output check. |
| Account department scores | `next_uk_nextads_account_department_scores.sql` | `sql/ranking/` | Production | Medium | Score output check. |

## Response Model / pCTR Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `response_model/customer_behaviour_features.py` | Customer behaviour features for response model. | `experiments/pctr/customer_behaviour_features.py` initially; reusable pieces to `src/next_ads/data/features/`. | Operational-transition | High | After pCTR route agreed. | Feature table contract check. | May become production feature code. |
| `response_model/pctr_advert_metadata_attribute_profile.py` | Advert metadata feature/profile build. | `experiments/pctr/` then `src/next_ads/data/features/advert_metadata.py`. | Operational-transition | High | After table contract agreed. | Feature output check. | pCTR model feature work. |
| `response_model/pctr_advert_semantic_embeddings.py` | Advert semantic embedding feature build. | `experiments/pctr/` then `src/next_ads/ranking/pctr/embeddings.py` and `jobs/model/pctr/`. | Operational-transition | High | After MLflow/volume strategy agreed. | Model/artifact load, feature output check. | ML/model artifact sensitive. |
| `response_model/pctr_build_training_snapshots.py` | Training snapshot build. | `jobs/model/pctr/build_training_snapshots.py` plus reusable `src/next_ads/data/features/`. | Operational-transition | High | After operational pCTR job route agreed. | Snapshot table contract check. | Writes model-ready data. |
| `response_model/pctr_product_embedding_features.py` | Product embedding feature build. | `experiments/pctr/` then `src/next_ads/ranking/pctr/product_embeddings.py`. | Operational-transition | High | After artifact strategy agreed. | Model load and feature output check. | Uses MLflow/SentenceTransformer. |
| `response_model/pctr_seasonal_product_features.py` | Seasonal product features. | `experiments/pctr/` then `src/next_ads/data/features/seasonal.py`. | Operational-transition | Medium | After feature contract agreed. | Feature output check. | Model feature component. |
| `response_model/pctr_spark_model_training.py` | Spark model training. | `jobs/model/pctr/train.py` plus `src/next_ads/ranking/pctr/training.py`. | Operational-transition | High | After model lifecycle agreed. | MLflow training run evidence. | Not a simple script move. |
| `response_model/pctr_score_ad_candidates.py` | pCTR candidate scoring. | `jobs/model/pctr/score_ad_candidates.py` plus `src/next_ads/ranking/pctr/scoring.py`. | Operational-transition | High | After trained model and alias strategy agreed. | Scoring output contract check. | Can affect ranking. |
| `response_model/pctr_tagged_click_training.py` | Tagged click training/label data. | `jobs/model/pctr/tagged_click_training.py` plus `src/next_ads/data/labels/`. | Operational-transition | High | After label contract agreed. | Label output contract check. | Model target generation. |

## Theme Affinity Model Map

The current `hackathon_model/` folder should be treated as the legacy home of
the Theme Affinity model. The model scores or ranks account-to-theme affinity
using theme interaction, views, baskets, add-to-bag, repurchase, popularity,
trending, and customer feature signals. It was created during a hackathon, but
the target production domain name should be `theme_affinity`.

This work is operational-transition work. It is not safe to delete or ignore
because current outputs are used by downstream NextAds assignment logic.
During migration, keep existing table names and config keys working as legacy
contracts until a separate output migration is agreed.

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| External Databricks `Hackathon_job` definition | Live scheduled Theme Affinity workflow that currently runs `hackathon_model` notebooks and writes legacy hackathon-named outputs. | `resources/jobs/mktg_next_uk_nextads_theme_affinity.yml` initially, then `pipelines/databricks/jobs/` if DAB resources move. | Operational-transition | High | Bring job definition into repo before moving notebooks or changing model code. | DAB validate and task/parameter contract checks; Databricks run evidence before replacing the external job. | Use Theme Affinity as the target job name while preserving legacy notebook paths and output parameters until a separate compatibility migration is agreed. |
| `hackathon_model/` | Legacy Theme Affinity model folder. | `experiments/theme_affinity/` for retained notebooks/assets, then productised code split across `src/next_ads/*/theme_affinity/` and `jobs/model/theme_affinity/`. | Operational-transition | High | After consumers and output tables are documented. | Current output contract check. | Rename the domain to Theme Affinity; keep legacy paths only as compatibility until replacement is proven. |
| `hackathon_model/config.py` | Model URI and feature list config. | `configs/model/theme_affinity.yaml` for durable config. Temporary notebook-only config may live under `experiments/theme_affinity/config.py` during transition. | Operational-transition | High | With MLflow route. | Model URI load test and feature-list compatibility check. | Current model URI points to a UC model whose name still contains `hackathon`; do not rename the registered model without a separate migration. |
| `hackathon_model/predict_model.ipynb` | Prediction notebook for scoring account/theme affinity. | Retained notebook to `experiments/theme_affinity/notebooks/predict_model.ipynb`; productised entrypoint to `jobs/model/theme_affinity/predict.py`; reusable logic to `src/next_ads/ranking/theme_affinity/predict.py`. | Operational-transition | High | After MLflow predict job exists. | Prediction job run and output contract check. | Do not remove until job replacement is proven. |
| `hackathon_model/clean_output.ipynb` | Cleans and writes the Theme Affinity output currently known by legacy hackathon table names. | Retained notebook to `experiments/theme_affinity/notebooks/clean_output.ipynb`; productised entrypoint to `jobs/model/theme_affinity/clean_output.py`; reusable output shaping to `src/next_ads/delivery/theme_affinity/clean_output.py`. | Operational-transition | High | After output contract is documented. | Output table shape and consumer check. | Preserve legacy output table names until a separate table rename/alias migration is agreed. |
| `hackathon_model/run_pipeline_predict.ipynb` | Pipeline prediction orchestration notebook. | Retained notebook to `experiments/theme_affinity/notebooks/run_pipeline_predict.ipynb`; productised orchestration to DAB job resources and `jobs/model/theme_affinity/`. | Operational-transition | High | With MLflow operationalisation. | Databricks run evidence. | Candidate for job replacement. |
| `hackathon_model/simple_rules_rank.ipynb` | Simple rules/ranking notebook, likely fallback or comparison logic for theme ranking. | `experiments/theme_affinity/notebooks/simple_rules_rank.ipynb`; reusable fallback logic, if operational, to `src/next_ads/ranking/theme_affinity/rules.py`. | Operational-transition | Medium | Move with folder, then assess whether logic is operational. | Notebook context retained; output comparison if productised. | Do not assume this is disposable without owner review. |
| `hackathon_model/ranking_encoders.joblib` | Model encoder artifact. | Keep initially under `experiments/theme_affinity/assets/`; later move to MLflow artifact or Databricks volume. | Operational-transition | High | Only after artifact strategy is agreed. | Artifact load test. | Do not lose binary artifact; do not rely on repo binary long term if MLflow can own it. |
| `hackathon_model/sql/*.sql` | Theme Affinity feature, training, spine, target, and master association SQL. | Retained SQL to `experiments/theme_affinity/sql/`; productised SQL to `sql/ranking/theme_affinity/features/`, `sql/ranking/theme_affinity/training/`, and `sql/ranking/theme_affinity/prediction/` as appropriate. | Operational-transition | High | After feature/output contracts are agreed. | Feature SQL/table output checks. | Current model support SQL; preserve output contracts during move. |
| `config/tables_settings.yaml` keys such as `hackathon_assignments`, `theme_score_components_hackathon*`, and `preranked_ads_from_themes_hackathon_latest` | Legacy table contracts consumed by current production code. | Keep current keys initially; later introduce `theme_affinity_*` aliases only through a separate compatibility migration. | Production | High | Do not rename in the first code move. | Config load tests and consumer output checks. | These names are legacy but operational; changing them is a production contract change. |
| `sql/create_table_*_hackathon*.sql` | Legacy table definitions for Theme Affinity-derived outputs. | Keep existing files until table naming migration is agreed; later target `sql/ranking/theme_affinity/` or `sql/decisioning/` depending on table role. | Production | High | After output contract and alias plan exists. | Table creation smoke and downstream consumer check. | File names can remain legacy longer than code folder names. |

## Ads V2 Map

The current `adsv2/` folder exists as a temporary
parallel-run area because the v2 route changes the control sheet and output
contract enough that it should not be mixed into the current production path
piecemeal. In the target repo, v2 should be represented as controlled
production-transition work: entrypoints under `jobs/nextads_v2/`, reusable
logic under `src/next_ads/`, and settings under `configs/adsv2/`.

Ads v2 is a candidate production route that affects how outputs
are shaped and consumed by downstream systems. That makes it high-risk
operational-transition work requiring explicit output contract checks and
parallel-run evidence.

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `adsv2/load_control_sheet.py` | Ads v2 control sheet loader. Reads the v2 Google Sheet, validates fields, writes v2 raw/latest/control tables, and writes exclusions. | `jobs/nextads_v2/load_control_sheet.py` as the Databricks entrypoint; reusable parsing/date/MASID/exclusion logic to `src/next_ads/control/adsv2/load_control_sheet.py`. | Operational-transition | High | After v2 control-sheet schema and downstream table contract are documented. | Control sheet v2 tests, v1/v2 output comparison, DEV Integration run, PREPROD validation before production adoption. | This changes an input/output contract, so it should not be hidden under experiments. |
| `adsv2/load_control_sheet_v2_settings.yaml` | Ads v2 control sheet source and read schema. | `configs/adsv2/load_control_sheet_settings.yaml`. | Operational-transition | High | With v2 config loader support or explicit Dynaconf include. | Config tests proving v2 settings load in dev/preprod/prod. | Keep separate from v1 settings until v2 becomes the default route. |
| `adsv2/tables_settings.yaml` | Ads v2 table write settings for raw/latest/control/exclusions outputs. | `configs/adsv2/tables_settings.yaml`. | Operational-transition | High | With v2 config loader support and table setup coverage. | Table config tests, table creation check in DEV Integration/PREPROD as appropriate. | Table names are output contracts; do not merge into common table settings without an explicit compatibility plan. |
| `adsv2/README.md` | Ads v2 transition context. | `docs/adsv2.md` or `docs/nextads_v2.md`. | Documentation | Low | With v2 migration PR. | Markdown review. | Should explain parallel run, output comparison, and cutover criteria. |
| `adsv2/__test_load_control_sheet_config.py` | Ads v2 config/prototype test. | `tests/unit/adsv2/test_load_control_sheet_config.py` if it is a unit/config test, or `tests/integration/adsv2/` if it needs Databricks/Sheets. | Operational-transition | Medium | Before moving the loader. | Test collection check and CI validation. | Rename from double-underscore form so pytest ownership is clear. |
| Future v2 DAB job resource | Databricks job definition for the v2 control/output route. | `resources/jobs/nextads_v2.yml` initially, later `pipelines/databricks/jobs/nextads_v2.yml` if DAB resources move. | Operational-transition | High | Only when v2 is ready to parallel-run through DEV Integration and PREPROD. | DAB validate, DEV Integration deployment, PREPROD validation, no production overwrite unless approved. | Keep separate from the v1 job until cutover. |

## Azure DevOps And Deployment Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `azure-pipelines.yml` | Manual CI/CD deployment pipeline. | Keep at root or move only if Azure pipeline definition supports path update. | Deployment | High | Do not move without Azure DevOps pipeline edit. | Pipeline runs. | Root path is configured in Azure DevOps. |
| `azure-pipelines-validation.yml` | PR validation pipeline. | Keep at root or move only if Azure build policy path is updated. | Deployment | High | Do not move without branch policy/pipeline edit. | PR validation runs. | Required by branch policies. |
| `.azuredevops/pull_request_template.md` | PR template. | Keep unless Azure supports new path. | Deployment | Low | Do not move until template discovery confirmed. | PR template appears. | Azure convention matters. |
| `devops/templates/deploy-dab.yml` | Deploy DAB template. | `deployment/azure_devops/templates/deploy-dab.yml` | Deployment | High | With pipeline path updates. | CI/CD pipeline run. | Used by `azure-pipelines.yml`. |
| `devops/templates/destroy-dab.yml` | Destroy DAB template. | `deployment/azure_devops/templates/destroy-dab.yml` | Deployment | High | With pipeline path updates. | Destroy route condition tests. | Destructive route. |
| `devops/templates/run-tests.yml` | Test/lint template. | `deployment/azure_devops/templates/run-tests.yml` | Deployment | High | With pipeline path updates. | PR validation run. | Required validation. |
| `devops/templates/validate-dab.yml` | DAB validation template. | `deployment/azure_devops/templates/validate-dab.yml` | Deployment | Medium | With pipeline path updates. | PR validation run. | Required validation. |
| `devops/scripts/install_databricks_cli.sh` | Installs Databricks CLI. | `deployment/azure_devops/scripts/install_databricks_cli.sh` | Deployment | Medium | With template updates. | Pipeline run. | Used by templates. |
| `devops/scripts/set_dab_vars.sh` | Sets DAB variables. | `deployment/azure_devops/scripts/set_dab_vars.sh` | Deployment | Medium | With template updates. | Pipeline run. | Used by deploy/validate. |
| `devops/scripts/start_db_cluster.sh` | Starts DB cluster for tests. | `deployment/azure_devops/scripts/start_db_cluster.sh` | Deployment | Medium | With test template updates. | PR validation run. | Required for tests. |
| `devops/variables/common.yml` | Common pipeline variables. | `deployment/azure_devops/variables/common.yml` | Deployment | Medium | With pipeline template move. | Pipeline run. | Shared config. |

## Documentation Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `README.md` | Project overview. | `README.md` | Documentation | Low | Keep root. | Markdown review. | Add links to target docs later. |
| `docs/developer_workflow_guide.md` | DS/release workflow. | `docs/developer_workflow_guide.md` | Documentation | Low | Keep. | Markdown review. | Needs updates as repo structure changes. |
| `docs/CICD/nextads_branch_release_route.md` | Release route docs. | `docs/CICD/nextads_branch_release_route.md` | Documentation | Low | Keep. | Markdown review. | Source for release-control process. |
| `docs/CICD/cicd_pipeline_guide.md` | CI/CD guide. | `docs/CICD/cicd_pipeline_guide.md` | Documentation | Low | Keep. | Markdown review. | Update if paths move. |
| `docs/repo_structure.md` | Target structure summary. | `docs/repo_structure.md` | Documentation | Low | Keep. | Markdown review. | 5111656 document. |
| `docs/repo_migration_map.md` | This migration control map. | `docs/repo_migration_map.md` | Documentation | Low | Keep. | Markdown review. | 5111778 document. |
| `docs/tables_setup_guide.md` | Table setup guide. | `docs/tables_setup_guide.md` | Documentation | Low | Keep. | Markdown review. | Update if table operations move. |
| `docs/dynaconf_guide.md` | Dynaconf guide. | `docs/dynaconf_guide.md` | Documentation | Low | Keep. | Markdown review. | Update if `config/` moves. |
| `docs/pandera_guide.md` | Pandera guide. | `docs/pandera_guide.md` | Documentation | Low | Keep. | Markdown review. | Update if validation package moves. |
| `docs/pctr_shopping_bag_feature_build.md` | pCTR/response model documentation. | `docs/pctr_shopping_bag_feature_build.md` | Documentation | Low | Keep; link to pCTR migration plan later. | Markdown review. | Important model context. |
| `docs/docs_for_ai/` | Maintained LLM context for models and assistants such as GitHub Copilot, Claude, Codex, and future coding agents. | `docs/docs_for_ai/` or `docs/llm_context/` if the team wants a clearer name. | Documentation | Low | Keep or rename only through a docs-focused PR. | Markdown review and link check. | This should explain repo structure, release route, domain concepts, safe-edit boundaries, output contracts, and known gotchas so assistants do not infer unsafe changes. |
| Future model/domain context docs | Compact context files for operational models such as Theme Affinity, pCTR, Ads v2, realtime, and assignment/decisioning. | `docs/docs_for_ai/` or `docs/llm_context/` alongside human-readable domain docs. | Documentation | Low | Add as model/domain areas are migrated. | Markdown review by domain owner. | These files should be written for both humans and LLMs: clear ownership, current/target paths, production outputs, allowed edits, validation commands, and release evidence expectations. |
| `docs/Conditional Probability Model/` | Conditional probability docs. | `docs/conditional_probability/` optional. | Documentation | Low | Optional rename only. | Link check. | Space in path may be awkward. |

## Tests Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `tests/unit/` | Unit tests. | `tests/unit/` | Production | Medium | Keep. Update imports as code moves. | Unit tests pass. | Add compatibility tests during migration. |
| `tests/integration/` | Integration tests. | `tests/integration/` | Production | High | Keep. Update imports as code moves. | Integration tests run where appropriate. | Some require Databricks/auth. |
| `tests/integration/adsv2/` | Ads v2 integration tests. | `tests/integration/adsv2/` | Operational-transition | Medium | Keep until adsv2 route decided. | Test collection check. | Helps adsv2 decision. |
| `tests/pytest_databricks.py` | Databricks pytest helper. | `tests/pytest_databricks.py` | Production | Medium | Keep. | Test helper import. | Confirm use before moving. |
| `tests/conftest.py` | Pytest fixtures. | `tests/conftest.py` | Production | Medium | Keep. | Tests pass. | Central test config. |

## Recommended Move Order

1. Create `src/next_ads` skeleton and compatibility bridge.
2. Add this migration map and agree risk/status labels.
3. Move pure package utilities with wrappers.
4. Move data validation schemas/checks.
5. Move config loader with compatibility support for `config/` and `configs/`.
6. Move control sheet and attribute parsing logic.
7. Move reporting/plotting helpers.
8. Move ranking/scoring logic with output comparison.
9. Move decisioning/assignment logic with output comparison.
10. Move legacy Theme Affinity notebooks/assets from `hackathon_model/` into `experiments/theme_affinity/` with operational-transition label and output contract.
11. Productise Theme Affinity MLflow pieces into `src/next_ads/ranking/theme_affinity/`, `src/next_ads/data/theme_affinity/`, `src/next_ads/delivery/theme_affinity/`, and `jobs/model/theme_affinity/`.
12. Move pCTR/response model pieces item by item.
13. Move Databricks entrypoints from `scripts/` to `jobs/`.
14. Move DAB resources to `pipelines/databricks/`.
15. Move Azure DevOps support files only after pipeline path updates are ready.
16. Retire old wrappers and old paths after no references remain.

## High-Risk Items Requiring Separate Stories

Do not move these until their contracts and validation are agreed:

- `next_ads/Assignment.py`
- `next_ads/Scoring.py`
- `scripts/assign_customer_cells.py`
- `scripts/build_page.py`
- `scripts/map_theme_scores_to_ads.py`
- `scripts/load_control_sheet.py`
- `scripts/conditional_probability_recs.py` and `sql/create_table_conditional_probability*.sql`
- `scripts/results_to_bigquery.py`
- `scripts/table_operations/create_tables.py`
- `config/tables_settings.yaml`
- `config/next_uk.json`
- `adsv2/` / NextAds v2 output-contract route
- `hackathon_model/` / legacy Theme Affinity outputs
- `response_model/` / pCTR model route
- `real_time/` / realtime outputs
- `databricks.yml`
- `resources/jobs/mktg_next_uk_nextads.yml`
- `resources/jobs/mktg_next_uk_nextads_realtime_results.yml`
- `resources/jobs/dev_integration_setup.yml`
- `resources/jobs/preprod_setup.yml`
- `azure-pipelines.yml`
- `azure-pipelines-validation.yml`

## Coverage Notes

This map is intentionally a control map, not a line-by-line manifest. Some
folders are mapped by family because listing every file would make the document
harder to use:

- SQL files under `sql/` are mapped by table family in the SQL Map.
- Theme Affinity SQL files under `hackathon_model/sql/` are mapped as a single
  model-support SQL family because they should move together after the model
  contract is documented.
- Unit and integration tests are mapped by test folder, with imports updated as
  corresponding production modules move.
- Local/generated folders such as `.venv/`, `.pytest_cache/`, `.ruff_cache/`,
  and `.databricks/` are explicitly not migration targets.
- LLM context docs are part of the target structure. They should be maintained
  alongside human docs so tools such as GitHub Copilot, Claude, Codex, and other
  assistants can follow the repo structure, release route, and production safety
  boundaries without guessing.

If a future PR moves a file that is only covered by a family row, that PR should
name the exact file in its own PR description and provide the validation listed
for that family.

## PR Evidence Required By Move Type

| Move type | Required PR evidence |
|---|---|
| Structure only | Import tests, Ruff on changed Python, focused unit tests, DAB validate. |
| Documentation/map only | Markdown review, linked story, no output impact stated. |
| Import move | Old import works, new import works, wrappers tested, unit tests pass. |
| Config move | Config manager tests, table config tests, dev/preprod/prod load evidence. |
| Job entrypoint move | Job-path tests, DAB validate, DEV Integration deploy, PREPROD smoke. |
| Operational model move | Model load evidence, output contract check, Databricks run link, consumer note. |
| Decisioning/ranking move | Unit tests, representative output comparison, DEV Integration run, PREPROD validation. |
| Deployment pipeline move | PR validation run, CI/CD run, branch/tag route-control tests. |

## Acceptance Criteria For This Map

- Current folders and key scripts are mapped to target structure.
- Production logic, experiments, job definitions, and documentation are identified separately.
- Legacy Hackathon model work is renamed to the Theme Affinity target domain and explicitly marked as operational-transition work with current outputs preserved.
- Migration order is defined for low-risk and decision-affecting code.
- Follow-up stories can use this map to choose what moves next.

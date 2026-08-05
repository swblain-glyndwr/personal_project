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
  pipelines/    # Databricks bundle, DLT, and Lakeflow definitions
  jobs/         # Databricks Python entrypoints
  configs/      # settings, policies, and future feature-layer config
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
    features/     # reusable feature definitions, grains, keys, and checks
    data/         # source access, data contracts, labels, and datasets
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
| `src/next_ads/features/` | Target home for reusable feature definitions, grains, keys, contracts, and quality checks. | `src/next_ads/features/` | Operational-transition | Low | Keep as first-class feature-layer home. | Import tests and package discovery tests. | Provides a package home for feature logic without changing current jobs or outputs. |
| Retired root `next_ads/` package | Former import bridge and compatibility wrappers. | `src/next_ads/` | Production | Low | Removed after all repository callers moved to canonical imports. | Canonical import tests, wheel inspection, unit tests, DAB validation. | Do not recreate the root package; `src/next_ads` is the only package root. |
| `scripts/` | Retired legacy entrypoint area. | Route-oriented `jobs/nextads_*` folders. | Deprecated candidate | Low | Removed after v2 entrypoints moved into route folders. | DAB validate and job-path tests. | Do not add new entrypoints here. |
| `jobs/features/` | Target home for feature-materialisation Databricks entrypoints. | `jobs/features/` | Operational-transition | Low | Keep as first-class target folder; populate when feature contracts are agreed. | Path/import checks until real jobs move. | Current jobs remain unchanged until entrypoint moves are agreed. |
| `jobs/model/` | Target home for model training/scoring entrypoints. | `jobs/model/` | Operational-transition | Low | Keep as target folder; populate only when model lifecycle is agreed. | Path/import checks until real jobs move. | Theme Affinity and pCTR should separate feature generation from model and scoring steps. |
| `jobs/nextads_*` route folders | Target home for current NextAds Databricks entrypoints, grouped by operational route rather than package domain. | `jobs/nextads_control/`, `jobs/nextads_cells/`, `jobs/nextads_candidates/`, `jobs/nextads_assignment/`, `jobs/nextads_data/`, `jobs/nextads_v2/`, `jobs/nextads_delivery/`, `jobs/nextads_reporting/`, and `jobs/orchestration/`. | Production | Medium | Keep as target route folders; reusable domain logic lives under `src/next_ads/...`. | Path/import checks, DAB validate, focused unit tests, DEV Integration smoke where output-affecting. | Avoid recreating domain-named job folders such as `jobs/ranking/` or `jobs/decisioning/`. |
| `jobs/nextads_v2/` | Target home for Ads v2 entrypoints that do not naturally belong to another route folder. | `jobs/nextads_v2/` | Operational-transition | Low | Keep current v2 route folders stable unless a file is clearly misplaced. | Path/import checks until real jobs move. | Ads v2 is active strategic work, not an experiment. `jobs/nextads_control`, `jobs/nextads_candidates`, and `jobs/nextads_delivery` remain valid homes for v2 control, candidate, and delivery entrypoints. |
| `pipelines/databricks/jobs/` | Databricks Asset Bundle job definitions. | `pipelines/databricks/jobs/` | Deployment | Medium | With DAB include-path update. | DAB validate for DEV Integration, PREPROD, PROD. | Keep `databricks.yml` at root unless agreed otherwise. |
| `pipelines/databricks/variables/` | Databricks cluster/library variables. | `pipelines/databricks/variables/` | Deployment | Medium | With DAB include-path update. | DAB validate. | Move with Databricks job resources or immediately after. |
| `config/` | Legacy fallback path for config files. | `configs/` | Production | Medium | Dynaconf is the active source under `configs/`; keep fallback only while old paths are retired. | Config manager tests, table config tests, DAB validate. | Client config now loads from Dynaconf YAML rather than JSON. |
| `sql/` | Production table/view/reporting SQL. | `sql/` | Production | Medium | Stay as canonical SQL folder; add grouping docs. | Table setup tests, DAB validate, table creation smoke where relevant. | Do not move unless grouping is agreed. |
| `experiments/sb_pctr/` | Retained pCTR/response-model feature and model scripts. | `experiments/sb_pctr/` initially, then `src/next_ads/features/pctr/`, `src/next_ads/ranking/pctr/`, and `jobs/model/pctr/` where productised. | Operational-transition | High | Future pCTR/Feature Store story. | Feature/model/table contract checks, Databricks run evidence. | Moved from `response_model/`; not currently used by scheduled jobs. |
| `experiments/analytics_pctr/` | DEV-only analytics pCTR notebook graph with feature build, prediction, training, and ranking evaluation. | `experiments/analytics_pctr/` initially, then split into `src/next_ads/features/pctr/`, `src/next_ads/ranking/pctr/`, and `jobs/model/pctr/` only when productised. | Operational-transition | High | Retain as experiment until model lifecycle and feature contracts are agreed. | DEV job validation, model/table contract checks before promotion. | The SQL notebooks are not just ranking; they include feature/data prep and training-history build. |
| `experiments/hackathon_theme_affinity_model/` | Retained legacy Theme Affinity notebooks/assets/SQL. | `experiments/hackathon_theme_affinity_model/` for reference material; productised code lives under `src/next_ads/*/theme_affinity/` and `jobs/model/theme_affinity/`. | Operational-transition | High | Keep as reference until replacement route is proven. | Model output contract check, MLflow load/run evidence, Databricks run link. | Moved from `hackathon_model/`. Production domain naming is Theme Affinity; legacy table/model contract strings may still contain `hackathon`. |
| Legacy realtime folder | Legacy real-time adjustment wrapper and config. | `src/next_ads/realtime/`, `jobs/realtime/`, and `configs/realtime/`. | Operational-transition | High | Moved. | Realtime output/contract checks, DAB validate if job-backed. | Legacy wrapper removed; runtime config now lives under `configs/realtime/`. |
| `adsv2/` | NextAds v2 control/output-contract work. This is a parallel-run route for a fundamental change in how NextAds outputs interact with downstream systems, not an experiment. | Route-oriented `jobs/nextads_*` entrypoints, `src/next_ads/control/adsv2/` for reusable control-sheet parsing/loading logic, `src/next_ads/delivery/adsv2/` for v2 output shaping, `configs/adsv2/` for v2 settings, and `sql/adsv2/` for v2 DDL. | Operational-transition | High | After v2 output contracts and downstream consumers are documented. | Config tests, output contract checks, DEV Integration run, PREPROD validation before production adoption. | Keep isolated from the current v1 production path until the parallel-run output comparison is accepted. Not every v2 entrypoint must live under `jobs/nextads_v2/`. |
| Legacy top-level notebook checks | Legacy notebooks and exploratory checks. | Relevant experiment/domain folders such as `experiments/theme_affinity/`. | Experiment | Low | Moved. | None beyond docs/file move review. | Notebook SQL may not lint as Python. |
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
| `src/next_ads/__init__.py` | Canonical package root. | `src/next_ads/__init__.py` | Production | Low | Complete. | Isolated `PYTHONPATH=src` import test and wheel inspection. | The former root-package bridge has been removed. |
| `src/next_ads/features/__init__.py` | Feature-layer package marker. | `src/next_ads/features/__init__.py` | Operational-transition | Low | Keep as target package home. | Import test. | This prepares for feature store work without implementing feature store behaviour. |
| `src/next_ads/decisioning/assignment.py` | Assignment, greedy allocation, preranked ads, NextGenAds, and algorithm-division logic. | Same | Production | High | Complete. | Assignment unit tests and representative output comparison before behavioural edits. | Canonical replacement for the removed `Assignment` wrapper. |
| `src/next_ads/control/attributes.py` | Attribute parsing and theme/control helpers. | Same | Production | Medium | Complete. | Attribute tests. | Canonical replacement for the removed `Attributes` wrapper. |
| `src/next_ads/ranking/scoring.py` | Model score retrieval and aggregation. | Same | Production | High | Complete. | Scoring and ranking tests. | Canonical replacement for the removed `Scoring` wrapper. |
| `src/next_ads/reporting/results.py` | Result aggregation, checks, and reporting helpers. | Same | Production | Medium | Complete. | Reporting tests and output sanity checks. | Canonical replacement for the removed `Results` wrapper. |
| `src/next_ads/reporting/plotting.py` | Graph plotting helpers. | Same | Operational-transition | Low | Complete. | Import test. | Canonical replacement for the removed `Plotting` wrapper. |
| `src/next_ads/delivery/export.py` | Experiment-ID and delivery export helpers. | Same | Production | Medium | Complete. | Export behaviour tests. | Moved with history after the accepted experiment-ID change. |
| `src/next_ads/delivery/cosmos.py` | Cosmos configuration and exclusions-export delivery helpers. | Same | Operational-transition | Medium | Complete. | Mocked configuration, SDK write, and error tests. | External-write behaviour remains unchanged. |
| `src/next_ads/common/config_manager.py` | Dynaconf configuration loading and environment resolution. | Same | Production | Medium | Complete. | Config tests for dev/preprod/prod and supported clients. | All repository callers use the canonical import. |
| `src/next_ads/common/etl.py` | Shared ETL/table-name helpers. | Same | Production | Medium | Complete. | ETL behaviour tests. | All repository callers use the canonical import. |
| `src/next_ads/delivery/google_sheets.py` | Google Sheets and PLP delivery helpers. | Same | Production | Medium | Complete. | PLP GS tests and smoke. | Canonical delivery owner. |
| `src/next_ads/data/validation/` | Pandera schemas and custom checks. | Same | Production | Medium | Complete. | Schema and isolated import tests. | The former `data_validation` alias has been removed. |

## Current Main Job Entrypoint Map

## Target Databricks Job Shape

The Databricks job structure should move toward clear operational boundaries
while preserving existing output contracts during the restructure.

| Target job | Intended role | Current migration position |
|---|---|---|
| `mktg_next_uk_nextads` | Core production generation: cells, control inputs, theme scoring, ad mapping, and page assignment. | Keep as the main generation job. Remove non-core concerns only in controlled slices. |
| `mktg_next_uk_nextads_assignment_validation` | Post-generation assignment checks and controlled cleanup after main assignment output exists. | Separate job submitted asynchronously after the main generation tasks so validation failure is visible internally without failing the main generation job. |
| `mktg_next_uk_nextads_results` | Reporting and results aggregation. | Already separate; keep separate. |
| `mktg_next_uk_nextads_realtime_results` | Realtime measurement outputs. | Already separate; keep separate. |
| `mktg_next_uk_nextads_features` | Future feature materialisation, including later Feature Store-style outputs. | Future slice after feature/model contracts are clearer. |
| `mktg_next_uk_nextads_delivery` | Future delivery/export outputs such as PLP Google Sheet, Bloomreach, Cosmos, and BigQuery-facing contracts. | Future slice after v1/v2 delivery contracts are clearer. |

| Current path | Current task/role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `jobs/nextads_control/load_control_sheet.py` | `load_control_sheet_v1` task; reads the v1 location control sheet and writes v1 control-sheet raw/latest outputs. | `jobs/nextads_control/load_control_sheet.py` | Production | High | Moved. | Job-path tests, DAB validate, DEV Integration run before release, PREPROD smoke/full validation in release context. | Writes critical control data. The main DAB job points here as `load_control_sheet_v1`. |
| `jobs/nextads_cells/assign_customer_cells.py` | Compatibility wrapper for the `assign_customer_cells` task. | `jobs/nextads_cells/assign_customer_cells.py` | Production | High | Moved; keep wrapper until references are gone. | Job-path tests, DAB validate, DEV Integration run before release; output comparison before behavioural edits. | Decision/output-affecting. The main DAB job now points at `jobs/nextads_cells/assign_customer_cells.py`. |
| `jobs/nextads_cells/combine_customer_cells.py` | `combine_customer_cells` task. | `jobs/nextads_cells/combine_customer_cells.py` | Production | High | Moved. | Job-path tests, DAB validate, DEV Integration run before release; output comparison before behavioural edits. | Output-affecting. The main DAB job points here. |
| `jobs/nextads_control/parse_attributes.py` | Compatibility wrapper for the `parse_attributes` task. | `jobs/nextads_control/parse_attributes.py` | Production | Medium | Moved in PR `246383`; keep wrapper until references are gone. | Job-path tests, DAB validate, attribute table sanity check before release. | Control metadata. The main DAB job now points at `jobs/nextads_control/parse_attributes.py`. |
| `jobs/nextads_control/parse_theme_mapping.py` | Shared `parse_theme_mapping` task. | `jobs/nextads_control/parse_theme_mapping.py` | Production | Medium | Moved in PR `246383`; keep shared in this route-split PR. | Job-path tests, DAB validate, theme mapping table sanity check before release. | Control metadata. This parses the copied v1 product Theme Mapping tab for the shared scoring route. The v2 workbook is the source of truth and is copied to v1 by Google Sheets Apps Script; the v1 tab should be locked. Candidate mapping uses the loaded control sheet `Themes` column for ad-theme joins. |
| `jobs/nextads_control/validate_theme_mapping_sync.py` | Hard-stop validation that the copied v1 Theme Mapping tab matches the v2 source tab. | `jobs/nextads_control/validate_theme_mapping_sync.py`; comparison logic in `src/next_ads/control/theme_mapping_sync.py`. | Production | Medium | Added with v1/v2 route split. | Unit tests, job-path tests, DAB validate, DEV Integration run before release. | Runs before `parse_theme_mapping`. Differences mean the Apps Script copy or locked-tab process has drifted and should be raised to Trade before shared theme scoring is refreshed. |
| `jobs/nextads_candidates/build_theme_scores.py` | `build_markov_scores` shadow-provider task. | `jobs/nextads_candidates/build_theme_scores.py` | Production | High | Moved into the independent Markov scoring job and adapted to the canonical provider contract. | Job-path tests, DAB validate, DEV Integration run before release; canonical/legacy score parity before behavioural edits. | Ranking/scoring-affecting. It stages one exact canonical build; the shared publisher validates it and derives the legacy output without gating the candidate route. |
| `jobs/nextads_candidates/build_theme_ad_candidates.py` | `map_theme_scores_to_ads_v1` task. | `jobs/nextads_candidates/build_theme_ad_candidates.py` | Production | High | Moved. | Job-path tests, DAB validate, DEV Integration run before release; representative ranking output comparison before behavioural edits. | Reads the exact portfolio attempt, captures the control Delta version, reuses identical serving-provider computation, publishes top-20 internal candidates manifest-last, and retains `preranked_ads_from_themes_latest` as the `best` compatibility output. |
| `jobs/nextads_candidates/validate_theme_affinity_theme_coverage.py` | Route-specific warning validation that active ad themes exist in the serving portfolio output. | `jobs/nextads_candidates/validate_theme_affinity_theme_coverage.py`; comparison logic in `src/next_ads/ranking/theme_coverage.py`. | Production | High | Added with v1/v2 route split. | Unit tests, job-path tests, DAB validate, DEV Integration run before release. | Each route reads the exact portfolio-bound provider attempt and Delta version after its own control audit, applying the same changed-theme quarantine as mapping. Missing business coverage is reported without hiding technical read or schema failures. |
| `jobs/nextads_assignment/build_page.py` | Compatibility wrapper for `build_page_primary` and `build_page_secondary`. | `jobs/nextads_assignment/build_page.py` | Production | High | Moved; keep wrapper until references are gone. | Job-path tests, DAB validate, DEV Integration run before release; page output comparison before behavioural edits. | Final assignment output-affecting. The page-build DAB job now points at `jobs/nextads_assignment/build_page.py`. |
| `jobs/orchestration/trigger_databricks_job.py` | Obsolete asynchronous downstream-job wrapper. | Removed. | Deprecated | Medium | Removed after all deployed callers moved to native `run_job_task`. | Orchestration tests, reference audit and DAB validate. | Candidate-to-page and page-to-delivery jobs now wait for child completion and propagate failures through their own route. |
| `jobs/nextads_delivery/plp_gs.py` | `nextads_plp_gs` task. | `jobs/nextads_delivery/plp_gs.py` | Production | Medium | Moved. | PLP GS tests and run evidence. | External/sheet integration. |
| `jobs/nextads_reporting/assignment_validation.py` | Assignment validation guardrail job. | `jobs/nextads_reporting/assignment_validation.py` | Production | Medium | Moved from legacy QA naming. | Assignment validation run evidence and DAB validate. | Validation/guardrail logic has its own run history and notification route without controlling the main generation job result. |
| `jobs/realtime/viewed_bought.py` | Compatibility wrapper for the realtime input `viewed_bought` task. | `jobs/realtime/viewed_bought.py` | Production | Medium | Moved with realtime/reporting branch; keep wrapper until references are gone. | Viewed-bought output sanity. | Feeds realtime/recommendation logic; table and config contracts unchanged. |
| `jobs/nextads_v2/build_page.py` | Alternative/v2 page build entrypoint. | `jobs/nextads_v2/build_page.py`; reusable output logic can later move to `src/next_ads/delivery/adsv2/` or `src/next_ads/decisioning/adsv2/` depending on final ownership. | Operational-transition | High | Keep current route path until behavioural productisation is agreed. | Import checks, v1/v2 output comparison, DEV Integration run, PREPROD validation if retained. | V2 output changes are production-transition work rather than experiment. |
| `jobs/nextads_candidates/build_page_type_candidates_v2.py` | `map_theme_scores_to_ads_v2` task. Reads the exact serving portfolio and pinned v2 inputs, then calls the shared candidate runtime at page-type grain. | `jobs/nextads_candidates/build_page_type_candidates_v2.py`; reusable publication in `src/next_ads/candidates/`. | Operational-transition | High | Active parallel route. | Output checks, partition replay, DAB validate, DEV Integration run. | Publishes the same internal candidate contract as v1, retains `preranked_ads_from_themes_v2_latest` from `best`, and does not depend on the v1 output. |
| `jobs/nextads_candidates/build_targeting_scores.py` | Targeting score build utility. | `jobs/nextads_candidates/build_targeting_scores.py` | Operational-transition | High | Moved as a route-oriented candidate/scoring utility. | Score output checks. | Uses `next_ads.ranking.scoring`. |
| `jobs/nextads_candidates/conditional_probability_recs.py` | Compatibility wrapper for conditional-probability retrieval. | `jobs/nextads_candidates/conditional_probability_recs.py`; reusable logic can later move under `src/next_ads/retrieval/conditional_probability/`. | Dormant candidate / operational-transition | Medium | Moved as an entrypoint only. | Output checks if retained or reactivated. | No active DAB job reference found during mapping, but it writes recommender-style outputs and should not be deleted without a product/domain decision. |
| `jobs/nextads_candidates/get_ad_items.py` | Compatibility wrapper for ad item retrieval. | `jobs/nextads_candidates/get_ad_items.py`; reusable logic can later move under `src/next_ads/retrieval/`. | Operational-transition | Medium | Moved as an entrypoint only. | Retrieval output checks. | May become package logic plus entrypoint. |
| `jobs/table_operations/truncate_assignments_latest.py` | Obsolete whole-table truncation utility. | Removed. | Deprecated | High | Removed after the production DAG stopped using it. | Reference audit and bundle validation. | The runtime task was removed before this migration; complete-build publication now protects serving assignments without a separate truncate step. |
| `deployment/databricks/start_stop_job.py` | Job utility. | `deployment/databricks/start_stop_job.py` or `jobs/admin/` | Deployment | Medium | Confirm usage. | Dry run or admin validation. | Operational admin script. |
| `scripts/__init__.py` | Legacy scripts package marker. | Removed. | Deprecated candidate | Low | Removed after scripts retired. | No imports depend on it. | `scripts/` is no longer a job-entrypoint area. |

## Table Operation And Smoke Entrypoint Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `jobs/table_operations/create_tables.py` | Creates configured tables; used by DEV Integration and PREPROD setup. | `jobs/table_operations/create_tables.py` | Deployment/Production | High | After job-path test coverage exists. | DAB validate, DEV Integration setup, PREPROD setup if selected. | Can create/write tables. |
| `jobs/table_operations/calculate_table_sizes.py` | Table size monitoring job. | `jobs/table_operations/calculate_table_sizes.py` | Production | Medium | With table operations move. | Table monitoring job validate/run. | Used by DAB job. |
| `jobs/table_operations/create_user_schemas.py` | User schema setup. | `jobs/table_operations/create_user_schemas.py` | Deployment | Medium | Confirm usage. | DEV schema setup smoke. | Environment setup. |
| `jobs/table_operations/init_starting_tables.py` | Starting table initialisation. | `jobs/table_operations/init_starting_tables.py` | Deployment | High | Confirm current usage and safeguards. | DEV-only run evidence. | Can write initial tables. |
| `jobs/table_operations/mirror_prod_tables_in_dev.py` | Mirrors prod tables in dev. | `jobs/table_operations/mirror_prod_tables_in_dev.py` | Deployment | High | Confirm permissions and route. | DEV-only smoke, no prod writes. | Sensitive due prod reads. |
| `jobs/table_operations/setup_dev_tables.py` | DEV table setup helper. | `jobs/table_operations/setup_dev_tables.py` | Deployment | Medium | With table operations move. | DEV setup smoke. | Local/dev support. |
| `jobs/table_operations/setup_dev_tables.sh` | Shell setup helper. | `jobs/table_operations/setup_dev_tables.sh` or `deployment/dev_setup/` | Deployment | Medium | Confirm usage. | DEV setup smoke. | Shell entrypoint. |
| `jobs/table_operations/truncate_tables_in_dev.py` | DEV truncation utility. | `jobs/table_operations/truncate_tables_in_dev.py` | Deployment | High | Confirm usage. | DEV-only safety evidence. | Destructive operation, DEV only. |
| `jobs/table_operations/__init__.py` | Package marker. | Remove after move or recreate under `jobs/table_operations`. | Deployment | Low | With move. | Import/path tests. | Keep while folder exists. |
| `jobs/smoke/preprod_dependency_smoke.py` | Read-only PREPROD dependency smoke. | `jobs/smoke/preprod_dependency_smoke.py` | Production/Deployment | Medium | With smoke job path move. | PREPROD smoke run. | Must remain metadata-only by default. |

## Results And Realtime Entrypoint Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `jobs/nextads_reporting/results_1.py` | Results job stage 1. | `jobs/nextads_reporting/results_1.py` | Production | Medium | Moved. | Results job run or reporting output sanity. | Active DAB job points here. |
| `jobs/nextads_reporting/results_2.py` | Results job stage 2. | `jobs/nextads_reporting/results_2.py` | Production | Medium | Moved. | Results job run or reporting output sanity. | Active DAB job points here. |
| `jobs/nextads_reporting/results_3.py` | Results job stage 3. | `jobs/nextads_reporting/results_3.py` | Production | Medium | Moved. | Results job run or reporting output sanity. | Active DAB job points here. |
| `jobs/nextads_reporting/results_agg.py` | Aggregated results job. | `jobs/nextads_reporting/results_agg.py` | Production | Medium | Moved. | Aggregated results sanity. | Active DAB job points here. |
| `jobs/nextads_reporting/results_performance_checks.py` | Results performance checks. | `jobs/nextads_reporting/results_performance_checks.py` | Production | Medium | Moved. | Performance-check run evidence. | Active DAB job points here. |
| `jobs/nextads_reporting/results_to_bigquery.py` | Results export to BigQuery. | `jobs/nextads_reporting/results_to_bigquery.py` | Production | High | Moved as path-only reporting entrypoint. | Export smoke and downstream impact check before release. | BigQuery table/export contracts unchanged. |
| `jobs/nextads_reporting/results_top_ads_by_location.py` | Top ads reporting output. | `jobs/nextads_reporting/results_top_ads_by_location.py` | Production | Medium | Moved. | Reporting output sanity. | Active DAB job points here. |
| `jobs/nextads_reporting/realtime_results.py` | Compatibility wrapper for realtime results job. | `jobs/nextads_reporting/realtime_results.py` | Production | High | Moved; keep wrapper until references are gone. | Realtime output contract check. | Active DAB job now points at `jobs/nextads_reporting/realtime_results.py`. |
| `src/next_ads/realtime/unknown.py` | Real-time unknown logic. | `src/next_ads/realtime/unknown.py` | Operational-transition | High | Moved. | Realtime output checks if operational. | Legacy top-level wrapper removed. |
| `configs/realtime/next_uk.json` | Realtime config. | `configs/realtime/next_uk.json` | Operational-transition | Medium | Moved. | Config load test. | Do not change behaviour silently. |

## Databricks Asset Bundle Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `databricks.yml` | Root bundle manifest and target definitions. | `databricks.yml` | Deployment | Medium | Keep at repo root unless team agrees otherwise. | DAB validate all relevant targets. | Root location is expected by normal DAB workflows. |
| `pipelines/databricks/jobs/mktg_next_uk_nextads.yml` | Main NextAds Databricks job definition. | `pipelines/databricks/jobs/mktg_next_uk_nextads.yml` | Production | High | After job entrypoints move. | DAB validate, DEV Integration deploy, PREPROD deploy. | Active main job. |
| `pipelines/databricks/jobs/mktg_next_uk_nextads_assignment_validation.yml` | Separate assignment validation job definition submitted asynchronously after the main output is expected to exist. | `pipelines/databricks/jobs/mktg_next_uk_nextads_assignment_validation.yml` | Production | Medium | Keep separate from the main job while operational job boundaries are introduced. | DAB validate and assignment validation run evidence. | Validation failure should be handled by the internal team without failing the main generation job or notifying the broader main-job audience. |
| `pipelines/databricks/jobs/mktg_next_uk_nextads_results.yml` | Results job definition. | `pipelines/databricks/jobs/mktg_next_uk_nextads_results.yml` | Production | Medium | With results job move. | DAB validate and results job run/smoke. | Active results route. |
| `pipelines/databricks/jobs/mktg_next_uk_nextads_realtime_results.yml` | Realtime results job definition. | `pipelines/databricks/jobs/mktg_next_uk_nextads_realtime_results.yml` | Production | High | With realtime move. | DAB validate and realtime smoke. | Active realtime route. |
| `pipelines/databricks/jobs/table_size_monitoring.yml` | Table size monitoring job. | `pipelines/databricks/jobs/table_size_monitoring.yml` | Production | Medium | With table operation move. | DAB validate and job smoke. | Monitoring/support route. |
| `pipelines/databricks/jobs/dev_integration_setup.yml` | DEV Integration setup/migration job. | `pipelines/databricks/jobs/dev_integration_setup.yml` | Deployment | High | After table operation path update. | DEV Integration setup validation. | Can create/drop dev integration tables. |
| `pipelines/databricks/jobs/preprod_setup.yml` | PREPROD setup job. | `pipelines/databricks/jobs/preprod_setup.yml` | Deployment | High | After table operation path update. | PREPROD setup validation if run. | Creates missing PREPROD tables in `ds_sandbox`. |
| `pipelines/databricks/jobs/preprod_dependency_smoke.yml` | PREPROD dependency smoke job. | `pipelines/databricks/jobs/preprod_dependency_smoke.yml` | Deployment | Medium | With smoke path update. | PREPROD smoke run. | Must stay read-only by default. |
| `pipelines/databricks/variables/clusters.yml` | DAB cluster config. | `pipelines/databricks/variables/clusters.yml` | Deployment | Medium | With DAB include update. | DAB validate. | Controls compute. |
| `pipelines/databricks/variables/libraries.yml` | DAB shared libraries. | `pipelines/databricks/variables/libraries.yml` | Deployment | Medium | With DAB include update. | DAB validate and job cluster library check. | Controls runtime deps. |

## Config Map

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `config/settings.yaml` | Environment/catalog/schema config. | `configs/runtime/settings.yaml` | Production | Medium | Moved with config loader fallback. | Config manager tests for dev/preprod/prod. | Central config. |
| `config/tables_settings.yaml` | Table read/write settings. | `configs/runtime/tables_settings.yaml` | Production | High | Moved with table config tests. | Table config tests and output route checks. | Affects table names and writes. |
| `config/load_control_sheet_settings.yaml` | Control sheet settings. | `configs/control/load_control_sheet_settings.yaml` | Production | High | Moved with control sheet test coverage. | Load control sheet config tests. | Affects control sheet ingestion. |
| `config/load_control_sheet_v2_settings.yaml` | Ads v2 control sheet settings. | `configs/adsv2/load_control_sheet_v2_settings.yaml` | Operational-transition | High | Moved with v2 config fallback. | Config tests proving v2 settings load. | Keep separate from v1 control settings. |
| `config/global_solution_settings.yaml` | Global solution settings. | `configs/delivery/global_solution_settings.yaml` | Production | Medium | Moved with config migration. | Config manager tests. | Delivery/global solution settings. |
| `config/model_settings.yaml` | Model settings. | `configs/model/model_settings.yaml` | Operational-transition | Medium | Moved with config migration. | Config manager tests. | Model/runtime settings. |
| `config/next_uk.json` | Legacy client config. | `configs/clients/next_uk.yaml` | Production | High | Moved to Dynaconf YAML. | Client config tests, validators, DAB validate. | JSON is no longer an operational source; `load_client_config()` is a deprecated Dynaconf-backed wrapper. |
| `config/next_gb.json` | Legacy client config. | `configs/clients/next_gb.yaml` | Production | High | Moved to Dynaconf YAML. | Client config tests, validators. | JSON is no longer an operational source; `load_client_config()` is a deprecated Dynaconf-backed wrapper. |
| `config/users.yaml` | User/schema config. | `configs/runtime/users.yaml` | Deployment | Medium | Moved with config migration. | DEV/user schema tests. | Affects dev deployment/schema. |

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

pCTR is not just ranking. The current retained work includes feature/data prep,
model training, model scoring, ranking, and output-table definitions. Keep
experiment SQL with the experiment until the feature/model contracts are ready
to split into production package and SQL domains.

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `experiments/analytics_pctr/` | Current DEV-only analytics pCTR notebook graph, including feature build SQL, prediction scoring, training, and ranking evaluation. | `experiments/analytics_pctr/`; later split feature prep to `src/next_ads/features/pctr/`, model/scoring to `src/next_ads/ranking/pctr/`, and entrypoints to `jobs/model/pctr/`. | Operational-transition | High | Retain as a self-contained experiment until productisation is agreed. | DEV job validation, model/table contract checks before promotion. | Job `mktg_next_uk_nextads_analytics_pctr` points here and remains DEV-only/paused. |
| `sql/ranking/pctr/create_table_nextads_analytics_pctr_predictions*.sql` | Analytics pCTR scored/ranked output table DDL. | `sql/ranking/pctr/` | Operational-transition | Medium | Moved with analytics pCTR layout cleanup. | SQL resolver tests and table contract review before run. | Only the scored/ranked prediction DDL moved to ranking SQL; feature-build notebook SQL stays with the experiment. |
| `experiments/sb_pctr/customer_behaviour_features.py` | Customer behaviour features for response model. | `experiments/sb_pctr/customer_behaviour_features.py` initially; reusable pieces to `src/next_ads/data/features/`. | Operational-transition | High | After pCTR route agreed. | Feature table contract check. | May become production feature code. |
| `experiments/sb_pctr/pctr_advert_metadata_attribute_profile.py` | Advert metadata feature/profile build. | `experiments/sb_pctr/` then `src/next_ads/data/features/advert_metadata.py`. | Operational-transition | High | After table contract agreed. | Feature output check. | pCTR model feature work. |
| `experiments/sb_pctr/pctr_advert_semantic_embeddings.py` | Advert semantic embedding feature build. | `experiments/sb_pctr/` then `src/next_ads/ranking/pctr/embeddings.py` and `jobs/model/pctr/`. | Operational-transition | High | After MLflow/volume strategy agreed. | Model/artifact load, feature output check. | ML/model artifact sensitive. |
| `experiments/sb_pctr/pctr_build_training_snapshots.py` | Training snapshot build. | `jobs/model/pctr/build_training_snapshots.py` plus reusable `src/next_ads/data/features/`. | Operational-transition | High | After operational pCTR job route agreed. | Snapshot table contract check. | Writes model-ready data. |
| `experiments/sb_pctr/pctr_product_embedding_features.py` | Product embedding feature build. | `experiments/sb_pctr/` then `src/next_ads/ranking/pctr/product_embeddings.py`. | Operational-transition | High | After artifact strategy agreed. | Model load and feature output check. | Uses MLflow/SentenceTransformer. |
| `experiments/sb_pctr/pctr_seasonal_product_features.py` | Seasonal product features. | `experiments/sb_pctr/` then `src/next_ads/data/features/seasonal.py`. | Operational-transition | Medium | After feature contract agreed. | Feature output check. | Model feature component. |
| `experiments/sb_pctr/pctr_spark_model_training.py` | Spark model training. | `jobs/model/pctr/train.py` plus `src/next_ads/ranking/pctr/training.py`. | Operational-transition | High | After model lifecycle agreed. | MLflow training run evidence. | Not a simple script move. |
| `experiments/sb_pctr/pctr_score_ad_candidates.py` | pCTR candidate scoring. | `jobs/model/pctr/score_ad_candidates.py` plus `src/next_ads/ranking/pctr/scoring.py`. | Operational-transition | High | After trained model and alias strategy agreed. | Scoring output contract check. | Can affect ranking. |
| `experiments/sb_pctr/pctr_tagged_click_training.py` | Tagged click training/label data. | `jobs/model/pctr/tagged_click_training.py` plus `src/next_ads/data/labels/`. | Operational-transition | High | After label contract agreed. | Label output contract check. | Model target generation. |

## Theme Affinity Model Map

The retained `experiments/hackathon_theme_affinity_model/` folder is the
legacy reference home of the Theme Affinity model. The model scores or ranks account-to-theme affinity
using theme interaction, views, baskets, add-to-bag, repurchase, popularity,
trending, and customer feature signals. It was created during a hackathon, but
the target production domain name should be `theme_affinity`.

This work is operational-transition work. It is not safe to delete or ignore
because current outputs are used by downstream NextAds assignment logic.
During migration, keep existing table names and config keys working as legacy
contracts until a separate output migration is agreed.

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| External Databricks `Hackathon_job` definition | Live scheduled Theme Affinity workflow that currently runs `hackathon_model` notebooks and writes legacy hackathon-named outputs. | `pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity.yml` initially, then `pipelines/databricks/jobs/` if DAB resources move. | Operational-transition | High | Bring job definition into repo before moving notebooks or changing model code. | DAB validate and task/parameter contract checks; Databricks run evidence before replacing the external job. | Use Theme Affinity as the target job name while preserving legacy notebook paths and output parameters until a separate compatibility migration is agreed. |
| `experiments/hackathon_theme_affinity_model/` | Retained legacy Theme Affinity model folder. | `experiments/hackathon_theme_affinity_model/` for retained notebooks/assets, then productised code split across `src/next_ads/*/theme_affinity/` and `jobs/model/theme_affinity/`. | Operational-transition | High | After consumers and output tables are documented. | Current output contract check. | Production domain naming is Theme Affinity; keep legacy contract strings only where compatibility requires them. |
| `experiments/hackathon_theme_affinity_model/config.py` | Model URI and feature list config. | `configs/model/theme_affinity.yaml` for durable config. Temporary notebook-only config remains under `experiments/hackathon_theme_affinity_model/config.py` during transition. | Operational-transition | High | With MLflow route. | Model URI load test and feature-list compatibility check. | Current model URI points to a UC model whose name still contains `hackathon`; do not rename the registered model without a separate migration. |
| `experiments/hackathon_theme_affinity_model/predict_model.ipynb` | Prediction notebook for scoring account/theme affinity. | Productised entrypoint to `jobs/model/theme_affinity/predict.py`; reusable logic to `src/next_ads/ranking/theme_affinity/predict.py`. | Operational-transition | High | After MLflow predict job exists. | Prediction job run and output contract check. | Retain notebook until job replacement is proven. |
| `experiments/hackathon_theme_affinity_model/clean_output.ipynb` | Cleans and writes the Theme Affinity output currently known by legacy hackathon table names. | Prediction and output shaping run together in `jobs/model/theme_affinity/model_predict.py`; reusable shaping remains in `src/next_ads/ranking/theme_affinity/clean_output.py`. | Operational-transition | High | After output contract is documented. | Output table shape and consumer check. | Preserve legacy accepted output table names until a separate table rename or alias migration is agreed. |
| `experiments/hackathon_theme_affinity_model/run_pipeline_predict.ipynb` | Pipeline prediction orchestration notebook. | Productised orchestration to DAB job resources and `jobs/model/theme_affinity/`. | Operational-transition | High | With MLflow operationalisation. | Databricks run evidence. | Candidate for job replacement. |
| `experiments/hackathon_theme_affinity_model/simple_rules_rank.ipynb` | Simple rules/ranking notebook, likely fallback or comparison logic for theme ranking. | Reusable fallback logic, if operational, to `src/next_ads/ranking/theme_affinity/rules.py`. | Operational-transition | Medium | Move with folder, then assess whether logic is operational. | Notebook context retained; output comparison if productised. | Do not assume this is disposable without owner review. |
| `experiments/hackathon_theme_affinity_model/ranking_encoders.joblib` | Model encoder artifact. | Keep initially under `experiments/hackathon_theme_affinity_model/`; later move to MLflow artifact or Databricks volume. | Operational-transition | High | Only after artifact strategy is agreed. | Artifact load test. | Do not lose binary artifact; do not rely on repo binary long term if MLflow can own it. |
| `experiments/hackathon_theme_affinity_model/sql/*.sql` | Theme Affinity feature, training, spine, target, and master association SQL. | Productised SQL to `sql/ranking/theme_affinity/features/`, `sql/ranking/theme_affinity/training/`, and `sql/ranking/theme_affinity/prediction/` as appropriate. | Operational-transition | High | After feature/output contracts are agreed. | Feature SQL/table output checks. | Current model support SQL; preserve output contracts during move. |
| `config/tables_settings.yaml` keys such as `hackathon_assignments`, `theme_score_components_hackathon*`, and `preranked_ads_from_themes_hackathon_latest` | Legacy table contracts consumed by current production code. | Keep current keys initially; later introduce `theme_affinity_*` aliases only through a separate compatibility migration. | Production | High | Do not rename in the first code move. | Config load tests and consumer output checks. | These names are legacy but operational; changing them is a production contract change. |
| `sql/create_table_*_hackathon*.sql` | Legacy table definitions for Theme Affinity-derived outputs. | Keep existing files until table naming migration is agreed; later target `sql/ranking/theme_affinity/` or `sql/decisioning/` depending on table role. | Production | High | After output contract and alias plan exists. | Table creation smoke and downstream consumer check. | File names can remain legacy longer than code folder names. |

## Ads V2 Map

The old `adsv2/` folder has been folded into the current route-oriented repo
layout because the v2 route is becoming a major part of NextAds. V2 entrypoints
can live in the route folder that describes what they do, for example
`jobs/nextads_control`, `jobs/nextads_candidates`, `jobs/nextads_v2`, or
`jobs/nextads_delivery`. Do not force every v2 file into `jobs/nextads_v2/`
when the current route folder is clearer.

Ads v2 is a candidate production route that affects how outputs
are shaped and consumed by downstream systems. That makes it high-risk
operational-transition work requiring explicit output contract checks and
parallel-run evidence.

CMS/data-pull work was reconciled through completed PR `249403`
(`feature/TL/cmsdata`). The current data-pull route now lives in the
Databricks data-pull job/pipeline resources plus `src/next_ads/data/sort_order/`
and `jobs/nextads_data/archive_sort_order_data.py`; do not treat it as deferred
Ads v2 cleanup.

| Current path | Current role | Target path | Status | Risk | Move timing | Validation required | Notes |
|---|---|---|---|---|---|---|---|
| `jobs/nextads_control/load_control_sheet_v2.py` | Ads v2 control sheet loader. Reads the v2 Google Sheet, validates fields, writes v2 raw/latest/control tables, and writes exclusions. | Keep current control route entrypoint; reusable parsing/date/MASID/exclusion logic can later move to `src/next_ads/control/adsv2/load_control_sheet.py`. | Operational-transition | High | Keep current route folder while contracts stabilise. | Control sheet v2 tests, v1/v2 output comparison, DEV Integration run, PREPROD validation before production adoption. | This changes an input/output contract, so it should not be hidden under experiments. |
| `adsv2/load_control_sheet_v2_settings.yaml` | Ads v2 control sheet source and read schema. | `configs/adsv2/load_control_sheet_v2_settings.yaml`; keep `v2` in the filename. | Operational-transition | High | Moved while preserving the v2-specific filename. | Config tests proving v2 settings load in dev/preprod/prod. | Keep separate from v1 settings until v2 becomes the default route. |
| `adsv2/tables_settings.yaml` | Former Ads v2 table write settings for raw/latest/control/exclusions outputs. | Retired; v2 table keys now live in `configs/runtime/tables_settings.yaml`. | Operational-transition | High | Consolidated into the shared table settings file. | Table config tests, table creation check in DEV Integration/PREPROD as appropriate. | Table names are output contracts; do not reintroduce a parallel v2 table settings file without an explicit compatibility plan. |
| `adsv2/README.md` | Ads v2 transition context. | `docs/adsv2.md` or `docs/nextads_v2.md`. | Documentation | Low | With v2 migration PR. | Markdown review. | Should explain parallel run, output comparison, and cutover criteria. |
| `adsv2/__test_load_control_sheet_config.py` | Ads v2 config/prototype test. | `tests/unit/adsv2/test_load_control_sheet_config.py` if it is a unit/config test, or `tests/integration/adsv2/` if it needs Databricks/Sheets. | Operational-transition | Medium | Before moving the loader. | Test collection check and CI validation. | Rename from double-underscore form so pytest ownership is clear. |
| `sql/adsv2/create_table_sort_order_v2*.sql` | Ads v2 sort-order table DDL. | `sql/adsv2/` | Operational-transition | Medium | Moved from root SQL as an Ads v2 table-contract file. | SQL resolver tests, table setup checks before run. | File move only; table names and contracts remain unchanged. |
| Candidate-build v2 tasks | Databricks task definitions for v2 control-sheet loading, page-type candidate mapping, validation, and v2 page-build trigger. | Keep in `pipelines/databricks/jobs/mktg_next_uk_nextads.yml` while shared upstream inputs are bundled with v1; later extract only if scheduling or ownership needs diverge. | Operational-transition | High | Active parallel route. Separate only after the shared input boundary and trigger contract are stable. | DAB validate, DEV Integration deployment, PREPROD validation, no production overwrite unless approved. | V2 runs beside v1 in candidate build. It must not depend on v1 mapping output. V2 owns the Theme Mapping tab, with Apps Script copying it to the locked v1 tab before the shared parser runs. |

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

The migration used domain-by-domain moves so package code and Databricks
entrypoints could move together while output contracts remained stable.

1. `feature/SWB/5128910-control-domain-move`
   Move control sheet, attribute parsing, theme mapping, control helpers, and
   matching entrypoints. This is draft PR `246383`.
2. `feature/SWB/5128910-main-job-entrypoint-move`
   Move remaining core main-job scripts into route-oriented `jobs/nextads_*`
   folders and update DAB job paths in the same branch.
3. `feature/SWB/5128910-ranking-domain-move`
   Move scoring, theme-score mapping, Theme Affinity ranking pieces, and
   ranking scripts into `src/next_ads/ranking` plus relevant job entrypoints.
4. `feature/SWB/5128910-decisioning-domain-move`
   Move assignment, customer-cell, and build-page logic into
   `src/next_ads/decisioning` and update entrypoints.
5. `feature/SWB/5128910-delivery-domain-move`
   Move PLP GS, MASID handoff, Bloomreach/v2 payload, and export logic into
   `src/next_ads/delivery` plus delivery job entrypoints.
6. `feature/SWB/5128910-features-models-foundation`
   Move feature generation, Theme Affinity DLT/Lakeflow feature prep, pCTR
   feature prep, and MLflow train/promote/scoring routes into
   `src/next_ads/features`, `src/next_ads/ranking/*`, and `jobs/model`. This is
   already active through the Feature Store foundation and Theme Affinity
   MLflow lifecycle PRs.
7. `feature/SWB/5128910-adsv2-domain-move`
   Keep v2 entrypoints in clear route folders, move reusable logic into Ads v2
   package subdomains after active v2 PRs and contracts are clear. CMS/data
   pull has already been reconciled through PR `249403`; its archive
   entrypoint now lives in `jobs/nextads_data`.
8. `feature/SWB/5128910-realtime-reporting-move`
   Move realtime and reporting/results helpers into `src/next_ads/realtime`
   and `src/next_ads/reporting`.
9. `feature/SWB/5128910-config-sql-layout`
   Move config and SQL into domain folders, update config loader/path
   resolution, and update tests without renaming live table contracts.
10. `feature/SWB/5128910-cleanup-legacy-paths`
    Remove old wrappers, stale scripts, empty folders, dead imports, and update
    docs once references are gone.

## High-Risk Items Requiring Separate Stories

Do not change the behaviour of these until their contracts and validation are
agreed:

- `src/next_ads/decisioning/assignment.py`
- `src/next_ads/ranking/scoring.py`
- `jobs/nextads_cells/assign_customer_cells.py`
- `jobs/nextads_assignment/build_page.py`
- `jobs/nextads_candidates/build_theme_ad_candidates.py`
- future control-sheet logic or output changes beyond the PR `246383` entrypoint
  move
- `jobs/nextads_candidates/conditional_probability_recs.py` and `sql/create_table_conditional_probability*.sql`
- behavioural or downstream-contract changes to `jobs/nextads_reporting/results_to_bigquery.py`
- `jobs/table_operations/create_tables.py`
- `configs/runtime/tables_settings.yaml`
- `configs/clients/next_uk.yaml`
- `adsv2/` / NextAds v2 output-contract route
- `experiments/hackathon_theme_affinity_model/` / legacy Theme Affinity outputs
- `experiments/sb_pctr/` / pCTR model route
- `experiments/analytics_pctr/` / analytics pCTR model route
- behavioural or output-contract changes to `jobs/realtime` and `src/next_ads/realtime`
- `databricks.yml`
- `pipelines/databricks/jobs/mktg_next_uk_nextads.yml`
- `pipelines/databricks/jobs/mktg_next_uk_nextads_realtime_results.yml`
- `pipelines/databricks/jobs/dev_integration_setup.yml`
- `pipelines/databricks/jobs/preprod_setup.yml`
- `azure-pipelines.yml`
- `azure-pipelines-validation.yml`

## Coverage Notes

This map is intentionally a control map, not a line-by-line manifest. Some
folders are mapped by family because listing every file would make the document
harder to use:

- SQL files under `sql/` are mapped by table family in the SQL Map.
- Theme Affinity SQL files under `experiments/hackathon_theme_affinity_model/sql/` are mapped as a single
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
- Legacy hackathon-named Theme Affinity model work is explicitly marked as operational-transition work with current outputs preserved.
- Migration order is defined for low-risk and decision-affecting code.
- Follow-up stories can use this map to choose what moves next.

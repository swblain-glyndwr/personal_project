# Historical Repo Restructure Wrap-Up Audit

> **Historical evidence record:** branch, PR, repository and validation states
> below describe the June and July 2026 restructure work. They are retained as
> evidence and must not be read as current branch or operational status.

Last updated: 2026-07-26.

Branch: `feature/SWB/5128910-restructure-wrap-up`
Baseline: `origin/develop` at `a891c0284d665441dff49afbceb00c8d8f46f34c`
Linked work item: `5128910`
Planned release reference: `nextads-v2026.06.29.1`

This file records the baseline for the final restructure change before moving
remaining non-v2 files. It is retained as review evidence, not current operating
guidance.

## Active PR Overlap

Checked with `az repos pr list --status active` on 2026-06-29.

| PR | Source | Target | State | Restructure impact |
| --- | --- | --- | --- | --- |
| 216633 | `feature/ET/map_theme_scores_duplicates_fix` | `main` | Active | Old main-targeting ranking change; not part of the recorded restructure change. |
| 216705 | `feature/AL/duplicate_acc_theme_error` | `main` | Active, conflicts | Old main-targeting champion/challenger change; not part of the recorded restructure change. |
| 217984 | `feature/AL/champ_challenger_score_to_ad` | `main` | Active, conflicts | Old main-targeting champion/challenger change; not part of the recorded restructure change. |
| 222364 | `feature/AL/associations_code` | `main` | Active | Old main-targeting association change; not part of the recorded restructure change. |
| 239447 | `feature/TL/v2_exclusions` | `main` | Draft, conflicts | Ads v2; explicitly out of scope. |
| 246973 | `feature/CWB/pctr_rt_feature_build` | `develop` | Draft | pCTR overlap; preserve as operational-transition/reference work. |
| 247185 | `feature/TL/datapullv2` | `develop` | Active | Ads v2; explicitly out of scope. |
| 247878 | `feature/TL/v2exclusions` | `develop` | Active | Ads v2; explicitly out of scope. |

## Baseline Leftovers

At the recorded baseline, these folders still existed on `origin/develop` and
were targeted by the restructure change:

- `experiments/hackathon_theme_affinity_model/`
- `experiments/sb_pctr/`
- legacy `scripts/`

The intended final state recorded for the restructure change was:

- `hackathon_model/` removed after retained reference material is moved under
  `experiments/hackathon_theme_affinity_model/`;
- `response_model/` removed after retained pCTR reference material is moved
  under `experiments/sb_pctr/`;
- DAB task paths and compatibility wrappers moved out of `scripts/`;
  Ads v2 entrypoints now live in route-oriented `jobs/nextads_*` folders.

## DAB References Resolved By The Restructure Change

Non-v2 references moved by the restructure change:

- `pipelines/databricks/jobs/dev_integration_setup.yml`
  - `../../../jobs/table_operations/create_tables.py`
- `pipelines/databricks/jobs/dev_setup.yml`
  - `../../../jobs/table_operations/setup_dev_tables.py`
- `pipelines/databricks/jobs/mktg_next_uk_nextads_feature_store.yml`
  - `../../../jobs/table_operations/create_feature_store_tables.py`
- `pipelines/databricks/jobs/mktg_next_uk_nextads_assignment_validation.yml`
  - `../../../jobs/nextads_reporting/assignment_validation.py`
- `pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity_model_monitor.yml`
  - `../../../jobs/model/theme_affinity/monitor_model.py`
- `pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity_model_train.yml`
  - `../../../jobs/model/theme_affinity/train_gpu_xgboost_model.py`
- `pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity_model_train_spark.yml`
  - `../../../jobs/model/theme_affinity/train_model.py`
- `pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity_quality_monitor_setup.yml`
  - `../../../jobs/model/theme_affinity/setup_quality_monitor.py`
- `pipelines/databricks/jobs/preprod_dependency_smoke.yml`
  - `../../../jobs/smoke/preprod_dependency_smoke.py`
- `pipelines/databricks/jobs/preprod_setup.yml`
  - `../../../jobs/table_operations/create_tables.py`
- `pipelines/databricks/jobs/prod_table_contract_smoke.yml`
  - `../../../jobs/smoke/prod_table_contract_smoke.py`
- `pipelines/databricks/jobs/table_size_monitoring.yml`
  - `../../../jobs/table_operations/calculate_table_sizes.py`

Ads v2 references moved to route folders:

- `pipelines/databricks/jobs/mktg_next_uk_nextads.yml`
  - `../../../jobs/nextads_control/load_control_sheet_v2.py`
  - `../../../jobs/nextads_candidates/build_page_type_candidates_v2.py`
- `pipelines/databricks/jobs/mktg_next_uk_nextads_page_build.yml`
  - `../../../jobs/nextads_v2/build_page.py`
- `pipelines/databricks/jobs/mktg_next_uk_nextads_payload_export.yml`
  - `../../../jobs/nextads_delivery/build_v2_payload.py`

## Documentation And Test Baseline

Repo-wide references cleaned or intentionally retained:

- Root `README.md` now points at `configs/` paths instead of old `config/`
  paths.
- `docs/repo_structure.md` now records the route-oriented `jobs/nextads_*`
  split.
- `docs/repo_migration_map.md` now lists retained experiment folders,
  route-oriented job folders, and v2 migration constraints explicitly.
- The follow-up cleanup branch removes the root `next_ads/` compatibility
  package, moves the remaining implementations into `src/next_ads`, and
  updates tests to use supported imports only.

## Scope Boundary

The restructure change was not intended to alter Ads v2 behaviour, table
contracts, registered model names, output table names, or production deployment
routing. Legacy names such as `nextads_hackathon_model` and
`next_uk_next_ads_hackathon_model_*` could remain where they were live external
contracts, while repository folders and docs used the Theme Affinity domain
name.

## Stage 1 Validation

Run on 2026-06-29 before committing this audit.

| Check | Result | Notes |
| --- | --- | --- |
| `git diff --check` | Passed | No whitespace errors. |
| `python -m pytest tests` | Blocked | `python` launcher is unavailable on this machine. |
| `.venv\Scripts\python.exe -m pytest tests` | Failed | Baseline Databricks/integration tests fail on local Databricks auth/profile resolution. Unit tests continue to run. |
| `.venv\Scripts\python.exe -m pytest tests\unit` | Passed | 242 passed, 16 skipped. |
| `databricks bundle validate -t DEV` | Blocked | Local profile ambiguity between `DEV` and `SANDBOX`; rerun with `--profile DEV`. |
| `databricks bundle validate -t DEV --profile DEV` | Passed | Existing workspace permissions warning only. |
| `databricks bundle validate -t DEV_INTEGRATION` | Blocked | Local profile ambiguity between `DEV` and `SANDBOX`; rerun with `--profile DEV`. |
| `databricks bundle validate -t DEV_INTEGRATION --profile DEV` | Passed | Existing workspace permissions warning only. |

## Final Wrap-Up Validation

Run on 2026-06-30 after the restructure wrap-up commits.

| Check | Result | Notes |
| --- | --- | --- |
| `git diff --check` | Passed | Line-ending warnings only. |
| `.venv\Scripts\python.exe -m pytest tests\unit` | Passed | 242 passed, 16 skipped. |
| `.venv\Scripts\python.exe -m pytest tests` | Blocked | Databricks/integration tests require a resolved Databricks profile/auth context; local run failed on `DEFAULT`/`PREPROD`/`PROD` profile ambiguity and Databricks Connect auth. Unit tests passed. |
| `databricks bundle validate -t DEV` | Blocked | Local profile ambiguity between `DEV` and `SANDBOX`; rerun with `--profile DEV`. |
| `databricks bundle validate -t DEV --profile DEV` | Passed | Existing workspace permissions warning only. |
| `databricks bundle validate -t DEV_INTEGRATION` | Blocked | Local profile ambiguity between `DEV` and `SANDBOX`; rerun with `--profile DEV`. |
| `databricks bundle validate -t DEV_INTEGRATION --profile DEV` | Passed | Existing workspace permissions warning only. |

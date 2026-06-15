### Purpose

- Production release promotion / hotfix:
- Goal of the change:
- Linked work item:

### Target Branch

- Target branch: `main`
- Confirm this follows the route in `docs/CICD/nextads_branch_release_route.md`:
  - `release/* -> main -> nextads-vYYYY.MM.DD.N tag -> PROD`
  - or `hotfix/* -> main -> nextads-vYYYY.MM.DD.N tag -> PROD`

### Release Evidence

- Validated `release/*` or `hotfix/*` branch:
- Included PRs/work items:
- PREPROD pipeline run link:
- PREPROD dependency smoke / Databricks validation link:
- Confirmation outputs were validated in `marketingdata_prod.ds_sandbox`:
- Confirmation PROD has not been run before this PR:

### Production Tag

- Planned tag: `nextads-vYYYY.MM.DD.N`
- Tag will point to the approved `main` commit after this PR completes:
- Release owner:

### File Changes

- `file1.py`: High-level description of changes.

### PROD Deployment Plan

- Manual pipeline: `mktg-next-ads-ci-cd`
- Pipeline version: production tag only
- Required stages:
  - [ ] Continuous Integration
  - [ ] Deploy to PROD
- Stages not to run from production tag:
  - [ ] DEV
  - [ ] DEV Integration
  - [ ] PREPROD
  - [ ] Destructive stages unless separately approved

### Hotfix Back-Merge

- [ ] Not a hotfix
- [ ] Hotfix must be PR'd/cherry-picked back to `develop`
- [ ] Hotfix must be PR'd/cherry-picked back to active `release/*`, where relevant

### Output and Deployment Impact

- [ ] No schema/output impact
- [ ] Schema or table output changed
- [ ] Databricks job or bundle config changed
- [ ] App/config/settings changed
- [ ] Downstream activation or reporting output changed
- [ ] Production risk or release-specific validation needed

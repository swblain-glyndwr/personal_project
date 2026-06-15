### Purpose

- Release-candidate update / release fix:
- Goal of the change:
- Linked work item:

### Target Branch

- Target branch: `release/*`
- Confirm this follows the route in `docs/CICD/nextads_branch_release_route.md`:
  - `develop -> release/*`
  - release fixes remain on `release/*` and are carried back to `develop`

### Release Candidate Scope

- Source branch or PR being brought into the release:
- Included PRs/work items:
- Changes intentionally excluded from this release:

### File Changes

- `file1.py`: High-level description of changes.

### PREPROD Validation Plan

- Required PREPROD stages:
  - [ ] Continuous Integration
  - [ ] Deploy PREPROD
  - [ ] Smoke PREPROD Dependencies
  - [ ] Initialize PREPROD Tables, only if agreed
- Databricks smoke/full validation link, where relevant:
- Expected output location: `marketingdata_prod.ds_sandbox`
- Confirmation PROD stages are not part of this PR/run:

### Output and Deployment Impact

- [ ] No schema/output impact
- [ ] Schema or table output changed
- [ ] Databricks job or bundle config changed
- [ ] App/config/settings changed
- [ ] Downstream activation or reporting output changed
- [ ] Release-specific validation needed

### Release Owner Notes

- Evidence to add to the work item/release record:
- Follow-up fixes or back-merge required:

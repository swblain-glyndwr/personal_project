### Purpose

- Is this a fix, feature, release fix or hotfix?
- What is the goal of the change?
- Linked work item:

### Target Branch

- Target branch:
- Confirm this follows the route in `docs/CICD/nextads_branch_release_route.md`:
  - `feature/* -> develop`
  - `release/* -> main`
  - `hotfix/* -> main` and back to `develop`

### File Changes

- `file1.py`: High-level description of changes.

### Validation Evidence

- Unit/lint validation:
- DEV or PREPROD run link, where relevant:
- Output/table sense-check evidence, where relevant:

### Output and Deployment Impact

- [ ] No schema/output impact
- [ ] Schema or table output changed
- [ ] Databricks job or bundle config changed
- [ ] App/config/settings changed
- [ ] Downstream activation or reporting output changed
- [ ] Production risk or release-specific validation needed

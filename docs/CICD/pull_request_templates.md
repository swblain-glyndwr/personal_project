# NextAds Pull Request Templates

This page explains the pull request templates used by the NextAds release
route. The templates live in `.azuredevops/` because Azure Repos reads PR
templates from the repository default branch.

Official Azure Repos guidance:
https://learn.microsoft.com/en-us/azure/devops/repos/git/pull-request-templates

## Template Locations

```text
.azuredevops/
  pull_request_template.md
  pull_request_template/
    branches/
      develop.md
      release.md
      main.md
```

## How Azure Repos Applies Them

Azure Repos applies branch-specific templates based on the first level of the
target branch name.

| Target branch | Template used | Purpose |
| --- | --- | --- |
| `develop` | `.azuredevops/pull_request_template/branches/develop.md` | Normal feature, fix and documentation work |
| `release/*` | `.azuredevops/pull_request_template/branches/release.md` | Release-candidate updates and release fixes |
| `main` | `.azuredevops/pull_request_template/branches/main.md` | Production release promotion and hotfix promotion |
| Anything else | `.azuredevops/pull_request_template.md` | Fallback/general template |

For example, `release.md` applies to pull requests targeting both `release`
and branches below it, such as `release/2026-06-03-nextads-release-validation`.

## What Each Template Should Capture

### Develop

Use the `develop` template for normal DS and engineering work.

It prompts for:

- linked work item;
- what changed;
- validation evidence;
- DEV or DEV Integration evidence, where relevant;
- output and deployment impact;
- reviewer focus areas.

### Release

Use the `release` template when bringing approved changes into a release
candidate or fixing something on a release branch.

It prompts for:

- release candidate scope;
- included and excluded PRs/work items;
- PREPROD validation plan;
- expected `marketingdata_prod.ds_sandbox` output location;
- confirmation that PROD stages are not part of the PREPROD route;
- release-owner evidence and back-merge notes.

### Main

Use the `main` template when promoting a validated release candidate or an
approved hotfix to the production baseline.

It prompts for:

- validated `release/*` or `hotfix/*` branch;
- PREPROD evidence;
- planned production tag in the `nextads-vYYYY.MM.DD.N` format;
- manual PROD deployment plan;
- hotfix back-merge checklist, where relevant;
- production impact.

## Maintenance Rules

- Keep templates short enough that contributors will actually complete them.
- Keep branch-specific templates aligned with
  `docs/CICD/nextads_branch_release_route.md`.
- Update the templates when the release route changes, not separately from it.
- Do not duplicate long process documentation inside the template. Link to the
  relevant docs instead.

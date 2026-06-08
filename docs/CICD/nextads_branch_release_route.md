# NextAds Branch and Release Route

Status: Proposed working agreement  
Related work item: User Story 5111663 / 5111799 / 5111813  
Release owner: Stephen Blain

## Purpose

This page defines the route for delivering NextAds changes from individual development through integration, release validation and production deployment.

The route keeps these stages separate:

- individual code changes;
- integrated changes being checked together;
- a release candidate being validated;
- a production deployment identified by a tag.

This is a delivery control for NextAds v2, not only a Git convention. It should make it clear which code is running in production, which changes have been integrated but not released, what has been validated outside production outputs, who agreed the release, and what version should be restored if a release causes an issue.

## Standard Route

```text
feature/* -> develop -> release/* -> main -> tag -> PROD
```

| Branch type | Purpose | Deployment position |
| --- | --- | --- |
| `feature/*` | Isolate one coherent change or user story | No production deployment |
| `develop` | Combine completed features for integrated testing | DEV integration validation only |
| `release/*` | Hold a controlled release candidate | PREPROD validation |
| `main` | Represent the approved production baseline | Production only through a `nextads-vYYYY.MM.DD.N` tag |
| `hotfix/*` | Correct an urgent production issue | Validate, merge to `main`, tag, then merge back |

Direct feature development into `main` is not part of this route.

## Environment and Output Route

The branch route controls code promotion. The deployment target controls where that code runs and where its outputs are written.

| Stage | Branch source | Deployment purpose | Output expectation |
| --- | --- | --- | --- |
| Development or integration validation | `develop` | Check combined changes before creating a release candidate | Write to `marketingdata_dev.nextads_integration`, not production outputs |
| Formal preproduction validation | `release/*` | Validate the release candidate before approval | Write validation outputs to `marketingdata_prod.ds_sandbox`, not `warehouse` |
| Production | Tagged commit on `main` | Run approved production code | Write approved production outputs to `marketingdata_prod.warehouse` |

For the current setup, PREPROD is schema-based: PREPROD runs in the production Databricks workspace using the `preprod` job parameter, but output must remain isolated in `marketingdata_prod.ds_sandbox`. Production output is `marketingdata_prod.warehouse`.

```text
validation output != production output
ds_sandbox        != warehouse
```

## DEV Integration Route

`DEV_INTEGRATION` is the shared development integration target for code that has already merged to `develop`.

```text
develop -> DEV_INTEGRATION -> marketingdata_dev.nextads_integration
```

Use this route to check whether approved team changes work together before creating a release branch. The deployment pipeline stage is intended to run from `develop`; ordinary feature branches should continue to use the developer-specific `DEV` target for isolated testing.

The smoke workflow for this route is:

1. Deploy `DEV_INTEGRATION` from `develop`.
2. Run the DEV integration table setup job to create any missing shared DEV tables.
3. Run `load_control_sheet`, and run `load_control_sheet_v2` when the v2 control sheet path is in scope.
4. Confirm the created or updated control sheet tables are in `marketingdata_dev.nextads_integration`.
5. Confirm no `marketingdata_prod.ds_sandbox` or `marketingdata_prod.warehouse` outputs changed.

If a merged change alters DEV integration table definitions, rerun the setup stage with `Recreate DEV integration tables` enabled. This drops and recreates configured write tables in `marketingdata_dev.nextads_integration` only; do not use it as a routine smoke step because it clears shared DEV integration outputs.

## Feature Route

1. Create `feature/*` from `develop`.
2. Develop and validate the change without writing to production `warehouse` outputs.
3. Raise a pull request from `feature/*` into `develop`.
4. Provide linked work item, validation evidence, and any output or deployment impact in the pull request.
5. Merge only after review and successful validation build.

## Release Route

1. Create `release/*` from `develop` when an agreed set of integrated changes is ready.
2. Deploy the release branch using the PREPROD route.
3. Run the metadata-only PREPROD dependency smoke check.
4. Validate that expected jobs, outputs, payloads or files are produced in `ds_sandbox`.
5. Fix release defects on `release/*` and carry those fixes back to `develop`.
6. Merge the approved release into `main`.
7. Create a production tag on the approved `main` commit.
8. Deploy production from that tag and record the deployed tag with the release evidence.

### PREPROD Release Validation

The CI/CD pipeline should be run from the `release/*` branch for PREPROD validation. Select `Continuous Integration`, `Deploy PREPROD`, and `Smoke PREPROD Dependencies`. Do not select PROD stages during PREPROD validation.

The PREPROD dependency smoke job is metadata-only by default. It validates package/config resolution, `job_env=preprod`, the `marketingdata_prod.ds_sandbox` output route, required schemas and configured read table metadata without reading rows or altering tables.

The PREPROD table setup job is intentionally non-destructive, but it still changes metadata by creating missing configured write tables in `marketingdata_prod.ds_sandbox`. Use `Initialize PREPROD Tables` only when setting up the route or after an agreed schema/table change. If a release needs destructive table migration, record that as a release migration activity outside this routine setup path.

### Tagged Production Release

Production deployment is a separate tagged release step. The CI/CD pipeline remains manually queued (`trigger: none`), and the Release Owner must select the production tag under pipeline version before starting a manual PROD pipeline run.

Use production tags in this format:

```text
nextads-vYYYY.MM.DD.N
```

Example:

```text
nextads-v2026.06.04.1
```

Increment `.N` when more than one production release is made on the same date. Tags must point to commits already merged to `main`. Do not create production tags from `develop`, `release/*`, `hotfix/*` or feature branches.

For a production release run, select `Continuous Integration` and `Deploy to PROD` only. Do not select DEV, DEV Integration, PREPROD or destructive stages. PROD deployment stages are guarded by tag conditions in `azure-pipelines.yml`; untagged `main` is not the production deployment route.

### Azure DevOps Setup

Configure branch policy for `release/*`, or for each release branch immediately after creation if folder policy is not available:

- pull request required;
- minimum one reviewer;
- approval reset when new changes are pushed;
- linked work item required;
- validation build required;
- direct pushes restricted to approved administrators only, if the project policy model requires an exception group.

The CI/CD pipeline must be allowed to use the Production library, production service connection and production agent pool. The validation pipeline should remain attached to PR policy for `develop`, `release/*`, `main` and `hotfix/*`.

Configure `main` branch policy before production promotion:

- pull request required;
- minimum one reviewer;
- approval reset when new changes are pushed;
- linked work item required;
- comment resolution required where the team wants comments to block;
- required build validation using `mktg-next-ads-validation`;
- build expiration immediately when `main` is updated;
- display name `NextAds main validation`.

Restrict production tag creation to Release Owners or approved administrators. Normal contributors should not be able to create, force-update or delete production tags.

### Databricks Setup

In the PROD Databricks workspace, confirm `marketingdata_prod.ds_sandbox` exists. The pipeline service principal must be able to deploy bundles, create and run jobs, create missing tables in `marketingdata_prod.ds_sandbox`, and read the required production-side input tables.

PREPROD validation must not require routine write access to `marketingdata_prod.warehouse`. PREPROD jobs run with `job_env=preprod` and should write validation outputs to `marketingdata_prod.ds_sandbox`.

## Hotfix Route

```text
main -> hotfix/* -> validation -> main -> tag (nextads-vYYYY.MM.DD.N) -> PROD
                             \-> develop
                             \-> active release/* where relevant
```

A hotfix is for an urgent correction to code already in production. It should be the smallest safe change required, validated through a non-production output route wherever possible, then merged back into `develop` and any active `release/*` branch that would otherwise reintroduce the issue.

Hotfix production deployment uses the same tag convention as normal releases:

1. Create `hotfix/*` from `main`.
2. Raise a PR back into `main` and wait for validation.
3. Tag the resulting `main` commit using `nextads-vYYYY.MM.DD.N`.
4. Manually run `mktg-next-ads-ci-cd` from the tag and select `Continuous Integration` and `Deploy to PROD`.
5. Record the hotfix branch, main PR, production tag and PROD pipeline run in release evidence.
6. PR or cherry-pick the hotfix back into `develop` and any active `release/*`.

## Pull Request and Branch Policy

| Target branch | Policy position |
| --- | --- |
| `develop` | Pull request required; minimum one reviewer; approval reset on new changes; linked work item; validation build required |
| `release/*` | Pull request required for release fixes; minimum one reviewer; approval reset on new changes; validation build required |
| `main` | Pull request required; minimum one reviewer; approval reset on new changes; production release requires a tag |
| `feature/*` | No target branch policy required |
| `preprod` | Not required as a separate branch when `release/*` is the release-candidate route |

## Release Evidence

Each release should record:

- release scope: work items, defects or pull requests included;
- validated `release/*` branch used for PREPROD;
- PREPROD deployment target or parameter and run result;
- PREPROD dependency smoke result;
- PREPROD table setup result, if run;
- validation output location, normally `marketingdata_prod.ds_sandbox`;
- smoke job links and output confirmation;
- confirmation that PROD stages were not run during PREPROD validation;
- main PR used to approve the release;
- production approval;
- production tag deployed, using `nextads-vYYYY.MM.DD.N`;
- production deployment result and post-release check;
- PROD pipeline run link;
- issues, rollback or follow-up required.

## Decisions for Later Rollout

The first rollout creates branch control plus validation. The later deployment-routing rollout should decide:

- exact branch-conditioned deployment triggers;
- whether branch-isolated PREPROD is added for exceptional risky changes;
- operational rollback steps for Databricks jobs and data outputs.

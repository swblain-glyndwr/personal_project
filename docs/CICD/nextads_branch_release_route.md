# NextAds Branch and Release Route

Status: Proposed working agreement  
Related work item: User Story 5111663 / 5111799  
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
| `main` | Represent the approved production baseline | Production only through a tag |
| `hotfix/*` | Correct an urgent production issue | Validate, merge to `main`, tag, then merge back |

Direct feature development into `main` is not part of this route.

## Environment and Output Route

The branch route controls code promotion. The deployment target controls where that code runs and where its outputs are written.

| Stage | Branch source | Deployment purpose | Output expectation |
| --- | --- | --- | --- |
| Development or integration validation | `develop` | Check combined changes before creating a release candidate | Must not write to production `warehouse` outputs |
| Formal preproduction validation | `release/*` | Validate the release candidate before approval | Write validation outputs to `marketingdata_prod.ds_sandbox`, not `warehouse` |
| Production | Tagged commit on `main` | Run approved production code | Write approved production outputs to `marketingdata_prod.warehouse` |

For the current setup, PREPROD is schema-based: PREPROD runs in the production Databricks workspace using the `preprod` job parameter, but output must remain isolated in `marketingdata_prod.ds_sandbox`. Production output is `marketingdata_prod.warehouse`.

```text
validation output != production output
ds_sandbox        != warehouse
```

## Feature Route

1. Create `feature/*` from `develop`.
2. Develop and validate the change without writing to production `warehouse` outputs.
3. Raise a pull request from `feature/*` into `develop`.
4. Provide linked work item, validation evidence, and any output or deployment impact in the pull request.
5. Merge only after review and successful validation build.

## Release Route

1. Create `release/*` from `develop` when an agreed set of integrated changes is ready.
2. Deploy the release branch using the PREPROD route.
3. Validate that expected jobs, outputs, payloads or files are produced in `ds_sandbox`.
4. Fix release defects on `release/*` and carry those fixes back to `develop`.
5. Merge the approved release into `main`.
6. Create a production tag on `main`.
7. Deploy production from that tag and record the deployed tag with the release evidence.

## Hotfix Route

```text
main -> hotfix/* -> validation -> main -> tag -> PROD
                             \-> develop
                             \-> active release/* where relevant
```

A hotfix is for an urgent correction to code already in production. It should be the smallest safe change required, validated through a non-production output route wherever possible, then merged back into `develop` and any active `release/*` branch that would otherwise reintroduce the issue.

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
- release branch used for validation;
- PREPROD deployment target or parameter and run result;
- validation output location, normally `marketingdata_prod.ds_sandbox`;
- production approval;
- production tag deployed;
- production deployment result and post-release check;
- issues, rollback or follow-up required.

## Decisions for Later Rollout

The first rollout creates branch control plus validation. The later deployment-routing rollout should decide:

- final release tag naming convention;
- exact branch-conditioned deployment triggers;
- whether branch-isolated PREPROD is added for exceptional risky changes;
- operational rollback steps for Databricks jobs and data outputs.

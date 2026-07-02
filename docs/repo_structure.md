# NextAds Repo Structure

Related work item: 5111786
Release route: `docs/CICD/nextads_branch_release_route.md`
Detailed migration map: `docs/repo_migration_map.md` once the 5111778
migration-map PR has landed

This page is the source-controlled reference for the target NextAds repo
structure. It explains where future work should live and how the repo should
evolve without changing production behaviour by accident.

The matching team-facing wiki page should summarise this guidance and link back
to this file. The repo remains the source of truth because it is reviewed,
versioned and released with code changes.

## Goal

The target structure separates:

- reusable production package code;
- Databricks job entry points;
- Databricks bundle/resource definitions;
- configuration and policy;
- SQL assets;
- experiments and notebooks;
- documentation for humans and coding assistants;
- tests and validation checks;
- deployment/release support.

This makes it easier to understand what a change can affect. A move into
`src/next_ads` means code is becoming reusable production package logic. A
change to `resources/jobs` or `azure-pipelines.yml` means deployment behaviour
may change. A change under `experiments` should not be treated as production
logic unless a later story promotes it.

## Target Top-Level Layout

```text
next-ads/
  src/           # reusable production package code
  pipelines/     # Databricks bundle, DLT, Lakeflow, and process-flow definitions
  jobs/          # Databricks entry points, grouped by route
  configs/       # settings, policies, environment config, and future feature-layer config
  sql/           # table, view, and reporting SQL
  experiments/   # safe exploration, notebooks and reference work
  docs/          # team, release and AI-assistant context
  tests/         # confidence checks
  deployment/    # release setup and operational deployment support
```

The current repo will not jump to this layout in one PR. The migration should
happen in controlled stories with compatibility wrappers, output checks and
release evidence where needed.

## Folder Purposes

| Folder | Purpose | What belongs here | What should not be placed here |
| --- | --- | --- | --- |
| `src/` | Reusable production package code. | Python modules used by scripts, jobs, tests and future Databricks entry points. | One-off notebooks, ad hoc scripts, generated files or deployment YAML. |
| `pipelines/` | Process-flow definitions, if introduced. | Higher-level pipeline orchestration or flow definitions that are not Databricks bundle resource YAML. | Python business logic or Databricks job YAML currently owned by `resources/jobs`. |
| `jobs/` | Databricks job entry points. | Thin executable scripts that parse job parameters and call reusable package code. | Shared business logic that should be testable in `src/next_ads`. |
| `configs/` | Settings, policy and environment configuration. | YAML/JSON settings, model settings, route config and policy-like config. | Secrets, credentials or generated runtime files. |
| `sql/` | Table, view and reporting SQL. | DDL, view definitions, SQL checks and reporting SQL grouped by functional area. | Python transformations or notebook-only exploration. |
| `experiments/` | Safe exploration and reference work. | Notebooks, prototype code, historical analysis and model exploration that is not yet the production route. | Code that production jobs import directly without an explicit promotion story. |
| `docs/` | Team, release and AI-assistant context. | Repo structure, release route, PR process, guides, model context, known gotchas and safe-edit boundaries. | Long-lived generated artefacts or confidential secrets. |
| `tests/` | Confidence checks. | Unit tests, route-control tests, bundle/config tests, integration tests and contract tests. | Manual evidence screenshots or production data extracts. |
| `deployment/` | Release setup and operational deployment support. | Scripts or docs for release setup, admin tasks and deployment operations that are not normal job entry points. | Core production decisioning or ranking logic. |

## Target Package Layout

Reusable production code should move toward:

```text
src/
  next_ads/
    common/       # shared utilities used across the repo
    features/     # reusable feature definitions, grains, keys, and checks
    data/         # source access, data contracts, labels, and datasets
    control/      # control sheet, ad metadata, and eligibility
    retrieval/    # creates the pool of ads that could be considered
    ranking/      # scores or orders candidate ads
    decisioning/  # applies rules and selects final ads
    delivery/     # prepares outputs for downstream systems
    reporting/    # reusable reporting and diagnostics logic
    realtime/     # real-time adjustment logic and contracts
```

### Package Area Guidance

| Package area | Use it for | Examples |
| --- | --- | --- |
| `common` | Utilities shared across unrelated areas. | Logging helpers, small pure helpers, common constants. |
| `data` | Data contracts, schema validation and reusable dataset definitions. | Pandera models, feature contracts, validation checks. |
| `control` | Control sheet, ad metadata and eligibility logic. | Control sheet parsing, placement/exclusion eligibility rules. |
| `retrieval` | Candidate creation before ranking/decisioning. | Product/ad retrieval, theme transitions, recommender candidate pools. |
| `ranking` | Scoring and ordering candidates. | Theme scores, pCTR, Theme Affinity/model scoring. |
| `decisioning` | Final choice rules. | Assignment, quota logic, dedupe/exclusion rules, final selection. |
| `delivery` | Output shaping for downstream consumers. | Page payloads, global solution payloads, activation/export shaping. |
| `reporting` | Reporting and diagnostics logic. | Result checks, reporting views, table size/reporting helpers. |
| `realtime` | Real-time adjustment contracts and logic. | Realtime payload preparation and adjustment utilities. |

## Where New Work Should Go

### New Production Logic

New reusable production logic should go under `src/next_ads/<area>/`.

Use this route when:

- the code is intended to be imported by more than one script/job/test;
- the code is part of the production route or a release candidate;
- the code needs unit tests without running a Databricks job;
- the code defines business logic, data contracts, scoring, decisioning or
  delivery behaviour.

Production logic should not be added directly to a Databricks job entry point
unless the entry point is only a thin wrapper.

### New Databricks Job Entrypoints

New job entry points should move toward `jobs/`, with shared logic imported
from `src/next_ads`.

Entry points should:

- parse job parameters;
- load config;
- call tested package code;
- write/log run evidence;
- stay thin enough to review easily.

During transition, existing production jobs can remain in `scripts/` until a
specific story moves them.

### New Databricks Bundle Resources

Databricks bundle resource YAML remains under `resources/` until a separate
deployment restructure is agreed.

Use:

- `resources/jobs/` for Databricks job definitions;
- `resources/pipelines/` for Databricks pipeline definitions, where present;
- `resources/variables/` for bundle variables and environment-specific resource
  settings.

Any change here may affect deployment behaviour and should include bundle
validation evidence.

### New Configuration

Operational settings now live under `configs/`, grouped by purpose:

```text
configs/runtime/
configs/control/
configs/adsv2/
configs/model/
configs/delivery/
configs/clients/
```

The config loader keeps compatibility fallbacks for legacy flat `config/`
paths during transition.

Do not move or rename config files without checking:

- scripts/jobs that load the config;
- Databricks bundle sync paths;
- tests that expect the old location;
- environment-specific values for DEV, DEV Integration, PREPROD and PROD.

### New SQL

New SQL should stay under `sql/`, grouped by functional area where possible.

Suggested grouping:

```text
sql/control/
sql/retrieval/
sql/ranking/
sql/decisioning/
sql/delivery/
sql/reporting/
sql/realtime/
```

Table and view changes should include output/contract evidence. If a SQL change
alters a production output table, it should be treated as release-affecting.

### New Experiments

New exploratory work should go under `experiments/`.

Use this route when:

- the work is exploratory;
- the output is not yet part of the agreed production route;
- notebooks or ad hoc analysis are needed;
- the work needs to be kept for context but should not be imported by
  production jobs.

Experimental work can later be promoted into `src/next_ads`, `jobs/`,
`resources/` and `configs/` through a separate story with validation evidence.

### Operational Model Work

Operational model work should not stay named after its original experiment if
that name no longer helps the team understand the domain.

For example, the historical `hackathon_model` work should be treated as the
Theme Affinity model area:

```text
experiments/theme_affinity/              # reference notebooks and historical work
src/next_ads/ranking/theme_affinity/     # reusable scoring/model logic
src/next_ads/data/theme_affinity/        # feature contracts and model input data
src/next_ads/delivery/theme_affinity/    # model output shaping
jobs/model/theme_affinity/               # train/promote/predict entry points
configs/model/theme_affinity.yml         # model settings
```

Do not drop experimental/reference material if its outputs are currently used.
Move it into a named domain and document its operational status.

### Docs and AI Context

`docs/` should contain both human-facing documentation and compact context that
helps coding assistants make safer changes.

Useful docs include:

- release route and PR templates;
- repo structure and migration map;
- table setup and config guides;
- model/domain notes;
- safe-edit boundaries;
- output contracts and validation expectations;
- known gotchas.

If `docs/docs_for_ai/` is used, it should point back to the human-readable docs
and should not become a second source of truth.

## Current Transition Rules

- `src/next_ads` is the future home for reusable production package code.
- Low-risk reusable code can move into `src/next_ads` before Databricks job
  entry points move, as long as old imports keep working.
- Existing Databricks job entry points remain in `scripts/` until moved by a
  specific story.
- When a story explicitly scopes a domain move, the package code and matching
  Databricks entry points can move together in one PR, provided compatibility
  wrappers remain and job paths are validated.
- Existing Databricks job definitions remain in `resources/jobs/` until the
  deployment layout is changed by a specific story.
- Operational config now lives under grouped `configs/` folders, with loader
  fallbacks retained for legacy flat `config/` paths during the transition.
- Existing imports from the top-level `next_ads` package must keep working
  during the transition.
- Decision-affecting logic should move only in follow-up stories with output
  equivalence checks.
- Databricks job entry-point changes should normally be isolated from broad
  foundation work, but can be included in the same PR as the package move when
  the branch is domain-scoped and preserves output contracts.
- `src/next_ads/features` is the target home for reusable feature definitions,
  grains, keys, and checks.
- Release-control changes should follow the route in
  `docs/CICD/nextads_branch_release_route.md`.

## Migration Principles

1. Move low-risk reusable utilities before output-affecting logic.
2. Keep compatibility wrappers while old imports remain in scripts/jobs.
3. Add tests that prove old and new import paths behave the same.
4. Move Databricks job entry points with core business logic only when the PR is
   explicitly domain-scoped, keeps compatibility wrappers where needed, and
   updates DAB paths and tests in the same change.
5. Treat assignment, scoring, delivery, model and table-definition changes as
   output-affecting until proven otherwise.
6. Use DEV Integration and PREPROD validation where the release route requires
   it.
7. Record evidence on the PR and work item when a change affects outputs,
   deployment, schemas or release flow.

## Current Domain Migration Order

Story `5128910` is now following a more direct domain-by-domain migration plan.
The order is:

1. `feature/SWB/5128910-control-domain-move`
2. `feature/SWB/5128910-main-job-entrypoint-move`
3. `feature/SWB/5128910-ranking-domain-move`
4. `feature/SWB/5128910-decisioning-domain-move`
5. `feature/SWB/5128910-delivery-domain-move`
6. `feature/SWB/5128910-features-models-foundation`
7. `feature/SWB/5128910-adsv2-domain-move`
8. `feature/SWB/5128910-realtime-reporting-move`
9. `feature/SWB/5128910-config-sql-layout`
10. `feature/SWB/5128910-cleanup-legacy-paths`

The first branch is represented by draft PR `246383`. The feature/model
foundation is also active through the Theme Affinity MLflow lifecycle and
Feature Store foundation PRs. That work links the DLT/Lakeflow feature prep,
Feature Store direction, MLflow model lifecycle, and future pCTR/challenger
algorithms to the 1%+ incrementality objective.

## Relationship To The Wiki

The repo docs are the implementation source of truth. The wiki should be the
team-facing explanation.

Use the wiki for:

- onboarding and team walkthroughs;
- "where should I put this?" guidance;
- release-owner and DS contributor summaries;
- links back to source-controlled repo docs.

Use repo docs for:

- exact paths;
- commands;
- migration rules;
- PR evidence expectations;
- changes that should be reviewed with code.

The wiki page for this topic should live under:

```text
eCommerce Data wiki > Data Science > Next Ads
```

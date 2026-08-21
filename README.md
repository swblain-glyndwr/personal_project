# NextAds

NextAds selects relevant adverts for customers browsing NEXT. It combines:

- the adverts and placements currently allowed by the control sheets;
- model scores that describe customer relevance;
- shared customer information such as customer cells, recent advert exposure and advert feedback;
- separate V1 and V2 assignment and delivery routes.

The result is a set of final advert assignments for each supported location or
page type, followed by delivery to the systems that serve them.

## Start Here

For the complete explanation, including plain definitions of scoring,
score-selection, foundations, advert candidates, builds and assignments, read
the [NextAds job and table flow](docs/architecture/nextads_job_table_flow.md).

The daily assignment route is:

1. **12:15 — prepare scoring inputs and calculate Theme Affinity scores.** The
   model-scoring job asks the main NextAds job to prepare fixed theme and item
   inputs, then publishes account-to-theme scores.
2. **13:00 — calculate Markov comparison scores.** Markov currently runs as a
   shadow score source and does not affect delivered assignments.
3. **16:00 — prepare shared customer information.** Customer cells, repeat-ad
   exposure and advert feedback are recorded together for the evening run.
4. **18:00 — build advert options and assignments.** The main NextAds job loads
   V1 and V2 controls, selects exact approved score outputs, maps those scores
   to eligible adverts, and calls the V1 and V2 page-build jobs.
5. **After the page builds — hand off and deliver.** V1 publishes
   location-based assignments, runs a read-only MASID handoff check and triggers
   PLP delivery. V2 publishes page-type-and-rank assignments and triggers the
   Bloomreach payload export.
6. **21:00 — publish compatibility outputs and validate.** The compatibility
   job derives the older preranked table shapes from accepted advert options
   and then runs assignment validation.

The main job is `mktg_next_uk_nextads_candidate_build`. Despite its saved-job
name, it has two separate operations:

- `PREPARE_SCORING_INPUTS` prepares fixed scoring inputs for the earlier
  model-scoring run and stops before advert-option or assignment work.
- `CANDIDATE_BUILD`, the scheduled default, creates the V1 and V2 advert
  options and invokes their assignment and delivery children. It does not
  train models or calculate customer cells.

## Documentation

| If you need to understand... | Start with... |
| --- | --- |
| The complete route from source data to scores, advert options, assignments and delivery | [NextAds job and table flow](docs/architecture/nextads_job_table_flow.md) |
| Which architecture page answers a particular question | [Architecture and data-flow guides](docs/architecture/README.md) |
| What runs when and how jobs call one another | [Databricks runtime map](docs/CICD/nextads_databricks_runtime_map.md) |
| Job parameters, defaults and operating settings | [Databricks job settings](docs/CICD/nextads_databricks_job_settings.md) |
| Which jobs exist in each bundle target | [Databricks job environment matrix](docs/CICD/nextads_databricks_job_environment_matrix.md) |
| Model research, review, registration and isolated evaluation | [Model research walkthrough](docs/model_research_walkthrough.md) |
| Feature Store builders and published feature data | [Feature Store guide](docs/feature_store/README.md) |
| Local development and shared-environment testing | [Developer workflow guide](docs/developer_workflow_guide.md) |
| Branch, release and deployment routes | [CI/CD pipeline guide](docs/CICD/cicd_pipeline_guide.md) |
| The repository layout | [Repository structure](docs/repo_structure.md) |

The diagrams and job tables are reference material. Begin with the written
walkthrough before using them to investigate a particular task or table.

## Repository Areas

| Path | Responsibility |
| --- | --- |
| `src/next_ads/` | Reusable NextAds application and data-processing code |
| `jobs/` | Databricks task entry points grouped by responsibility |
| `pipelines/databricks/jobs/` | Databricks Asset Bundle job declarations |
| `configs/` | Client, feature, model and runtime declarations |
| `sql/` | Table contracts and SQL-based transformations |
| `tests/` | Unit, contract and orchestration tests |
| `docs/` | Architecture, operation, development and model guidance |

Feature work normally targets `develop`. The release route is documented in
the [branch and release guide](docs/CICD/nextads_branch_release_route.md).
Production job changes, deployments and destructive table operations require
the relevant controlled runbook and approval; the root README does not provide
shortcuts for those actions.

# NextAds Architecture And Data-Flow Guides

Start with the [plain-language job and table flow](nextads_job_table_flow.md).
It explains what NextAds does, what the repeated terms mean, how the daily route
fits together and what each job consumes and produces. The other pages answer
more focused questions and assume that introduction has been read.

## Choose A Route

| Reader or question | Read these pages |
| --- | --- |
| New to NextAds | [Job and table flow](nextads_job_table_flow.md), beginning with “What NextAds Does” and “Terms Used In This Guide” |
| Operating or investigating a daily run | [Databricks runtime map](../CICD/nextads_databricks_runtime_map.md) for schedules and hand-offs, then [job settings](../CICD/nextads_databricks_job_settings.md) for parameters and defaults |
| Investigating V1/V2 candidate, assignment or delivery behavior | [V1/V2 parallel route](v1_v2_parallel_route.md) for exact task dependencies and failure boundaries |
| Understanding operational Theme Affinity scoring | [Theme Affinity operational flow](theme_affinity_operational_flow.md) for its input preparation, prediction, publication and checks |
| Building reusable model data | [Feature Store flow](feature_store_flow.md) for task order and the [Feature Store table design](../feature_store/feature_store_table_design.md) for table grain, keys and refresh rules |
| Researching or reviewing a model | [Model research walkthrough](../model_research_walkthrough.md) for declared research, AutoML, review selection and isolated evaluation |
| Moving an approved model between environments | [MLflow model lifecycle](mlflow_model_lifecycle.md) for exact version movement and its separation from serving activation |
| Connecting a future score source | [Future model adoption](future_model_adoption.md) for the shared feature, scoring and evaluation contracts |
| Releasing or checking target availability | [Job environment matrix](../CICD/nextads_databricks_job_environment_matrix.md) and [branch and release route](../CICD/nextads_branch_release_route.md) |

## Page Responsibilities

- The [job and table flow](nextads_job_table_flow.md) owns the complete written
  walkthrough, shared vocabulary and cross-job input/output inventory.
- The [runtime map](../CICD/nextads_databricks_runtime_map.md) owns declared
  schedules, job triggers and dated runtime observations.
- The [job settings](../CICD/nextads_databricks_job_settings.md) page owns
  parameters, defaults and valid values.
- Detailed route pages own task order, data contracts and failure behavior for
  their named area; they should link back to the complete walkthrough instead
  of repeating it.

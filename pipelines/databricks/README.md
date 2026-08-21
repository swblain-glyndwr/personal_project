# Databricks Bundle Resources

Databricks Asset Bundle job, pipeline, and variable definitions live here.

- `jobs/` contains bundle job resources.
- `pipelines/` contains Lakeflow/DLT pipeline resources.
- `variables/` contains shared cluster and library variables.

The root `databricks.yml` remains at the repository root because Databricks
tooling expects the bundle definition there.

Start with the documentation that matches your question:

- [NextAds job and table flow](../../docs/architecture/nextads_job_table_flow.md): what the jobs do from scoring inputs through delivery.
- [Job settings](../../docs/CICD/nextads_databricks_job_settings.md): parameters, defaults and valid values.
- [Runtime map](../../docs/CICD/nextads_databricks_runtime_map.md): schedules, child-job calls and dated runtime observations.
- [Environment matrix](../../docs/CICD/nextads_databricks_job_environment_matrix.md): which bundle targets declare each job.

These pages describe repository declarations unless they explicitly cite a workspace run. A successful bundle check does not prove that the corresponding job has run successfully in an environment.

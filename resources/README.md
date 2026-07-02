# Databricks Bundle Resources

This folder contains Databricks Asset Bundle resource definitions and variables.
Changes here can alter deployed jobs, pipelines, permissions, schedules or
runtime dependencies.

Use target-scoped resources by default. A top-level `resources.jobs` declaration
is global to every target and must not be used for normal operational jobs.

See `docs/CICD/nextads_databricks_job_environment_matrix.md` for the intended
job availability by target.

# Databricks Job Resources

This folder defines Databricks jobs for the bundle targets in `databricks.yml`.

Normal operational jobs should be declared as reusable YAML anchors and then
included under explicit `targets.<target>.resources.jobs` blocks. Do not add
top-level `resources.jobs` for ordinary jobs, because that deploys the job to
every target, including `DEV_FEATURE_STORE`.

`DEV_FEATURE_STORE` is single-purpose and should contain only
`mktg_next_uk_nextads_feature_store`.

For current job ownership and operation:

- [NextAds job and table flow](../../../docs/architecture/nextads_job_table_flow.md) explains the daily process in plain language.
- [Job settings](../../../docs/CICD/nextads_databricks_job_settings.md) lists parameters, defaults and task responsibilities.
- [Runtime map](../../../docs/CICD/nextads_databricks_runtime_map.md) shows schedules and parent/child relationships.
- [Environment matrix](../../../docs/CICD/nextads_databricks_job_environment_matrix.md) lists the targets in which each resource is declared.

The main `mktg_next_uk_nextads_candidate_build` resource has two operations. `PREPARE_SCORING_INPUTS` is called by model scoring earlier in the day and stops before advert selection. The scheduled default, `CANDIDATE_BUILD`, builds advert options and invokes the V1/V2 assignment jobs.

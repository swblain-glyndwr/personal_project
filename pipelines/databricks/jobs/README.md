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

The route keeps job ownership separate. The unscheduled
`mktg_next_uk_nextads_scoring_inputs` resource creates the reusable accepted
input snapshot and is called by `mktg_next_uk_nextads_model_scoring` for the same
date. The independently scheduled `mktg_next_uk_nextads_candidate_build`
resource then selects accepted scoring outputs, builds V1/V2 advert options and
invokes the assignment jobs. Candidate Build contains no scoring-input or model
scoring tasks.

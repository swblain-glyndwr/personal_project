# Databricks Job Resources

This folder defines Databricks jobs for the bundle targets in `databricks.yml`.

Normal operational jobs should be declared as reusable YAML anchors and then
included under explicit `targets.<target>.resources.jobs` blocks. Do not add
top-level `resources.jobs` for ordinary jobs, because that deploys the job to
every target, including `DEV_FEATURE_STORE`.

`DEV_FEATURE_STORE` is single-purpose and should contain only
`mktg_next_uk_nextads_feature_store`.

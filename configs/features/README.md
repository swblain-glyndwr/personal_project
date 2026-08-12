# Offline Feature Contracts

`nextads_feature_store.yaml` is the repository source of truth for the logical
offline feature catalogue and its physical environment bindings.

Each feature has an explicit delivery state:

- `ACTIVE`: a reusable feature or support table with an implemented builder.
- `COMPATIBILITY`: an implemented model-specific table retained while consumers
  move to reusable feature lookups.
- `SCAFFOLD`: a schema placeholder with named missing contracts. It is not an
  implemented feature even if the current setup route creates an empty table.

Logical definitions contain keys, grain, builder, ownership, freshness and
consumer metadata. `store_bindings` separately resolves those definitions to
DEV, PREPROD and PROD namespaces. `repository_declared` reports only whether
the job target is present in this repository; it is not evidence that a job or
table is live. Only the DEV job target is currently declared. PREPROD and PROD
remain plans until their later deployment changes land.
The logical `builder` defaults to the existing `source_job`; an explicit
override records the actual writer where the legacy ownership metadata differs.
Existing jobs continue to consume `source_job` in this contract-only change.

PREPROD table names include a readable release stem and a stable hash of the
exact release identifier, so distinct release candidates cannot collapse onto
the same sandbox tables after punctuation is normalized. Supply `--release-id`
to resolve exact PREPROD names; without it the plan deliberately shows the
`{release_id}` template.

Inspect all bindings without making platform changes:

```powershell
.\.venv\Scripts\python.exe jobs\features\nextads\plan_offline_feature_store.py `
  --environment ALL --format text
```

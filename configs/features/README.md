# Offline Feature Contracts

## Source of Truth

| Artifact | Responsibility |
| --- | --- |
| `nextads_feature_store.yaml` | Logical offline feature catalogue and physical environment bindings. |
| `../../sql/features/nextads/` | Physical table and compatibility-view schemas. |
| `../../pipelines/databricks/jobs/mktg_next_uk_nextads_feature_store.yml` | Current Feature Store task graph and declared bundle targets. |

## Delivery States

| State | Meaning |
| --- | --- |
| `ACTIVE` | Reusable feature or support table with an implemented builder. |
| `COMPATIBILITY` | Implemented model-specific table retained while consumers move to reusable feature lookups. |
| `SCAFFOLD` | Table shell with named missing contracts; it must not be presented as implemented. |

## Delivery Gates

| Gate | Requirement |
| --- | --- |
| `CONTRACT_BASELINE` | Every intended definition is classified honestly and resolves through the planner. |
| `DEV_COMPLETE` | All intended physical tables have implemented builders and populated shared-DEV evidence at their declared cadence; both compatibility views resolve implemented sources; `SCAFFOLD=0`. |
| `DEV_SNAPSHOT_SAFE` | Shared DEV uses validated build-scoped staging and atomic publication, retaining the previous READY snapshot after failure. |
| `ENVIRONMENT_PARITY` | Matching PREPROD and PROD jobs use the completed logical graph after both DEV gates pass. |

- A SQL contract, empty registered table or planner entry does not satisfy `DEV_COMPLETE`.
- `SCAFFOLD=0` must be achieved by implementing each named source and materializer contract. Removing an intended feature merely to pass the gate is not allowed.
- A feature that is explicitly removed from the intended inventory requires a separate reviewed inventory decision.

## Contract Layers

| Layer | Contents | Rule |
| --- | --- | --- |
| `OfflineFeatureDefinition` | Keys, grain, builder, owner, freshness, consumers and delivery state. | Must not contain environment-specific locations. |
| `OfflineStoreBinding` | Catalog, schema, bundle target and table-name template. | May vary by environment without changing the logical feature graph. |
| Legacy `source_job` | Existing ownership metadata used by current jobs. | Retained unchanged for compatibility. |
| Logical `builder` | Task that currently writes the feature. | Defaults to `source_job`; use an override only when the actual writer differs. |

## Environment Bindings

| Environment | Namespace | Bundle target | Repository state | Table naming |
| --- | --- | --- | --- | --- |
| DEV | `marketingdata_dev.nextads_feature_store` | `DEV_FEATURE_STORE` | Declared | `{feature_id}` |
| PREPROD | `marketingdata_prod.ds_sandbox` | `PREPROD` | Planned | `{release_id}__{feature_id}` |
| PROD | `marketingdata_prod.nextads_feature_store` | `PROD` | Planned | `{feature_id}` |

- `repository_declared` records only whether the job target exists in this repository. It does not prove that a job or table is live.
- PREPROD uses a bounded readable stem plus a stable hash of the exact release identifier, preventing distinct release IDs from collapsing onto the same table name after punctuation is normalized.
- Omitting `--release-id` leaves PREPROD locations as explicit `{release_id}` templates.

## Inspect the Plan

All environments:

```powershell
.\.venv\Scripts\python.exe jobs\features\nextads\plan_offline_feature_store.py `
  --environment ALL --format text
```

One release-isolated PREPROD plan:

```powershell
.\.venv\Scripts\python.exe jobs\features\nextads\plan_offline_feature_store.py `
  --environment PREPROD --release-id release/2026.08.12 --format text
```

| Planner term | Meaning |
| --- | --- |
| `REPO_DECLARED` | The job target is present in repository configuration. |
| `PLANNED` | The binding is defined but its job target is not yet present. |
| `CONTRACT_READY` | The compatibility view has an implemented repository source contract. |
| `BLOCKED` | The source feature still has named missing contracts. |
| `RELEASE_ID_REQUIRED` | An exact PREPROD location needs `--release-id`. |

- None of these terms is evidence of a deployed job, populated table or READY immutable snapshot.

## Inspect Runtime Evidence

The Feature Store quality task audits registry-defined implemented contracts rather than a separate hand-maintained table list. Search its Databricks task output for `FEATURE_STORE_DEV_AUDIT_MANIFEST=` to retrieve the stable JSON evidence for physical paths, row/key checks, ordered schema hashes, Feature Engineering keys, table/reference-date commit tags, exact final Delta versions, skipped on-demand contracts and compatibility views.

`CURRENT_IMPLEMENTED_PASS` is not `DEV_COMPLETE`. The manifest keeps `dev_complete=false` while any scaffold remains, any implemented contract is skipped or either compatibility view is not ready.

`write_mode` is part of the logical definition. Daily point-in-time tables default to keyed `merge`; `next_uk_nextads_fs_item_attributes_latest` uses an atomic whole-table `overwrite` so removed source items cannot survive as stale current features.

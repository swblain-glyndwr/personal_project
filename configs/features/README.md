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

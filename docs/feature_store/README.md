# Next Ads Feature Store

| Work item | Purpose |
| --- | --- |
| 5111595 | Reusable offline Feature Store. |
| 5111881 | Documentation and migration backlog. |

## Feature Store Scope And Serving Boundary

| Area | Current boundary |
| --- | --- |
| Ownership | Logical contracts, builders, quality rules and model lookups remain in this repository. |
| Serving mode | Batch/offline first. Realtime and online publication are separate work. |
| Shared DEV | `marketingdata_dev.nextads_feature_store`. |
| Personal proof | Model-author runs write to the author's `marketingdata_dev` schema. |
| Customer impact | Feature and `EVALUATE` jobs do not change a serving portfolio, assignment or payload. |

## Start Here: Build A Shopping Bag pCTR Model

Use
[`building_a_challenger_model.md`](building_a_challenger_model.md)
for the complete individual data-scientist route:

1. Frame and research the modelling question.
2. Validate observed impression and click labels.
3. Select named Feature Store inputs.
4. Publish two accepted dates.
5. Build a point-in-time training receipt.
6. Compare models and register the exact result in DEV MLflow.
7. Prove an identical retry reuses that version.
8. Score a bounded Shopping Bag population in isolated `EVALUATE`.

That walkthrough deliberately stops before cross-environment promotion or
customer serving.

## Registry Contract Status

The read-only plan currently reports:

```text
ACTIVE=18 COMPATIBILITY=4 SCAFFOLD=0
```

| State | Count | Meaning |
| --- | ---: | --- |
| `ACTIVE` | 18 | Repository-owned operational feature contracts. |
| `COMPATIBILITY` | 4 | Older model-facing shapes retained while consumers move to logical contracts. |
| `SCAFFOLD` | 0 | No registered physical contract is presented as an unimplemented feature. |

These are repository contract states. They do not, by themselves, prove that a
particular environment or date has a READY snapshot.

The registry contains 22 physical contracts and two compatibility views.

## Feature Store Delivery Gates

| Order | Gate | Exit condition |
| ---: | --- | --- |
| 0 | `CONTRACT_BASELINE` | Every intended definition is classified and the graph is inspectable. |
| 1 | `DEV_COMPLETE` | All 22 physical contracts have accepted DEV data for their required cadence and both views resolve. |
| 2 | `DEV_SNAPSHOT_SAFE` | READY snapshots pin exact source and output Delta versions and failed attempts cannot replace accepted data. |
| 3 | `DEV_OPERATION_PROVEN` | DBR 15.4 smoke, complete run, retry and failure-retention evidence pass. |
| 4 | `ENVIRONMENT_PARITY` | The same logical graph is bound to the agreed release locations. |
| 5 | `OFFLINE_ACTIVATED` | Scheduling and alerts are enabled only after manual parity proof. |

An empty table shell is not a populated feature. An on-demand contract is not
exempt from evidence; it is proved through an explicit dated run rather than a
daily schedule.

## Feature Store Documentation Map

| Document | Purpose |
| --- | --- |
| [`building_a_challenger_model.md`](building_a_challenger_model.md) | Worked Shopping Bag route from research to DEV MLflow and isolated `EVALUATE`. |
| [`adding_sql_built_feature.md`](adding_sql_built_feature.md) | Contract, implementation, testing and evidence steps for a reusable feature built with Spark SQL. |
| [`reusable_feature_inventory.md`](reusable_feature_inventory.md) | Reusable signals and migration candidates. |
| [`feature_store_table_design.md`](feature_store_table_design.md) | Complete current table/view design, including grain, keys, dates, cadence and training safety. |
| [`candidate_similarity.md`](candidate_similarity.md) | Offline similarity diagnostics concept; not a current model input. |
| [`migration_backlog.md`](migration_backlog.md) | Prioritised migrations and dependencies. |
| [`../architecture/nextads_job_table_flow.md`](../architecture/nextads_job_table_flow.md) | Start here for the operational assignment/delivery route and the in-flight Feature Store/model jobs, including what they consume and produce. |
| [`../architecture/feature_store_flow.md`](../architecture/feature_store_flow.md) | Detailed task order and parallel branches inside the shared DEV Feature Store job. |

## Executable Feature Store Contracts

| Artifact | Responsibility |
| --- | --- |
| `configs/features/nextads_feature_store.yaml` | Logical definitions, states, keys, freshness, consumers and environment bindings. |
| `sql/features/nextads/` | Physical Feature Store and compatibility-view schemas. |
| `jobs/table_operations/create_feature_store_tables.py` | Safe Feature Engineering table creation. |
| `jobs/features/nextads/` | Builders, preflight, quality checks and the read-only plan. |
| `pipelines/databricks/jobs/mktg_next_uk_nextads_feature_store.yml` | Personal, Integration and shared DEV Feature Store graph. |
| `pipelines/databricks/jobs/mktg_next_uk_nextads_shopping_bag_feature_preparation.yml` | Manual account, advert and observed-label inputs for the Shopping Bag model example. |
| `pipelines/databricks/jobs/mktg_next_uk_nextads_model_development.yml` | Manual declared model training, DEV MLflow registration and retry. |
| `pipelines/databricks/jobs/mktg_next_uk_nextads_shopping_bag_ongoing_evaluation.yml` | Manual isolated Shopping Bag candidate scoring. |
| `configs/models/nextads_models.yaml` | Model problems, lookups, runtimes and plug-ins. |

## READY Snapshot Publication Contract

Every accepted build follows the same boundary:

1. Record a `BUILDING` attempt.
2. Read and record exact source Delta versions.
3. Write one feature partition or whole latest table atomically.
4. Validate schema, keys, rows, freshness, drift and value checksum.
5. Record the exact output Delta version and write receipt.
6. Mark the feature group and overall snapshot `READY` only after all required
   outputs pass.

A failed first attempt has no READY snapshot. A failed retry leaves the earlier
READY snapshot selectable.

Model code uses `read_ready_feature`, which opens the exact backing-table Delta
version recorded in the snapshot. A direct physical-table read does not provide
the same reproducibility guarantee.

## Repository Contract Plan Commands

All environments:

```powershell
.\.venv\Scripts\python.exe jobs\features\nextads\plan_offline_feature_store.py `
  --environment ALL `
  --format text
```

DEV only:

```powershell
.\.venv\Scripts\python.exe jobs\features\nextads\plan_offline_feature_store.py `
  --environment DEV `
  --format text
```

| Planner term | Meaning |
| --- | --- |
| `REPO_DECLARED` | The job target is present in repository configuration. |
| `PLANNED` | A physical binding exists but its job target has not been added. |
| `CONTRACT_READY` | A compatibility view has an implemented source contract. |
| `BLOCKED` | Named contracts are still missing. |
| `RELEASE_ID_REQUIRED` | The PREPROD location needs an exact release ID. |

Planner state is not live-run evidence.

## Registry-Driven Runtime Quality Audit

The final quality task derives its scope from the same registry and reports one
result for every implemented physical contract in scope.

| Audit field | Meaning |
| --- | --- |
| `CURRENT_IMPLEMENTED_PASS` | Every non-skipped current contract and implemented compatibility view passed its declared checks. |
| `current_implemented_complete` | No current implemented contract was skipped or failed. |
| `dev_complete` | Every intended contract and both views meet the DEV gate. |
| `BLOCKED` | A view or table still depends on missing contracts. |

On-demand contracts are reported as skipped on the normal daily route unless
an explicit historical date is supplied. This keeps the audit honest rather
than claiming that an unrequested training build ran.

The quality table's own persisted row uses `MANIFEST_ONLY`: a row cannot contain
the Delta version created by writing itself. The deterministic manifest emitted
after the write contains the resulting quality-table version.

## Personal DEV Shopping Bag Evidence

The Shopping Bag walkthrough proves a complete model-author slice without
starting the full Feature Store job.

| Evidence | Result |
| --- | --- |
| [Feature preparation 416308956466968](https://adb-6694370232251359.19.azuredatabricks.net/?o=6694370232251359#job/703044906198087/run/416308956466968) | READY account and advert features for 2026-08-04 and observed labels for 2026-08-05. The same-session label has 23,324 exposures and 234 positives. |
| [Feature preparation 276597138782516](https://adb-6694370232251359.19.azuredatabricks.net/?o=6694370232251359#job/703044906198087/run/276597138782516) | READY account and advert features for 2026-08-05 and observed labels for 2026-08-06. The same-session label has 26,147 exposures and 263 positives. |
| [DBR 15.4 smoke 789568309210262](https://adb-6694370232251359.19.azuredatabricks.net/?o=6694370232251359#job/571453160608086/run/789568309210262) | Pinned libraries imported, a future feature was rejected and the guard performed no writes. |
| [Model build 362286891923190](https://adb-6694370232251359.19.azuredatabricks.net/?o=6694370232251359#job/383960843241650/run/362286891923190) | READY 49,471-row receipt; logistic regression selected; exact DEV model version 3 registered; promotion disabled. |
| [Identical retry 1082000054818636](https://adb-6694370232251359.19.azuredatabricks.net/?o=6694370232251359#job/383960843241650/run/1082000054818636) | Reused the same receipt, build, MLflow run, version and digest; no version 4 was created. |
| [Isolated EVALUATE 1040614784030488](https://adb-6694370232251359.19.azuredatabricks.net/?o=6694370232251359#job/763237716435981/run/1040614784030488) | READY bounded scoring build for 10,000 accounts and both SB1/SB2; 398,964 score rows; no serving output changed. |

This is personal DEV proof. It does not claim PREPROD, PROD, realtime or online
Feature Store activation.

## Feature Table Design Ownership

The complete physical catalogue, compatibility views, grain, keys, dates,
cadence, ownership and training-safety boundary are maintained once in
[`feature_store_table_design.md`](feature_store_table_design.md). The executable
form is [`nextads_feature_store.yaml`](../../configs/features/nextads_feature_store.yaml).
The counts in Current Repository Status are derived from that registry.

## Runtime Dependencies And Activation Boundaries

- DEV Feature Engineering and Unity Catalog permissions must allow the run-as
  identity to create or update the declared personal/shared targets.
- Source assignment, control-sheet, item, session, page and action contracts
  must remain available for the requested dates.
- App Shopping Bag telemetry needs separate route and CMS validation before it
  joins the worked web V1 label.
- The Analytics pCTR adopter remains separate from the Shopping Bag model.
- A longer model-research period is required before any serving decision.
- PREPROD, PROD, realtime and online publication require separate evidence and
  activation routes.

## Feature Store Delivery Acceptance Criteria

| Acceptance criterion | Evidence |
| --- | --- |
| Feature catalogue created or updated | [`feature_store_table_design.md`](feature_store_table_design.md) and the registry plan. |
| Ownership and refresh approach recorded | [`feature_store_table_design.md`](feature_store_table_design.md). |
| Remaining migrations prioritised | [`migration_backlog.md`](migration_backlog.md). |
| A DS can build a model from accepted features | [`building_a_challenger_model.md`](building_a_challenger_model.md) and linked DEV runs. |
| Customer output remains unchanged | Isolated `EVALUATE` boundary and manual jobs. |

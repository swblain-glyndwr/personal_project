# Building A Challenger Model

This is the route a NextAds model author will follow. It starts with the decision the model should improve and ends with a challenger that can be evaluated without changing the current customer route.

The existing Analytics pCTR is the first model being brought onto this route and will remain its own `EVALUATE` provider. The Shopping Bag pCTR example below is a separate model that will be rebuilt afterwards to demonstrate how a new author starts from the reusable Feature Store contracts.

## 1. Write Down The Decision

Complete this short brief before choosing an algorithm or opening a training notebook.

| Question | What you record | Example |
| --- | --- | --- |
| What decision will improve? | The choice made by NextAds. | Rank eligible Shopping Bag adverts for an account and session. |
| What is one prediction about? | The entity keys required at scoring time. | Account, advert and session. |
| When is the prediction made? | The observation timestamp used for every feature lookup. | Session start time. |
| What outcome represents success? | The label and its measurement window. | Advert click within the session. |
| What must not enter training? | Information created after the observation timestamp. | Later clicks, purchases or refreshed affinities. |
| What is the baseline? | The current rule or model the challenger must beat. | Current Shopping Bag pCTR ordering. |
| How will it be judged? | Offline metrics and the later evaluation policy. | Calibration, log loss, ranking lift and coverage. |

The brief becomes part of `ModelDefinition`; it is not left only in a notebook or presentation.

## 2. Research The Problem

| Action | Evidence to keep |
| --- | --- |
| Review current ranking behaviour and failure cases. | Queries, charts and named examples. |
| Check label volume, balance, delay and missingness. | Date range, row counts and label statistics. |
| Compare simple and complex model families. | A short decision record explaining the chosen baseline and candidate. |
| Define the offline acceptance threshold before training. | Metric names, minimum improvements and unacceptable regressions. |
| Record operational limits. | Scoring grain, expected volume, runtime profile and latency or batch deadline. |

Research may use notebooks, but the chosen inputs and training route must move into repository contracts before the result can be promoted.

## 3. Choose Named Feature Store Inputs

Run the read-only plan and inspect the contracts before building a training set.

```powershell
.\.venv\Scripts\python.exe jobs\features\nextads\plan_offline_feature_store.py `
  --environment DEV --format text
```

For each selected feature, record its logical name, selected columns, entity-key mapping and observation timestamp. Do not join a mutable physical table path directly in the training notebook.

| Check | Required result |
| --- | --- |
| Contract state | `ACTIVE` or an approved `COMPATIBILITY` contract. |
| Training safety | `training_safe=true`. |
| Keys | Match the model observation grain. |
| Timestamp | Supports a point-in-time lookup when the feature changes over time. |
| Snapshot | Resolves through one complete `READY` Feature Snapshot. |
| Quality | Schema, null-key, duplicate-key, freshness and row-drift checks passed. |

In a DBR 15.4 model-development notebook, read the logical feature through its READY receipt:

```python
from next_ads.features import read_ready_feature

pctr_features, feature_receipt = read_ready_feature(
    spark,
    "next_uk_nextads_fs_pctr_model_input",
    catalog="marketingdata_dev",
    schema="nextads_feature_store",
    reference_date="2026-08-01",
)
```

Keep `feature_receipt.feature_snapshot_id`, `feature_receipt.delta_version` and `feature_receipt.value_checksum` with the research run. Do not replace this call with `spark.table(...)`: that would read a moving physical table and would not prove which accepted feature build was used.

## 4. Declare The Model

The model-development-kit PR will make this a repository object rather than bespoke orchestration.

| Contract | What the author supplies |
| --- | --- |
| `ModelDefinition` | Model name, problem statement, label, runtime, feature lookups, trainer, score provider and candidate adapter. |
| `FeatureLookupSpec` | Logical feature, selected columns, key mapping, observation timestamp, renames and defaults. |
| `Trainer` | The approved training implementation and parameters. |
| `ScoreProvider` | Conversion from model output to `account_entity_scores/v1`. |
| `CandidateAdapter` | How canonical scores are applied to eligible NextAds candidates. |

Adding a model must not require an edit to the main algorithm or its orchestration YAML.

## 5. Build A Time-Correct Training Set

The training command will resolve the last complete READY snapshot, apply the declared point-in-time lookups and write a `TrainingSetReceipt` before training begins.

| Receipt evidence | Why it matters |
| --- | --- |
| Feature Snapshot ID | Identifies the complete feature set used. |
| Exact Delta versions | Makes the joined data reproducible after tables change. |
| Observation and label windows | Proves the intended time boundary. |
| Leakage result | Blocks future feature values. |
| Schema and data checksum | Detects an accidental input change on retry. |
| Repository SHA | Identifies the code and definition used. |

A future-dated lookup must fail without creating a READY receipt or model build.

## 6. Train And Compare Models In DEV

Training will run only on an approved runtime profile: DBR 15.4 Spark/CPU or the existing Theme Affinity DBR 18.1 GPU profile. Start with a simple baseline, compare the researched candidates and keep the test set fixed.

The resulting `ModelBuild` records the training receipt, MLflow run, registered model name and version, artifact URI and digest, parameters, metrics, runtime and final status. Retrying the same definition and receipt reuses the existing build instead of registering a duplicate version.

## 7. Promote The Exact Artifact

Promotion copies the registered artifact through DEV, Integration, PREPROD and PROD without retraining. Each step compares the artifact digest with the original `ModelBuild`.

Promotion is evidence that the same model is available in the next environment. It does not make that model the production champion.

## 8. Produce Challenger Scores

The declared `ScoreProvider` converts the exact promoted model output to `account_entity_scores/v1`. The declared `CandidateAdapter` applies those scores to eligible candidates while preserving provider build IDs, portfolios, deterministic ranking, retries and the public v1/v2 outputs.

The new provider is registered as `EVALUATE`. Running the same candidate input twice must return the same ordered output.

## 9. Review The Demo Evidence

The author should be able to show the following links in order.

1. Problem and model-choice record.
2. DEV Feature Snapshot and quality evidence.
3. Point-in-time training-set receipt.
4. MLflow experiment comparison.
5. Exact registered model version and artifact digest.
6. Promotion record showing no retraining.
7. Canonical challenger score output.
8. Deterministic candidate result with the provider still in `EVALUATE`.

## Current Delivery Boundary

| Capability | Current position |
| --- | --- |
| Named offline features and contracts | All 20 physical contracts have builders. The Analytics pCTR model input is populated from the exact versioned Analytics feature output. |
| Exact build identity and Delta write receipts | Implemented for the Analytics pCTR feature group; the remaining feature groups still need to publish through the same contract. |
| READY Feature Snapshot resolution | The Analytics pCTR builder records exact READY Delta bindings after its source, table and retry checks pass. `read_ready_feature` opens that exact version and fails rather than falling back to a moving latest table. Linked DEV proof is still required. |
| Declarative training set and receipt | Model-development-kit PR. |
| Exact MLflow promotion without retraining | Model-development-kit PR, built on the existing lifecycle. |
| Generic challenger provider and candidate adapter | Model-development-kit PR. |
| Champion activation | Separate policy-only change after evaluation. |

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

Add one entry to `configs/models/nextads_models.yaml`. This is the part the model author owns. It explains the problem, names the Feature Store inputs and selects the training, scoring and candidate plug-ins.

| Contract | What the author supplies |
| --- | --- |
| `ModelDefinition` | Model name, problem statement, label, runtime, feature lookups, trainer, score provider and candidate adapter. |
| `FeatureLookupSpec` | Logical feature, selected columns, key mapping, observation timestamp, renames and defaults. |
| `Trainer` | The approved training implementation and parameters. |
| `ScoreProvider` | Conversion from model output to `account_entity_scores/v1`. |
| `CandidateAdapter` | How canonical scores are applied to eligible NextAds candidates. |

The Analytics pCTR and Shopping Bag pCTR definitions are separate entries. Adding either model does not require a model-specific edit to the main algorithm or its orchestration YAML.

## 5. Build A Time-Correct Training Set

Deploy the model-development branch to personal DEV, then open the manual `mktg_next_uk_nextads_model_development` job. Supply the accepted Feature Store dates; do not leave either date as `REQUIRED`.

| Job parameter | What the author enters |
| --- | --- |
| `model_name` | The name from `nextads_models.yaml`, for example `shopping_bag_pctr`. |
| `feature_reference_dates` | One or more comma-separated dates that have READY feature snapshots. |
| `label_end` | The last date labels are allowed to use. It cannot precede the observations or permit a future feature lookup. |
| `registered_model_name` | The personal DEV Unity Catalog model name. |
| `experiment_path` | The DEV MLflow experiment used for the research comparison. |
| `evaluation_scores_table` | A personal DEV table; never the live assignment output. |
| `promotion_model_name` | Leave blank during research. Supply an Integration model name only when the exact DEV build has passed review. |

The job resolves the requested READY snapshots, applies the declared point-in-time lookups and writes a `TrainingSetReceipt` before training begins.

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

First run `mktg_next_uk_nextads_model_development_runtime_smoke`. It proves that the job is using DBR 15.4 with the pinned Feature Engineering, Dynaconf and MLflow versions. It also proves that the training guard rejects a future-dated feature binding without writing a receipt or model.

Run the manual job on the approved runtime in the definition: DBR 15.4 Spark/CPU or the existing Theme Affinity DBR 18.1 GPU profile. The Shopping Bag example compares a Spark logistic-regression baseline with a Spark gradient-boosted-tree candidate, using the same deterministic validation split and selecting by PR-AUC.

The resulting `ModelBuild` records the training receipt, MLflow run, registered model name and version, artifact URI and digest, parameters, metrics, runtime and final status. Retrying the same definition and receipt reuses the existing build instead of registering a duplicate version.

## 7. Promote The Exact Artifact

Rerun the same manual job with the same definition and receipt, then supply `promotion_model_name` and `promotion_alias=integration_candidate`. The job reuses the existing `ModelBuild`, copies that exact registered artifact and compares its digest with the DEV artifact. It does not retrain the model.

Promotion is evidence that the same model is available in the next environment. It does not make that model the production champion.

## 8. Produce Challenger Scores

The same run uses the declared `ScoreProvider` to write `account_entity_scores/v1` into the personal DEV provider-signals table. It records a `READY_FOR_NEXTADS` provider build only after that exact Delta write is complete. The declared `CandidateAdapter` applies those scores only to eligible adverts and ranks ties by advert ID. The job runs the adapter twice and fails if the ordered result changes.

The model is registered as an available account-advert provider, but it is not added to a portfolio. A later policy-only change can add that exact build as an `EVALUATE` challenger. Training a model therefore cannot change a customer assignment, public payload or champion policy by itself.

### Bringing The Existing Analytics pCTR Onto The Route

Analytics pCTR already has two trained models and a prediction job, so it follows an adoption route instead of pretending it was trained by the new Shopping Bag example.

1. Run the existing Analytics prediction route for the chosen date.
2. Record the producing Databricks run, exact prediction-table Delta version and the numeric MLflow versions for both component models.
3. Open `mktg_next_uk_nextads_analytics_pctr_adoption` in personal DEV.
4. Enter the exact source table, Delta version, date and producing run ID.
5. Run the job. It checks that both registered versions still resolve to the recorded MLflow runs.
6. Review the external score receipt, canonical `analytics_pctr` signals and `READY_FOR_NEXTADS` provider-build record in the personal DEV tables.

The adopter is manual, DEV-only and `EVALUATE`. It does not retrain Analytics pCTR and it does not change the main algorithm.

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
| Exact build identity and Delta write receipts | All reusable feature builders publish complete groups through exact source and output versions. The quality event remains an append-only audit output. |
| READY Feature Snapshot resolution | Consumers use the exact Delta version in a READY snapshot and fail rather than falling back to a moving latest table. The Analytics pCTR complete-run, retry and failure-retention proof is in progress. |
| Declarative training set and receipt | Implemented for repository model definitions, exact feature bindings and point-in-time joins. DEV job evidence is still required. |
| Exact MLflow promotion without retraining | Implemented with source and copied-artifact digest comparison. DEV job evidence is still required. |
| Generic challenger provider and candidate adapter | Implemented for account-advert scores. Shopping Bag trains through the generic route; Analytics uses its separate exact-output adopter. Both publish selectable provider builds but remain outside the serving portfolios. |
| Champion activation | Separate policy-only change after evaluation. |

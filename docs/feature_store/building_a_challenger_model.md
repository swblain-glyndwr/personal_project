# Build A Model From The Feature Store

This is the individual data-scientist route from a modelling question to an
isolated Shopping Bag pCTR evaluation in DEV.

The worked route ends with:

- named, versioned Feature Store inputs;
- observed impression and click labels;
- a point-in-time training receipt;
- model comparison and registration in DEV MLflow;
- an identical retry that reuses the registered version; and
- bounded `EVALUATE` scores for Shopping Bag locations `SB1` and `SB2`.

It does not promote a model to another environment, add a provider to a serving
portfolio, run the main NextAds job, change a customer assignment or change a
payload.

## The Route At A Glance

| Step | What the data scientist does | Repository evidence |
| ---: | --- | --- |
| 1 | Frame the decision and research question. | `ModelDefinition` problem and scope. |
| 2 | Check that the label represents the intended outcome. | Observed-impression label funnel and quality checks. |
| 3 | Choose logical Feature Store inputs. | Registry plan and model lookups. |
| 4 | Publish at least two mature observation dates. | READY snapshots with exact Delta versions. |
| 5 | Review or add a model declaration. | Runtime, features, trainer and plug-ins in YAML. |
| 6 | Build the point-in-time training set. | READY `TrainingSetReceipt`. |
| 7 | Compare models in DEV MLflow. | Metrics, exact model version and digest. |
| 8 | Retry the same build. | Same receipt, build and version; no duplicate version. |
| 9 | Score current Shopping Bag candidates. | Isolated READY `EVALUATE` scoring build. |

## 1. Frame The Modelling Question

Write down the decision before choosing an estimator or opening a training
notebook.

| Question | Shopping Bag worked example |
| --- | --- |
| What decision are we improving? | Rank adverts already eligible for a web Shopping Bag placement. |
| What is one observation? | One observed account-advert impression. |
| When is the prediction made? | At the advert exposure timestamp. |
| What is the outcome? | A click on that advert after the impression in the same session. |
| What is the first scope? | Web V1, locations `SB1` and `SB2`. |
| What must not enter training? | Features created after the exposure or immature future clicks. |
| What is the operational comparison? | The accepted candidate order and score already produced for the same Shopping Bag population. |
| How will research be judged? | PR-AUC, ROC-AUC, log loss, calibration gap and lift at 5%. |
| How can it affect customers? | It cannot. The first route remains `EVALUATE`. |

The implemented brief is the `shopping_bag_pctr` entry in
[`configs/models/nextads_models.yaml`](../../configs/models/nextads_models.yaml#L81).
The declaration is executable configuration, not a note that can drift away
from the training code.

### Research before training

Keep a short research record covering:

1. The current ranking behaviour and named failure cases.
2. Impression and click volume by date and location.
3. Label balance, delay, missingness and maturity.
4. A simple baseline and at least one candidate model family.
5. Metrics and acceptance thresholds chosen before looking at the final result.
6. Expected scoring grain, volume and runtime limit.

This two-date example proves the route works. It is not enough data to make a
production model-quality decision. A real conclusion needs a longer,
representative and fully mature observation window.

## 2. Prove That The Label Means What You Say It Means

The model does not use the earlier inferred-assignment label. It uses
`next_uk_nextads_fs_shopping_bag_click_labels`, which begins with observed
telemetry.

### Label rule

For each label row, the builder:

1. Starts with `Banner Impression - Next Ads` on the Shopping Bag surface.
2. Resolves the visit to exactly one account.
3. Rejects ambiguous account mappings.
4. Reconstructs the advert assignment for the route and date.
5. Excludes control, suppressed, `NoAd` and unresolved assignments.
6. Matches the observed advert exactly before allowing a campaign match.
7. Rejects ambiguous impression-to-assignment matches.
8. Finds `Banner Click - Next Ads` strictly after the impression.
9. Assigns each click deterministically to one eligible exposure.
10. Emits only labels whose measurement horizon is mature.

The model declaration filters the published labels to:

```text
route = v1
platform = WEB
label_horizon_days = 0
label_is_mature = true
impression_count = 1
```

That makes the demonstrated target an observed same-session click after an
observed web impression.

### Code to show

| Behaviour | Code |
| --- | --- |
| Event route and advert parsing | [`classify_shopping_bag_event_route` and `normalize_shopping_bag_advert_id`](../../src/next_ads/features/nextads_core.py#L492) |
| Maturity and assignment eligibility | [`shopping_bag_label_is_mature` and assignment exclusions](../../src/next_ads/features/nextads_core.py#L526) |
| Raw impression and click events | [`build_raw_shopping_bag_events_df`](../../src/next_ads/features/nextads_core.py#L842) |
| V1 assignment reconstruction | [`_shopping_bag_v1_assignments`](../../src/next_ads/features/nextads_core.py#L883) |
| Exposure and click attribution | [`build_observed_shopping_bag_click_labels_df`](../../src/next_ads/features/nextads_core.py#L1052) |
| Bounded funnel and quality evidence | [`collect_shopping_bag_label_evidence`](../../src/next_ads/features/shopping_bag_label_evidence.py#L289) |
| Watermark guard and READY publication | [`build_shopping_bag_click_labels.py`](../../jobs/features/nextads/build_shopping_bag_click_labels.py#L94) |

### Read one accepted label snapshot

Run this on DBR 15.4 after the feature-preparation job has published the date:

```python
from pyspark.sql import functions as F

from next_ads.features import read_ready_feature

labels, binding = read_ready_feature(
    spark,
    "next_uk_nextads_fs_shopping_bag_click_labels",
    catalog="marketingdata_dev",
    schema="Stephen_Blain",
    reference_date="2026-08-05",
)

same_session = labels.where(
    """
    route = 'v1'
    AND platform = 'WEB'
    AND label_horizon_days = 0
    AND label_is_mature = true
    AND impression_count = 1
    """
)

same_session.agg(
    F.count("*").alias("exposures"),
    F.sum("clicked").alias("positives"),
    F.avg("clicked").alias("observed_click_rate"),
).show(truncate=False)

print(binding)
```

[`read_ready_feature`](../../src/next_ads/features/snapshot_reader.py#L74)
opens the exact Delta version recorded in the last matching READY snapshot. It
also verifies the accepted schema and row count. Do not replace it with a plain
`spark.table(...)` call, which would read a moving table.

### Proven label numbers

| Observation date | Accepted exposures | Same-session positives | Observed rate | Quality result |
| --- | ---: | ---: | ---: | --- |
| 2026-08-05 | 23,324 | 234 | 1.0033% | Zero duplicate keys, immature rows, invalid treatments or clicks before exposure. |
| 2026-08-06 | 26,147 | 263 | 1.0059% | Zero duplicate keys, immature rows, invalid treatments or clicks before exposure. |

The reporting comparison is a directional sense check only. The reporting
tables use soft page and URL measures with a different denominator. They are
not the source of truth for these training labels and are not expected to have
equal counts.

## 3. Find The Named Feature Store Inputs

Run the repository plan:

```powershell
.\.venv\Scripts\python.exe jobs\features\nextads\plan_offline_feature_store.py `
  --environment DEV `
  --format text
```

Expected repository summary:

```text
ACTIVE=18 COMPATIBILITY=4 SCAFFOLD=0
```

The plan proves that contracts, builders and locations are declared. It does
not prove that your requested dates have READY data.

### Inputs selected by the worked model

| Logical contract | What it contributes | Time rule |
| --- | --- | --- |
| `next_uk_nextads_fs_shopping_bag_click_labels` | Exposure, account, advert, location, observation time and `clicked`. | The label horizon must be mature. |
| `next_uk_nextads_fs_shopping_bag_account_activity_90d` | Browsing, Shopping Bag and add-to-bag history plus recency. | At least one day behind the exposure. |
| `next_uk_nextads_fs_advert_core_daily` | Campaign, theme, category, brand and template. | Latest READY version no later than the exposure. |

The physical location is deliberately absent from the model declaration.
Environment binding resolves the logical name to the personal DEV table.

The registry entries are in
[`configs/features/nextads_feature_store.yaml`](../../configs/features/nextads_feature_store.yaml).
The selected columns, defaults and one-day account-activity lag are in
[`configs/models/nextads_models.yaml`](../../configs/models/nextads_models.yaml#L97).

For a different model, choose only the logical contracts and columns that are
available at its prediction time. Do not start with a prejoined model-specific
table path.

## 4. Publish Two Mature Training Dates

Open the manual DEV job:

```text
mktg_next_uk_nextads_shopping_bag_feature_preparation
```

Run it once per observation date.

| Parameter | First run | Second run |
| --- | --- | --- |
| `reference_date` | `2026-08-05` | `2026-08-06` |
| `feature_reference_date` | `2026-08-04` | `2026-08-05` |
| `label_end` | `2026-08-13` | `2026-08-14` |
| `source_catalog` | `marketingdata_prod` | `marketingdata_prod` |
| `source_schema` | `warehouse` | `warehouse` |

The production warehouse is read as a source. Every output is written to the
personal DEV schema.

The job performs only the worked example's preparation:

- create the required tables if they are absent;
- publish account activity for `feature_reference_date`;
- publish advert features for `feature_reference_date`; and
- publish observed labels for `reference_date`.

It does not recreate tables, run the complete Feature Store or start the main
NextAds job. The task graph is declared in
[`mktg_next_uk_nextads_shopping_bag_feature_preparation.yml`](../../pipelines/databricks/jobs/mktg_next_uk_nextads_shopping_bag_feature_preparation.yml).

### READY data used by the proof

| Feature | Date | Delta version | Rows | Value checksum |
| --- | --- | ---: | ---: | --- |
| Shopping Bag account activity | 2026-08-04 | 3 | 3,670,790 | `3f0e9bfce793ef8a62570c44733fb97ba02b4c777c76ed7183e0c12296d3b942` |
| Advert core | 2026-08-04 | 20 | 9,454 | `53a22179ae3f7f427885f8ab511d1e86b0bbbc4222ec1a312d86fe182f7a77c6` |
| Shopping Bag click labels | 2026-08-05 | 4 | 69,972 | `e082da4d99ab2df1207e62e1e81d67beb7007d47c2ca14cc2e7d9bbebcc2af9b` |
| Shopping Bag account activity | 2026-08-05 | 5 | 3,672,314 | `4a078cdbf674d478c328acb508c5642928670fb5b7ef75fac145ba62b6b97506` |
| Advert core | 2026-08-05 | 21 | 9,286 | `0dd142f80ee1f3572ac3911e360de521f0bf4c753c3446bd88305976d222961b` |
| Shopping Bag click labels | 2026-08-06 | 5 | 78,441 | `8bb05f1f6503a69326fb7baedf7f8a35b597e2fba93feb888a125f10a14297c6` |

Every listed output passed schema, non-null-key, duplicate-key, freshness and
row-drift validation.

Two observation dates are mandatory because
[`temporal_validation_cutoff`](../../src/next_ads/model_development/spark_training.py#L170)
trains on earlier complete dates and validates on a later complete date.

## 5. Review Or Add The Model Declaration

For this demonstration, review the existing `shopping_bag_pctr` definition.

For a new model, add a separate definition rather than adding model-specific
conditions to the main algorithm.

| Contract | What the data scientist declares |
| --- | --- |
| `ModelDefinition` | Problem, entity, observation time, label, metrics, runtime, lookups and plug-ins. |
| `FeatureLookupSpec` | Logical feature, columns, key mapping, availability lag, renames and defaults. |
| `Trainer` | Approved training implementation. |
| `ScoreProvider` | Conversion to `account_entity_scores/v1`. |
| `CandidateAdapter` | How scores rank eligible candidates. |

The generic contracts are defined in
[`src/next_ads/model_development/contracts.py`](../../src/next_ads/model_development/contracts.py):

- `FeatureLookupSpec` at line 90;
- `ModelDefinition` at line 241;
- `TrainingSetReceipt` at line 452; and
- `ModelBuild` at line 526.

The worked trainer compares two repository-approved Spark pipelines:

- logistic regression; and
- gradient-boosted trees.

Both receive the same features and temporal holdout. The trainer selects the
higher PR-AUC result. The current definition does not expose arbitrary
estimators or hyperparameters in YAML; a different family should be added as a
separate trainer plug-in and tested independently.

All new declarations remain `EVALUATE`. Training cannot make a model a
champion.

## 6. Prove The Runtime And Leakage Guard

Run:

```text
mktg_next_uk_nextads_model_development_runtime_smoke
```

The smoke run proves:

- DBR `15.4.x-scala2.12`;
- `databricks-feature-engineering==0.12.1`;
- `mlflow==3.11.1`;
- `dynaconf==3.2.12`;
- a future-dated feature binding is rejected; and
- the rejected check performs no writes.

The worked training run also executes successfully on DBR 15.4, so the smoke is
not the only runtime evidence.

## 7. Build The Point-In-Time Training Receipt

Open the manual DEV job:

```text
mktg_next_uk_nextads_model_development
```

Use these parameters:

| Parameter | Value |
| --- | --- |
| `model_name` | `shopping_bag_pctr` |
| `feature_catalog` | `marketingdata_dev` |
| `feature_schema` | `Stephen_Blain` |
| `model_catalog` | `marketingdata_dev` |
| `model_schema` | `stephen_blain` |
| `observation_reference_dates` | `2026-08-05,2026-08-06` |
| `feature_reference_dates` | `2026-08-04,2026-08-05` |
| `label_end` | `2026-08-14` |
| `registered_model_name` | `marketingdata_dev.stephen_blain.nextads_shopping_bag_pctr` |
| `experiment_path` | Keep the personal DEV default. |
| `provider_signals_table` | Keep the personal DEV default. |
| `provider_builds_table` | Keep the personal DEV default. |
| `promotion_model_name` | Blank. |
| `promotion_mode` | `NONE` |

`promotion_mode=NONE` is the control. A blank destination name is not a
substitute for it.

### What the job does

1. Resolves the exact READY label and feature snapshots.
2. Enforces the observation timestamp and availability lag for each lookup.
3. Rejects a feature binding that is later than the observation.
4. Writes a reproducible `TrainingSetReceipt`.
5. Splits complete earlier and later observation dates.
6. Trains and compares the approved candidates.
7. Logs the receipt, parameters and metrics to DEV MLflow.
8. Registers the selected exact DEV model version.
9. Writes historical holdout scores and a READY provider build.
10. Emits one `MODEL_DEVELOPMENT_EVIDENCE=` record.

Point-in-time lookup and receipt construction are in
[`training_sets.py`](../../src/next_ads/model_development/training_sets.py).
The job entry point and evidence record are in
[`run_declared_model.py`](../../jobs/model/development/run_declared_model.py#L124).

### Receipt created by the proof

| Field | Value |
| --- | --- |
| Training receipt | `ba87655b7329877029cf7b373ce6f0e78d4d64db8deabcb8acdf49141561ef8c` |
| Status | `READY` |
| Repository SHA | `c6aae1c04aa6ba9dd0379894d362efcf343ad7ae` |
| Observation dates | 2026-08-05 to 2026-08-06 |
| Training rows | 49,471 |
| Positives | 497 |
| Fit rows and positives | 23,324 and 234 |
| Validation rows and positives | 26,147 and 263 |
| Feature bindings | Six exact label, account-activity and advert-core bindings. |
| Leakage result | `PASS` |

The receipt is the reproducibility record. The notebook or job run by itself is
not enough because it does not pin all six input versions.

## 8. Compare Models In DEV MLflow

Open the MLflow run recorded by the job and compare the two candidates on the
later date.

| Metric | Logistic regression | Gradient-boosted trees |
| --- | ---: | ---: |
| PR-AUC | 0.013193 | 0.012859 |
| ROC-AUC | 0.581728 | 0.548850 |
| Log loss | 0.055922 | 0.071783 |
| Lift at 5% | 1.900196 | 1.824188 |
| Predicted click rate | 1.0129% | 3.8428% |
| Observed click rate | 1.0059% | 1.0059% |
| Calibration gap | 0.0070 percentage points | 2.8370 percentage points |

Logistic regression was selected by PR-AUC and was also better calibrated on
this holdout.

### Exact registered result

| Field | Value |
| --- | --- |
| Model build | `4f9ab805e48a12e5dd27e342c90d031f130bf19355ced644fef38c9bc6bd5073` |
| Status | `READY` |
| MLflow run | `21d2ee0f05e64e9cbac4dd292fa45a16` |
| Registered model | `marketingdata_dev.stephen_blain.nextads_shopping_bag_pctr` |
| Exact model URI | `models:/marketingdata_dev.stephen_blain.nextads_shopping_bag_pctr/3` |
| Artifact digest | `3691fbba9abe465961284b4542273ffd22cabf3d086b2e9bfb7ffcb445ceb7d2` |
| Runtime profile | `dbr_15_4_spark_cpu` |
| Promotion | None |

Training and registration are implemented in
[`SparkBinaryClassifierTrainer`](../../src/next_ads/model_development/spark_training.py#L248).
Build persistence and reuse are implemented in
[`train_or_reuse_model`](../../src/next_ads/model_development/runtime.py#L63).

These numbers show that the framework works. They do not establish that this
two-date research model should serve customers.

## 9. Retry And Prove Reuse

Run the model-development job again without changing any parameter, definition
or deployed SHA.

Required evidence:

| Check | Proven result |
| --- | --- |
| `model_reused` | `true` |
| Training receipt | Same `ba87655b...` receipt. |
| Model build | Same `4f9ab805...` build. |
| MLflow run | Same `21d2ee0f...` run. |
| Numeric model URI | Same version 3 URI. |
| Artifact digest | Same `3691fbba...` digest. |
| Duplicate registration | No version 4 created. |
| Promotion | None. |

A different repository SHA intentionally creates a different receipt and model
build. That is a new build, not an identical retry.

## 10. Score Current Shopping Bag Candidates In `EVALUATE`

The training job's holdout scores explain offline validation. To show how the
model would score an accepted candidate population, run the separate manual job:

```text
mktg_next_uk_nextads_shopping_bag_ongoing_evaluation
```

Use the exact READY model build and candidate attempt:

| Parameter | Value |
| --- | --- |
| `model_build_id` | `4f9ab805e48a12e5dd27e342c90d031f130bf19355ced644fef38c9bc6bd5073` |
| `run_date` | `2026-08-07` |
| `feature_catalog` | `marketingdata_dev` |
| `feature_schema` | `Stephen_Blain` |
| `feature_reference_dates` | `2026-08-05` |
| `model_catalog` | `marketingdata_dev` |
| `model_schema` | `stephen_blain` |
| `v1_candidate_build_attempt_id` | `candidates_v1_20260807_f5b555dc7956852fdba5:attempt:1:249701592970222` |
| `candidate_serving_slot` | `best` |
| `account_limit` | `10000` |

The job:

1. Validates the READY `ModelBuild` and exact numeric model version.
2. Recalculates and checks the artifact digest.
3. Validates the exact accepted candidate attempt.
4. Requires both `SB1` and `SB2` to contain candidates.
5. Selects the same deterministic cohort of at most 10,000 accounts.
6. Keeps all candidate adverts for those accounts.
7. Reads label-free features from exact READY snapshots.
8. Scores the candidates through `account_entity_scores/v1`.
9. Writes scores before marking the scoring build READY.

The label-free lookup is in
[`scoring_sets.py`](../../src/next_ads/model_development/scoring_sets.py#L153).
The bounded candidate route is in
[`ongoing_evaluation.py`](../../src/next_ads/model_development/ongoing_evaluation.py#L338).
The job validation and evidence record are in
[`run_shopping_bag_ongoing_evaluation.py`](../../jobs/model/development/run_shopping_bag_ongoing_evaluation.py#L166).

### Proven evaluation result

| Field | Value |
| --- | --- |
| Scoring build | `a9b1ab5352f3156509fb7394ef4350196af554eabd4321d0264809878ddb5e30` |
| Status | `READY` |
| Input accounts | 10,000 |
| Scope | V1 `SB1` and `SB2`, both non-empty. |
| Model | Exact version 3 and digest above. |
| Account-activity input | 2026-08-05, Delta version 5. |
| Advert-core input | 2026-08-05, Delta version 21. |
| Evaluation score rows | 398,964, Delta version 1. |
| Canonical provider signals | 199,482 `account_entity_scores/v1` rows. |
| Provider value checksum | `da3bc9f565a764e524595319cda0bef8cb5bf06e62fa3b2f34ed594400b40493` |
| Activation mode | `EVALUATE` |

This is a manual one-day evaluation proof. Repeat the isolated job for accepted
daily candidate builds to collect a longer evaluation period. It is not yet a
scheduled challenger in the main algorithm.

The job writes only:

- `next_uk_nextads_model_evaluation_scoring_builds`; and
- `next_uk_nextads_model_evaluation_scores`.

It does not write a serving portfolio, assignment or payload table.

## 11. Evidence To Show In The Walkthrough

Use this order when presenting the route.

| Order | Evidence | Link or value |
| ---: | --- | --- |
| 1 | Problem and scope | [`shopping_bag_pctr` declaration](../../configs/models/nextads_models.yaml#L81) |
| 2 | Feature Store plan | `ACTIVE=18 COMPATIBILITY=4 SCAFFOLD=0` |
| 3 | First Feature Store date | [Run 416308956466968](https://adb-6694370232251359.19.azuredatabricks.net/?o=6694370232251359#job/703044906198087/run/416308956466968) |
| 4 | Second Feature Store date | [Run 276597138782516](https://adb-6694370232251359.19.azuredatabricks.net/?o=6694370232251359#job/703044906198087/run/276597138782516) |
| 5 | DBR 15.4 and future-feature guard | [Run 789568309210262](https://adb-6694370232251359.19.azuredatabricks.net/?o=6694370232251359#job/571453160608086/run/789568309210262) |
| 6 | Exact branch deployment | [Pipeline 2078000](https://dev.azure.com/Next-Technology/DirectoryMarketing.Personalisation/_build/results?buildId=2078000) |
| 7 | READY training receipt and model | [Run 362286891923190](https://adb-6694370232251359.19.azuredatabricks.net/?o=6694370232251359#job/383960843241650/run/362286891923190) |
| 8 | DEV MLflow comparison | [MLflow run 21d2ee0f05e64e9cbac4dd292fa45a16](https://adb-6694370232251359.19.azuredatabricks.net/ml/experiments/1849707330628115/runs/21d2ee0f05e64e9cbac4dd292fa45a16) |
| 9 | Identical retry and reuse | [Run 1082000054818636](https://adb-6694370232251359.19.azuredatabricks.net/?o=6694370232251359#job/383960843241650/run/1082000054818636) |
| 10 | Isolated current-candidate scores | [Run 1040614784030488](https://adb-6694370232251359.19.azuredatabricks.net/?o=6694370232251359#job/763237716435981/run/1040614784030488) |

End the walkthrough with these statements:

- The model was trained from named, accepted Feature Store snapshots.
- The receipt pins exact Delta versions, values, dates and repository code.
- DEV MLflow records the comparison and exact registered version.
- The same build can be retried without creating another model version.
- The exact model can score a bounded Shopping Bag candidate population.
- The result remains isolated `EVALUATE`; no customer route changed.

## Appendix A. Code Map

| Question from a reviewer | Where to go |
| --- | --- |
| Where is the modelling question declared? | [`configs/models/nextads_models.yaml`](../../configs/models/nextads_models.yaml#L81) |
| Where are logical features registered? | [`configs/features/nextads_feature_store.yaml`](../../configs/features/nextads_feature_store.yaml) |
| Where is the observed label built? | [`build_observed_shopping_bag_click_labels_df`](../../src/next_ads/features/nextads_core.py#L1052) |
| Where are label funnel checks collected? | [`shopping_bag_label_evidence.py`](../../src/next_ads/features/shopping_bag_label_evidence.py#L289) |
| Where is account activity built? | [`shopping_bag_account_activity.py`](../../src/next_ads/features/shopping_bag_account_activity.py) |
| Where are immutable feature snapshots read? | [`snapshot_reader.py`](../../src/next_ads/features/snapshot_reader.py#L74) |
| Where are model contracts defined? | [`contracts.py`](../../src/next_ads/model_development/contracts.py#L90) |
| Where is the point-in-time training set assembled? | [`training_sets.py`](../../src/next_ads/model_development/training_sets.py) |
| Where are LR and GBT compared? | [`spark_training.py`](../../src/next_ads/model_development/spark_training.py#L248) |
| Where is retry reuse decided? | [`runtime.py`](../../src/next_ads/model_development/runtime.py#L63) |
| Where are score and candidate plug-ins selected? | [`plugins.py`](../../src/next_ads/model_development/plugins.py#L316) |
| Where is the manual training job defined? | [`mktg_next_uk_nextads_model_development.yml`](../../pipelines/databricks/jobs/mktg_next_uk_nextads_model_development.yml) |
| Where is isolated ongoing scoring implemented? | [`ongoing_evaluation.py`](../../src/next_ads/model_development/ongoing_evaluation.py#L338) |
| Where is the ongoing job defined? | [`mktg_next_uk_nextads_shopping_bag_ongoing_evaluation.yml`](../../pipelines/databricks/jobs/mktg_next_uk_nextads_shopping_bag_ongoing_evaluation.yml) |

## Appendix B. What Changes For The Next Model

Keep the framework and change only the model-specific declaration and plug-ins:

1. Write a separate `ModelDefinition`.
2. Select logical Feature Store inputs and columns.
3. Define the observation timestamp and label maturity rule.
4. Choose an approved runtime and trainer.
5. Implement a score provider only if the canonical one does not fit.
6. Implement a candidate adapter for that model's entity and scope.
7. Prove the same receipt, MLflow, retry and isolated-evaluation sequence.

Analytics pCTR is a separate adopter of these shared contracts. It is not the
same model as Shopping Bag pCTR and is deliberately outside this worked
walkthrough.

# Model Lifecycle Foundation

This PR establishes the shared model lifecycle shape for Next Ads models. Theme
Affinity is the first implementation, but the reusable code lives under
`src/next_ads/ml/lifecycle` so pCTR, direct-ad challengers and later models can
use the same promotion and monitoring contracts.

The foundation is:

- `ModelLifecycleSpec`: model identity, Unity Catalog registered model name,
  experiment path, training table, feature columns and monitoring defaults.
- Registry helpers: Databricks tracking URI, Unity Catalog registry URI, model
  alias URIs, alias setting and preprod-to-prod model version copy.
- Drift metrics: numeric PSI, categorical total variation, categorical
  Jensen-Shannon divergence, row counts and missing-rate metrics.
- Drift assessment: shared warn/fail thresholds that turn metrics into
  `pass`, `warn` or `fail`, plus retrain and promotion-blocking flags.
- MLflow logging: drift metrics, thresholds and assessment tags are logged to
  native MLflow runs, without `marketingdata_utils`.
- Databricks data quality monitoring: model input/output tables can be profiled
  through Unity Catalog quality monitors, giving native metric tables, drift
  tables and dashboards alongside MLflow promotion evidence.

Operationally, each model should add a small adapter that resolves its
`ModelLifecycleSpec` from repo config, then use shared lifecycle code from
training, promotion and monitoring jobs. Model-specific code should own only
model fitting, scoring and feature contracts.

## After This PR Lands

After this PR is completed, new operational models should treat
`src/next_ads/ml/lifecycle` as the shared contract. A new model should not copy
Theme Affinity lifecycle code, add another MLflow helper module, or introduce a
model-specific promotion framework.

For a new model, add model-specific code in its own domain package, for example:

- `src/next_ads/ranking/direct_ad_challenger/` for a direct advert challenger.
- `src/next_ads/ranking/pctr/` for a pCTR model.
- `jobs/model/<model_name>/` for thin Databricks model entrypoints.
- `jobs/model/lifecycle/` for generic lifecycle movement entrypoints shared by
  all model families.
- `resources/jobs/mktg_next_uk_nextads_<model_name>_model_train.yml`.
- `resources/jobs/mktg_next_uk_nextads_<model_name>_model_monitor.yml`.
- `resources/jobs/mktg_next_uk_nextads_<model_name>_model_promote.yml`.

The shared lifecycle package should stay model-agnostic. It should know how to
configure MLflow, register model versions, set aliases, copy reviewed versions
between environments and log drift evidence. It should not know how Theme
Affinity, pCTR or a direct-ad challenger builds features, fits a model, scores
candidates or writes serving outputs.

Lifecycle movement jobs should call generic scripts and pass model-specific
registered model names, versions, aliases and guardrail prefixes as parameters.
For example, `jobs/model/lifecycle/promote_model.py` can move a fixed model
version from DEV integration to PREPROD or from PREPROD to PROD. The Databricks
job resource may still be model-specific so it can set safe defaults, libraries,
clusters and target scoping for that model.

## Required New Model Shape

Each operational model should add:

- A config section containing model name, registered model name, experiment
  path, training table, train/test split, feature columns, categorical drift
  columns and drift thresholds.
- A small lifecycle adapter that resolves that config into `ModelLifecycleSpec`.
- A train script that calls shared MLflow registry helpers and registers the
  trained model with an environment alias.
- A monitor script that calls `log_table_drift_to_mlflow` and logs drift status,
  retrain recommendation and promotion-blocking evidence.
- A Databricks quality monitor setup job or resource for the model's serving,
  inference or scored feature table when the table is a stable Delta/Unity
  Catalog contract. Prefer an `inference_log` monitor when the table contains
  model id,
  prediction, timestamp and optional label columns; otherwise use a
  `time_series` or `snapshot` monitor on the model feature/output table.
- A promote job that uses the generic lifecycle promotion script with
  model-specific parameters, and does not import `marketingdata_utils`.
- Databricks jobs for train, monitor and promote. These jobs should be
  unscheduled unless a separate operational decision explicitly schedules them.
- Unit tests proving the model resolves its lifecycle spec, uses the shared
  lifecycle package, has correctly scoped DAB resources and avoids old
  one-off artifacts.

## Direct-Ad Challenger Fit

For the direct-ad challenger plan, this means the challenger should be a sibling
of Theme Affinity rather than a modification inside Theme Affinity lifecycle
code. Theme Affinity can remain the champion signal, while the challenger model
uses the shared lifecycle foundation to train, register, monitor and promote its
own model.

The direct-ad challenger should therefore add only the challenger-specific
parts:

- candidate advert feature building;
- account/ad training rows and labels;
- challenger scoring and ranking logic;
- output contract for challenger advert choice;
- a lifecycle adapter that maps its config into `ModelLifecycleSpec`;
- train, monitor and promote jobs that reuse `next_ads.ml.lifecycle`.

This keeps the future pCTR/direct-ad work aligned with the Theme Affinity
operationalisation without making the generic lifecycle package depend on any
one model.

## Theme Affinity Training Backend

Theme Affinity has two DEV training routes in this PR, and both create
challenger model versions rather than replacing the current production model
URI.

Both training routes now start from the same explicit training-frame contract.
The old notebook flow trained from the manually prepared
`marketingdata_prod.ds_sandbox.dev_adrienne_complete_ranked` table, which was
already reduced to roughly 11.2 million rows before pandas/GPU training. The
operational DLT ranked table is a full scoring candidate table and can be
billions of rows, so it must not be handed directly to either trainer.

`src/next_ads/ranking/theme_affinity/training_data.py` therefore builds a
bounded, deterministic training frame before splitting:

- keep rows up to `ranking_model.training_frame.rank_filter_threshold`, while
  retaining positive labels even when their rules rank is outside that cutoff;
- build account/reference-date strata from label availability, repurchase
  stage, `GmaName`, activity buckets and retrieval-method buckets;
- use Spark `sampleBy()` with configured fractions and a fixed seed so the
  sample is representative of normal customer/theme behaviour across the
  underlying millions of rows, rather than being the first accounts that fit
  on the cluster;
- retain positive rows for selected ranking groups, then sample normal
  negative candidates across simple-rules rank bands;
- cap selected account/reference-date groups with
  `ranking_model.training_frame.max_accounts`;
- cap candidates per account/reference-date group with
  `ranking_model.training_frame.max_candidates_per_account`;
- fail fast if the resulting frame exceeds
  `ranking_model.training_frame.max_rows`;
- fail the GPU/local XGBoost path before `.toPandas()` if it exceeds
  `ranking_model.training_frame.max_pandas_rows`;
- fail before model fitting if the training frame, or any train/validation/test
  split, has no positive or no negative labels.

The default values are aligned to the original notebook training scale rather
than to the full operational scoring table. Any future change to these limits
should be treated as a modelling decision and validated through challenger
metrics, not as a Databricks plumbing tweak.

The sampled frame is not a classifier dataset with a single global threshold.
Theme Affinity is a ranking problem, so MLflow evidence is ranking-specific:
`hit@k`, `recall@k`, `precision@k`, `MRR`, `NDCG@k`, top-k confusion matrices,
score/label separation, lift by score decile and pre/post sample distribution
plots. These artifacts are logged alongside a machine-readable
`sample_profile.json` that records the source table, sample config and
population-versus-sample strata counts.

A successful Databricks job run is not sufficient evidence of a valid model.
For example, a stale DEV Spark train job once ran against
`marketingdata_dev.stephen_blain.next_uk_nextads_theme_affinity_predict_ranked`
without an explicit `--input_table` override and produced
`training_frame_positive_rows = 0`. That run completed technically, but all
ranking metrics were zero because it trained on an unlabeled scoring snapshot.
The current trainers now fail that input before fitting or registering a model.

The standard challenger route is the GPU/local XGBoost trainer. It uses the
existing Python `XGBoostRankingModel`, collects the train/validation/test splits
to pandas on a single GPU ML cluster, forces XGBoost `device: cuda`, logs the
model as an MLflow pyfunc model and registers it with a
`<environment>_gpu_xgboost` alias.

The second challenger route is the Spark-native trainer. It uses
`xgboost.spark.SparkXGBRanker` on the existing Databricks Spark job cluster
configuration. It reads the ranked training table, splits by account, encodes
categorical features in a Spark ML pipeline, fits the distributed XGBoost
ranker and logs the fitted Spark pipeline to MLflow with a
`<environment>_spark_xgboost` alias.

Neither challenger is byte-for-byte identical to the current production model,
so keep the existing production `ranking_model.model_uri` unchanged until
challenger evidence has been reviewed.

## Theme Affinity Promotion Flow

Theme Affinity model movement is version-based. The model imported into PREPROD
must be the exact Unity Catalog model version that was trained and reviewed in
DEV integration, not a retrained copy from the same code.

This assumes the PROD Databricks workspace service principal has read access to
the controlled DEV integration model namespace:

`marketingdata_dev.nextads_integration`

The controlled flow is:

- Personal DEV: run the GPU or Spark challenger train job to prove the branch,
  data contract and metrics in a user schema.
- DEV integration: after merge to `develop`, run the reviewed GPU or Spark
  challenger train job in `DEV_INTEGRATION`. This registers into
  `marketingdata_dev.nextads_integration.nextads_theme_affinity_ranker` with a
  `dev_gpu_xgboost` or `dev_spark_xgboost` alias.
- DEV integration to PREPROD: from the PREPROD target, run
  `mktg_next_uk_nextads_theme_affinity_model_import_dev` with the reviewed
  DEV integration model version. This copies that exact model artifact into
  `marketingdata_prod.ds_sandbox.nextads_theme_affinity_ranker` and sets a
  `preprod_gpu_xgboost` or `preprod_spark_xgboost` alias.
- PREPROD to PROD: run
  `mktg_next_uk_nextads_theme_affinity_model_promote` from the PROD target with
  the reviewed PREPROD model version. The job sets the `preprod` alias on that
  version, registers it into
  `marketingdata_prod.warehouse.nextads_theme_affinity_ranker` and sets the
  `prod` alias.

For future models, prefer a Spark-native or distributed trainer when the
training table is already a Spark/Delta contract. Use a local pandas trainer
only when the dataset is deliberately small enough to fit on the driver and the
operational constraints are documented.

## Databricks-Native Monitoring

Theme Affinity now has two monitoring layers:

- `resources/jobs/mktg_next_uk_nextads_theme_affinity_quality_monitor_setup.yml`
  creates or updates a Databricks quality monitor over
  `next_uk_nextads_theme_affinity_predict_ranked`. This is target-scoped to
  SANDBOX, DEV, DEV_INTEGRATION and PREPROD and is intentionally unscheduled.
  Run it only after the DLT ranked table exists in the target schema, because
  Databricks creates the monitor against an existing Unity Catalog table.
- `scripts/theme_affinity/monitor_model.py` remains the Theme Affinity MLflow evidence job. It
  compares baseline and candidate tables, logs metrics and writes retrain /
  promotion-blocking tags that can be used in review.

The monitor setup is a job rather than a direct Declarative Automation Bundle
`quality_monitors` resource because the current DEV and PREPROD bundle targets
use target-level service-principal `run_as`. Databricks quality monitor
resources cannot be deployed when the resource owner differs from the target
`run_as` identity, so the job creates the native monitor under the identity that
runs the setup task and avoids blocking normal bundle validation.

For future models, prefer a true inference-log table for Databricks monitoring:
include model id or version, prediction, timestamp, input feature columns and
the eventual label when available. That allows Databricks to profile model
inputs, predictions, drift and performance by model version. Theme Affinity does
not yet have that full inference-log contract, so this PR starts with a
time-series monitor over the ranked DLT table.

## Guardrails

Do not add the following for future model lifecycle work:

- new `marketingdata_utils` dependencies;
- duplicate MLflow client-manager wrappers;
- model-specific copies of registry alias logic;
- train/promote jobs that bypass `ModelLifecycleSpec`;
- production schedule changes inside the same PR as model lifecycle plumbing;
- changes to existing production scoring outputs without separate validation.

If a future model needs lifecycle behaviour that does not fit this foundation,
extend `src/next_ads/ml/lifecycle` first, then consume that extension from the
model-specific package.

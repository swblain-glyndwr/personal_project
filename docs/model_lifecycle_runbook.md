# Next Ads Model Lifecycle Runbook

Status: Working runbook

This is the current DS-facing path for testing the Next Ads model lifecycle process. New DEV model work is declared in `configs/models/nextads_models.yaml` and run through the centrally owned `mktg_next_uk_nextads_model_development` job. Theme Affinity remains the worked operational example later in this runbook, but its existing training, movement and monitoring resources are transitional controls rather than a template for creating more model-specific saved jobs.

This runbook documents the jobs as they operate today. Do not run PREPROD or PROD model movement without release-owner agreement.

For the visual model movement path, see [architecture/mlflow_model_lifecycle.md](architecture/mlflow_model_lifecycle.md).

## Reference Files

- Shared lifecycle contract: [`src/next_ads/ml/lifecycle/spec.py`](../src/next_ads/ml/lifecycle/spec.py)
- Shared registry and promotion helpers: [`src/next_ads/ml/lifecycle/registry.py`](../src/next_ads/ml/lifecycle/registry.py)
- Generic promotion job: [`jobs/model/lifecycle/promote_model.py`](../jobs/model/lifecycle/promote_model.py)
- Theme Affinity config-to-lifecycle mapping example: [`src/next_ads/ranking/theme_affinity/lifecycle_spec.py`](../src/next_ads/ranking/theme_affinity/lifecycle_spec.py)
- Theme Affinity Spark training example: [`src/next_ads/ranking/theme_affinity/mlflow_lifecycle.py`](../src/next_ads/ranking/theme_affinity/mlflow_lifecycle.py)
- Theme Affinity GPU training example: [`src/next_ads/ranking/theme_affinity/gpu_xgboost_lifecycle.py`](../src/next_ads/ranking/theme_affinity/gpu_xgboost_lifecycle.py)
- Generic DEV model lifecycle resource: [`pipelines/databricks/jobs/mktg_next_uk_nextads_model_development.yml`](../pipelines/databricks/jobs/mktg_next_uk_nextads_model_development.yml)
- Existing Theme Affinity transition resources: [`pipelines/databricks/jobs/mktg_next_uk_nextads_model_import_dev_integration.yml`](../pipelines/databricks/jobs/mktg_next_uk_nextads_model_import_dev_integration.yml), [`pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity_model_import_dev.yml`](../pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity_model_import_dev.yml), [`pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity_model_promote.yml`](../pipelines/databricks/jobs/mktg_next_uk_nextads_theme_affinity_model_promote.yml)

## Principles

- Train the reviewed challenger once in DEV unless there is a specific reason to recreate evidence. Use lifecycle movement jobs to copy exact reviewed model versions into controlled namespaces.
- Promote exact Unity Catalog model versions between environments. Do not retrain in PREPROD or PROD to recreate a reviewed model.
- Prefer explicit `source_model_version` parameters for import and promotion evidence. `source_alias` is supported when it already resolves to the reviewed version, but it is less explicit in release notes.
- Keep the current production prediction URI unchanged until the challenger version has been reviewed and deliberately selected. For Theme Affinity, that means leaving the existing production model URI alone while the lifecycle process is being proven.
- Do not run PREPROD or PROD jobs from a feature branch. Use the normal release route.

## 1. Personal DEV Proof

Use this step to prove the branch, data contract and model evidence before the change is merged. Add the model declaration and reusable plug-ins, then use the generic lifecycle job; do not add a model-specific job resource.

1. Deploy or use the `DEV` target for the feature branch.
2. Confirm `configs/models/nextads_models.yaml` declares the labelled observation source, point-in-time feature lookups, split policy, trainer and evidence contract.
3. Run `mktg_next_uk_nextads_model_development` with the declared `model_name` and `operation=BUILD`, or use `operation=RESEARCH` followed by `operation=REVIEW_SELECT` when candidate comparison and human review are required.
4. In MLflow, review the model's derived personal-DEV experiment path and the immutable training or research receipts.
5. Record the Databricks run id, MLflow run id, registered model name, exact model version and evidence summary. The generic DEV job does not set or move an alias.

Minimum evidence to check for any model using this lifecycle:

- training frame row count is within configured limits;
- positive and negative labels exist in train, validation and test splits;
- ranking metrics are non-zero and make sense for the challenger objective;
- `sample_profile.json` and training evidence plots are present;
- the registered model version and artifact URI match the immutable build or reviewed-selection receipt, with existing aliases unchanged.

If the job fails because the training frame has no positive labels, the input table is probably an unlabelled scoring table. Stop and fix the input before trying to train again. The exact label-quality checks may differ for future models, but each model must have equivalent evidence that its training data is valid.

## 2. Review And Merge To Develop

Open the feature PR into `develop`. The PR should include:

- the DEV run id and MLflow run id;
- the registered model version created in DEV and the before/after alias state;
- the training input table used;
- the key ranking metrics and artifact checks;
- whether the Spark or GPU backend is proposed for integration.

Merging the code does not promote or select a production model. It only makes the reviewed code available to the shared integration target.

## 3. Move The Reviewed DEV Version Into DEV Integration

After the PR is completed and the code has landed in `develop`, copy the exact reviewed DEV model version into the controlled DEV Integration namespace. This is not a second training run. It preserves the exact artifact that DS reviewed and avoids training the same model again just to move it through the release route.

This extra copy exists because `marketingdata_dev.nextads_integration` is a stable shared release source. A personal DEV namespace is useful for isolated DS work, but it depends on an individual schema and can be harder for release owners and PREPROD jobs to rely on. Copying the reviewed version into `nextads_integration` gives the release route a predictable source namespace without changing the model artifact.

Run `mktg_next_uk_nextads_model_import_dev_integration` in `DEV_INTEGRATION` with:

| Parameter | Value |
| --- | --- |
| `source_model_name` | The reviewed personal DEV registered model. Theme Affinity example: `marketingdata_dev.<user_schema>.nextads_theme_affinity_ranker`. |
| `source_model_version` | The reviewed DEV model version from the PR evidence. Preferred. |
| `source_alias` | Leave blank when `source_model_version` is provided, or provide a reviewed alias if copying by alias. |
| `target_model_name` | The shared DEV Integration registered model. Theme Affinity example: `marketingdata_dev.nextads_integration.nextads_theme_affinity_ranker`. |
| `target_alias` | The reviewed backend or selection alias. Theme Affinity examples: `dev_spark_xgboost` or `dev_gpu_xgboost`. |
| `model_family` | A short family name for tags. Theme Affinity example: `theme_affinity`. |

The job uses the shared [`jobs/model/lifecycle/promote_model.py`](../jobs/model/lifecycle/promote_model.py) script to copy one Unity Catalog model version into another registered model namespace. For the current process test, the controlled DEV Integration target model is `marketingdata_dev.nextads_integration.nextads_theme_affinity_ranker`. Future models should use their own controlled DEV Integration registered model namespace.

Record the DEV Integration model version that was created by the copy job. That copied version, not a retrained replacement, is the source version that can later be imported into PREPROD.

## 4. Import DEV Integration Version To PREPROD

This is a release-owner controlled step from the `PREPROD` target. It copies the reviewed DEV Integration artifact into the PREPROD model namespace. Theme Affinity uses the concrete job and model names below as the reference implementation; future models should use the same version-based import pattern with their own registered model names.

Run `mktg_next_uk_nextads_theme_affinity_model_import_dev` with:

| Parameter | Value |
| --- | --- |
| `source_model_name` | `marketingdata_dev.nextads_integration.nextads_theme_affinity_ranker` |
| `source_model_version` | The reviewed DEV Integration version. Preferred. |
| `source_alias` | Leave blank when `source_model_version` is provided, or provide a reviewed alias if importing by alias. |
| `target_model_name` | `marketingdata_prod.ds_sandbox.nextads_theme_affinity_ranker` |
| `target_alias` | `preprod_spark_xgboost` or `preprod_gpu_xgboost`, matching the reviewed backend. |

The job supports either `source_model_version` or `source_alias`. If only `source_alias` is supplied, it resolves the alias to a concrete source version before copying. For release evidence, use `source_model_version` unless there is a clear reason not to.

After the import job completes, record the PREPROD model version created under: `marketingdata_prod.ds_sandbox.nextads_theme_affinity_ranker`.

## 5. PREPROD Operational Validation

Use the normal PREPROD release validation route to prove that the imported model works in the workflow that will consume it. This is wider than just copying the model or loading it in isolation: run the model's PREPROD batch job, serving path, notebook workflow or equivalent operational route, then review that route's output and sense checks.

For the Theme Affinity reference path, this means running the PREPROD `mktg_next_uk_nextads_theme_affinity` job with the `model_uri` job parameter set to the imported PREPROD model version or alias. The job parameter defaults from `theme_affinity_model_uri` and is passed to the Theme Affinity prediction task as `--model_uri`, so the validation run loads the reviewed imported model rather than the default/current model. Example URI:

```text
models:/marketingdata_prod.ds_sandbox.nextads_theme_affinity_ranker/<version>
```

or:

```text
models:/marketingdata_prod.ds_sandbox.nextads_theme_affinity_ranker@preprod_spark_xgboost
```

Review the full Theme Affinity run evidence: DLT/data-prep status, prediction task status, clean-output task status, model sense checks and output-table movement. For future models, use the same principle with that model's own consuming workflow and equivalent operational checks. Keep the PREPROD run ids, model URI, output tables and review outcome in the release evidence.

## 6. Promote PREPROD Version To PROD

This is a release-owner controlled step from the `PROD` target after the normal main/tag production route is agreed. It copies the reviewed PREPROD artifact into the production model namespace. Theme Affinity uses the concrete job and model names below to prove the process; future models should follow the same exact-version promotion pattern.

Run `mktg_next_uk_nextads_theme_affinity_model_promote` with:

| Parameter | Value |
| --- | --- |
| `source_model_name` | `marketingdata_prod.ds_sandbox.nextads_theme_affinity_ranker` |
| `source_model_version` | The reviewed PREPROD version. Preferred. |
| `source_alias` | `preprod` when you want the job to stamp that alias onto the reviewed PREPROD version before copying, or a resolving alias when promoting by alias only. |
| `target_model_name` | `marketingdata_prod.warehouse.nextads_theme_affinity_ranker` |
| `target_alias` | `prod` |

The safest PROD run provides `source_model_version`. With the current job defaults, `source_alias=preprod` is also passed; when a source version is provided the job sets that alias on the source version before copying it to PROD. If `source_model_version` is blank, `source_alias` must already resolve to the reviewed PREPROD version.

After the promotion job completes, record the PROD model version created under: `marketingdata_prod.warehouse.nextads_theme_affinity_ranker`.

## 7. Production Selection And Monitoring

Promotion registers the model and sets the `prod` alias. It does not by itself prove the production scoring output has changed. In the Theme Affinity reference path, confirm the deployed `theme_affinity_model_uri` used by `mktg_next_uk_nextads_theme_affinity` points to the intended production model URI before treating the challenger as live. Future models need the equivalent serving or scoring URI check.

After production output exists for the model under review:

1. Run `mktg_next_uk_nextads_theme_affinity_quality_monitor_setup` only if the native Databricks quality monitor needs creating or updating.
2. Run `mktg_next_uk_nextads_theme_affinity_model_monitor` to log MLflow drift evidence for the configured baseline and candidate tables.
3. Review drift status, retrain recommendation and promotion-blocking tags in the MLflow monitor run.
4. Keep monitor run ids and Databricks quality monitor evidence in the release notes.

## Stop Conditions

Stop and ask for review before moving a model forward if:

- the training input is not a labelled training table;
- a split has no positive or no negative labels;
- ranking metrics are zero or obviously inconsistent with the selected backend;
- the source version cannot be tied back to a reviewed DEV Integration run;
- `source_alias` points to a different version than the one in the release evidence;
- PREPROD sense checks show unexpected output movement;
- production scoring would require a new schedule or output contract change.

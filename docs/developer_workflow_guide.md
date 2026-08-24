# Data Scientist Workflow: Local Development to Production Deployment

This guide walks you through the complete workflow for developing and deploying code using Databricks and Azure DevOps CI/CD pipeline.

## Prerequisites

Before starting, ensure you have:

- Access to Databricks workspace
- Access to Azure DevOps project
- Git and Azure CLI installed locally
- Python 3.11 with Poetry installed
- Databricks CLI configured
- Setup project tables as required
- Setup project configuration for local development

### Setup Check

```bash
# Verify Git
git --version

# Verify Python
python --version  # Should be 3.11.x

# Verify Poetry
poetry --version

# Verify Databricks CLI
databricks --version

# Verify Azure CLI
az --version

# Verify Databricks connection
databricks workspace list /root -t DEV
databricks workspace list /root -t PROD
```

If local Databricks auth has expired, refresh the named CLI profiles used by the bundle:

```bash
databricks auth login --host https://adb-6694370232251359.19.azuredatabricks.net/ -p SANDBOX
databricks auth login --host https://adb-6694370232251359.19.azuredatabricks.net/ -p DEV
databricks auth login --host https://adb-6188831950334199.19.azuredatabricks.net/ -p PREPROD
databricks auth login --host https://adb-6188831950334199.19.azuredatabricks.net/ -p PROD
```

`DEV` and `SANDBOX` share the same workspace host, and `PREPROD` and `PROD` share the same production host. For bundle commands, pass an explicit profile to avoid ambiguous-profile resolution:

```bash
databricks bundle validate --target DEV --profile DEV
databricks bundle plan --target DEV --profile DEV
databricks bundle deploy --target DEV --profile DEV
```

---

## Complete Workflow: Development to Production

### **Phase 1: Develop Code**

Write your code in the Databricks workspace or your local IDE.

---

### **Phase 2: Run Unit Tests**

Once your code works, create unit tests before moving to production.

#### **Step 2.1: Create Test Files Locally**

#### **Step 2.2: Run Tests Locally**

```bash
# Run all tests
poetry run pytest tests/unit/

# Run specific test
poetry run pytest tests/unit/test_specific_file.py -v

```

---

### **Phase 3: Deploy Code to DEV with Databricks CLI**

Deploy the existing bundle to the personal DEV target before committing to Git. Use the centrally owned job for the responsibility you are testing; do not create a developer-, model-, theme- or experiment-specific saved job for ordinary data-science work.

Do not use PREPROD for ordinary feature branch testing. PREPROD is the Release Owner route for an agreed `release/*` candidate.

For model work, add or update the model and optional research declaration in `configs/models/nextads_models.yaml`, add reusable feature contracts through the Feature Store registry and builders, and use `mktg_next_uk_nextads_model_development`. A new saved job requires a stable operational responsibility with distinct ownership, scheduling or runtime needs that the centrally owned route cannot represent.

```bash
# Step 1: Source environment variables
source devops/scripts/set_tags.sh

# Step 2: If new packages have been added to Poetry then export dependency to requirements.txt file format
poetry export -f requirements.txt --output requirements.txt --without-hashes

# Step 3: Validate bundle
databricks bundle validate -t DEV --profile DEV

# Step 4: Plan bundle to see changes
databricks bundle plan -t DEV --profile DEV

# Step 5: Deploy to DEV
databricks bundle deploy -t DEV --profile DEV
```

Run the relevant centrally owned job manually in the Databricks UI and verify successful completion.

---

### **Phase 4: Commit and Push to Feature Branch**

Create feature branches from `develop`, not `main`.

```bash
git fetch origin
git switch develop
git pull
git switch -c feature/<work-item-id>-<short-description>
```

Now that code is tested and working in DEV in a developer feature branch, the final code can be committed to Git.

---

### **Phase 5: Trigger CI/CD Pipeline from DevOps**

Use Azure DevOps to automatically test and deploy your code.

Manual trigger pipeline in Azure DevOps.

Now, let the automation take over. This ensures the deployment is repeatable and identical across all environments.

1. Go to Azure DevOps -> Pipelines.

2. Select the project pipeline, i.e. mktg-next-ads-ci-cd.

3. Important: Select your feature/your-feature-name branch from the dropdown.

4. (Optional) Select specific stages in the pipeline you want to run. For feature branch testing select `Deploy to DEV`; for merged `develop` integration testing select `Deploy DEV Integration`; for the shared DEV model-building feature store select `Deploy DEV Feature Store` from `develop`.

5. Click Run Pipeline.

6. Monitor pipeline execution.


#### **What Happens During Pipeline**

| Stage | What It Does |
|-------|---|
| **CI** | Runs unit tests, linting, validation |
| **Deploy DEV** | Deploys to DEV workspace, tags jobs with git info |
| **Deploy DEV Integration** | Deploys `develop` to the shared `DEV_INTEGRATION` target |
| **Deploy DEV Feature Store** | Deploys the scheduled shared DEV feature-store target only |
| **(Optional) Destroy DEV** | Deletes DEV DABs (helps with DAB development) |
| **Deploy PREPROD** | Deploys only from `release/*` using the PREPROD route |
| **Smoke PREPROD Dependencies** | Runs a metadata-only PREPROD dependency check without reading rows or altering tables |
| **Initialize PREPROD Tables** | Optional setup stage that creates missing PREPROD validation tables in `marketingdata_prod.ds_sandbox` |
| **Deploy PROD** | Runs only from an approved `nextads-vYYYY.MM.DD.N` production tag on `main` |

---

> NOTE: The deployment pipeline is still manually queued. Select the intended branch or tag explicitly; branch conditions prevent PREPROD from running outside `release/*` and PROD from running outside tags.

For the Databricks jobs expected in each bundle target, see [`CICD/nextads_databricks_job_environment_matrix.md`](CICD/nextads_databricks_job_environment_matrix.md).

#### DEV Integration Smoke Check

After feature PRs have merged to `develop`, run the deployment pipeline from `develop` and select `Deploy DEV Integration` and `Initialize DEV Integration Tables`. This deploys the `DEV_INTEGRATION` target to the DEV Databricks workspace, creates any missing shared DEV tables, and writes through `USER_SCHEMA=nextads_integration`.

Leave `Recreate DEV integration tables` unticked for normal runs. Tick it only when a merged change intentionally changes table definitions and the shared DEV integration tables need to be dropped and recreated.

For smoke evidence, run `load_control_sheet_v1`, and run `load_control_sheet_v2` when v2 control sheet changes are in scope. Confirm the output tables are created or updated in `marketingdata_dev.nextads_integration` and that no PREPROD or PROD outputs have changed.

#### DEV Feature Store

After the feature-store route has merged to `develop`, run the deployment pipeline from `develop` and select `Deploy DEV Feature Store`. This deploys only the `DEV_FEATURE_STORE` target to the DEV Databricks workspace.

The shared feature-store job writes reusable model-building features to `marketingdata_dev.nextads_feature_store` and reads stable Theme Affinity source outputs from `marketingdata_prod.warehouse`. It is scheduled daily at 21:00 Europe/London; run it manually after deployment when immediate validation or repair evidence is needed.

Feature branches can deploy a separate personal Feature Store job through `git deploy-dev`. That copy has no schedule, permits one run at a time and writes only to the last commit author's DEV schema. Use it for branch-level runtime validation. `DEV_FEATURE_STORE` remains the sole scheduled shared copy and is deployed only from `develop`.

#### Declared Model Development

The normal model-author workflow is declaration plus parameter selection, not creation of another Databricks job. Add or update `configs/models/nextads_models.yaml`, deploy the feature branch to personal `DEV`, then run `mktg_next_uk_nextads_model_development` with a declared `model_name` and one operation:

| Operation | Supply | Result |
| --- | --- | --- |
| `BUILD` | `observation_reference_dates`, `feature_reference_dates`, `label_end` | Point-in-time training receipt, selected build and registered personal-DEV model version. |
| `RESEARCH` | `label_end` | Model-option comparison using train, validation, test, feature-date and selection-policy rules from the declaration. |
| `REVIEW_SELECT` | `research_build_id`, `candidate_id`, `written_reason`, `reviewed_by` | Durable reviewed decision, selected-model-only test evaluation and registered personal-DEV model version. |
| `EVALUATE` | `model_build_id`, `run_date`; optional feature dates, account limit, serving slot and candidate-build attempt | Isolated evaluation evidence with no serving, assignment or payload change. |

The job derives the personal DEV namespaces, registered-model name, control tables and MLflow path. It cannot promote, set an alias or copy a model to another environment. For optional AutoML, run the centrally owned `mktg_next_uk_nextads_model_discovery` job with `enabled=true`, the same declared `model_name`, an exact `research_build_id` and an optional bounded timeout; its separate ML runtime never registers or activates a model.

The table above is only the launch summary. Before declaring or running model research, read [Model research: data scientist guide](model_research_walkthrough.md) for every DS-selectable option, declaration-owned choice, platform-controlled value, metric/evidence requirement, output destination, retry rule, current limitation and the worked Shopping Bag values.

### **Phase 6: Create Azure DevOps Pull Request**

Once you're satisfied with results, create a PR to merge the feature branch into `develop`.

```text
feature/* -> develop
```

The PR should link the work item, include validation evidence, and call out any schema, Databricks job, config, downstream output or production risk.

Do not raise day-to-day feature work directly into `main`.

---

### **Step 7: Release Validation and Production**

When an agreed set of integrated changes is ready, create a release branch from `develop`:

```text
develop -> release/*
```

Deploy the release branch using the PREPROD route and validate the output before approving production. In the current setup, PREPROD runs in the PROD Databricks workspace using `job_env=preprod`, but writes validation outputs to `marketingdata_prod.ds_sandbox`, not `marketingdata_prod.warehouse`.

The Release Owner runs the pipeline from the `release/*` branch and selects `Continuous Integration`, `Deploy PREPROD`, and `Smoke PREPROD Dependencies`. This smoke check is metadata-only by default and does not read rows, create, delete, append, overwrite or otherwise alter tables.

Use `Initialize PREPROD Tables` only when the release owner has agreed that missing PREPROD validation tables should be created. The table setup stage is non-destructive, but it still changes metadata by creating missing configured write tables.

Before the first PREPROD run for a release, confirm the Azure DevOps pipeline can use the Production library, production service connection and production agent pool. In the PROD Databricks workspace, confirm the pipeline service principal can deploy bundles, create and run jobs, create missing tables in `marketingdata_prod.ds_sandbox`, and read required production-side inputs.

Record the release branch, pipeline run, PREPROD deploy result, metadata-only PREPROD dependency smoke result, and output route in the release evidence. PREPROD evidence should confirm the configured output route is `marketingdata_prod.ds_sandbox`, that no PREPROD tables were altered by the smoke check, and that PROD stages were not run.

Once approved:

1. Merge `release/*` into `main` by pull request.
2. Create a production tag on the approved `main` commit using `nextads-vYYYY.MM.DD.N`, for example `nextads-v2026.06.04.1`.
3. Manually run `mktg-next-ads-ci-cd` from that tag.
4. Select `Continuous Integration` and `Deploy to PROD` only.
5. Record the validated `release/*` branch, PREPROD evidence, main PR, production tag, PROD pipeline run and included work items in the release evidence.

Do not create production tags from `develop`, `release/*`, `hotfix/*` or feature branches. The production tag identifies the exact approved `main` version deployed to `marketingdata_prod.warehouse`.

Before promoting to production, configure the `main` branch policy with required PR review, linked work item, approval reset, and required build validation using `mktg-next-ads-validation` with display name `NextAds main validation`. Production tag creation should be restricted to Release Owners or approved administrators.

Hotfixes follow a separate urgent route: create `hotfix/*` from `main`, validate by PR back into `main`, tag the resulting `main` commit using `nextads-vYYYY.MM.DD.N`, manually deploy PROD from that tag, then merge or cherry-pick the hotfix back into `develop` and any active `release/*`.

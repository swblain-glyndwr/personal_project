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

Deploy your code as job to DEV environment before committing to Git. Create a developer specific feature job for light testing.

Optionally, deploy to PREPROD to run full scale job mirrorring PROD version.

```bash
# Step 1: Source environment variables
source devops/scripts/set_tags.sh

# Step 2: If new packages have been added to Poetry then export dependency to requirements.txt file format
poetry export -f requirements.txt --output requirements.txt --without-hashes

# Step 3: Validate bundle
databricks bundle validate -t DEV

# Step 4: Plan bundle to see changes
databricks bundle plan -t DEV

# Step 5: Deploy to DEV
databricks bundle deploy -t DEV
```

Run the job manually in Databricks UI and verify successful completion.

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

4. (Optional) Select specific stages in the pipeline you want to run, e.g. Deploy to DEV or Deploy to PREPROD.

5. Click Run Pipeline.

6. Monitor pipeline execution.


#### **What Happens During Pipeline**

| Stage | What It Does |
|-------|---|
| **CI** | Runs unit tests, linting, validation |
| **Integration Tests** | Runs integration tests in PROD |
| **Deploy DEV** | Deploys to DEV workspace, tags jobs with git info |
| **(Optional) Destroy DEV** | Deletes DEV DABs (helps with DAB development) |
| **Deploy PREPROD to PROD** | Deploys the selected branch using the PREPROD route |
| **Deploy PROD** | Run only from an approved production tag on `main` |

---

> NOTE: The existing deployment pipeline is still manually controlled during the first branch-control rollout. Select the intended branch or tag explicitly and do not use it as an automatic promotion route until the branch-conditioned deployment pipeline has been implemented.

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

Once approved:

1. Merge `release/*` into `main` by pull request.
2. Create a production tag on `main`.
3. Deploy PROD from the tagged commit.
4. Record the production tag and release evidence.

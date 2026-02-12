# Steps to create new pipeline

## Project Structure Setup

```md
next-ads/
├── azure-pipelines.yml          # Main pipeline
├── databricks.yml               # DAB configuration
├── devops/                      # DevOps resources
│   ├── scripts/
│   ├── templates/
│   └── variables/
├── resources/                   # DAB resources
│   └── jobs/
```

## Setup AZ CLI and DevOps Pipelines

```sh
# 1. Login to Azure
az login

# 2. Set subscription (if you have multiple): Marketing.Prod
az account set --subscription "<your-subscription-id>"

# 3. Configure Azure DevOps CLI
az devops configure --defaults organization=https://dev.azure.com/Next-Technology project=DirectoryMarketing.Personalisation

# 4. Verify configuration
az devops configure --list

# 5. List your pipelines
az pipelines list

# 6. Manually trigger your pipeline
az pipelines run --name "mktg-next-ads-ci-cd-dev"
```

## Create pipeline

Create the YAML Wrapper, recommended path to be ".azure-pipelines.yml".

You might want to setup a dev pipeline first to test your changes. Then once ready delete it and merge code to main branch (i.e. main pipeline).

## Connect pipeline to repo and create new pipeline

1. Commit your changes

```sh
git checkout -b feature/new-pipeline
git add .
git commit -m "Add pipeline scripts and YAML"
git push -u origin feature/new-pipeline
```

2. Create the Pipeline (pointing to your feature branch) Use the CLI to create the pipeline definition, but explicitly tell it to look at your feature branch for now.

```sh
az pipelines create \
  --name "mktg-next-ads-ci-cd-dev" \
  --repository "next-ads" \
  --branch "feature/new-pipeline" \
  --yml-path "azure-pipelines.yml" \
  --repository-type tfsgit
```

3. Grant the DevOps pipeline permissions 
    1. to access DevOps variable groups
    2. to access Agent Pool

# Variables stored in DevOps library groups

```
ARM_TENANT_ID
AzureBuildAgentPool
AzureRegion
BIS_source_catalog_name
DATABRICKS_CLUSTER_ID
DATABRICKS_CLUSTER_ID_15_4
DATABRICKS_HOST
DBK_SearchSchemaContributor
DataFactoryName
DatafactoryResourceGroupName
LogicAppURL
ServiceConnectionName
ServicePrincipalName
StorageAccountResourceGroup
SubscriptionId
SubscriptionName
SupportEmailFrom
SupportEmailTo
bundle_target
dlt_policy_id
ecom_env_name
ecom_subscription_id
nonpii_storage_adf_trigger
policy_id
promoDBName
schedule_pause_status
service_principal_name
single_node_policy_id
spark_conf_credentials
spark_conf_initial_catalog_name
spark_conf_private_key
spark_conf_private_key_id
strKeyVaultName
strPIIExponeaStorageAccountName
strPIIStorageAccountName
strStorageAccountName
```
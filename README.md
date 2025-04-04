# Introduction and Scope
> *Up to date as of v1.5.1*  

`next-ads` is a process that assigns relevant adverts to customers browsing the NEXT website. The code enclosed within this repo - sometimes referred to as the "Next Ads engine" - uses pre-calculated model scores to determine which ad is 'best' for each customer, as well as building the control cells and results tables to measure performance of these personalised ads.

The model scores input into this 'engine' can take multiple forms (e.g. propensity scores, item recommendations), but the modelling itself falls outwith the scope of this repo.  

# Overview

## Process Inputs
- Next Ads Control Sheet (Google Sheet - specifically the _Control Sheet_ tab)
    - [Google Sheet](https://docs.google.com/spreadsheets/d/1ZVZxP6pms8t0THY7BLoFHh4INQwfhxGWcuLEXsPX2JI/edit?gid=1718512789#gid=1718512789)
    - This is managed by the On-Site Advertising (OSA) team within the business' Trade team. 
- Latest model scores for all relevant customers and models.
    - [marketingdata_prod.warehouse.next_uk_nextads_model_scores_latest](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/warehouse/next_uk_nextads_model_scores_latest?o=6188831950334199)
- Latest recommender scores for all live ads.
    -  [marketingdata_prod.warehouse.next_uk_nextads_recommender_scores_latest](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/warehouse/next_uk_nextads_recommender_scores_latest?o=6188831950334199)

## Process Stages
### Engine
1. Load and validate Ads from control sheet in Google Sheets
2. Assign new customers to fixed cells, and update all customers' transient cells
3. Utilise model scores to automatically assign one Ad per customer per Location, in accordance with the targeting specified in the configuration file
4. Store the latest ad assignments
### Results
1. Combine ad assignments with browsing data to infer impressions and clicks
2. Calculate KPIs
3. Store the results and transmit to Big Query for ingestion by the Next Ads dashboard

## Process Outputs
### Ad Assignments
- The run of Ad assignmetns are appended to the "assignments" table and overwrite the "assignments_latest" table (see config for table paths).  
- The run of Ad results are output to the "results_*" tables (overwriting dates that already exist), which are then passed to the corresponding "Big Query""tables" (see config for table paths).

 </br>

# Configuration
Config files are stored as json with the following naming convention: `config/{domain}.json`, where domain is of the form `{client}_{country}` (e.g. `next_uk.json`). These files contain all parameters and references to resources that are required by the process for that domain.

## DevOps
This projects employs the `job_env:schema` convention for tables that the process can write to. As such 

`job_env` is a result of parsing the name of the Databricks workflow from which the code is being run.
- When the code is being run via a Databricks workflow that starts with "dev_*", or the code is being run interactively, the `job_env` is _dev_
- When the code is being run via a Databricks workflow that does not start with "dev_*", the `job_env` is _prod_

The config file contains the `job_env:schema` mapping. This maps the process' "write" tables - also specified in the config - to identical tables in different schemas depending on whether the process is running in the _dev_ or _prod_ `job_env`.

The process' "read" tables are always "prod" data, regardless of `job_env`, which enables the process to be run interactively, or end-to-end via the _dev_ workflow in a way that is maximally identical to the _prod_ workflow, enabling more thorough development and testing before changes are "productionised".


### Workflows
- **dev**:
    - [dev_mktg_next_uk_nextads](https://adb-6188831950334199.19.azuredatabricks.net/jobs/518755454712672?o=6188831950334199)
- **prod**:
    - [mktg_next_uk_nextads](https://adb-6188831950334199.19.azuredatabricks.net/jobs/851069914792732?o=6188831950334199)
    - [mktg_next_uk_nextads_results](https://adb-6188831950334199.19.azuredatabricks.net/jobs/876285369413830?o=6188831950334199)*

_*There is no dev results workflow as the results do not pose the same operational risks as the main 'engine'. Furthermore, results can be back-calculated for any date range, so unexpected issues can be repaired._ 

### Key Process Entities
- `UniqueAdID` - This is a unique idendifier for every Ad.
- `MASIDToken` - This is the suffix which tells the site's Content Management System (CMS) which Ad to display; there should only be one `MASIDToken` per `UniqueAdID` per `Location`.
- `Location` (alias: 'Placement') - Prefix representing each MASID "slot" (e.g. "SB1") - combined with the `MASIDToken` to form a MASID "segment" (e.g. SB1_AABB if AABB was the corresponding token).
- `AlgoDivision` - High-level product categories (e.g. _Womens_, _Mens_, _Home_...) that may be used by the algorithm to control ad assignment. (N.B. `AlgoDivision` is similar to, but not the same as `TradeDivision`, which is a more grandular view of Divisions used by the trading teams; `TradeDivision` only serves to label ads within the results processing and has no bearing on Ad assignments).
- `Audience` - A pre-determined group of customers with a specific label. This label can be used to override or influence the default targeting of the engine.
- `Treatment` - This is a label determining which type of targeting should be applied to a given customer (e.g. _Best_ or _Basic_ targeting).
- __Fixed Customer Cells__ - Customer labelling that does not change over time (e.g. which `Treatment` or `Control` group a customer is in).
- __Transient Customer Cells__ - Customer labelling that may change over time. Labels that do not change but are ephemeral, e.g. to facilitate and ad hoc test are treated as Transient Customer Cells.

</br>  

# Testing

## Unit Tests - *WIP*
*Pipeline run on PR from dev to staging:*
1. *Test modules for internal consistency*

## Integration Tests - *WIP*
*Pipeline run on PR from dev to staging:*
1. *Assert that all "read" tables exist in dev and prod schemas*
2. *Assert that all "write" tables exist in dev and prod schemas*
3. *Assert that the schema of all "read" tables is correct*
4. *Assert that the schema of all "write" tables is correct*

</br>  

# Development
Poetry has been used for environment and dependency management of this project. Guidance on how to install Poetry and install project dependencies into a local environment can be found on the [Poetry website](https://python-poetry.org/)

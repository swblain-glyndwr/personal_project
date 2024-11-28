# Introduction 
> *Correct as of v1.0.0*  

NEXT Ads is an initiative to serve personalised adverts to customers browsing the NEXT website.  The primary purpose of the **next-ads** algorithm is to assign each customer the 'best' ad for them, as well as building the control cells and results tables to measure performance of this ad assignment.  

## Overview

### Pre-requisites
- Next Ads Control Sheet [GSheet](https://docs.google.com/spreadsheets/d/1ZVZxP6pms8t0THY7BLoFHh4INQwfhxGWcuLEXsPX2JI/edit?gid=1718512789#gid=1718512789)
- Latest model scores for all relevant customers and models. These are currently captured within the view: [marketingdata_prod.warehouse.next_uk_nextads_model_scores_latest](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/warehouse/next_uk_nextads_model_scores_latest?o=6188831950334199)

### Process Overview
**1. Load the control sheet**  
Read the NEXT Ads control sheet, which contains all eligible ads (and ad metadata). This sheet is managed in collaboration with the OSA (On-Site Advertising) team.  
**2. Assign each customer to cells**  
Customers are assigned two types of cells:
- *Fixed Cells*: Membership of these cells does not change (e.g. `FallowControl` group - once a customer has been assigned to this group, they will stay in this group until overall cells are refreshed)  
- *Transient Cells*: These are cells that a customer can change membership of, without refreshing the cells overall (e.g. `AlgoDivision`), or cells that might be temporary (e.g. a bespoke, predetermined audience to be used for a short period).
*N.B. Transient and Fixed Cells are subsequently combined into an overall Customer Cells table*
**3. Assign each customer a Targeting Score for each live Ad**  
Calculate a score for each customer for each live `TargetingCriteria` (based on the models assigned to each ad).  
**4. Assign Ads to each customer for each Location in accordance with their model scores and cell membership**  
Assign an ad to each customer for each `Location`. This may be an ad targeted by *Best* or *Basic* methodologies no Ad (if they are in a control cell). The `UniqueAdID` assigned to each customer for each `Location` is then mapped to the corresponding MASID entry for that ad (e.g. "HN1_AABB")  
**5. Build results tables** - *REWRITE IN PROGRESS*  
Generate tables contianing required metrics for the NEXT Ads dashboard.

### Pseudo-DevOps
**dev** - Running any `task_...` scripts interactively, or via a workflow starting with the substring "dev_" will run the task in "development". This means that any tables written to by the task will be in the dev schema, specified in `resources.json`.  
**prod** - Running any `task_...` scripts using a workflow that does not begin with the substring "dev_" will run the task in "prod". This means that any tables written to by the task will be in the prod schema, specified in `resources.json`.

### Configuration
There are two main config files:
- `resources.json` - this stores references to and tables, files or other data assets that the process requires.
- `parameters.json` - this stores constants used by the process (e.g. size of the Fallow Control group, or how each page is being targeted)

### Workflows
- **dev**: [dev_mktg_next_uk_nextads](https://adb-6188831950334199.19.azuredatabricks.net/jobs/395299271123005?o=6188831950334199)
- **prod**: [mktg_next_uk_nextads](https://adb-6188831950334199.19.azuredatabricks.net/jobs/851069914792732?o=6188831950334199)

### Terms
- `UniqueAdID` - This is unique for every Ad. A `UniqueAdID` should have one `MASIDToken`
- `Location` - MASID slot prefix (e.g. "HN1")
- `Models`: The models to use for targeting (convention, comma separated list: *"model_ref_1, model_ref_2,... model_ref_n"*)
- `ModelCombination`: An operator that describes how the models should be combined (N.B. only *"and"* operator currently supported)
- `TargetingCriteria` is the combination of `Models` and `ModelCombination` (convention: `ModelCombination`|`Models`, e.g. *"and|ww_dresses, ww_floral"*. This instructs the algorithm *how* to target a given Ad using the available propensity models. The example given above would result in the Ad being targeted at those with relatively high scores for women's dresses, *and* women's floral items.
- `TargetingScore` score resulting from targeting criteria
- `AlgoDivision` are high-level product categories (e.g. "Womens") and may be used by the algorithm to control ad assignment. (N.B. `AlgoDivision` is similar to, but not the same as `TradeDivision`, which is a greater number of high-level categories used by the trading teams).

## Testing

### Unit Tests - *WIP*
*Test modules for internally consistency*

### Integration Tests - *WIP*
*Pipeline run on PR from dev to staging:*
1. *Assert that all read-only tables exist in production*
2. *Assert that all read-write tables exist in production*
3. *Assert that the schema of all read-write tables in production match their equivalents in dev*

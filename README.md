# Introduction 
NEXT Ads is an initiative to serve personalised adverts to customers browsing the NEXT website.  The primary purpose of the **next-ads** code is to assign each customer the 'best' ad for them, as well as generate datasets that can be used for monitoring performance of these personalised adverts against a control group(s).  

# Overview

### Pre-requisites
- [Next Ads Control Sheet Google Sheet](https://docs.google.com/spreadsheets/d/1ZVZxP6pms8t0THY7BLoFHh4INQwfhxGWcuLEXsPX2JI/edit?gid=1718512789#gid=1718512789)
- Latest model scores for all relevant customers and models. These are currently captured within the view: [marketingdata_prod.warehouse.next_uk_nextads_model_scores_latest](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/warehouse/next_uk_nextads_model_scores_latest?o=6188831950334199)

### Process Overview
**1. Load the control sheet**  
Read the NEXT Ads control sheet, managed by the OSA (On-Site Advertising) team  
**2. Assign each customer to measurement cells and overall division** - *REWRITE IN PROGRESS*  
    - Overall: *Fallow*  
    - Localised: *Control*, *Random*\*, *Personalised*  
**3. Assign each customer a score for each live Ad**  
Calculate a score for each customer based on the targeting criteria assigned to each active ad  
**4. Assign Ads to each customer for each location in accordance with overall division and measurement cells**  
Assign an ad for each customer for each location. This will either be a  *Personalised* ad, a *Random*\* ad, or no/default ad (depending on which measurement cells they have been assigned), and output the corresonding piece of the MASID (e.g. `HN1_AABB`)  
**5. Build results tables** - *REWRITE IN PROGRESS*  
Generate tables contianing key metrics and statistics that supply the NEXT Ads results dashboard

\* *Random assignment is is not truly random, it is random within a customer's 'best' Division, so could be thought of as naive targeting.*


### Downstream Resources
Results dashboard: [Next Ads Report - All Divisions](https://lookerstudio.google.com/reporting/9705d228-ea55-4c4f-ac90-80c1358ff1dd/page/p_d16bxlvakd)


# Stages
### 1. Load the control sheet
Script: `load_control_sheet.py`  

Workflow:
- dev:
    - [dev_mktg_next_uk_nextads_load_control_sheet](https://adb-6188831950334199.19.azuredatabricks.net/jobs/371817027044918?o=6188831950334199)
- prod:
    - [mktg_pf_ReadControlSheetV2](https://adb-6188831950334199.19.azuredatabricks.net/jobs/963813621407596?o=6188831950334199)  

Output tables:
- dev:
    - [next_uk_nextads_control_sheet](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/ds_sandbox/nextadscontrolsheet?o=6188831950334199)
    - [next_uk_nextads_control_sheet_latest](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/ds_sandbox/nextadscontrolsheet_latest?o=6188831950334199)
- prod:
    - [nextadscontrolsheetV2](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/warehouse/nextadscontrolsheetv2?o=6188831950334199)
    - [nextadscontrolsheetV2_latest](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/warehouse/nextadscontrolsheetv2_latest?o=6188831950334199)

### 2. Assign each customer to measurement cells and overall division - IN PROGRESS
Script: `overall_cells.py`  

Workflow:  TBC

Output table:  TBC

> Current prod solution is the following Delta Lake file: abfss://sandbox@datastmktprodeuw.dfs.core.windows.net/MASID/UK/NextAds/TESTCONTROL_V4_SEP24_BKUP

### 3. Assign each customer a score for each live Ad
Script: `build_targeting_scores.py`  

Workflow:   
- dev:
    - [dev_mktg_next_uk_nextads_targeting_scores](https://adb-6188831950334199.19.azuredatabricks.net/jobs/71616821429298?o=6188831950334199)
- prod:
    - *This stage does not exist in current prod process*  

Output table:
- dev:
    - [next_uk_nextads_targeting_scores_latest](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/ds_sandbox/next_uk_nextads_targeting_scores_latest?o=6188831950334199)
- prod:
    - *This stage does not exist in current prod process*

### 4. Assign Ads to each customer for each Location in accordance with overall division and measurement cells
Script: `build_page.py` (parameterised: takes `Location` as argument, e.g. "HN1")  

Workflow: 
- dev:
    - [dev_mktg_next_uk_nextads_page_build](https://adb-6188831950334199.19.azuredatabricks.net/jobs/805312351683307?o=6188831950334199)
- prod:
    - [mktg_next_uk_masid_tier3](https://adb-6188831950334199.19.azuredatabricks.net/jobs/1036134149090946?o=6188831950334199) (N.B. The Next Ads page builds are a not the only scripts in this workflow)  

Output tables:
- dev:
    - [next_uk_nextads_assignments](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/ds_sandbox/next_uk_nextads_assignments?o=6188831950334199)
    - [next_uk_nextads_assignments_latest](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/ds_sandbox/next_uk_nextads_assignments_latest?o=6188831950334199)
- prod:
    - *Current prod solution is a mix of tables and files, one for each Location*  

### 5. Build results tables - *IN PROGRESS*
Script: `build_results.py`  

Workflow:  

Output tables:  


# Key Terms
- `UniqueAdID` - This is unique for every Ad. A `UniqueAdID` should have one `MASIDToken`
- `Location` - MASID slot prefix (e.g. "HN1") (TODO: change to `Placement` to avoid protected word in SQL?)
- `Models`: The models to use for targeting as comma separated list (convention: *"model_ref_1, model_ref_2,... model_ref_n"*)
- `ModelCombination`: An operator that describes how the models should be combined (N.B. only *"and"* operator currently supported, planned development of *"or"*, *"max"*, *"mean"*)
- `TargetingCriteria` is the combination of `Models` and `ModelCombination` (convention: `ModelCombination`|`Models`, e.g. *"and|ww_dresses, ww_floral"*. This instructs the algorithm *how* to target a given Ad. The example given above would result in the Ad being targeted at those with relatively high scores for women's dresses, *and* women's floral items.
- `TargetingScore` score resulting from targeting criteria
- `Division` - It should be noted that Division can mean different things in different contexts, hence this is in the progress of being delineated to:
    - `AlgoDivision` are macro product groups that are category-led (e.g. Womens) and may be used by the algorithm to partition Ad assignment.
    - `TradeDivision` are macro product groups categories that reflect the trading activity of the business and can be category-led (e.g. "Womens") or brand-led (e.g. "Brands"). These are not used by the algorithm.
- *TBC...*


# Testing
### Unit Tests - *IN PROGRESS*
*Test that modules are internally consistent*

### Integration Tests - *IN PROGRESS*
*Pipeline run on PR from dev to staging:*
1. *Assert that all read-only tables exist in production*
2. *Assert that all read-write tables exist in production*
3. *Assert that the schema of all read-write tables in production match their equivalents in dev*

*Once PR accepted to staging branch, engineering can contol merge to main and deployment?*

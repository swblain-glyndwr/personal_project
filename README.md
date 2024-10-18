# Introduction 
NEXT Ads is an initiative to serve personalised adverts to customers browsing the NEXT website.  The primary purpose of the **next-ads** code is to assign each customer the 'best' ad for them. 'Best' can be thought of as the most relevant to them, and that which generates the most incremental revenue for the business.  

# Process
## Macro Process

#### Pre-requisites
Current model scores for all relevant customers and models. These are currently captured within the view: [marketingdata_prod.warehouse.next_uk_nextads_model_scores_latest](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/warehouse/next_uk_nextads_model_scores_latest?o=6188831950334199)

Within the scope of the **next-ads** code:
1. Read the NEXT Ads control sheet, managed by the OSA (On-Site Advertising) team
2. Assign customers to measurement cells
    - Overall: **Fallow Control**
    - Page-wise: **Control**, **Random**\*, **Personalised**
3. Assign each customer their 'best' Division, i.e. that for which they have the highest score
4. Calculate a score for each customer based on the targeting criteria assigned to each active ad
5. For each page, assign an ad for each customer, that is either a **Random**\* ad, or their **Personalised** ad (depending on the measurement cell), and output the corresonding code for the respective element of the MASID (e.g. `HN1_AABB`)
6. *Results? In scope of this repo?*

\* Random assignment is is not truly random, it is random within a customer's 'best' Division, so could be thought of as naive targeting.

# Key Terms and Concepts
- `UniqueAdID` - This is unique for every Ad. A `UniqueAdID` has one or more `MASID suffixes`
- `Location` - MASID slot prefix (e.g. "HN1") (change to `Placement` to avoid protected word in SQL?)
- `Models`: The models to use for targeting (convention: *"model_ref_1, model_ref_2,... model_ref_n"*)
- `ModelCombination`: An operator that describes how the models should be combined (convention: *"operator"* - N.B. only *"and"* currently supported, planned development of *"or"*, *"max"*, *"mean"*)
- `TargetingCriteria` is the combination of `Models` and `ModelCombination` (convention: `ModelCombination`|`Models`, e.g. *"and|ww_dresses, ww_floral"*. This instructs the algorithm to target a given Ad. The example given would result in the Ad being targeted at those with relatively high scores for women's dresses, *and* women's floral items.
- `Division` - It should be noted that Division can mean different things in different contexts
    - `AlgoDivision` are macro product groups that are category-led (e.g. Womens) and may be used by the algorithm to partition Ad assignment.
    - `TradeDivision` are macro product groups categories that reflect the trading activity of the business and can be category-led (e.g. "Womens") or brand-led (e.g. "Brands"). These are not used by the algorithm.


# Key Tables
### control_sheet
- next_uk_nextads_control_sheet
    - [dev](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/ds_sandbox/nextadscontrolsheet?o=6188831950334199)
    - [prod](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/warehouse/nextadscontrolsheetv2?o=6188831950334199)
- next_uk_nextads_control_sheet_latest
    - [dev](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/ds_sandbox/nextadscontrolsheet_latest?o=6188831950334199)
    - [prod](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/warehouse/nextadscontrolsheetv2_latest?o=6188831950334199)

### overall_cells_and_division
*next_uk_nextads_overall_cells_and_division - in progress* 

### targeting_scores
- next_uk_nextads_targeting_scores
    - [dev](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/ds_sandbox/next_uk_nextads_targeting_scores?o=6188831950334199)

### assignments
- next_uk_nextads_assignments
    - [dev](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/ds_sandbox/next_uk_nextads_assignments?o=6188831950334199)
- next_uk_nextads_assignments_latest
    - [dev](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/ds_sandbox/next_uk_nextads_assignments_latest?o=6188831950334199)


# Workflows
load_control_file

build_targeting_scores
build_page(s)


# Key Files
### next_ads/load_control_sheet.py
This file reads the [Next Ads Control Sheet](https://docs.google.com/spreadsheets/d/1ZVZxP6pms8t0THY7BLoFHh4INQwfhxGWcuLEXsPX2JI/edit?gid=0#gid=0) from Google Sheets, validates and formats the input, and inserts into the **control_sheet** table.


# Testing
## Unit Tests - WIP
*Test that modules are internally consistent*

## Integration Tests - WIP
*Pipeline run on PR from dev to staging:*
1. *Assert that all read-only tables exist in production*
2. *Assert that all read-write tables exist in production*
3. *Assert that the schema of all read-write tables in production match their equivalents in dev*

*Once PR accepted to staging branch, engineering can contol merge to main*


# Getting Started
TODO: Guide users through getting your code up and running on their own system. In this section you can talk about:
1.	Installation process
2.	Software dependencies
3.	Latest releases
4.	API references

# Build
TODO: Describe and show how to build your code and run the tests. 

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)
- [Chakra Core](https://github.com/Microsoft/ChakraCore)
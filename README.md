# Introduction and Scope
> *Up to date as of v2.10*  

`next-ads` is a process that assigns relevant adverts to customers browsing the NEXT website. The code enclosed within this repo - sometimes referred to as the "Next Ads engine" - uses pre-calculated model scores to determine which ad is 'best' for each customer, as well as building the control cells and results tables to measure performance of these personalised ads.

The model scores input into this 'engine' can take multiple forms (e.g. propensity scores, item recommendations), but the modelling itself falls outwith the scope of this repo.  

# Overview

## Process Inputs
- Next Ads Control Sheet (Google Sheet - specifically the _Control Sheet_ tab)
    - [Google Sheet](https://docs.google.com/spreadsheets/d/1ZVZxP6pms8t0THY7BLoFHh4INQwfhxGWcuLEXsPX2JI/edit?gid=1718512789#gid=1718512789)
    - This is managed by the On-Site Advertising (OSA) team within the business' Trade team. 
- "Next Best Label" preranked ad scores (see the [next-ads-incrementality](https://dev.azure.com/Next-Technology/DirectoryMarketing.Personalisation/_git/next-ads-incrementality) repo for the model):
    - [marketingdata_prod.ds_sandbox.next_uk_nextads_preranked_ads_latest](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/ds_sandbox/next_uk_nextads_preranked_ads_latest?o=6188831950334199)


#### Previous inputs - now obselete:
- Latest model scores for all relevant customers and models
    - [marketingdata_prod.warehouse.next_uk_nextads_model_scores_latest](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/warehouse/next_uk_nextads_model_scores_latest?o=6188831950334199)
- Latest recommender scores (GRU model) for live ads
    -  [marketingdata_prod.ds_sandbox.next_uk_nextads_recommender_scores_gru_latest](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/ds_sandbox/next_uk_nextads_recommender_scores_gru_latest?o=6188831950334199)
- Latest recommender scores (GRU with ALS ) for live ads
    -  [marketingdata_prod.search.next_ads_als_deployment_normalized_all](https://adb-6188831950334199.19.azuredatabricks.net/explore/data/marketingdata_prod/search/next_ads_als_deployment_normalized_all?o=6188831950334199)

## Process Stages
### Engine
1. Load and validate Ads from control sheet in Google Sheets
2. Assign new customers to fixed cells, and update all customers' transient cells
3. Utilise model/ad scores to automatically assign one Ad per customer per Location, in accordance with the targeting specified in the configuration file
4. Store the latest ad assignments
### Results
1. Combine ad assignments with browsing data to infer impressions and clicks
2. Calculate KPIs
3. Store the results and transmit to Big Query for ingestion by the Next Ads dashboard

## Process Outputs
### Ad Assignments
- The run of Ad assignments are appended to the `"assignments"` table and overwrite the `"assignments_latest"` table (see config for table paths).  
- The run of Ad results are output to the "results_*" tables (overwriting dates that already exist), which are then passed to the corresponding "Big Query""tables" (see config for table paths).

 </br>

### Algorithmic Components
#### Relevance Scoring
Relevance scoring can be supplied via `TargetingScore` (e.g. Propensity) input, `RecommenderScore` (e.g. ALS, GRU) input. Relevance scoring can also be performed entirely outside of the next-ads engine, in which case, a table of the preranked ads table with columns `AccountNumber, Location, UniqueAdID, Score` can be supplied to the `Assignment.assign_preranked_ads()` function during assignment.

`TargetingScore` is the score associated with model (or model combination) that has been assigned to an ad for targeting (e.g. ww_dresses and ww_floral will use a combination of the women's dresses propensity model score and the women's floral propensity model score as a measure of relevance of that ad to the customer). To supply these scores to the algorithm for relevance, the `TARGETING_SCORES_TABLE` needs to be specified in the `task_build_page.py` script, and the `assign_best_ads` function should be utilised.

`RecommenderScore` is a relevance score associated with an ad (commonly an aggregation of customer-item relevance scores of the items 'behind' an ad). To supply these scores to the algorithm for relevance, the `RECOMMENDER_SCORES_TABLE` needs to be specified in the `task_build_page.py` script, and the `assign_best_ads_rec` function should be utilised.

#### Ad Feedback Loop
If the Ad Feedback Loop is a mechanism for boosting/penalising ads that are showing better/worse commercial performance. This can be layered on top of any type of relevance scoring. The magnitude of this boosting/penalising can be adjusting using the `ad_feedback_weight` parameter in the `task_build_page.py` script. Extracting this constant to the config or setting of this weight algorithmically would be beneficial, however until this can be implemented, values that provide a subjectively assessed 'balanced' influence of the Ad Feedback Loop on tested input model types are as follows.

| Relevance Score Type | Model Type | Recommended `ad_feedback_weight` |
|----|----|----|
|`TargetingScore`| Propensity | 0.5-0.6 |
|`RecommenderScore`| ALS | 0.01-0.02 |
|`RecommenderScore`| GRU | TBC |


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

## Unit and Integration Tests
Existing tests can be contained in `tests/`. These are largely designed to test the validity of the supplied config file(s), such that any implicit requirements of the config structure can be checked before running end-to-end tests. These tests can be run using `pytest` directly or via the testing integration in VS Code.

## End-to-End Tests
End-to-end testing can be conducted via the dev workflow (see [Workflows](#workflows) section above).

</br>  

# Environment and Dependency Management
Poetry has been used for environment and dependency management of this project. Guidance on how to install Poetry and install project dependencies into a local environment can be found on the [Poetry website](https://python-poetry.org/)

# Attribute and Theme Item-Mapping
The following scripts have been created to parse and create the following mappings:
- `item:attribute` (one-to-many)
- `theme:attribute` (one-to-many)
- `item:theme` (one-to-many*)

* one-to-one can be achieved by using ranking mode `adtype-themefreq` and selecting the top ranked theme per item.

### `task_parse_attributes.py`

Purpose:
- Parse and clean selected attributes from `warehouse.product_catalog`, and produce a mapping of `item:attribute`.
    - The attributes to parse are specified in the `"attributes"` config key, along with other parameters (e.g. lookback period, frequency cutoffs based on item counts, or counts of orders featuring those items).

Process:
- An "attribute set" is a fixed set of attributes and values to be included in all downstream mappings and are stored in the `attribute_set[_latest]` table.
    - To invoke creating a new "attribute "set", `task_parse_attributes.py` must be run with the `--set` flag.
- Running without the `--set` flag will take the latest "attribute set" and and apply this mapping to the items (`pid`) in `warehouse.product_catalog` (going as far back as the lookback period), outputting the item-attribute mapping to `item_attributes[_latest]` table.

### `task_parse_theme_mapping.py`

Purpose:
- Parse and clean theme mapping defined by trade in the Next Ads Control Sheet, and product a mapping of `item:theme`.

Process:
- A "theme mapping" is a fixed set of themes and its corresponding attributes. This is defined in the Next Ads Control Sheet Google Sheet (see `"theme_mapping"` config key for details).
    - To invoke reading and setting a new theme mapping, use the `--set` flag. This will cause the script to output a new theme mapping to the `theme_mappping[_latest]` table.
- The script then maps themes to items and outputs to `item_themes[_latest]`, via the cleaned attributes in the `item_attributes_latest` table.
- A given item might have multiple themes, as such, themes are ranked within-item. There are currently two options for ranking:
    - `--theme-ranking-mode adtype-themefreq` results in the themes being ranked by AdType (column specified in the theme mapping tab of the Next Ads Control Sheet Google Sheet) followed by theme frequency. Ranking by theme frequency means that the theme with the smaller number of matching items will take precedence, the idea being that this will naturally rank niche themes higher, resulting in less overall convergence around the most common themes.
    - `--theme-ranking-mode adtype-themetype` results in the themes being ranked by AdType, then ThemeType, which are both specified manually by the trade team in the theme mapping tab of the Next Ads Control Sheet Google Sheet.

### `task_markov_chain.py`

Purpose:
- Lightweight directional graph of theme associations.
- Simply model that models each theme as a 'node' or 'state' and the probability of buying one theme after another as directional state transition or 'edge weights'.
- The probability of transferring from one theme to another is calculated via global frequencies of transitioning from one state to another, i.e. customer A buys 'womens jeans', and 'womens casualwear' is in their next basket, this would be a count for the 'womens jeans' to 'womens casualwear' transition. These frequencies are calculated globally, and form theme transition probabilities. Fractional counting is utilised to account for the fact that multiple themes may exist per basket.

Process:
- To "train" the markov chain, run the script with the `--train` flag. This will take baskets from the specified history period and calculate these theme transition probabilities, outputting these probabilities to the `theme_transitions[_latest]` table.
- Running the script without the `--train` flag, runs it in 'scoring' mode, which looks at the customer's last N baskets (defined by `--score-last-n-baskets`). This will output "next theme scores" for each customer into the `next_theme_scores[_latest]` table, featuring a global next theme probability and the raw score rebased to this global average for each customer.

Diagnostics:
- The basket item and theme history along with predictions can be obtained from this script by running it with the `--test-account` argument (if the account of interest was ABC123, you would pass `--test-account ABC123`).

### WIP - Greedy Assignment to give minimum volume to niche themes
- A function `Assignmet.greedy_batch_assignment()` is in development. The idea is that this would rank themes from least to most common and assignment of N customers would occur for themes sequentially. This would prevent customers with high scores across all themes being assigned the most common theme, and guarantee niche themes a minimum volume. Due to its sequential nature, this greedy assignment approach is currently slow, but offers a pragmatic solution to minimum volumes, when full optimisation (i.e. MIP or CP) might be overkill due to its computational expense.


## Summary

`python task_parse_attributes.py --set` refreshes `{schema}.{client}_nextads_attribute_set[_latest]`  
`python task_parse_attributes.py` refreshes `{schema}.{client}_nextads_item_attributes[_latest]`  

`python task_parse_theme_mapping.py --set` refreshes `{schema}.{client}_nextads_theme_mapping[_latest]` and `{schema}.{client}_nextads_item_themes[_latest]`  
`python task_parse_theme_mapping.py` refreshes `{schema}.{client}_nextads_item_themes[_latest]` (N.B. this refresh will respect any changes to the theme hierarchy in the Next Ads Control Sheet Google Sheet)  

`python task_build_markov_chain.py --train` refreshes `{schema}.{client}_nextads_theme_transitions[_latest]`  
`python task_build_markov_chain.py` refreshes `{schema}.{client}_nextads_item_next_theme_scores[_latest]`  
`python task_build_markov_chain.py --test-account ########` logs diagnostics for that account to the console  

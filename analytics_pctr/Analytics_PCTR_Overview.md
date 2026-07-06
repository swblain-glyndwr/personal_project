# Analytics PCTR model

### TODO: 
* Implementation of additional post-processing rules 
* Migrate feature build to dlt & feature store 
* Migrate over to V2 tagging once live 
* Include App shoppingbag app sessions and other locations once sufficient data available 
* Integrate fully to codebase with config & .py scripts


## Aim 
The aim of this project was to build a basic propensity-to-click-through (pCTR) model for known customers, focusing on a direct approach to identifying interactions with a specific advert. The initial scope is limited to ads displayed only on the shopping bag, as this is the location where ad impressions and click data are currently available for sufficient history through GA. Although a real-time approach is a long-term goal, the primary concern for the initial build is simplicity and establishing the basic pCTR model for a batch approach. 

## Key considerations
* The model is currently only trained with website sessions/ clicks, due to feed issues with app click and impression data.
* The scope for the page has been restricted to focus exclusively on the Shopping Basket, due to issues with click data for other pages - however this can be expanded for future 
* Data gaps on website click & impressions exist between Feb 18, 2026, and Mar 30, 2026. Training data is viable from Apr 29, 2026, onwards to satisfy the 30-day prior click/ impressions lookback requirement.
* Due to issues with naming convention for adverts feeding through from GA data we have had to utilise matching logic to map the GA data to the unique advertids. This will need to be reviwed in future as V2 goes live as it will not be applicable 
* In order not to lose click/ impression  history if an advert ad the same campaign number & title then the process will utilise the click/ impressions data from all adverts with this over the time window - so new adverts CTR slowly replace the prior advert overtime
* There is a dependency on the following table to enable simplification of geolocation data : `marketingdata_prod.search.nov_country_mapping`

## Current Modelling Process:

* Due to the advert and customer global features (such as customer propensity to click/ advert click through rates) having such a large impact we opted for a 2 stage approach to the modelling - utilising the global features to generate a 'popularity' model - to indicate popularity of advert/ customer overall likelihood to click and then an 'affinity' model trained on the residuals of the popularity model, only utilising affinity features built from customer interaction data to enable a refinement layer.  
* Smoothing is applied to the popularity model (to prevent low impression number, high CTR adverts from being over-represented) 
* The scores of both models are combined ( with a 4x weighting applied to the affinity model due to the difference in variance of the two model scores) . This is done in an additive manner to generate the final overall score & this is utilised for advert ranking 
* Note: the final overall score will not be on a 0-1 scale due to the weighting application 
* Additional decision points for splitting equally weighted scores are applied as part of the ranking (overall ad popularity & advert item revenue) for the small proportion of cases where 2 adverts result in the same combined score for a customer 
* Models utilised are SparkXGBoost Classifier & Regressor models for the popularity & affinity models respectively 

### Popularity model features:

* `cash_acc` - inidicator of whether the customer is a cash account customer
*  `advert_ctr`-  click through rate of the advert 
*  `device_ctr` - click through rate of the last used device type
*  `geo_ctr` -  click through rate of the last sessions geolocation
*  `gender_ctr` - click through rate of the users gender
*  `dod_ctr_change` - day on day change in CTR for specific advert
*  `wow_ctr_change` - week on week change in CTR for advert 
*  `age_imputed` - customer age (imputed with median)
*  `number_pages_viewed` - number of pages customer has viewed over last 30 days
*  `prior_30_day_order_value` - order value of customer over last 30 days
*  `customer_total_clicks` - number of advert clicks over last 30 days
*  `customer_total_unique_adverts_clicked` - number of unique adverts customer has clicked over last 30 days 
*  `customer_advert_previous_click_number` - how many times customer has clicked the specific advert in last 30 days
*  `number_clicks_same_algodivision` - how many adverts customer has clickedi n the algodivision in last 30 days
*  `advert_impressions` - number of impressions the advert has (30 days)
*  `device_impressions` - number of impressions the device type has had (30 days)
*  `geo_impressions` - number of impressions the simplified geolocation has (30 days)
*  `gender_impressions` - number of impressions the customer gender has (30 days)
*  `day_impressions` - number of impressions the advert has (last day)
*  `prior_day_impressions` -number of impressions the advert has (prior day)


### Affinity model features: 
* `view_theme_score` - the last viewed items relationship to the adverts themes decayed over time 
* `perc_order_value_cat_affinity` - the order value percentage associated with the advert category (year timescale)
* `perc_30_day_order_value_cat_affinity` - the order value percentage associated with the advert category 
* `perc_order_qty_cat_affinity` - the order quantity  percentage associated with the advert category (year timescale)
* `view_highest_catid_weight` - the advert association weight of the catid of last viewed item 
* `view_lift_adjusted` - the adverts lift of the catid for the last viewed item
* `purchase_highest_catid_weight` - the advert association weight of the catid of last purchased item 
* `purchase_lift_adjusted` - the adverts lift of the catid for the last purchased item
* `purchase_theme_affinity` -the last purchased items relationship to the advert themes


## Deployment 

Deployment has been set up in the job:  `mktg_next_uk_nextads_analytics_pctr` 

This is currently running independent of the existing NextAds jobs workflows. 

The job currently contains both the feature build stages as well as the prediction pipeline although these should be seperated out as part of further developments 

### Key Output tables: 
`next_uk_nextAds_analytics_pctr_predictions`-  history table for daily view of scored advert rankings 
`next_uk_nextAds_analytics_pctr_predictions_latest`-  latest run of the scoring for all elibgible accounts & adverts


## Training 

To retrain the model two datasets need to be built out; a training dataset & a validation dataset. These should be over sequential timeseries. The training dataset is purely built around the adverts & customers with impressions and clicks over the set timeperiod & is used to train, optimise and test the model. The validation dataset is built based on customers with impressisons in the timeframe but for all adverts available. This enables validation of the ranking output of the model. 

To build these datasets - the same SQL scripts ( with relevant start_date/end_date timeframes) from predictions can be utilised with the following changes: 
* 02_customer_advert_base-training or 02_customer_advert_base-validation instead of 02_customer_advertbase-predictions
* alterations of code in steps: 05_page_views, 06_purchases & 08_view_advert_affinity to utilise history table joins rather than latest tables
* alteration of table name in step 09 to _features_training
* running of step 10_history_insert with either _validation_history or _training_history as the table name dependent upon which dataset is being created

The train_model.py script has been generated to retrain a model & has the flexibility to expand out for further parameter testing and alternative models 

## Real-time migration considerations 

* A similar structure of model could be utilised for a real time model (for known cusotmers) where the last session details such as device type, last viewed items are swapped out in favour of  real time feeds

## Additional resources: 

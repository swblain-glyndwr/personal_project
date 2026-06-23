# Analytics PCTR model

### TODO: 
* Migrate feature build to feature store 
* Migrate over to V2 tagging once live 
* Integrate fully to codebase 
* Expand out to other locations

## Aim 
The aim of this project was to build a basic propensity-to-click-through (pCTR) model for known customers, focusing on a direct approach to identifying interactions with a specific advert. The initial scope is limited to ads displayed only on the shopping bag, as this is the location where ad impressions and click data are currently available through GA. Although a real-time approach is a long-term goal, the primary concern for the initial build is simplicity and establishing the basic pCTR model for a batch approach. 

## Key considerations
* The model is currently only trained with website sessions/ clicks, due to feed issues with app click and impression data.
* The scope for the page has been restricted to focus exclusively on the Shopping Basket, due to issues with click data for other pages - however this can be expanded for future 
* Data gaps on website click & impressions exist between Feb 18, 2026, and Mar 30, 2026. Training data is viable from Apr 29, 2026, onwards to satisfy the 30-day prior click/ impressions lookback requirement.
* Due to issues with naming convention for adverts feeding through from GA data we have had to utilise matching logic to map the GA data to the uniqueadvertids. This will need to be reviwed in future as V2 goes live as it will not be applicable 
* In order not to lose click/ impression  history if an advert ad the same campaign number & title then the process will utilise the click/ impressions data from all adverts with this over the time window - so new adverts CTR slowly replace the prior advert overtime

## Current Modelling Process:
* Due to the advert and customer global features (such as customer propensity to click/ advert click through rates) having such a large impact we opted for a 2 stage approach to the modelling - utilising the global features to generate a 'popularity' model - to indicate popularity of advert/ customer overall likelihood to click and then an 'affinity' model trained on the residuals of the popularity model, only utilising affinity features built from customer interaction data to enable a refinement layer.  
* Smoothing is applied to the popularity model (to prevent low impression number, high CTR adverts from being over-represented) 
* The scores of both models are combined ( with a 4x weighting applied to the affinity model due to the difference in variance of the two model scores) . This is done in an additive manner to generate the final overall score & this is utilised for advert ranking 
* Note: the final overall score will not be on a 0-1 scale due to the weighting application 
* Additional decision points for splitting equally weighted scores are applied as part of the ranking (overall ad popularity & advert item revenue) for the small proportion of cases where 2 adverts result in the same combined score for a customer 

## Predictions

Deployment has been set up in the job: `mktg_next_uk_nextads_analytics_pctr`

This currently contains both the feature build stages as well as the prediction pipeline. 

### Output tables: 
`next_uk_nextAds_analytics_pctr_predictions`-  history table for daily view of scored advert rankings 
`next_uk_nextAds_analytics_pctr_predictions_latest`-  latest run of the scoring for all elibgible accounts & adverts


## Training 
To retrain the model two datasets need to be built out; a training dataset & a validation dataset. These should be over sequential timeseries. The training dataset is purely built around the adverts & customers with impressions and clicks over the set timeperiod & is used to train, optimise and test the model. The validation dataset is built based on customers with impressisons in the timeframe but for all adverts available. This enables validation of the ranking output of the model. 

To build these datasets - the same SQL scripts ( with relevant start_date/end_date timeframes) from predictions can be utilised with the following changes: 
* 02_customer_advert_base-training or 02_customer_advert_base-validation instead of 02_customer_advertbase-predictions
* alterations of code in steps: 05_page_views, 06_purchases & 08_view_advert_affinity to utilise history table joins rather than latest tables
* alteration of table name in step 09 to _features_training
* running of step 10_history_insert or 10_validation_history_insert 

The train_model.py script has been generated to retrain a model & has the flexibility to expand out for further parameter testing and alternative models 






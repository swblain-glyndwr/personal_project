create table {catalog}.{schema}.{client}_nextads_analytics_pctr_predictions_latest (
     account_number string
    , uniqueAdID string 
    , popularity_smoothed_score double
    , regression_weighted_score double
    , popularity_prob_click double
    , residual_predictions double
    , combined_weighted_score double
    , weighted_ranking int
    , advert_impressions_30days int
    , advert_item_revenue double    
    , rundate date
    , constraint pk_{client}_nextads_analytics_pctr_predictions_latest PRIMARY KEY (rundate , account_number , uniqueAdID)
)
partitioned by (rundate)
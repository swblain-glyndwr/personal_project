-- Databricks notebook source

-- CREATE WIDGETS
CREATE WIDGET TEXT catalog_schema_prefix  'marketingdata_dev.ds_sandbox';
CREATE WIDGET TEXT table_prefix  'next_uk_nextAds_analytics_pctr';


SET spark.sql.adaptive.enabled = true;
SET spark.sql.execution.arrow.pyspark.enabled = true;



/* Training History */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_history')
 (  
   rundate date
    , account_number string
    , uniqueAdID string 
    , potnumber string 
    , campaignnumber string 
    , seasonal_flag int
    , total_ad_impressions int
    , total_ad_perc_impressions double
    , total_ad_cumulative_perc_impressions double
    , customer_lifespan int
    , age int
    , gender STRING
    , postcodearea string
    , mailoptout int
    , cash_acc int
    , staff_indicator int
    , total_time_on_site int
    , total_sessions int 
    , avg_site_time DOUBLE
    , med_time_onsite double 
    , advert_ctr double 
    , device_ctr double 
    , geo_ctr double 
    , channel_ctr double 
    , dayofweek_ctr double 
    , gender_ctr double 
    , dod_ctr_change double 
    , wow_ctr_change double 
    , perc_viewtimedow double 
    , number_departments_viewed int
    , number_pages_viewed int 
    , number_pages_viewed_last_week int
    , purchase_theme_affinity double 
    , view_theme_score double 
    , perc_order_value_cat_affinity double 
    , perc_30_day_order_value_cat_affinity double
    , highest_spend_cat_alignment int
    , perc_order_qty_cat_affinity double 
    , total_order_value double 
    , prior_30_day_order_value double 
    , total_number_items int
    , customer_total_impressions int
    , customer_total_unique_adverts int
    , customer_total_clicks int
    , customer_total_unique_adverts_clicked int
    , customer_advert_previous_impression_number int
    , customer_advert_previous_click_number int 
    , number_algodivisions_clicked int
    , number_algodivisions_impressions int
    , number_impressions_same_algodivision int
    , number_clicks_same_algodivision int
    , number_unique_adverts_same_algodivision int 
    , number_unique_adverts_clicked_same_algodivision int
, view_highest_catid_weight double
, view_support12 double
, view_support1 double
, view_support2 double
, view_lift double
, view_lift_adjusted double
, view_cs double
, purchase_highest_catid_weight double
, purchase_support12 double
, purchase_support1  double
, purchase_support2 double
, purchase_lift double
, purchase_lift_adjusted double
, purchase_cs double
, advert_impressions int
, device_impressions int
, geo_impressions int 
, channel_impressions int 
, dayofweek_impressions int
, gender_impressions int
, day_impressions int 
, prior_day_impressions  int
, week_impressions int
, prior_week_impressions int
    , ad_clicked int
    , treatment_type STRING
    , location int
    , PRIMARY KEY (rundate , account_number , uniqueAdID)
 );

/* Validation History */
CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_validation_history')
(  
   rundate date
    , account_number string
    , uniqueAdID string 
    , potnumber string 
    , campaignnumber string 
    , seasonal_flag int
    , total_ad_impressions int
    , total_ad_perc_impressions double
    , total_ad_cumulative_perc_impressions double
    , customer_lifespan int
    , age int
    , gender STRING
    , postcodearea string
    , mailoptout int
    , cash_acc int
    , staff_indicator int
    , total_time_on_site int
    , total_sessions int 
    , avg_site_time DOUBLE
    , med_time_onsite double 
    , advert_ctr double 
    , device_ctr double 
    , geo_ctr double 
    , channel_ctr double 
    , dayofweek_ctr double 
    , gender_ctr double 
    , dod_ctr_change double 
    , wow_ctr_change double 
    , perc_viewtimedow double 
    , number_departments_viewed int
    , number_pages_viewed int 
    , number_pages_viewed_last_week int
    , purchase_theme_affinity double 
    , view_theme_score double 
    , perc_order_value_cat_affinity double 
    , perc_30_day_order_value_cat_affinity double
    , highest_spend_cat_alignment int
    , perc_order_qty_cat_affinity double 
    , total_order_value double 
    , prior_30_day_order_value double 
    , total_number_items int
    , customer_total_impressions int
    , customer_total_unique_adverts int
    , customer_total_clicks int
    , customer_total_unique_adverts_clicked int
    , customer_advert_previous_impression_number int
    , customer_advert_previous_click_number int 
    , number_algodivisions_clicked int
    , number_algodivisions_impressions int
    , number_impressions_same_algodivision int
    , number_clicks_same_algodivision int
    , number_unique_adverts_same_algodivision int 
    , number_unique_adverts_clicked_same_algodivision int
, view_highest_catid_weight double
, view_support12 double
, view_support1 double
, view_support2 double
, view_lift double
, view_lift_adjusted double
, view_cs double
, purchase_highest_catid_weight double
, purchase_support12 double
, purchase_support1  double
, purchase_support2 double
, purchase_lift double
, purchase_lift_adjusted double
, purchase_cs double
, advert_impressions int
, device_impressions int
, geo_impressions int 
, channel_impressions int 
, dayofweek_impressions int
, gender_impressions int
, day_impressions int 
, prior_day_impressions  int
, week_impressions int
, prior_week_impressions int
    , ad_clicked int
    , treatment_type STRING
    , location int
    , PRIMARY KEY (rundate , account_number , uniqueAdID)
 );

/* Predictions History */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_predictions')
(  
     
     account_number string
    , uniqueAdID string 
    , popularity_smoothed_score double
    , regression_weighted_score double
    , popularity_prob_click double
    , residual_predictions double
    , combined_weighted_score double
    , weighted_ranking int
    , advert_impressions_30days int
    , advert_item_revenue double    , rundate date
    , PRIMARY KEY (rundate , account_number , uniqueAdID)
 );

/* Predictions Latest */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_predictions_latest')
(  
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
    , PRIMARY KEY (rundate , account_number , uniqueAdID)
 );
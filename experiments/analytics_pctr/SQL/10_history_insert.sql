-- Databricks notebook source

-- CREATE WIDGETS
CREATE WIDGET TEXT catalog_schema_prefix DEFAULT 'OUTPUT_LOCATION_REQUIRED';
CREATE WIDGET TEXT table_prefix DEFAULT 'next_uk_nextAds_analytics_pctr';
CREATE WIDGET TEXT output_table_name DEFAULT '_features';
CREATE WIDGET TEXT table_name DEFAULT '_training_history';


SET spark.sql.adaptive.enabled = true;
SET spark.sql.execution.arrow.pyspark.enabled = true;


MERGE INTO
  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || :table_name) AS t
USING  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || :output_table_name) AS f
ON (t.rundate = f.rundate 
AND t.account_number = f.account_number 
AND t.UniqueAdID = f.UniqueAdID)
-- 1. If a match is found, update the existing row
WHEN MATCHED THEN
    UPDATE SET 
      t.rundate=f.rundate
    , t.account_number= f.account_number
    , t.uniqueAdID=f.UniqueAdID
    , t.potnumber=f.potnumber
    , t.campaignnumber=f.campaignnumber
    , t.seasonal_flag=f.seasonal_flag
    , t.total_ad_impressions=f.total_ad_impressions
    , t.total_ad_perc_impressions=f.total_ad_perc_impressions
    , t.total_ad_cumulative_perc_impressions=f.total_ad_cumulative_perc_impressions
    , t.customer_lifespan=f.customer_lifespan
    , t.age=f.age 
    , t.gender=f.gender
    , t.postcodearea=f.postcodearea
    , t.mailoptout=f.mail_optout
    , t.cash_acc=f.cash_acc
    , t.staff_indicator=f.staff_indicator
    , t.total_time_on_site=f.total_time_on_site
    , t.total_sessions=f.total_sessions
    , t.avg_site_time=f.avg_site_time
    , t.med_time_onsite=f.med_time_onsite
    , t.advert_ctr=f.advert_ctr
    , t.device_ctr=f.device_ctr
    , t.geo_ctr=f.geo_ctr
    , t.channel_ctr=f.channel_ctr
    , t.dayofweek_ctr=f.dayofweek_ctr
    , t.gender_ctr=f.gender_ctr
    , t.dod_ctr_change=f.dod_ctr_change
    , t.wow_ctr_change=f.wow_ctr_change
    , t.perc_viewtimedow=f.perc_viewtimedow
    , t.number_departments_viewed=f.number_departments_viewed
    , t.number_pages_viewed=f.number_pages_viewed
    , t.number_pages_viewed_last_week=f.number_pages_viewed_last_week
    , t.purchase_theme_affinity=f.purchase_theme_affinity
    , t.view_theme_score=f.view_theme_score
    , t.perc_order_value_cat_affinity=f.perc_order_value_cat_affinity
    , t.perc_30_day_order_value_cat_affinity=f.perc_30_day_order_value_cat_affinity
    , t.highest_spend_cat_alignment=f.highest_spend_cat_alignment
    , t.perc_order_qty_cat_affinity=f.perc_order_qty_cat_affinity
    , t.total_order_value=f.total_order_value
    , t.prior_30_day_order_value=f.prior_30_day_order_value
    , t.total_number_items=f.total_number_items
    , t.customer_total_impressions=f.customer_total_impressions
    , t.customer_total_unique_adverts=f.customer_total_unique_adverts
    , t.customer_total_clicks=f.customer_total_clicks
    , t.customer_total_unique_adverts_clicked =f.customer_total_unique_adverts_clicked
    , t.customer_advert_previous_impression_number =f.customer_advert_previous_impression_number
    , t.customer_advert_previous_click_number =f.customer_advert_previous_click_number
    , t.number_algodivisions_clicked = f.number_algodivisions_clicked
    , t.number_algodivisions_impressions = f.number_algodivisions_impressions
    , t.number_impressions_same_algodivision = f.number_impressions_same_algodivision
    , t.number_clicks_same_algodivision = f.number_clicks_same_algodivision
    , t.number_unique_adverts_same_algodivision = f.number_unique_adverts_same_algodivision
    , t.number_unique_adverts_clicked_same_algodivision = f.number_unique_adverts_clicked_same_algodivision
    , t.ad_clicked=f.ad_clicked
    , t.treatment_type=f.treatment_type
    , t.location=f.location

, t.view_highest_catid_weight=f.view_highest_catid_weight
, t.view_support12=f.view_support12
, t.view_support1=f.view_support1
, t.view_support2=f.view_support2
, t.view_lift=f.view_lift
, t.view_lift_adjusted=f.view_lift_adjusted
, t.view_cs=f.view_cs
, t.purchase_highest_catid_weight=f.purchase_highest_catid_weight
, t.purchase_support12=f.purchase_support12
, t.purchase_support1 =f.purchase_support1
, t.purchase_support2=f.purchase_support2
, t.purchase_lift=f.purchase_lift
, t.purchase_lift_adjusted=f.purchase_lift_adjusted
, t.purchase_cs=f.purchase_cs
,  t.advert_impressions=f.advert_impressions
, t.device_impressions=f.device_impressions
, t.geo_impressions=f.geo_impressions
, t.channel_impressions=f.channel_impressions
, t.dayofweek_impressions=f.dayofweek_impressions
, t.gender_impressions=f.gender_impressions
,t.day_impressions=f.day_impressions
, t.prior_day_impressions=f.prior_day_impressions
, t.week_impressions =f.week_impressions
, t.prior_week_impressions =f.prior_week_impressions
-- 2. If no match is found, insert a brand new row
WHEN NOT MATCHED THEN
    INSERT (rundate 
    , account_number 
    , uniqueAdID  
    , potnumber  
    , campaignnumber  
    , seasonal_flag 
    ,total_ad_impressions
    , total_ad_perc_impressions
    , total_ad_cumulative_perc_impressions
    , customer_lifespan 
    , age 
    , mailoptout 
    , cash_acc 
    , staff_indicator
    , total_time_on_site 
    , total_sessions  
    , avg_site_time 
    , med_time_onsite  
    , advert_ctr  
    , device_ctr  
    , geo_ctr  
    , channel_ctr  
    , dayofweek_ctr  
    , gender_ctr  
    , dod_ctr_change  
    , wow_ctr_change  
    , perc_viewtimedow  
    , number_departments_viewed 
    , number_pages_viewed  
    , number_pages_viewed_last_week 
    , purchase_theme_affinity  
    , view_theme_score  
    , perc_order_value_cat_affinity  
    , perc_30_day_order_value_cat_affinity 
    , highest_spend_cat_alignment 
    , perc_order_qty_cat_affinity  
    , total_order_value  
    , prior_30_day_order_value
    , total_number_items 
    , customer_total_impressions 
    , customer_total_unique_adverts 
    , customer_total_clicks 
    , customer_total_unique_adverts_clicked 
    , customer_advert_previous_impression_number 
    , customer_advert_previous_click_number  
    , number_algodivisions_clicked 
    , number_algodivisions_impressions 
    , number_impressions_same_algodivision 
    , number_clicks_same_algodivision 
    , number_unique_adverts_same_algodivision  
    , number_unique_adverts_clicked_same_algodivision 
    
, view_highest_catid_weight
, view_support12
, view_support1
, view_support2
, view_lift
, view_lift_adjusted
, view_cs
, purchase_highest_catid_weight
, purchase_support12
, purchase_support1 
, purchase_support2
, purchase_lift
, purchase_lift_adjusted
, purchase_cs
, advert_impressions
, device_impressions
, geo_impressions
, channel_impressions
, dayofweek_impressions
, gender_impressions
, day_impressions
, prior_day_impressions 
, week_impressions 
, prior_week_impressions 
    , ad_clicked
    ,treatment_type
    , location )
    VALUES (
      f.rundate
      , f.account_number
      , f.uniqueAdID
      , f.potnumber
      , f.campaignnumber
      , f.seasonal_flag
      , f.total_ad_impressions
      , f.total_ad_perc_impressions
      , f.total_ad_cumulative_perc_impressions
      , f.customer_lifespan
      , f.age 
      , f.mail_optout
      , f.cash_acc
      , f.staff_indicator
      , f.total_time_on_site
      , f.total_sessions
      , f.avg_site_time
      , f.med_time_onsite
      , f.advert_ctr
      , f.device_ctr
      , f.geo_ctr
      , f.channel_ctr
      , f.dayofweek_ctr
      , f.gender_ctr
      , f.dod_ctr_change
      , f.wow_ctr_change
      , f.perc_viewtimedow
      , f.number_departments_viewed
      , f.number_pages_viewed
      , f.number_pages_viewed_last_week
      , f.purchase_theme_affinity
      , f.view_theme_score
      , f.perc_order_value_cat_affinity
      , f.perc_30_day_order_value_cat_affinity
      , f.highest_spend_cat_alignment
      , f.perc_order_qty_cat_affinity
      , f.total_order_value
      , f.prior_30_day_order_value
      , f.total_number_items
      , f.customer_total_impressions 
    , f.customer_total_unique_adverts 
    , f.customer_total_clicks 
    , f.customer_total_unique_adverts_clicked 
    , f.customer_advert_previous_impression_number 
    , f.customer_advert_previous_click_number  
    , f.number_algodivisions_clicked 
    , f.number_algodivisions_impressions 
    , f.number_impressions_same_algodivision 
    , f.number_clicks_same_algodivision 
    , f.number_unique_adverts_same_algodivision  
    , f.number_unique_adverts_clicked_same_algodivision
        
, f.view_highest_catid_weight
, f.view_support12
, f.view_support1
, f.view_support2
, f.view_lift
, f.view_lift_adjusted
, f.view_cs
, f.purchase_highest_catid_weight
, f.purchase_support12
, f.purchase_support1 
, f.purchase_support2
, f.purchase_lift
, f.purchase_lift_adjusted
, f.purchase_cs
, f.advert_impressions
, f.device_impressions
, f.geo_impressions
, f.channel_impressions
, f.dayofweek_impressions
, f.gender_impressions
, f.day_impressions
, f.prior_day_impressions 
, f.week_impressions 
, f.prior_week_impressions 
      , f.ad_clicked
      , f.treatment_type
      , f.location);

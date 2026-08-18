-- Databricks notebook source

-- CREATE WIDGETS
CREATE WIDGET TEXT catalog_schema_prefix DEFAULT 'OUTPUT_LOCATION_REQUIRED';
CREATE WIDGET TEXT table_prefix DEFAULT 'next_uk_nextAds_analytics_pctr';
CREATE WIDGET TEXT lookback_period DEFAULT '30';

SET spark.sql.adaptive.enabled = true;
SET spark.sql.execution.arrow.pyspark.enabled = true;

/* Customer AlgoDivision click data */ 

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_advert_algodivision_impressions') AS (
WITH cte_advert_algodivisions AS (
SELECT 
AdvertID, 
date, 
Algodivision
 FROM  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_clicks_lookback')
GROUP BY 
AdvertID, 
date, 
Algodivision
)
SELECT 
      c.account_number
    , c.rundate 
    , ad.algodivision
    , SUM(CASE WHEN a.action =  'Banner Impression - Next Ads'  THEN 1 ELSE 0 END ) AS number_impressions
    , COUNT(DISTINCT CASE WHEN a.action =  'Banner Impression - Next Ads' THEN a.AdvertID END ) AS number_unique_adverts_impressions
    , SUM(CASE WHEN a.action =  'Banner Click - Next Ads' THEN 1 ELSE 0 END ) AS number_clicks
    , COUNT(DISTINCT CASE WHEN a.action =  'Banner Click - Next Ads' THEN a.AdvertID END ) AS number_unique_adverts_clicks
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_customer_base') AS c
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_ad_clicks_impressions_base') AS a 
        ON c.account_number=a.account_number
        AND  a.date BETWEEN c.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND c.rundate - INTERVAL '1' DAY
    LEFT JOIN cte_advert_algodivisions AS ad 
        ON ad.AdvertID=a.AdvertID
        AND ad.date=a.date
GROUP BY 
     c.account_number
    , c.rundate 
    , ad.algodivision
);


/* Customer Advert Exposure */
 
CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_customer_advert_impressions') AS (
WITH cte_overall AS (
SELECT 
    c.account_number
    , c.rundate 
    , SUM(CASE WHEN a.action =  'Banner Impression - Next Ads'  THEN 1 ELSE 0 END ) AS number_impressions
    , COUNT(DISTINCT CASE WHEN a.action =  'Banner Impression - Next Ads' THEN a.AdvertID END ) AS number_unique_adverts
    , SUM(CASE WHEN a.action =  'Banner Click - Next Ads' THEN 1 ELSE 0 END ) AS number_clicks
    , COUNT(DISTINCT CASE WHEN a.action =  'Banner Click - Next Ads' THEN a.AdvertID END ) AS number_unique_adverts_clicked
FROM
 IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_customer_base') AS c
 INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_ad_clicks_impressions_base') AS a 
    ON c.account_number=a.account_number
   AND  a.date BETWEEN c.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND c.rundate - INTERVAL '1' DAY
GROUP BY 
  c.account_number
    , c.rundate 
)
,cte_advert_prior_stats  AS (
SELECT 
      c.account_number
    , c.rundate 
    , c.control_sheet_AdID
    , SUM(CASE WHEN  a.action =  'Banner Impression - Next Ads' THEN 1 ELSE 0 END) AS advert_previous_impression_number
    , SUM(CASE WHEN a.action =  'Banner Click - Next Ads' THEN 1 ELSE 0 END) AS advert_previous_click_number
FROM
  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base') AS c

 INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_ad_clicks_impressions_base') AS a 
    ON c.account_number=a.account_number
    AND c.pot=a.pot 
    AND c.campaign=a.campaign
    AND c.versionnumber= a.versionnumber
   AND  a.date BETWEEN c.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND c.rundate - INTERVAL '1' DAY
GROUP BY 
       c.account_number
    , c.rundate 
    , c.control_sheet_AdID
) 
SELECT 
      c.account_number
    , c.rundate 
    , c.control_sheet_AdID
    , COALESCE(o.number_impressions,0)AS customer_total_impressions
    , COALESCE(o.number_unique_adverts,0) AS customer_total_unique_adverts
    , COALESCE(o.number_clicks,0) AS customer_total_clicks
    , COALESCE(o.number_unique_adverts_clicked,0) AS customer_total_unique_adverts_clicked
    , COALESCE(aps.advert_previous_impression_number,0) AS customer_advert_previous_impression_number
    , COALESCE(aps.advert_previous_click_number,0) AS customer_advert_previous_click_number
    , COUNT(DISTINCT CASE WHEN i.number_clicks> 0 THEN i.algodivision END) AS number_algodivisions_clicked
    , COUNT(DISTINCT CASE WHEN i.number_impressions> 0 THEN i.algodivision END) AS number_algodivisions_impressions
    , SUM(CASE WHEN i.algodivision=a.algodivision THEN i.number_impressions ELSE 0 END) AS number_impressions_same_algodivision
    , SUM(CASE WHEN i.algodivision=a.algodivision THEN i.number_clicks ELSE 0 END) AS number_clicks_same_algodivision
    , SUM(CASE WHEN i.algodivision=a.algodivision THEN i.number_unique_adverts_impressions ELSE 0 END) AS number_unique_adverts_same_algodivision
    , SUM(CASE WHEN i.algodivision=a.algodivision THEN i.number_unique_adverts_clicks ELSE 0 END) AS number_unique_adverts_clicked_same_algodivision
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base') AS c
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ads_base') AS a 
        ON a.uniqueadid=c.control_sheet_adid 
        AND  a.rundate= c.rundate
    LEFT JOIN cte_overall AS o 
        ON o.account_number=c.account_number
        AND o.rundate=c.rundate
    LEFT JOIN cte_advert_prior_stats AS aps 
        ON aps.account_number=c.account_number
        AND aps.rundate=c.rundate
        AND aps.control_sheet_AdID=c.control_sheet_AdID
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_advert_algodivision_impressions') AS i
        ON i.account_number=c.account_number
        AND i.rundate=c.rundate
GROUP BY 
      c.account_number
    , c.rundate 
    , c.control_sheet_AdID
    , customer_total_impressions
    ,  customer_total_unique_adverts
    , customer_total_clicks
    , customer_total_unique_adverts_clicked
    ,  customer_advert_previous_impression_number
    ,  customer_advert_previous_click_number
);

-- Databricks notebook source

-- CREATE WIDGETS
CREATE WIDGET TEXT catalog_schema_prefix DEFAULT 'OUTPUT_LOCATION_REQUIRED';
CREATE WIDGET TEXT table_prefix DEFAULT 'next_uk_nextAds_analytics_pctr';
CREATE WIDGET TEXT lookback_period DEFAULT '30';

SET spark.sql.adaptive.enabled = true;
SET spark.sql.execution.arrow.pyspark.enabled = true;

/* All Feature level aggregations of CTR */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_summary') AS (
SELECT 
      d.rundate
    , 'Overall' AS split_type
    , 'All' AS feature_name
    -- , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
    -- Take overall CTR for all ads in period if unknown 
    , SUM(a.number_clicks)/ SUM(a.number_impressions) AS ctr
    , SUM(a.number_impressions) AS num_impressions
    , SUM(a.number_clicks) AS num_clicks
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates') AS d
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_clicks_lookback') AS a
        ON a.date BETWEEN d.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND d.rundate - INTERVAL '1' DAY
GROUP BY    
      d.rundate
    -- , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
    , feature_name
    , split_type 
--Device Type
UNION 
SELECT 
    d.rundate
    , 'Device' AS split_type
    , a.device_simple AS feature_name
    -- , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
    , SUM(number_clicks)/SUM(number_impressions) AS ctr 
    , SUM(a.number_impressions) AS num_impressions
    , SUM(a.number_clicks) AS num_clicks
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates')  AS d
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_clicks_lookback') AS a
        ON a.date BETWEEN d.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND d.rundate - INTERVAL '1' DAY
GROUP BY    
      device_simple
    --  , a.control_sheet_AdID
     , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
    , split_type
    , d.rundate

UNION
--Channel ctr
SELECT 
      d.rundate
    , 'Channel' AS split_type
    , channel_simple AS feature_name
    -- , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
    , SUM(number_clicks)/SUM(number_impressions)AS ctr 
    , SUM(a.number_impressions) AS num_impressions
    , SUM(a.number_clicks) AS num_clicks
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates')  AS d
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_clicks_lookback') AS a
        ON a.date BETWEEN d.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND d.rundate - INTERVAL '1' DAY
GROUP BY    
      split_type
    , feature_name
    -- , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
    , d.rundate
UNION
-- Geo-clicks 
SELECT 
     d.rundate
    , 'GeoCountry' AS split_type
     , geocountry_simple AS feature_name
    -- , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
    , SUM(number_clicks)/SUM(number_impressions) AS ctr 
    , SUM(a.number_impressions) AS num_impressions
    , SUM(a.number_clicks) AS num_clicks
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates')  AS d
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_clicks_lookback') AS a
        ON a.date BETWEEN d.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND d.rundate - INTERVAL '1' DAY

GROUP BY    
     split_type
    , feature_name
    -- , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
    , d.rundate

UNION 
    -- DOW 
SELECT 
    d.rundate
    , 'DOW' AS split_type
    , dow AS feature_name
    -- , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
    , SUM(number_clicks)/SUM(number_impressions) AS ctr 
    , SUM(a.number_impressions) AS num_impressions
    , SUM(a.number_clicks) AS num_clicks
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates')  AS d
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_clicks_lookback') AS a
        ON a.date BETWEEN d.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND d.rundate - INTERVAL '1' DAY 
GROUP BY    
     split_type
    , feature_name
    -- , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
    , d.rundate
UNION 
-- Gender 
SELECT 
    d.rundate
    , 'Gender' AS split_type
     , gender AS feature_name
    -- , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
    , SUM(number_clicks)/SUM(number_impressions) AS ctr 
    , SUM(a.number_impressions) AS num_impressions
    , SUM(a.number_clicks) AS num_clicks
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates')  AS d
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_clicks_lookback') AS a
        ON a.date BETWEEN d.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND d.rundate - INTERVAL '1' DAY

GROUP BY    
    d.rundate
    , split_type
    , feature_name
    -- , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
);

/* Imputation of feature level CTR */ 

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_imputation') AS  (
SELECT 
    split_type
    ,feature_name 
    , algodivision
    , PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ctr) AS med_ctr
    , PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY num_impressions) AS med_impressions
    , rundate
FROM
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ctr_summary') 
GROUP BY 
      split_type
    , feature_name 
    , algodivision
    , rundate
);

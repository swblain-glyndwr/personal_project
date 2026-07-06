-- Databricks notebook source

-- CREATE WIDGETS
CREATE WIDGET TEXT catalog_schema_prefix DEFAULT 'marketingdata_dev.ds_sandbox';
CREATE WIDGET TEXT table_prefix DEFAULT 'next_uk_nextAds_analytics_pctr';
CREATE WIDGET TEXT output_table_name DEFAULT '_features';
CREATE WIDGET TEXT table_name DEFAULT '_training_history';


/* Rules based ranking */ 

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_rulesbasedranking') AS (
WITH cte_purchase_affinity AS(
    SELECT 
        max(purchase_theme_affinity) AS max_purchase_theme
        , min(purchase_theme_affinity) AS min_purchase_theme
    FROM 
        IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_all_features') 
)
, cte_scoring AS (
SELECT 
    * 
,  -- Negative weighting of ones unlikely to click first
    CASE WHEN view_theme_score = 0 AND purchase_theme_affinity = 0  AND number_pages_viewed< 5 THEN -0.5 ELSE 0 END 
    -- 1. Last Items viewed by Theme affinity
    +  view_theme_score *2 
    -- 2. Page views  (browsing behaviour indication- bucketed )
    + CASE WHEN number_pages_viewed > 100 THEN +0.2 
            WHEN number_pages_viewed > 50 THEN +0.1
            WHEN number_pages_viewed > 25 THEN +0.05 
            ELSE 0
      END 
     -- 3. DOW CTR vs general ctr 
    + CASE WHEN dayofweek_ctr > advert_ctr THEN 0.1 ELSE 0 END   
    -- 4. DOD CTR change 
    + CASE WHEN dod_ctr_change > 0.001 THEN 0.1 ELSE 0 END 
    -- 5. Total Sessions behaviour
    + CASE WHEN total_sessions > 5 THEN 0.1 ELSE 0 END
    -- 6. Spread of spend over departments 
    + number_departments_viewed /MAX(number_departments_viewed) OVER() * 0.1
    -- 7. Puchase Theme Affinity normalised (buying behaviour)
    +  COALESCE((purchase_theme_affinity - pa.min_purchase_theme) * 1.0 / NULLIF( pa.max_purchase_theme - pa.min_purchase_theme,0), 0)  
    -- 8. Affinity of order volume to category 
    + perc_order_qty_cat_affinity *0.1 
    -- 9. Pages viewed prior week
    + CASE WHEN number_pages_viewed >0 THEN number_pages_viewed_last_week/ number_pages_viewed *0.5 ELSE 0 END 
    -- CTR specific metrics 
    --  Advert CTR 
    + CASE WHEN advert_ctr > 0.05 THEN 0.2
        WHEN advert_ctr > 0.01 THEN 0.1 
        ELSE 0 END 
    -- 4. CTR aspects vs general ctr 
    + CASE WHEN gender_ctr > advert_ctr THEN 0.1 ELSE 0 END 
    + CASE WHEN channel_ctr > advert_ctr THEN 0.01 ELSE 0 END 
    + CASE WHEN device_ctr > advert_ctr THEN 0.1 ELSE 0 END 
    + CASE WHEN geo_ctr > advert_ctr THEN 0.01 ELSE 0 END 
    -- Ad Stability/ Change 
    + CASE WHEN wow_ctr_change BETWEEN  -0.000001 AND 0.000001 THEN  0.05 ELSE 0 END 
    AS advert_scoring
FROM 
    cte_purchase_affinity AS pa, 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_all_features')  AS af 
)
SELECT 
    * 
    , ROW_NUMBER() OVER (PARTITION BY account_number, rundate
                ORDER BY advert_scoring DESC) AS score_based_advert_ranking
    , ROW_NUMBER() OVER (PARTITION BY account_number, rundate
                ORDER BY 
                    -- Rank to bottom if theme score AND purchase affinity is zero 
                    --( remove a lot of what could be false positives)
                    CASE WHEN view_theme_score = 0 AND purchase_theme_affinity = 0 THEN 0 ELSE 1 END DESC,
                    -- latest views 
                    view_theme_score DESC,
                    -- ctr related to segments 
                    gender_ctr + device_ctr +  dayofweek_ctr + channel_ctr + geo_ctr  DESC, 
                    -- Page views 
                    number_pages_viewed DESC,
                    -- Order affinity 
                    perc_order_value_cat_affinity DESC
                  ) AS basic_ranking
FROM 
    cte_scoring AS s 
);


/*  Basic Ranking metrics */


WITH cte_ctr AS (
SELECT
 CASE WHEN basic_ranking IN (1,2) THEN 1 ELSE 0 END AS basic_ranking_1
, SUM(ad_clicked)/ COUNT(ad_clicked) AS ctr 
, COUNT(*) AS total_number_predicted
, SUM(ad_clicked) AS total_number_clicked
FROM
 IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_rulesbasedranking') 
WHERE ad_Clicked IS NOT NULL  
GROUP BY 
  basic_ranking_1
)
SELECT 
  SUM(total_number_clicked) FILTER (WHERE basic_ranking_1=1) / 
  SUM(total_number_clicked + total_number_predicted) FILTER (WHERE basic_ranking_1=1) AS precision 
  , SUM(total_number_clicked) FILTER (WHERE basic_ranking_1=1) / 
  SUM(total_number_clicked) AS recall
FROM cte_ctr;

/* Scoring CTR metrics */ 


WITH cte_ctr AS ( 
SELECT
 CASE WHEN score_based_advert_ranking IN (1,2) THEN 1 ELSE 0 END AS score_ranking_1
  --Calculating the CTR (of ones in position 1 from ranking vs in all other positions)
, SUM(ad_clicked)/ COUNT(ad_clicked) AS ctr 
, COUNT(*) AS total_number_predicted
, SUM(ad_clicked) AS total_number_clicked
FROM
 IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_rulesbasedranking') 
WHERE ad_Clicked IS NOT NULL  
GROUP BY 
  score_ranking_1
)
SELECT 
  SUM(total_number_clicked) FILTER (WHERE score_ranking_1=1) / 
  SUM(total_number_clicked + total_number_predicted) FILTER (WHERE score_ranking_1=1) AS precision 
  , SUM(total_number_clicked) FILTER (WHERE score_ranking_1=1) / 
  SUM(total_number_clicked) AS recall
FROM cte_ctr
;
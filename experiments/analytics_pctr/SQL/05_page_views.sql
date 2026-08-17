-- Databricks notebook source

-- CREATE WIDGETS
CREATE WIDGET TEXT catalog_schema_prefix DEFAULT 'OUTPUT_LOCATION_REQUIRED';
CREATE WIDGET TEXT table_prefix DEFAULT 'next_uk_nextAds_analytics_pctr';
CREATE WIDGET TEXT lookback_period DEFAULT '30';

SET spark.sql.adaptive.enabled = true;
SET spark.sql.execution.arrow.pyspark.enabled = true;


/* Aggregated Page Views Metrics */ 

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_views_aggregated') AS (
SELECT 
      pv.account_number 
    , c.rundate
    , COALESCE(SUM(pv.viewtimespentsecs) FILTER (WHERE DAYOFWEEK(pv.timestamp)= dayofweek(c.rundate - interval '1 day'))/ SUM(pv.viewtimespentsecs), 0) AS perc_viewtimedow
    , COALESCE(COUNT(Distinct pv.department),0) AS number_departments_viewed 
    , COALESCE(COUNT(pv.account_number),0) AS number_pages_viewed 
    , COALESCE(COUNT(pv.account_number)  FILTER (WHERE  pv.timestamp BETWEEN c.rundate - interval '8 days' AND c.rundate - interval '1 day'),0 ) AS number_pages_viewed_last_week
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_customer_base') AS c 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_build_page_views') AS pv
        ON pv.account_number=c.account_number
        AND pv.viewdate BETWEEN c.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND c.rundate - INTERVAL '1' DAY

GROUP BY   
    pv.account_number 
    , c.rundate
);

/* Page View Themes Affinity */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_view_themes') AS (
WITH cte_latest_views AS (
  SELECT 
    pv.* 
  , c.rundate
  , SUM(pv.viewtimespentsecs) OVER (PARTITION BY pv.account_number) AS total_time_spent 
FROM 
  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_build_page_views')  AS pv
  INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_customer_base') AS c 
    ON pv.account_number=c.account_number
    AND pv.viewdate BETWEEN c.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND c.rundate - INTERVAL '1' DAY
-- WHERE 
--    --taking last 50 items viewed
--     pv.view_order <=50 
)
SELECT 
  pv.account_number
, pv.rundate
,  regexp_replace(t.theme, '[^a-zA-Z0-9]', '') AS themes
 -- Without date/time accounted for 
-- , SUM(((pv.viewtimespentsecs / pv.total_time_spent)* 1/t.theme_rank::numeric) / (date_diff(pv.rundate, pv.timestamp::date) +1)) AS view_theme_score
, SUM(((pv.viewtimespentsecs / pv.total_time_spent)* 1/t.theme_rank::numeric)  * (exp(-0.0231 * (datediff(pv.rundate, pv.timestamp::date))))) AS view_theme_score
FROM 
    cte_latest_views AS pv
    -- Switch this for training data
    INNER JOIN marketingdata_prod.warehouse.next_uk_nextads_item_themes_latest AS t 
        ON pv.pid=t.pid
    -- INNER JOIN marketingdata_prod.warehouse.next_uk_nextads_item_themes AS t 
    --     ON pv.pid=t.pid
        -- want current day themes
        -- AND t.rundate=pv.rundate
GROUP BY 
  pv.account_number
, pv.rundate
, themes
); 

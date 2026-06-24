-- Databricks notebook source

-- CREATE WIDGETS
CREATE WIDGET TEXT catalog_schema_prefix DEFAULT 'marketingdata_dev.ds_sandbox';
CREATE WIDGET TEXT table_prefix DEFAULT 'next_uk_nextAds_analytics_pctr';
CREATE WIDGET TEXT lookback_period DEFAULT '30';

SET spark.sql.adaptive.enabled = true;
SET spark.sql.execution.arrow.pyspark.enabled = true;

/* Last Session features */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_sessions_aggregation') AS (
-- Filter to only latest sessions and get the latest session 
WITH cte_latest_sessions AS (
   SELECT s.* 
    , RANK() OVER (
          PARTITION BY c.account_number, c.rundate
          ORDER BY
            s.date DESC,
            --Tiebreakers - if multiple sessions on same day
            s.timeonsite_seconds DESC,
            s.UniqueVisitID DESC
        ) AS new_session_order
    , c.rundate
FROM
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_sessions') AS s 
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_customer_base') AS c
        ON c.account_number = s.account_number
        AND s.date  BETWEEN c.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND c.rundate  - INTERVAL '1' DAY

WHERE
 -- we are not yet getting app data for shopping bag!  
 s.device !='App'
)
, cte_last_session_filters AS (
SELECT 
   s.account_number
  , s.rundate
  , s.device_simple
  , s.geocountry_simple
  , s.channel_simple
  , s.session_dow
FROM 
   cte_latest_sessions AS s 
WHERE 
    s.new_session_order=1 
)
, cte_session_metrics AS (
    SELECT 
         c.account_number
        , c.rundate
        , SUM(s.TimeOnSite_Seconds) AS total_time_on_site
        -- Sense check distribution here 
        , AVG(s.TimeOnSite_Seconds) AS avg_site_time
        , percentile_cont(0.5) WITHIN GROUP (ORDER BY s.TimeOnSite_Seconds) AS med_time_onsite
        , COUNT(DISTINCT s.UniqueVisitID) AS total_sessions
    FROM 
        IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_sessions')  AS s
        INNER JOIN cte_latest_sessions AS c
        ON c.account_number = s.account_number
        AND  s.date BETWEEN c.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND c.rundate - INTERVAL '1' DAY
    
    GROUP BY 
         c.account_Number
        , c.rundate
)
SELECT 
     l.device_simple   
    , l.geocountry_simple
    , l.channel_simple
    , l.session_dow
    , m.total_time_on_site
    , m.avg_site_Time
    , m.med_time_onsite
    , m.total_sessions
    , l.account_number
    , l.rundate
FROM 
    cte_last_session_filters AS l
    INNER JOIN cte_session_metrics AS m
        ON l.account_number = m.account_Number
        AND l.rundate= m.rundate
);
 
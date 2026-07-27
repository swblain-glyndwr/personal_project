-- Databricks notebook source

-- CREATE WIDGETS
CREATE WIDGET TEXT catalog_schema_prefix DEFAULT 'marketingdata_dev.ds_sandbox';
CREATE WIDGET TEXT table_prefix DEFAULT 'next_uk_nextAds_analytics_pctr';
CREATE WIDGET TEXT start_date DEFAULT '2026-06-01';
CREATE WIDGET TEXT end_date DEFAULT '2026-06-01';
CREATE WIDGET TEXT lookback_period DEFAULT '30';
CREATE WIDGET TEXT year_lookback_period DEFAULT '365';

SET spark.sql.adaptive.enabled = true;
SET spark.sql.execution.arrow.pyspark.enabled = true;

/* Table of all run dates */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates') AS (
SELECT :start_date AS rundate
);


/* All Adverts */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ads_base') AS (
WITH cte_all_ads AS (
SELECT
      c.UniqueAdID
    , d.rundate
    , c.PotNumber
    , UPPER(c.CampaignNumber) AS campaignnumber
    , REGEXP_EXTRACT(c.UniqueAdID, '^.*_(V[1-9])_.*$', 1) AS versionnumber
    , c.Title 
    , c.AlgoDivision
    , c.TradeDivision
    , c.Items 
    , regexp_replace(c.Themes, '[^a-zA-Z0-9]', '')  AS theme
    -- Can possibly improve on this with more logic added for this but is a starting point!
    , CASE WHEN LOWER(c.UniqueAdID) LIKE ANY('%fathers%', '%mothers%', '%christmas%', '%easter%', '%valentine%', '%halloween%', '%eid%') THEN 1 ELSE 0 END AS seasonal_flag
  FROM
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates') AS d,
    marketingdata_prod.warehouse.next_uk_nextads_control_sheet_latest AS c
  WHERE 
    c.AudienceOnly=0
    -- FILTERED ATM FOR SHOPPINGBAG- if to expand will need to build this out as part of the additional data columns 
    AND c.PageGroup ='ShoppingBag' 
  -- Group due to 2 diff SB locations 
  GROUP BY 
      c.UniqueAdID
    , d.rundate
    , c.PotNumber
    , UPPER(c.CampaignNumber)
    , REGEXP_EXTRACT(c.UniqueAdID, '^.*_(V[1-9])_.*$', 1) 
    , c.Title 
    , c.AlgoDivision
    , c.TradeDivision
    , c.Items 
    , theme
    , seasonal_flag
)
, cte_aggregated_impressions AS ( 
SELECT 
   a.UniqueAdID
   , a.rundate
  , SUM(COALESCE(i.number_impressions,0)) AS number_impressions
FROM 
    cte_all_ads AS a
    -- LEFT JOIN IDENTIFIER(:catalog_schema_prefix || '.pctr_training_clicks_lookback') AS i
    --   ON i.control_sheet_AdID= a.UniqueAdID
    --   AND i.date BETWEEN a.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND a.rundate- INTERVAL '1' DAY
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_clicks_lookback') AS i
      ON i.campaign=a.campaignnumber
      and i.versionnumber = a.versionnumber
      and i.algodivision = a.algodivision
      and i.title=a.title
      AND i.date BETWEEN a.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND a.rundate- INTERVAL '1' DAY
  GROUP BY 
    a.UniqueAdID
   , a.rundate
)
, cte_total_impressions AS (
  SELECT 
  c.rundate 
  ,COALESCE(SUM(c.number_impressions),0) AS total_impressions 
FROM 
  cte_aggregated_impressions AS c
GROUP BY 
  c.rundate 
)
, cte_cumulative_impressions AS (
SELECT 
   a.UniqueAdID
   , a.rundate
   , COALESCE(a.number_impressions,0) AS number_impressions
   , COALESCE(a.number_impressions,0)/ t.total_impressions AS percentage_impressions
   , SUM(COALESCE(a.number_impressions,0)) OVER (PARTITION BY a.rundate ORDER BY a.number_impressions DESC) / t.total_impressions AS cumulative_percentage_impressions
FROM 
  cte_aggregated_impressions AS a
  LEFT JOIN cte_total_impressions AS t
    ON t.rundate = a.rundate
)
SELECT 
  a.* 
  , COALESCE(c.number_impressions,0) AS number_impressions
  , COALESCE(c.percentage_impressions,0) AS percentage_impressions
  , COALESCE(c.cumulative_percentage_impressions,0) AS cumulative_percentage_impressions
FROM 
  cte_all_ads AS a
  INNER JOIN cte_cumulative_impressions AS c
    ON c.UniqueAdID = a.UniqueAdID
    AND c.rundate = a.rundate
);
  

/* Customer & Adverts Base:
All customers who have bought in 365 days OR viewed in 60 days for all adverts 
*/
CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base') AS (
WITH cte_all_accounts AS (
SELECT 
    d.rundate
    ,account_number 
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_build_year_baskets') AS b
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates') AS d 
        ON b.order_date >= d.rundate - interval '365 days'
GROUP BY 
    d.rundate
    ,account_number 
UNION 
SELECT 
    d.rundate
    ,account_number
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_build_page_views') AS v 
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates') AS d 
    ON v.viewdate >= d.rundate - interval '60 days'
GROUP BY 
    d.rundate
    ,account_number
)
SELECT 
     d.rundate
    , ac.account_number
    , CAST(NULL AS string) AS AdvertID
    , a.potnumber AS pot
    , a.campaignnumber AS campaign
    , a.versionnumber
    , c.accountstartdate
    , c.age 
    , c.gender
    , CASE WHEN c.mailoptout ='N' THEN 0 ELSE 1 END as mail_optout
    , c.postcodearea
    , CASE WHEN c.specialaccountindicator ='S' THEN 1 ELSE 0 END AS staff_indicator
    , CASE WHEN c.cashindicator= 'C' THEN 1 ELSE 0 END AS cash_acc
    , CAST(NULL AS timestamp) AS  ImpressionTimestamp
    , CAST(NULL AS timestamp) AS ClickTimestamp
    , CAST(NULL AS int) AS Ad_clicked 
    , a.UniqueAdID AS control_sheet_AdID
    , CAST(NULL AS string) AS treatment_type
    , CAST(NULL AS int) AS location
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates') AS d
    INNER JOIN cte_all_accounts AS ac
        ON  ac.rundate =d.rundate 
    INNER JOIN marketingdata_prod.warehouse.svoccust AS c
        ON c.account_number=ac.account_number
        AND c.countrycode='GB'
        AND c.client='NEXT'
    --Join to adverts 
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ads_base') AS a
        ON  a.rundate=d.rundate
);


ALTER TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base')
ALTER COLUMN account_number SET NOT NULL;
ALTER TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base')
ALTER COLUMN control_sheet_AdID SET NOT NULL;
ALTER TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base')
ALTER COLUMN rundate SET NOT NULL ;
ALTER TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base')
ADD PRIMARY KEY (account_number , control_sheet_AdID, rundate);

/* Unique Customers */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_customer_base')  
AS ( 
SELECT 
     rundate 
    , account_number 
FROM 
   IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base') 
GROUP BY rundate, account_number
);  
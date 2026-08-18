-- Databricks notebook source

-- CREATE WIDGETS
CREATE WIDGET TEXT catalog_schema_prefix DEFAULT 'OUTPUT_LOCATION_REQUIRED';
CREATE WIDGET TEXT table_prefix DEFAULT 'next_uk_nextAds_analytics_pctr';
CREATE WIDGET TEXT start_date DEFAULT '2026-06-01';
CREATE WIDGET TEXT end_date DEFAULT '2026-06-01';
CREATE WIDGET TEXT lookback_period DEFAULT '30';
CREATE WIDGET TEXT year_lookback_period DEFAULT '365';

SET spark.sql.adaptive.enabled = true;
SET spark.sql.execution.arrow.pyspark.enabled = true;

/* Table of all run dates */
CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates') AS (
-- SELECT explode(sequence(to_date('2026-06-01'), to_date('2026-06-07'), interval 1 day)) AS rundate
SELECT explode(sequence(to_date(:training_start_date), to_date(:training_end_date), interval 1 day)) AS rundate

);

/* All Adverts for run dates */ 

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ads_base') AS (
WITH cte_all_ads AS (
SELECT
      c.UniqueAdID
    , c.rundate
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
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates') AS d 
    INNER JOIN marketingdata_prod.warehouse.next_uk_nextads_control_sheet AS c
      ON d.rundate=c.rundate
  WHERE 
    -- FILTERED ATM FOR SHOPPINGBAG- if to expand will need to build this out as part of the additional data columns 
     c.PageGroup ='ShoppingBag' 
  -- Group due to 2 diff SB locations 
  GROUP BY 
      c.UniqueAdID
    , c.rundate
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
  , SUM(number_impressions) AS number_impressions
FROM 
  cte_all_ads AS a
  INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_clicks_lookback') AS i
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
   , SUM(COALESCE(a.number_impressions,0))OVER (PARTITION BY a.rundate ORDER BY a.number_impressions DESC ) /t.total_impressions AS cumulative_percentage_impressions
FROM 
  cte_aggregated_impressions AS a
  INNER JOIN cte_total_impressions AS t
    ON t.rundate = a.rundate
)
-- Filter to ONLY include adverts above the impressions threshold 
SELECT 
  a.* 
  , COALESCE(c.number_impressions,0) AS number_impressions
  , COALESCE(c.percentage_impressions,0) AS percentage_impressions
  , COALESCE(c.cumulative_percentage_impressions,0) AS cumulative_percentage_impressions
FROM 
  cte_all_ads AS a
  LEFT JOIN cte_cumulative_impressions AS c
    ON c.UniqueAdID = a.UniqueAdID
    AND c.rundate = a.rundate
);
  
  

  /* All Click & Impressions data */

-- Build of All training clicks/ impressions 
CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base') AS (
WITH cte_all_impressions AS ( 
SELECT 
     d.rundate
    , i.account_number
    --, i.AdvertID
    , i.pot
    , i.campaign
    , i.versionnumber
    , c.accountstartdate
    , c.age 
    , c.gender
    , CASE WHEN c.mailoptout ='N' THEN 0 ELSE 1 END as mail_optout
    , c.postcodearea
    , CASE WHEN c.specialaccountindicator ='S' THEN 1 ELSE 0 END AS staff_indicator
    , CASE WHEN c.cashindicator= 'C' THEN 1 ELSE 0 END AS cash_acc
    , MIN(CASE WHEN i.action='Banner Impression - Next Ads' THEN i.timestamp END) AS ImpressionTimestamp
    , MIN(CASE WHEN i.action='Banner Click - Next Ads' THEN i.timestamp END) AS ClickTimestamp
    , CASE WHEN SUM(CASE WHEN i.action='Banner Click - Next Ads' THEN 1 ELSE 0 END) >0 THEN 1 ELSE 0 END AS Ad_clicked 
    , a.UniqueAdID AS control_sheet_AdID
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates') AS d
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_ad_clicks_impressions_base') AS i
        ON  i.date =d.rundate + interval '1 day'
    INNER JOIN marketingdata_prod.warehouse.svoccust AS c
        ON c.account_number=i.account_number
        AND c.countrycode='GB'
        AND c.client='NEXT'
    --Join to adverts (needs to be outer)
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ads_base') AS a
        ON  a.potnumber=i.pot 
        AND a.campaignnumber=i.campaign 
        AND a.versionnumber=i.versionnumber
        AND a.rundate=d.rundate
GROUP BY 
      d.rundate
    , i.account_number
    --, i.AdvertID
    , i.pot
    , i.campaign
    , i.versionnumber
    , c.accountstartdate
    , c.age
    , c.gender
    , mail_optout
    , c.postcodearea
    , staff_indicator
    , cash_acc
    , a.UniqueAdID
)
, cte_max_ad_assignments AS (
SELECT
      i.account_number
    , i.control_sheet_adid
    , i.rundate
    , CASE WHEN asmt.UniqueAdIDAssigned ='NoAd' THEN 'Control'
        WHEN asmt.Treatment='Basic' THEN 'Basic'
        WHEN asmt.Treatment='Best' THEN 'Best'
        ELSE 'Other' 
        END AS treatment_type 
, CASE WHEN asmt.location='SB1' THEN 1 WHEN asmt.location='SB2' THEN 2 END AS location 
, asmt.rundate AS assignment_run_date
, MAX(asmt.rundate) OVER (PARTITION BY
         i.account_number
    , i.control_sheet_adid
    , i.rundate
) AS latest_run_date
FROM 
        cte_all_impressions  AS i
        --- have to explode te join here - MASID doesnt refresh until after first interaction so may be prior days 
        INNER JOIN marketingdata_prod.warehouse.next_uk_nextads_assignments AS asmt
            ON asmt.rundate <= i.rundate
            AND asmt.AccountNumber=i.account_number
            AND asmt.location in ('SB1', 'SB2') 
            AND asmt.UniqueAdIDAssigned= i.control_sheet_adid
        GROUP BY 
    i.account_number
    , i.control_sheet_adid
    , i.rundate
    ,assignment_run_date
    , treatment_type
    , location
)
,cte_all_accs AS (
SELECT 
    rundate 
    , account_number 
    , accountstartdate
    , age 
    , gender
    , mail_optout
    , postcodearea
    ,staff_indicator
    , cash_acc
FROM 

    cte_all_impressions AS i 
GROUP BY 
      rundate 
    , account_number 
    , accountstartdate
    , age 
    , gender
    , mail_optout
    , postcodearea
    ,staff_indicator
    , cash_acc
)
SELECT 
      ad.rundate
    , ac.account_number 
  --  , i.AdvertID
    -- change to ad? 
    , ad.PotNumber as pot 
    , ad.campaignnumber as campaign
    , ad.versionnumber 
    , ac.accountstartdate
    , ac.age 
    , ac.gender
    , ac.mail_optout
    , ac.postcodearea
    , ac.staff_indicator
    , ac.cash_acc
    , i.ImpressionTimestamp
    , i.ClickTimestamp
    , i.Ad_clicked 
    , ad.UniqueAdID AS control_sheet_AdID
    , a.treatment_type
    , a.location
FROM  
    -- build a cross join view of all the adverts data 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ads_base') AS ad 
    INNER JOIN cte_all_accs AS ac
        ON ac.rundate=ad.rundate 
    LEFT JOIN cte_all_impressions AS i 
        ON i.control_sheet_AdID=ad.UniqueAdID
        AND ac.account_number=i.account_number
        AND ac.rundate=i.rundate
    LEFT JOIN cte_max_ad_assignments AS a 
        ON a.account_number=i.account_number 
        AND a.control_sheet_adid=i.control_sheet_adid 
        AND a.rundate=i.rundate 
        And a.assignment_run_date=a.latest_run_date
);
            

ALTER TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base')
ALTER COLUMN account_number SET NOT NULL;
ALTER TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base')
ALTER COLUMN control_sheet_AdID SET NOT NULL;
ALTER TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base')
ALTER COLUMN rundate SET NOT NULL ;
ALTER TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base')
ADD PRIMARY KEY (account_number , control_sheet_AdID, rundate);

/* All Unique Customers by rundate */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_customer_base')  
AS ( 
SELECT 
     rundate 
    , account_number 
FROM 
   IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base') 
GROUP BY rundate, account_number
);

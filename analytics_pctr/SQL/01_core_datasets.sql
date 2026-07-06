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


/* Customer Page Views */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix ||'_build_page_views') AS ( 
-- Some pids have 2 diff next_category against the product!! Take the most common 
WITH cte_products AS (
SELECT 
 pc.pid
, pc.department
, concat_ws("_", CASE WHEN pc.department='childrenswear' THEN pc.next_gender ELSE pc.department END, pc.brand, pc.next_category) AS cat_id
, COUNT(*) AS number_items
FROM 
    marketingdata_prod.warehouse.product_catalog AS pc
GROUP BY 
    pc.pid
    , pc.department
    , cat_id
)
, cte_best_catid_match AS (
SELECT 
    pc.pid
    , pc.department
    , pc.cat_id
    , pc.number_items
    , ROW_NUMBER() OVER (PARTITION BY pc.pid ORDER BY pc.number_items  DESC, pc.cat_id ) AS ranking
FROM 
    cte_products AS pc
)
, cte_web AS (
SELECT 
     s.account_number
    , v.timestamp 
    , v.timestamp::date AS viewdate
    , s.UniqueVisitID 
    , v.viewtimespentsecs
    , v.ProductSKU
    , pc.pid
    , pc.department
    , pc.cat_id
    , RANK() OVER (PARTITION BY  s.account_number
    , v.timestamp ORDER BY pc.number_items DESC, cat_id  ) AS max_cat_id
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_sessions') AS s 
    INNER JOIN marketingdata_prod.warehouse.bq_views_next_uk AS v
         ON s.UniqueVisitID=v.UniqueVisitID
         AND v.EventType ILIKE '%pdp_view%'
         -- Assume your view time has to be greater than zero to have actually viewed 
         AND  v.viewtimespentsecs >0  
    INNER JOIN cte_best_catid_match AS pc
        ON pc.pid=v.ProductSKU
        AND pc.ranking=1
WHERE 
    s.customer_filter
GROUP BY 
     s.account_number
    , v.timestamp 
    , viewdate
    , s.UniqueVisitID 
    , v.viewtimespentsecs
    , v.ProductSKU
    , pc.pid
    , pc.department
    , pc.cat_id
    , pc.number_items
) 
, cte_app AS (
SELECT 
     s.account_number
    , v.timestamp 
    , v.timestamp::date AS viewdate
    , s.UniqueVisitID 
    , v.viewtimespentsecs
    , v.ProductSKU
    , pc.pid
    , pc.department
    , pc.cat_id
    , RANK() OVER (PARTITION BY  s.account_number
    , v.timestamp ORDER BY pc.number_items DESC , cat_id ) AS max_cat_id
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_sessions') AS s 
    INNER JOIN marketingdata_prod.warehouse.bq_views_next_uk_app AS v
         ON s.UniqueVisitID=v.UniqueVisitID
         AND v.ScreenName = "PDP"
         -- Assume your view time has to be greater than zero to have actually viewed 
         AND  v.viewtimespentsecs >0  
    INNER JOIN cte_best_catid_match AS pc
        ON pc.pid=v.ProductSKU
        AND pc.ranking=1
WHERE 
    s.customer_filter
GROUP BY 
     s.account_number
    , v.timestamp 
    , viewdate
    , s.UniqueVisitID 
    , v.viewtimespentsecs
    , v.ProductSKU
    , pc.pid
    , pc.department
    , pc.cat_id
    , pc.number_items
)
-- ,cte_all AS (
SELECT 
* 
FROM 
    cte_web
WHERE max_cat_id=1
UNION
SELECT 
    * 
FROM 
 cte_app
 WHERE max_cat_id=1
);

/* Clicks & Impressions Daily Aggregations */ 


CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix ||'_training_clicks_lookback') AS (
WITH cte_all_ads AS (
SELECT 
    AdvertID
  , pot
  , UPPER(campaign) AS campaign
  , versionnumber
  , date
  , CONCAT('P', cast((int(regexp_replace(pot, '[^0-9]', ''))-1) as string)) AS prior_pot_number

FROM 
  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_ad_clicks_impressions_base') 
GROUP BY 
 AdvertID
  , pot
  , date 
  ,  UPPER(campaign)
  , versionnumber
  , prior_pot_number
) 
,cte_algodata AS (
SELECT 
      a.advertId
    , a.pot
    , a.prior_pot_number
    , a.campaign
    , a.versionnumber
    , ab.algodivision
    , a.date 
    , ab.UniqueAdID AS control_sheet_AdID
    , ab.title
    , row_number() OVER (PARTITION by a.advertId,a.date ORDER BY ab.rundate DESC) AS algorow 
FROM 
    cte_all_ads AS a
    INNER JOIN marketingdata_prod.warehouse.next_uk_nextads_control_sheet AS ab 
        ON ab.potnumber=a.pot 
        AND UPPER(ab.campaignnumber)=a.campaign
        AND ab.PageGroup ='ShoppingBag' 
        AND a.versionnumber= REGEXP_EXTRACT(ab.UniqueAdID, '^.*_(V[1-9])_.*$', 1)
        -- To account for if there is a missing date take the last date prior 
        --( assume should have ran at least once in a week )
        AND ab.rundate<=a.date AND ab.rundate > a.date - interval '1 week'
        AND ab.algodivision IS NOT NULL 
) 
SELECT 
     a.date
    , a.dow
    , a.woy 
    , a.geocountry_simple
    , a.channel_simple
    , a.device_simple 
    , a.gender
    , a.AdvertID
    , a.pot
    , CONCAT('P', cast((int(regexp_replace(a.pot, '[^0-9]', ''))-1) as string)) AS prior_pot_number
    , UPPER(a.campaign) AS campaign
    , a.versionnumber
    , a.PagePath 
    , SUM(CASE WHEN a.action =  'Banner Impression - Next Ads' THEN 1 ELSE 0 END ) AS number_impressions
    , COUNT(DISTINCT CASE WHEN a.action =  'Banner Impression - Next Ads' THEN a.UniqueVisitID END ) AS number_unique_session_impressions
    , SUM(CASE WHEN a.action =  'Banner Click - Next Ads' THEN 1 ELSE 0 END ) AS number_clicks
    , COUNT(DISTINCT CASE WHEN a.action =  'Banner Click - Next Ads' THEN a.UniqueVisitID END ) AS number_unique_session_clicks
    , COALESCE(i.algodivision, 'Unknown') AS AlgoDivision
    , i.control_sheet_AdID
    , i.title
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_ad_clicks_impressions_base') AS a
    LEFT JOIN cte_algodata AS i
        ON i.advertid=a.advertid 
        AND i.date=a.date 
        AND i.algorow=1
GROUP BY 
      a.date
    , a.dow
    , a.woy 
    , a.geocountry_simple
    , a.channel_simple
    , a.device_simple 
    , a.gender
    , a.AdvertID
    , a.pot 
    , prior_pot_number
    , a.campaign 
    , a.versionnumber
    , a.PagePath 
    , COALESCE(i.algodivision, 'Unknown')
    , i.control_sheet_AdID
    , i.title
);

/* CTR WOW Agggregations */ 

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix ||'_wow_ctr') AS (
WITH cte_woy_ctr AS (
SELECT 
      DATE_TRUNC('week', a.date) AS week 
    -- , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision 
    -- Take overall CTR for all ads in period if unknown 
    , SUM(a.number_clicks )/ SUM(a.number_impressions) AS ctr
    , SUM(a.number_clicks) AS total_clicks
    , SUM(a.number_impressions) AS total_impressions
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_clicks_lookback') AS a 
GROUP BY 
    week
    -- , a.control_sheet_AdID
     , a.title 
    , a.campaign
    , a.algodivision
    , a.versionnumber 
)
SELECT 
     a.* 
    , FIRST_VALUE(a.total_impressions) OVER (PARTITION BY  a.title , a.campaign,  a.versionnumber , a.algodivision
                ORDER BY  week ASC
                RANGE BETWEEN INTERVAL 7 days PRECEDING AND INTERVAL 7 days PRECEDING) AS prior_week_impressions
    , FIRST_VALUE(a.ctr) OVER (PARTITION BY   a.title , a.campaign,  a.versionnumber , a.algodivision
                ORDER BY  week ASC
                RANGE BETWEEN INTERVAL 7 days PRECEDING AND INTERVAL 7 days PRECEDING) AS prior_week_ctr
    , ctr- (FIRST_VALUE(a.ctr) OVER (PARTITION BY  a.title , a.campaign,  a.versionnumber , a.algodivision
                ORDER BY  week ASC
                RANGE BETWEEN INTERVAL 7 days PRECEDING AND INTERVAL 7 days PRECEDING) ) AS wow_ctr_change
FROM 
    cte_woy_ctr as a 
) ;


/* CTR DOD Aggregations */ 

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_dod_ctr') AS (
WITH cte_dod_ctr AS (
SELECT 
     a.date 
    -- , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
    -- Take overall CTR for all ads in period if unknown 
    , SUM(a.number_clicks)/ SUM(a.number_impressions) AS ctr
    , SUM(a.number_clicks) AS total_clicks
    , SUM(a.number_impressions) AS total_impressions
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_clicks_lookback') AS a
GROUP BY 
    a.date
    --  , a.control_sheet_AdID
    , a.title 
    , a.campaign
    , a.versionnumber 
    , a.algodivision
)
SELECT 
    * 
     , FIRST_VALUE(a.total_impressions) OVER (PARTITION BY   a.title , a.campaign,  a.versionnumber , a.algodivision
                ORDER BY  a.date ASC
                RANGE BETWEEN  1  PRECEDING AND  1  PRECEDING) AS prior_day_impressions
    , FIRST_VALUE(a.ctr) OVER (PARTITION BY  a.title , a.campaign,  a.versionnumber , a.algodivision
                ORDER BY  a.date ASC
                RANGE BETWEEN  1  PRECEDING AND  1  PRECEDING) AS prior_day_ctr
    , ctr- (FIRST_VALUE(a.ctr) OVER (PARTITION BY   a.title , a.campaign,  a.versionnumber , a.algodivision
                ORDER BY  a.date ASC
                RANGE BETWEEN  1  PRECEDING AND  1  PRECEDING) ) AS dod_ctr_change

FROM 
    cte_dod_ctr AS a
);

/* Purchase Data */


CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_build_year_baskets') AS (
WITH cte_all_baskets AS (
SELECT 
  b.account_number
, b.orderdate::date AS order_date
, b.itemno
, b.orderid
, SUM(b.s740orderstakenvalue) AS order_value 
, SUM(b.s740orderstakenqty) AS order_qty
, RANK() OVER (PARTITION BY b.account_number ORDER BY b.order_date DESC) AS bought_items_order
FROM 
  marketingdata_prod.warehouse.baskets_uk_3y AS b 
  WHERE 
      b.order_date BETWEEN :start_date - (interval '1 day'*(:year_lookback_period +1)) AND :end_date - interval '1 day'
      AND b.s740orderstakenvalue >0 
  GROUP BY 
  b.account_number
, b.orderdate
, b.itemno
, b.orderid
, order_date
)
-- Get last rundate updated prior to analysis date  for all items - if doesnt exist then USE CURRENT
,cte_latest_prod_history AS ( 
SELECT 
   b.itemno
 , MAX(pc.rundate) AS last_run 
 FROM 
  cte_all_baskets AS b 
  LEFT JOIN marketingdata_prod.warehouse.product_catalog_history AS pc 
    ON pc.pid=b.itemno
    AND pc.rundate < CURRENT_DATE
GROUP BY
  b.itemno
)
, cte_product_departments AS (
SELECT
     p.itemno
    , pc.department
    , COALESCE(pc.department, c.department) AS product_department
    , COALESCE(pc.gender, c.gender) AS product_gender 
    , COALESCE(concat_ws("_", CASE WHEN pc.department='childrenswear' THEN pc.next_gender ELSE pc.department END, pc.brand,pc.next_category),
     concat_ws("_", CASE WHEN c.department='childrenswear' THEN c.next_gender ELSE c.department END, c.brand,c.next_category)) AS cat_id
FROM 
    cte_latest_prod_history AS p 
    LEFT JOIN marketingdata_prod.warehouse.product_catalog_history AS pc 
    ON pc.pid=p.itemno
    AND pc.rundate =p.last_run
    LEFT JOIN marketingdata_prod.warehouse.product_catalog AS c
      ON c.pid =p.itemno
GROUP BY 
  p.itemno
  , product_department
 , pc.department
 , product_gender
 , cat_id
)
SELECT 
  c.*
  , d.product_department
  , d.product_gender
  , d.cat_id
FROM 
  cte_all_baskets AS c 
  LEFT JOIN cte_product_departments AS d 
    ON c.itemno=d.itemno
);


/* Unique Items Viewed in sessions */


CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_item_views') as (
WITH products as (
SELECT
  pid AS itemnumber,
  concat_ws("_", CASE WHEN department='childrenswear' THEN next_gender ELSE department END, brand, next_category) as cat_id
FROM
  marketingdata_prod.warehouse.product_catalog
),
views_web as (
SELECT
  uniquevisitid,
  MIN(date) AS date , 
  MIN(timestamp) AS timestamp, 
  productsku as itemnumber,
  Category
FROM
  marketingdata_prod.warehouse.bq_views_next_uk AS bq
where
   date  BETWEEN :start_date - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND :end_date - INTERVAL '1 DAY'
  and EventType regexp "pdp_view" 
  and ViewTimespentSecs >0
GROUP BY 
  uniquevisitid,
  itemnumber,
  Category
),
views_app as (
SELECT
  uniquevisitid,
  MIN(date) AS date , 
  MIN(timestamp) AS timestamp, 
  productsku as itemnumber,
  Category
FROM
  marketingdata_prod.warehouse.bq_views_next_uk_app
  where  date  BETWEEN :start_date - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND :end_date - INTERVAL '1 DAY'
  and ScreenName = "PDP"
   and ViewTimespentSecs >0
GROUP BY 
  uniquevisitid,
  itemnumber,
  Category
  )
SELECT
	v.*
  , p.cat_id
FROM 
	views_web AS v
  inner join products AS p 
  ON v.itemnumber = p.itemnumber
UNION DISTINCT 
	(SELECT 
	v.*
  , p.cat_id
   FROM
    views_app AS v
    inner join products AS p 
  ON v.itemnumber = p.itemnumber)
);

/* Unique Add to baskets in sessisosn  */


CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_item_atbs') AS (
WITH products as (
SELECT
  pid AS itemnumber,
  concat_ws("_", CASE WHEN department='childrenswear' THEN next_gender ELSE department END, brand, next_category) as cat_id
FROM
  marketingdata_prod.warehouse.product_catalog
),
atbs_web as (
SELECT
  uniquevisitid,
  min(date) AS date, 
  min(timestamp) as timestamp, 
  productsku as itemnumber,
  Category
FROM
  marketingdata_prod.warehouse.bq_atbs_next_uk
where date  BETWEEN :start_date - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND :end_date - INTERVAL '1 DAY'
 GROUP BY 
  uniquevisitid,
  itemnumber,
  Category
),
atbs_app as (
SELECT
  uniquevisitid,
  min(date) as date, 
  min(timestamp) timestamp, 
  productsku as itemnumber,
  Category
FROM
  marketingdata_prod.warehouse.bq_atbs_next_uk_app
  where date  BETWEEN :start_date - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND :end_date - INTERVAL '1 DAY'
GROUP BY 
  uniquevisitid,
  itemnumber,
  Category
)
SELECT
	a.*
  ,p.cat_id
FROM 
	atbs_web AS a
  INNER JOIN products AS p 
    ON p.itemnumber = a.itemnumber
UNION DISTINCT 
	(SELECT a.*,p.cat_id FROM atbs_app AS a
  INNER JOIN products AS p 
    ON p.itemnumber = a.itemnumber)
);

/* View Basket Affinity Pairs */


CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_catid_view_basket_affinity_pairs') AS ( 
SELECT DISTINCT
  uniquevisitid, 
  date,
  t1.cat_id as cat_id1,
  t2.cat_id as cat_id2,
  t1.Category as category1,
  t2.Category as category2
FROM
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_item_views') t1
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_item_atbs') t2
    USING (uniquevisitid, date)
WHERE 
    t2.timestamp >= t1.timestamp
  AND t1.itemnumber != t2.itemnumber
);
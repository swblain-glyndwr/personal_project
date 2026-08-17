-- Databricks notebook source

-- CREATE WIDGETS
CREATE WIDGET TEXT catalog_schema_prefix DEFAULT 'OUTPUT_LOCATION_REQUIRED';
CREATE WIDGET TEXT table_prefix DEFAULT 'next_uk_nextAds_analytics_pctr';
CREATE WIDGET TEXT lookback_period DEFAULT '30';
CREATE WIDGET TEXT year_lookback_period DEFAULT '365';

SET spark.sql.adaptive.enabled = true;
SET spark.sql.execution.arrow.pyspark.enabled = true;

/* Customer AlgoDivison Segmentation spend data */
-- simplified to use perc of spend on cat
-- Risk is homewear- might need to adjust  based on proportion of items too 
-- Seperated out boys vs girls from childrenswear -this might need some imporevment- ust simple regexp for now 

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_customer_segmentation') AS (
WITH cte_overall AS (
SELECT 
      al.account_number
    , al.rundate
    , 'Overall' AS segment_type 
    , 'All' AS segment_group
    , SUM(b.order_value) AS total_order_value 
    , SUM(b.order_qty) AS total_order_qty
    , COUNT(DISTINCT b.itemno) AS total_number_items
    , SUM(CASE WHEN b.order_date >= al.rundate - ((:lookback_period +1)* interval '1 day') AND   b.order_date < al.rundate THEN order_value ELSE 0 END) AS prior_30_day_order_value
FROM
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_customer_base') AS al 
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_build_year_baskets') AS b
        ON b.account_number=al.account_number 
        AND b.order_date BETWEEN al.rundate- (interval '1 day'*(:year_lookback_period +1)) AND al.rundate - interval '1 day'
GROUP BY 
      al.account_number
      , al.rundate
      , segment_type
      , segment_group 
)
, cte_boys AS ( 
SELECT 
     t.account_number
    , t.rundate
    , 'Department' AS segment_type
    , 'Boys' AS segment_group
    , SUM(b.order_value)/ t.total_order_value AS total_spend_perc 
    , SUM(b.order_qty)/ t.total_order_qty AS total_qty_perc
    , COUNT(DISTINCT b.itemno) AS total_number_items
    , SUM(CASE WHEN b.order_date >=  t.rundate  - ((:lookback_period +1)* interval '1 day') AND   b.order_date <  t.rundate  THEN order_value ELSE 0 END) / t.prior_30_day_order_value AS prior_30_day_order_perc
FROM 
    cte_overall AS t
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_build_year_baskets') AS b 
      ON b.account_Number=t.account_number 
      AND b.order_date BETWEEN t.rundate- (interval '1 day'*(:year_lookback_period +1)) AND t.rundate - interval '1 day'
WHERE 
  b.product_department ='childrenswear'
  AND  b.product_gender regexp '^.*(boy|man|men).*$'
GROUP BY 
    t.account_number
    , t.rundate
    , segment_type
    , segment_group
    , t.total_order_value
    , t.total_order_qty
    , t.prior_30_day_order_value
)
,cte_girls AS ( 
SELECT 
       t.account_number
    , t.rundate
    , 'Department' AS segment_type
    , 'Girls' AS segment_group
    , SUM(b.order_value)/ t.total_order_value AS total_spend_perc 
    , SUM(b.order_qty)/ t.total_order_qty AS total_qty_perc
    , COUNT(DISTINCT itemno) AS total_number_items
    , SUM(CASE WHEN b.order_date >=  t.rundate  - ((:lookback_period +1)* interval '1 day') AND   b.order_date <  t.rundate  THEN order_value ELSE 0 END) / t.prior_30_day_order_value AS prior_30_day_order_perc
FROM 
    cte_overall AS t
    INNER JOIN  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_build_year_baskets') AS b 
      ON b.account_Number=t.account_number 
      AND b.order_date BETWEEN t.rundate- (interval '1 day'*(:year_lookback_period +1)) AND t.rundate - interval '1 day'
WHERE 
  b.product_department ='childrenswear'
  AND b.product_gender regexp '^.*(woman|girl|women).*$'
GROUP BY 
      t.account_number
    , t.rundate
      , segment_type
      , segment_group
      , t.total_order_value
      , t.total_order_qty
      , t.prior_30_day_order_value
)
, cte_sales_by_dep AS (
SELECT 
      t.account_number
    , t.rundate
    , 'Department' AS segment_type
  , CASE WHEN b.product_department = 'womenswear' THEN 'Womens'
         WHEN b.product_department = 'menswear' THEN 'Mens'
         WHEN b.product_department ='homeware' THEN 'Home'
         WHEN b.product_department ='beauty' THEN 'Beauty'
         -- Need to seperate out to boys/ girls - but could be both?!
         WHEN b.product_department ='childrenswear' AND b.product_gender NOT regexp '^.*(woman|girl|women|boy|men|man).*$'  THEN 'Children'
         ELSE 'Other' END AS segment_g
    , SUM(b.order_value)/ t.total_order_value AS total_spend_perc
    , SUM(b.order_qty)/ t.total_order_qty AS total_qty_perc
    , COUNT(DISTINCT b.itemno) AS total_number_items
    , SUM(CASE WHEN b.order_date >=  t.rundate  - ((:lookback_period +1)* interval '1 day') AND   b.order_date <  t.rundate  THEN order_value ELSE 0 END) / t.prior_30_day_order_value AS prior_30_day_order_perc
FROM 
    cte_overall AS t
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_build_year_baskets') AS b 
      ON b.account_Number=t.account_number 
      AND b.order_date BETWEEN t.rundate- (interval '1 day'*(:year_lookback_period +1)) AND t.rundate - interval '1 day'
GROUP BY 
       t.account_number
      , t.rundate
      , segment_type
      , segment_g
      , t.total_order_value
      , t.total_order_qty
      , t.prior_30_day_order_value
)
, cte_segments AS (
    SELECT 
      * 
    FROM 
    cte_boys 
    UNION 
    SELECT 
      * 
    FROM
       cte_girls 
    UNION  
      SELECT
        *
      FROM 
        cte_sales_by_dep
      WHERE segment_g !='Other'
)
SELECT 
      * 
      , 0 AS spend_perc_ranking 
FROM 
    cte_overall
    UNION 
SELECT 
      *
      , RANK() OVER (PARTITION BY account_number, rundate ORDER BY   total_spend_perc DESC) AS spend_perc_ranking
FROM 
    cte_segments 
);


/* Last spend item affinities to themes */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_purchase_themes') AS ( 
WITH cte_ranked_items AS (
SELECT 
      b.account_number
    , al.rundate
    , b.itemno
    , RANK() OVER (PARTITION BY al.account_number ORDER BY b.order_date DESC) AS items_order
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_customer_base') AS al 
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_build_year_baskets') AS b
        ON b.account_number=al.account_number 
        AND b.order_date BETWEEN al.rundate- (interval '1 day'*(:year_lookback_period +1)) AND al.rundate - interval '1 day'
)
SELECT 
      b.account_number
    , b.rundate
    , regexp_replace(t.theme, '[^a-zA-Z0-9]', '') AS themes
    , SUM((1/t.theme_rank) * (1/b.items_order)) AS theme_affinity  
FROM 
   cte_ranked_items AS b
    -- Switch this for training data
    INNER JOIN marketingdata_prod.warehouse.next_uk_nextads_item_themes_latest AS t 
        ON b.itemno=t.pid
    -- INNER JOIN marketingdata_prod.warehouse.next_uk_nextads_item_themes AS t 
    --     ON b.itemno=t.pid
    --     AND t.rundate=b.rundate
WHeRE 
    b.items_order  <=10
GROUP BY 
      b.account_number
    , b.rundate
    , themes
);

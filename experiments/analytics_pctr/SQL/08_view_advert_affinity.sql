-- Databricks notebook source

-- CREATE WIDGETS
CREATE WIDGET TEXT catalog_schema_prefix DEFAULT 'OUTPUT_LOCATION_REQUIRED';
CREATE WIDGET TEXT table_prefix DEFAULT 'next_uk_nextAds_analytics_pctr';
CREATE WIDGET TEXT lookback_period DEFAULT '30';

SET spark.sql.adaptive.enabled = true;
SET spark.sql.execution.arrow.pyspark.enabled = true;

/* All adverts catid affinities by date */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_advert_attribute_weighting') AS (
WITH cte_products as (
SELECT
  pid AS itemnumber,
  concat_ws("_", department, brand, next_category) as cat_id,
  p.*
FROM
  marketingdata_prod.warehouse.product_catalog AS p
)
, weighted_products as (
select
  a.rundate,
  a.UniqueAdID,
  s.items as itemnumber,
  if(replace(s.in_ad_items, '-', '')  regexp s.items, 1, 0) as in_ad_items_flag,
  s.item_pos,
  round(1.0 / (1.0 + 0.2 * (s.item_pos - 1)),2) AS position_weight,
  s.URL,
  p.* except(itemnumber, url, rundate)
FROM
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_ads_base') AS a 
    --Switch this for training 
    INNER JOIN marketingdata_prod.warehouse.next_ads_sort_order_latest AS s
     ON  a.uniqueadid=s.UniqueAdID
    -- INNER JOIN marketingdata_prod.warehouse.next_ads_sort_order AS s
    --  ON s.rundate=a.rundate
    --  AND a.uniqueadid=s.UniqueAdID
    inner join cte_products p ON p.itemnumber = s.items
where
     s.item_pos <= 9 and lower(s.Status) == 'active'

)
--Unpivot and process attributes
, exploded_attributes AS (
  SELECT
    w.UniqueAdID,
    w.rundate,
    w.URL,
    w.item_pos,
    attr_type,
    -- e.g. if position has use partywear| occasionwear, the 1 value gets equally split 0.5/ 0.5
    w.position_weight / size(split(attr_value, '\\|')) AS weight_per_value,
    concat(attr_type, '_', trim(single_value)) AS attr_key
  FROM weighted_products w 
  LATERAL VIEW stack(3, 
    -- "use", use,
    -- "next_gender", gender,
    -- "colour", primary_colour,
    "department", CASE WHEN department ='childrenswear' THEN next_gender ELSE department END,
    "brand", brand,
    -- "material", material,
    -- "pattern", pattern,
    -- "style", style,
    -- "room", room,
    -- "activity", activity,
    -- "collaboration", collaboration,
    -- "fit", fit,
    "cat_id", cat_id
  ) AS attr_type, attr_value
  LATERAL VIEW explode(split(attr_value, '\\|')) AS single_value
  WHERE attr_value IS NOT NULL and attr_value != "" and single_value != "Other"
) 
, attribute_level_stats AS (
  SELECT
    UniqueAdID,
    rundate,
    URL,
    attr_type,
    attr_key,
    SUM(weight_per_value) AS weighted_sum,
    COUNT(*) AS item_count,
    MIN(item_pos) AS highest_position,
    collect_list(item_pos) AS all_positions
  FROM exploded_attributes
  GROUP BY 
    UniqueAdID,
    rundate,
    URL,
    attr_type,
    attr_key
), 
normalised_profiles AS (
  SELECT
    UniqueAdID,
    rundate,
    URL,
    attr_type,
    attr_key,
    weighted_sum / SUM(weighted_sum) OVER (PARTITION BY UniqueAdID, rundate,  attr_type) AS normalised_weight,
    highest_position,
    item_count
  FROM attribute_level_stats
),
dominant_attributes AS (
    SELECT
        UniqueAdID,  -- or UniqueAdID for ads
        rundate,
        attr_type,
        attr_key AS attr_key,
        row_number() OVER (
            PARTITION BY UniqueAdID, rundate , attr_type
            ORDER BY normalised_weight DESC, highest_position, item_count desc 
        ) AS dominant_attr_key_flag
    FROM normalised_profiles
    --where normalised_weight > 0.3
    qualify dominant_attr_key_flag = 1
)
-- -- Final ad profiles
SELECT
  UniqueAdID,rundate,
   URL,
  attr_type,
  attr_key, 
  normalised_weight,
  dominant_attr_key_flag,
  -- map_from_entries(
  --   collect_list(struct(attr_key, normalised_weight))
  -- ) AS ad_profile,
  AVG(highest_position) AS avg_attribute_position
FROM normalised_profiles
LEFT JOIN
    dominant_attributes 
using (uniqueadid, rundate, attr_type, attr_key)
GROUP BY all
ORDER BY 1, attr_type desc, normalised_weight DESC
);

/* Affinity of views to baskets by catid */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_catid_view_basket_affinity') AS (
WITH total as (
SELECT
   COUNT(DISTINCT r.uniquevisitid) as freq0
   , d.rundate 
FROM
  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_catid_view_basket_affinity_pairs') AS r 
  INNER JOIN  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates') AS d 
    ON r.date BETWEEN d.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND d.rundate - INTERVAL '1 DAY'
GROUP BY 
    d.rundate
),
views_count as (
SELECT
  r.cat_id1, 
  d.rundate,
  count(distinct r.uniquevisitid) as views
FROM
  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_catid_view_basket_affinity_pairs') AS r 
    INNER JOIN  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates') AS d 
    ON r.date BETWEEN d.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND d.rundate - INTERVAL '1 DAY'
GROUP BY 
    d.rundate
    ,r.cat_id1
),
ATBs_count as (
SELECT
  r.cat_id2, 
  d.rundate,
  count(distinct r.uniquevisitid) as atbs
FROM
  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_catid_view_basket_affinity_pairs') AS r
    INNER JOIN  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates') AS d 
    ON r.date BETWEEN d.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND d.rundate - INTERVAL '1 DAY'
GROUP BY 
    d.rundate
    ,r.cat_id2
),
pair_count as (
SELECT
  r.cat_id1,
  r.cat_id2, 
  d.rundate,
  max(if(r.category1 = r.category2, 1, 0)) as same_category,
  count(distinct r.uniquevisitid) as freq12,
  sum(exp(-0.0231 * (datediff(d.rundate, date)))) as freq12_decayed
FROM
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_catid_view_basket_affinity_pairs') AS r 
    INNER JOIN  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_dates') AS d 
    ON r.date BETWEEN d.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND d.rundate - INTERVAL '1 DAY'
GROUP BY 
r.cat_id1,
  r.cat_id2, 
  d.rundate
),

stats_raw as (
SELECT
  t0.rundate,
  t0.cat_id1,
  t0.cat_id2, 
  t0.same_category,
  t0.freq12, 
  t1.views as freq1,
  t2.atbs as freq2,
  
  t1.views/(t.freq0 ) as support1,
  t2.atbs/(t.freq0 ) as support2,

  t0.freq12/(t.freq0) as support12,
  t0.freq12/(sqrt(t1.views) * sqrt(t2.atbs)) as cosine_similarity

FROM 
  total AS t
LEFT JOIN 
  pair_count t0
USING (rundate)
LEFT JOIN
  views_count t1
USING (cat_id1, rundate)
LEFT JOIN
  atbs_count t2
USING (cat_id2, rundate)
)

SELECT
  s.rundate,
  cat_id1, 
  cat_id2,
  same_category,
  freq12,
  freq1,
  freq2, 
  t.freq0 as all_customers,
  ROUND(support12,8) AS support12, 
  ROUND(support1,8) as support1,
  ROUND(support2,8) as support2,
  ROUND(support12/(support1*support2),3) as lift, 
  ROUND((support12/(support1*support2)) * POWER(support2, 0.25), 4) as lift_adjusted,
  ROUND(cosine_similarity,3) as CS
FROM
  total AS t 
  INNER JOIN stats_raw AS s
    ON t.rundate=s.rundate
WHERE
  freq12 >= 3
  -- WHERE (freq12 > 100 OR (freq12 > 5 AND support12/support1 > 0.01))
-- QUALIFY ROW_NUMBER() OVER(PARTITION BY cat_id1 ORDER BY freq12 DESC) < 100
ORDER BY freq1 desc, lift_adjusted desc
);



/* Last Viewed Cat Advert Affinity */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_viewed_latest_advert_catid_affinity') AS (
WITH cte_view_item AS (
SELECT 
   c.rundate
  , c.account_Number
  , pv.cat_id
  , pv.timestamp
  , MIN(pv.timestamp) OVER (PARTITION BY c.rundate, c.account_number) AS first_view_item
  -- pid is just the tiebreaker - should not be necessary
  , RANK() OVER (PARTITION BY c.rundate, c.account_number ORDER BY   timestamp DESC, viewtimespentsecs DESC, pid) AS view_order
FROM 
  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_build_page_views')  AS pv
  INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_customer_base') AS c 
    ON pv.account_number=c.account_number
    AND pv.viewdate BETWEEN c.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND c.rundate - INTERVAL '1' DAY
) 
SELECT 
     b.account_number
    ,b.rundate
   -- ,b.advertid 
    , b.control_sheet_AdID
    , v.cat_id AS last_viewed_catid
    , regexp_replace( aw.attr_key, 'cat_id_','') AS higest_associated_catid
    , aw.normalised_weight AS highest_associated_catid_weight
    , cb.support12
    , cb.support1
    , cb.support2 
    , cb.lift 
    , cb.lift_adjusted
    ,cb.cs 
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base') AS b
    INNER JOIN cte_view_item AS v 
    ON v.account_number=b.account_number
     AND b.rundate=v.rundate
     AND v.view_order=1 
    LEFT JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_advert_attribute_weighting') AS aw 
        ON aw.UniqueAdID=b.control_sheet_AdID
        AND aw.rundate=b.rundate
        AND aw.dominant_attr_key_flag=1
        AND aw.attr_type ='cat_id'
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_catid_view_basket_affinity') AS cb 
        ON cb.cat_id2=regexp_replace( aw.attr_key, 'cat_id_','')
        AND cb.rundate=b.rundate
        AND cb.cat_id1=v.cat_id
);



/* Purchased Latest Advert Category Affinity */

CREATE OR REPLACE TABLE IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_purchased_latest_advert_catid_affinity') AS (
WITH cte_purchased_item AS (
SELECT 
   c.rundate
  , c.account_Number
  , pv.cat_id
  , ROW_NUMBER() OVER (PARTITION BY c.rundate, c.account_number ORDER BY pv.order_date DESC, pv.order_value DESC
  , pv.order_qty DESC) AS last_purchase_item
FROM 
  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_build_year_baskets')  AS pv
  INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_customer_base') AS c 
    ON pv.account_number=c.account_number
    AND pv.order_date BETWEEN c.rundate - (INTERVAL '1 DAY' * (:lookback_period + 1)) AND c.rundate - INTERVAL '1' DAY
) 
SELECT 
     b.account_number
    ,b.rundate
    --,b.advertid 
    , b.control_sheet_AdID
    , v.cat_id AS last_viewed_catid
    , regexp_replace( aw.attr_key, 'cat_id_','') AS higest_associated_catid
    , aw.normalised_weight AS highest_associated_catid_weight
    , cb.support12
    , cb.support1
    , cb.support2 
    , cb.lift 
    , cb.lift_adjusted
    ,cb.cs 
FROM 
    IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_training_sessions_base') AS b
    INNER JOIN  cte_purchased_item AS v 
    ON v.account_number=b.account_number
     AND b.rundate=v.rundate
     AND v.last_purchase_item=1
    LEFT JOIN  IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_advert_attribute_weighting') AS aw 
        ON aw.UniqueAdID=b.control_sheet_AdID
        AND aw.rundate=b.rundate
        AND aw.dominant_attr_key_flag=1
        AND aw.attr_type ='cat_id'
    INNER JOIN IDENTIFIER(:catalog_schema_prefix||'.'|| :table_prefix || '_catid_view_basket_affinity') AS cb 
        ON cb.cat_id2=regexp_replace( aw.attr_key, 'cat_id_','')
        AND cb.rundate=b.rundate
        AND cb.cat_id1=v.cat_id
);

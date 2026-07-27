--TODO: Review all the tables. Are we joining everything together? Are we missing anything? - missing advanced features and target
CREATE OR REPLACE TEMPORARY VIEW spine AS
with base as (
  select distinct account_number,itemno,theme
  from warehouse.baskets_uk_3y
  inner join(
    select distinct pid, theme from warehouse.next_uk_nextads_item_themes_latest
  )
  on pid = itemno
  where order_date >= date_add(date"{reference_date}", -365)
  and theme is not null
),
base_filtered as (
  select distinct account_number from base
),
0_theme_mapping as (
  SELECT  
  distinct *, regexp_replace(theme, '[^a-zA-Z0-9]', '') as theme_clean
FROM
  warehouse.next_uk_nextads_item_themes_latest
  -- where rundate = "2026-02-04"
  where theme_rank = 1
),
themes as (
  select distinct *, date"{reference_date}" as reference_date from base_filtered 
  cross join (select distinct theme_clean from 0_theme_mapping)
),
spine as (
  SELECT reference_date, account_number, theme_clean
  FROM (
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {catalog}.{table_prefix}_algo_atbs1
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {catalog}.{table_prefix}_algo_atbs5
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {catalog}.{table_prefix}_algo_baskets1
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {catalog}.{table_prefix}_algo_baskets5
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {catalog}.{table_prefix}_algo_views1
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {catalog}.{table_prefix}_algo_views5
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean
    FROM {catalog}.{table_prefix}_atbs_bytheme
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean
    FROM {catalog}.{table_prefix}_baskets_bytheme
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean
    FROM {catalog}.{table_prefix}_views_bytheme
    WHERE reference_date = date"{reference_date}"
    UNION
    SELECT reference_date, account_number, theme_clean
    FROM {catalog}.{table_prefix}_repurchase
    WHERE reference_date = date"{reference_date}"
  )
)

select a.* , spine.* except(account_number,theme_clean,reference_date)
from (select * from themes group by all) a
left join spine
using(account_number, theme_clean, reference_date)
where a.account_number is not null

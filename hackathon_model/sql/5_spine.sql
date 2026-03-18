--TODO: Review all the tables. Are we joining everything together? Are we missing anything? - missing advanced features and target
CREATE OR REPLACE TEMPORARY VIEW spine AS
with base as (
  select distinct account_number
  from warehouse.baskets_uk_3y
  where order_date >= date_add(current_date(), -365)
),
spine as (
  SELECT DISTINCT reference_date, account_number, theme_clean
  FROM (
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {catalog}.{table_prefix}_algo_atbs1
    WHERE reference_date = date"{reference_date}"
    UNION ALL
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {catalog}.{table_prefix}_algo_atbs5
    WHERE reference_date = date"{reference_date}"
    UNION ALL
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {catalog}.{table_prefix}_algo_baskets1
    WHERE reference_date = date"{reference_date}"
    UNION ALL
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {catalog}.{table_prefix}_algo_baskets5
    WHERE reference_date = date"{reference_date}"
    UNION ALL
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {catalog}.{table_prefix}_algo_views1
    WHERE reference_date = date"{reference_date}"
    UNION ALL
    SELECT reference_date, account_number, theme_clean2 AS theme_clean
    FROM {catalog}.{table_prefix}_algo_views5
    WHERE reference_date = date"{reference_date}"
    UNION ALL
    SELECT reference_date, account_number, theme_clean
    FROM {catalog}.{table_prefix}_atbs_bytheme
    WHERE reference_date = date"{reference_date}"
    UNION ALL
    SELECT reference_date, account_number, theme_clean
    FROM {catalog}.{table_prefix}_baskets_bytheme
    WHERE reference_date = date"{reference_date}"
    UNION ALL
    SELECT reference_date, account_number, theme_clean
    FROM {catalog}.{table_prefix}_views_bytheme
    WHERE reference_date = date"{reference_date}"
    UNION ALL
    SELECT reference_date, account_number, theme_clean
    FROM {catalog}.{table_prefix}_repurchase
    WHERE reference_date = date"{reference_date}"
  )
)

select spine.* 
from base
left join spine
on base.account_number = spine.account_number
where spine.account_number is not null

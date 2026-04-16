-- with baskets_themes as (
  select /*+ BROADCAST(tm) */
    date"{reference_date}" as reference_date,
    account_number,
    theme,
    theme_clean,
    order_date,
    count(distinct itemno) as num_items_purchased,
    sum(s740orderstakenvalue) as s740orderstakenvalue,
    current_date() as rundate
  FROM {catalog}.{table_prefix}_baskets
  INNER JOIN 0_theme_mapping ON pid = itemno
  WHERE order_date between date"{start_date}" and date"{end_date}"
  group by 1,2,3,4,5
-- )
-- select
--   date"{reference_date}" as reference_date,
--   account_number,
--   theme,
--   theme_clean,
--   order_date,
--   s740orderstakenvalue,
--   num_items_purchased, 
--   current_date() as rundate
-- from base
-- INNER JOIN baskets_themes USING (account_number)

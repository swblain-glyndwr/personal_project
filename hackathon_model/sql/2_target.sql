with baskets as (
  SELECT 
    *
  FROM {catalog}.{table_prefix}_baskets
  WHERE order_date between date"{target_month_start}" and date"{target_month_end}"
),
baskets_themes as (
  select
    account_number,
    theme,
    theme_clean,
    order_date,
    count(distinct itemno) as num_items_purchased,
    sum(s740orderstakenvalue) as s740orderstakenvalue
  from baskets
  INNER JOIN 0_theme_mapping ON pid = itemno
  group by 1,2,3,4
)
select
  date"{reference_date}" as reference_date,
  account_number,
  theme,
  theme_clean,
  min(order_date) as order_date,
  sum(num_items_purchased) as num_items_purchased,
  sum(s740orderstakenvalue) as s740orderstakenvalue, 
  current_date() as rundate
FROM baskets_themes
group by 1,2,3,4

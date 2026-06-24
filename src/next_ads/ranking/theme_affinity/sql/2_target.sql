with baskets_themes as (
  SELECT
    account_number,
    theme,
    theme_clean,
    order_date,
    count(distinct itemno) as num_items_purchased,
    sum(s740orderstakenvalue) as s740orderstakenvalue
  FROM {schema}.{table_prefix}_baskets
  INNER JOIN 0_theme_mapping ON pid = itemno
  WHERE order_date between date"{target_month_start}" and date"{target_month_end}"
  GROUP BY 1,2,3,4
)
SELECT
  date"{reference_date}" as reference_date,
  account_number,
  theme,
  theme_clean,
  min(order_date) as order_date,
  sum(num_items_purchased) as num_items_purchased,
  sum(s740orderstakenvalue) as s740orderstakenvalue,
  current_date() as rundate
FROM baskets_themes
GROUP BY 1,2,3,4

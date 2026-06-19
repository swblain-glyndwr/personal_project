  select
    date"{reference_date}" as reference_date,
    b.account_number,
    tm.theme,
    tm.theme_clean,
    b.order_date,
    count(distinct itemno) as num_items_purchased,
    sum(s740orderstakenvalue) as s740orderstakenvalue,
    current_date() as rundate
  FROM {schema}.{table_prefix}_baskets b
  INNER JOIN 0_theme_mapping tm ON tm.pid = b.itemno
  WHERE order_date between date"{start_date_baskets}" and date"{end_date_baskets}"
  group by 1,2,3,4,5

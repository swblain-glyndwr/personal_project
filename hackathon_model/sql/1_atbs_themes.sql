with atbs as (
  SELECT *
  FROM {catalog}.{table_prefix}_atbs
  WHERE date between date"{start_date}" and date"{end_date}"
)
  select
    date"{reference_date}" as reference_date,
    accountnumber_rpid as account_number,
    theme,
    theme_clean,
    date,
    max(timestamp) as timestamp,
    count(distinct itemnumber) as num_items_purchased,
    current_date() as rundate
  from atbs 
  INNER JOIN 0_theme_mapping ON pid = itemnumber
  group by 1,2,3,4,5
-- )
-- select
--   date"{reference_date}" as reference_date,
--   account_number,
--   theme,
--   theme_clean,
--   date,
--   timestamp,
--   num_items_purchased, 
--   current_date() as rundate
-- from base
-- INNER JOIN atbs_themes USING (account_number)

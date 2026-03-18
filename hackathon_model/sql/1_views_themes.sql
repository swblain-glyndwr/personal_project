with views as (
  SELECT *
  FROM {catalog}.{table_prefix}_views
  WHERE date between date"{start_date}" and date"{end_date}"
)
-- views_themes as (
  select
    date"{reference_date}" as reference_date,
    accountnumber_rpid as account_number,
    theme,
    theme_clean,
    date,
    max(timestamp) as timestamp,
    count(distinct itemnumber) as num_items_purchased,
    current_date() as rundate
  from views
  INNER JOIN 0_theme_mapping ON pid = itemnumber
  group by 1,2,3,4,5
-- )
-- select
--   date"{reference_date}" as reference_date, -- clean up to line 11
--   account_number,
--   theme,
--   theme_clean,
--   date,
--   timestamp,
--   num_items_purchased, 
--   current_date() as rundate
-- from base
-- INNER JOIN views_themes USING (account_number)

SELECT /*+ BROADCAST(tm) */
  date"{reference_date}" as reference_date,
  accountnumber_rpid as account_number,
  theme,
  theme_clean,
  date,
  max(timestamp) as timestamp,
  count(distinct itemnumber) as num_items_purchased,
  current_date() as rundate
FROM {catalog}.{table_prefix}_views
INNER JOIN 0_theme_mapping ON pid = itemnumber
WHERE date between date"{start_date}" and date"{end_date}"
GROUP BY 1,2,3,4,5

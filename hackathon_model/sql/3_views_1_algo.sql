SELECT
  T1.reference_date,
  T1.account_number,
  T2.theme_clean2,
  sum(freq12) as theme_clean2_views_freq12,
  sum(lift) as theme_clean2_views_lift,
  max(CASE WHEN freq12 >= 5 THEN lift else 0 end) as theme_clean2_views_lift_max,
  sum(cs) as theme_clean2_views_cs,
  array_agg(distinct theme_clean1) as seed_theme, 
  current_date() as rundate
FROM {catalog}.{table_prefix}_views_bytheme T1
LEFT JOIN {catalog}.{table_prefix}_vatb T2
  ON T1.theme_clean = T2.theme_clean1 AND T1.reference_date = T2.reference_date
WHERE recency_rank = 1
  AND T1.reference_date = date"{reference_date}"
GROUP BY 1, 2, 3

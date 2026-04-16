with pairs_raw as (
  SELECT
    reference_date,
    account_number,
    date,
    t1.theme_clean as theme_clean1,
    t2.theme_clean as theme_clean2
  FROM {catalog}.{table_prefix}_views_themes t1
  INNER JOIN {catalog}.{table_prefix}_atbs_themes t2
  USING (account_number, date, reference_date)
  WHERE
    t2.timestamp >= t1.timestamp
    AND t1.theme_clean != t2.theme_clean
    AND t1.reference_date = date"{reference_date}"
),
total as (
  SELECT
    reference_date,
    COUNT(DISTINCT account_number) as freq0
  FROM pairs_raw
  GROUP BY 1
),
views_count as (
  SELECT
    reference_date,
    theme_clean1,
    count(distinct account_number) as views
  FROM pairs_raw
  GROUP BY 1,2
),
ATBs_count as (
  SELECT
    reference_date,
    theme_clean2,
    count(distinct account_number) as atbs
  FROM pairs_raw
  GROUP BY 1,2
),
pair_count as (
  SELECT
    reference_date,
    theme_clean1,
    theme_clean2,
    count(distinct account_number) as freq12
  FROM pairs_raw
  GROUP BY 1,2,3
),
stats_raw as (
  SELECT
    reference_date,
    t0.theme_clean1,
    t0.theme_clean2,
    t0.freq12,
    freq0,
    t1.views as freq1,
    t2.atbs as freq2,
    t1.views/freq0 as support1,
    t2.atbs/freq0 as support2,
    t0.freq12/freq0 as support12,
    t0.freq12/(sqrt(t1.views) * sqrt(t2.atbs)) as cosine_similarity
  FROM pair_count t0
  LEFT JOIN views_count t1 USING (theme_clean1, reference_date)
  LEFT JOIN atbs_count t2 USING (theme_clean2, reference_date)
  LEFT JOIN total USING (reference_date)
)
SELECT
  reference_date,
  theme_clean1,
  theme_clean2,
  freq12,
  freq1,
  freq2,
  freq0 as all_customers,
  ROUND(support12,8) AS support12,
  ROUND(support1,8) as support1,
  ROUND(support2,8) as support2,
  ROUND(support12/(support1*support2),3) as lift,
  ROUND((support12/(support1*support2)) * POWER(support2, 0.25), 4) as lift_adjusted,
  ROUND(cosine_similarity,3) as CS,
  current_date() as rundate
FROM stats_raw
WHERE freq12 >= 3
QUALIFY ROW_NUMBER() OVER(PARTITION BY theme_clean1 ORDER BY freq12 DESC) < 100

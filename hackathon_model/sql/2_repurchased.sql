WITH purchase_history as (
  SELECT
    *, --TODO: if someone has bought the same theme e.g. twice on the same date, what is the value of prev_order_date? still the previous date
    LAG(order_date) over(partition by account_number, theme_clean ORDER BY order_date) as prev_order_date
  FROM {catalog}.{table_prefix}_baskets_themes
  WHERE reference_date = date"{reference_date}"
),
repurchase_gaps as (
  SELECT
    *,
    date_diff(order_date, prev_order_date) as days_between_purchases
  FROM purchase_history
  WHERE prev_order_date is not null
    AND date_diff(order_date, prev_order_date) > 60
),
customer_theme_median as (
  SELECT
    account_number,
    theme_clean,
    percentile_approx(days_between_purchases, 0.5) as customer_median_days,
    count(*) as num_gaps,
    ARRAY_AGG(DISTINCT days_between_purchases) AS days_between_purchases_array
  FROM repurchase_gaps
  GROUP BY account_number, theme_clean
  HAVING num_gaps = 1
),
theme_stats AS (
  SELECT
    theme_clean,
    PERCENTILE_APPROX(customer_median_days, 0.5)*1.2 AS median_repurchase_days,
    COUNT(DISTINCT account_number) AS customers_with_repurchases,
    PERCENTILE_APPROX(customer_median_days, 0.25)*1.2 AS p25_repurchase_days,
    PERCENTILE_APPROX(customer_median_days, 0.75)*1.2 AS p75_repurchase_days,
    AVG(customer_median_days) AS mean_repurchase_days,
    STDDEV(customer_median_days) AS stddev_repurchase_days
  FROM customer_theme_median
  GROUP BY theme_clean
),
customer_last_purchase AS (
  SELECT
    account_number,
    theme_clean,
    MAX(order_date) AS last_order_date,
    COUNT(*) AS total_purchases_in_theme_clean,
    MIN(order_date) AS first_order_date
  FROM {catalog}.{table_prefix}_baskets_themes
  WHERE reference_date = date"{reference_date}"
  GROUP BY account_number, theme_clean
)
SELECT
  date"{reference_date}" as reference_date,
  clp.account_number,
  clp.theme_clean,
  clp.last_order_date,
  clp.first_order_date,
  clp.total_purchases_in_theme_clean,
  DATEDIFF(date"{reference_date}", clp.last_order_date) AS days_since_last_purchase,
  ts.median_repurchase_days,
  ts.p25_repurchase_days,
  ts.p75_repurchase_days,
  ts.mean_repurchase_days,
  ts.stddev_repurchase_days,
  ts.customers_with_repurchases,
  CASE
    WHEN ts.median_repurchase_days IS NULL THEN NULL
    ELSE ROUND(
      DATEDIFF(date"{reference_date}", clp.last_order_date) / ts.median_repurchase_days,
      2
    )
  END AS repurchase_ratio,
  CASE
    WHEN ts.median_repurchase_days IS NULL THEN 'insufficient_data'
    WHEN DATEDIFF(date"{reference_date}", clp.last_order_date) < ts.p25_repurchase_days THEN 'too_soon'
    WHEN DATEDIFF(date"{reference_date}", clp.last_order_date) < ts.median_repurchase_days THEN 'approaching'
    WHEN DATEDIFF(date"{reference_date}", clp.last_order_date) < ts.p75_repurchase_days THEN 'due'
    ELSE 'overdue'
  END AS repurchase_stage, 
  current_date() as rundate
FROM customer_last_purchase clp
LEFT JOIN theme_stats ts ON clp.theme_clean = ts.theme_clean

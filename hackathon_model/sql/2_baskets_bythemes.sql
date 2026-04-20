with base as (
  SELECT
    account_number,
    reference_date,
    theme_clean,
    max(s740orderstakenvalue) as s740orderstakenvalue_max,
    max(order_date) as date_max,
    count(distinct order_date) as frequency,
    max(if(order_date between reference_date - 28 and "{end_date}", 1, 0)) as recency28,
    max(if(order_date between reference_date - 7 and "{end_date}", 1, 0)) as recency7
  FROM {catalog}.{table_prefix}_baskets_themes
  WHERE reference_date = date"{reference_date}"
  GROUP BY ALL
)
SELECT
  reference_date,
  account_number,
  theme_clean,
  recency28,
  recency7,
  datediff(reference_date, date_max) as recency,
  frequency,
  row_number() OVER(PARTITION BY reference_date, account_number ORDER BY date_max DESC, round(s740orderstakenvalue_max, 2) DESC, theme_clean ASC) as recency_rank,
  current_date() as rundate
FROM base
QUALIFY recency_rank <= 15

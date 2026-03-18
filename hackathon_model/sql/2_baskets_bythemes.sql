with baskets as (
  select *
  FROM {catalog}.{table_prefix}_baskets_themes
  WHERE reference_date = date"{reference_date}"
),
base as (
  SELECT
    baskets.account_number,
    baskets.reference_date,
    baskets.theme_clean,
    max(s740orderstakenvalue) as s740orderstakenvalue_max,
    max(baskets.order_date) as date_max,
    count(distinct baskets.order_date) as frequency,
    max(if(baskets.order_date between baskets.reference_date -28 and "{end_date}", 1, 0)) as recency28,
    max(if(baskets.order_date between baskets.reference_date -7 and "{end_date}", 1,0)) as recency7
  FROM baskets
  group by all
)
SELECT distinct
  reference_date,
  account_number,
  theme_clean,
  recency28,
  recency7,
  date_diff(base.reference_date, date_max) as recency,
  frequency,
  row_number() over(partition by reference_date, account_number ORDER BY base.date_max desc, s740orderstakenvalue_max desc) as recency_rank, 
  current_date() as rundate
FROM base
QUALIFY recency_rank <= 15

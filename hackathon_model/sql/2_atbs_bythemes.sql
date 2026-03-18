with atbs as (
  SELECT DISTINCT
    account_number,
    date,
    theme_clean,
    timestamp,
    reference_date
  FROM {catalog}.{table_prefix}_atbs_themes
  WHERE reference_date = date"{reference_date}"
),
base as (
  SELECT
    atbs.account_number,
    reference_date,
    theme_clean,
    max_by(timestamp, struct(date, timestamp)) as timestamp,
    max(atbs.date) as date,
    count(distinct atbs.date) as frequency,
    max(if(atbs.date between reference_date -28 and "{end_date}", 1, 0)) as recency28,
    max(if(atbs.date between reference_date - 7 and "{end_date}", 1,0)) as recency7
  FROM atbs
  group by ALL
)
SELECT distinct
  reference_date,
  account_number,
  theme_clean,
  recency28,
  recency7,
  date_diff(reference_date, base.date) as recency,
  frequency,
  IF( row_number() over(partition by reference_date, account_number ORDER BY base.date desc, frequency desc) = 1, 1, 0) AS most_recent,
  row_number() over(partition by reference_date, account_number ORDER BY base.date desc, timestamp desc, frequency desc) as recency_rank, 
  current_date() as rundate
FROM base
QUALIFY recency_rank <= 30

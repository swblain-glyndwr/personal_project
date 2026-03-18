with 0_theme_mapping as (
  SELECT  
  distinct *, regexp_replace(theme, '[^a-zA-Z0-9]', '') as theme_clean
FROM
  warehouse.next_uk_nextads_item_themes_latest
  -- where rundate = "2026-02-04"
  where theme_rank = 1)
,views_ly as (
  select distinct theme_clean, date, count(*) as daily_count 
  from {catalog}.{table_prefix}_views_ly
  left join 0_theme_mapping
  on itemnumber = pid
  group by all
),
views as (
  select distinct theme_clean, date, count(*) as daily_count 
  from {catalog}.{table_prefix}_views
  left join 0_theme_mapping
  on itemnumber = pid
  group by all
),
baskets_ly as (
  select distinct theme_clean, order_date as date, count(*) as daily_count 
  from {catalog}.{table_prefix}_baskets_ly
  left join 0_theme_mapping
  on itemno = pid
  group by all
),
baskets as (
  select distinct theme_clean, order_date as date, count(*) as daily_count 
  from {catalog}.{table_prefix}_baskets
  left join 0_theme_mapping
  on itemno = pid
  group by all
),
ly_metrics as (
  select distinct a.theme_clean,
  sum(case when a.date >= date_sub('{end_date_views_ly}', 7) then a.daily_count else 0 end) as views_ly_7,
  sum(case when a.date >= date_sub('{end_date_views_ly}', 30) then a.daily_count else 0 end) as views_ly_30,
  sum(case when b.date >= date_sub('{end_date_baskets_ly}', 7) then b.daily_count else 0 end) as baskets_ly_7,
  sum(case when b.date >= date_sub('{end_date_baskets_ly}', 30) then b.daily_count else 0 end) as baskets_ly_30,
  sum(case when c.date >= date_sub('{end_date_views}', 7) then c.daily_count else 0 end) as views_7,
  sum(case when c.date >= date_sub('{end_date_views}', 30) then c.daily_count else 0 end) as views_30
  from views c
  left join views_ly a 
  on c.theme_clean = a.theme_clean
  left join baskets_ly b
  on a.theme_clean = b.theme_clean
  group by all
),
avg_metrics as (
  select distinct *, 
  (views_7 / 7) as avg_views_7, 
  (views_30 / 30) as avg_views_30 
  from ly_metrics
  group by all),

trending_metrics as (
  select distinct * except(avg_views_7, avg_views_30), 
  (avg_views_7/avg_views_30) as trending_7x30 
  from avg_metrics 
  group by all
)

select 
  date"{reference_date}" as reference_date,
  *,
  current_date() as rundate 
from 
  trending_metrics
group by all
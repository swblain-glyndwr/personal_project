with base as (
  select distinct account_number,itemno,theme,s740orderstakenvalue, order_date, date"{reference_date}" as reference_date
  from marketingdata_prod.warehouse.baskets_uk_3y
  inner join(
    select distinct pid, theme from marketingdata_prod.warehouse.next_uk_nextads_item_themes_latest
  )
  on pid = itemno
  where order_date >= date_add(date"{reference_date}", -365)
  -- and theme is not null
),
base_filtered as (
  select distinct account_number, itemno, s740orderstakenvalue, order_date from base
),
 baskets_product as (
  select
    a.account_number, a.s740orderstakenvalue, a.itemno ,
    b.department
  FROM base_filtered a
  left join {schema}.{table_prefix}_product_catalog b
  on itemno = pid
  WHERE order_date <= date"{end_date_baskets}"
  group by all
),

t1 as (
  SELECT
    account_number, SUM(womenswear) AS womenswear, SUM(menswear) AS menswear, SUM(childrenswear) AS childrenswear, SUM(homeware) AS homeware, SUM(beauty) AS beauty
  FROM
    baskets_product
  PIVOT (
    COUNT(DISTINCT itemno)
    FOR department IN ('womenswear', 'menswear', 'childrenswear', 'homeware', 'beauty')
  )
  group by 1
),

t2 as (
  select
    account_number,
    CASE
      WHEN childrenswear >0 AND (womenswear >0 OR menswear >0) THEN 1 else 0 end as FamilyCustSeg, -- removed mens AND womens requirements to account for single parent families
    CASE
      WHEN womenswear >0 AND menswear>0 THEN 1 else 0 end as CoupleCustSeg,
    CASE
      WHEN womenswear >0 THEN 1 else 0 end as WomensCustSeg,
    CASE
      WHEN menswear >0 THEN 1 else 0 end as MenCustSeg,
    CASE
      WHEN beauty >0 THEN 1 else 0 end as BeautyCustSeg,
    CASE
      WHEN homeware >0 THEN 1 else 0 end as HomewareCustSeg,
    t1.* except(account_number)

  FROM
    t1
)

, output_1 as (
SELECT
  account_number,
  FamilyCustSeg, CoupleCustSeg, WomensCustSeg, MenCustSeg, BeautyCustSeg, HomewareCustSeg,
  CASE
    WHEN FamilyCustSeg = 1 THEN CAST(LEAST(womenswear, menswear, childrenswear) AS DECIMAL) / GREATEST(womenswear, menswear, childrenswear)
    ELSE null
  END AS Familyconfidence_score,
  CASE
    WHEN CoupleCustSeg = 1 THEN CAST(LEAST(womenswear, menswear) AS DECIMAL) / GREATEST(womenswear, menswear)
    ELSE null -- 1.0 Perfect confidence for single-category segments
  END AS Coupleconfidence_score,
  CASE
    WHEN WomensCustSeg = 1 THEN CAST(womenswear AS DECIMAL) / GREATEST(womenswear, menswear, childrenswear, homeware, beauty)
    ELSE null -- 1.0 Perfect confidence for single-category segments
  END AS Womenswearconfidence_score,
  CASE
    WHEN MenCustSeg = 1 THEN CAST(menswear AS DECIMAL) / GREATEST(womenswear, menswear, childrenswear, homeware, beauty)
    ELSE null -- 1.0 Perfect confidence for single-category segments
  END AS Menswearconfidence_score,
  CASE
    WHEN BeautyCustSeg = 1 THEN CAST(beauty AS DECIMAL) / GREATEST(womenswear, menswear, childrenswear, homeware, beauty)
    ELSE null -- 1.0 Perfect confidence for single-category segments
  END AS Beautyconfidence_score,
  CASE
    WHEN HomewareCustSeg = 1 THEN CAST(homeware AS DECIMAL) / GREATEST(womenswear, menswear, childrenswear, homeware, beauty)
    ELSE null -- 1.0 Perfect confidence for single-category segments
  END AS Homeconfidence_score,
  * except(account_number, FamilyCustSeg, CoupleCustSeg, WomensCustSeg, MenCustSeg, BeautyCustSeg, HomewareCustSeg)
FROM t2
)

, total_spending as(
  select distinct account_number, sum(s740orderstakenvalue) as total_spend
  from baskets_product
  group by all
  order by total_spend desc
),

bucket_spending as (
  select
    account_number,
    total_spend,
    case
      when percent_rank() over (order by total_spend desc) <= 0.25 then 3
      when percent_rank() over (order by total_spend desc) <= 0.50 then 2
      when percent_rank() over (order by total_spend desc) <= 0.75 then 1
      else 0
    end as spend_bucket
  from total_spending
),

output_final as (
  select distinct a.*, b.total_spend, b.spend_bucket
  from output_1 a
  left join bucket_spending b
  on a.account_number = b.account_number
  group by all
)

select distinct
date"{reference_date}" as reference_date,
*,
current_date() as rundate
from output_final
group by all

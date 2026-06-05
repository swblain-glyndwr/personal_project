-- create view marketingdata_prod.ds_sandbox.next_uk_nextads_account_department_weight_latest as
with baskets as (
  select 
    distinct account_number,itemno,order_date
  from warehouse.baskets_uk_3y
  where order_date between date_add(current_date, -730) and current_date
),

baskets_product as (
  select 
    a.account_number, a.itemno, 
    1.0 - (date_diff(current_date, a.order_date) / 730.0) as purchase_weight,
    case 
        when (
            b.department regexp 'childrenswear' or department regexp 'baby')
            and (b.gender regexp 'newborn') then 'baby'
        when b.department regexp 'childrenswear' and b.gender regexp 'boys' then 'boyswear'
        when b.department regexp 'childrenswear' and b.gender regexp 'girls' then 'girlswear'
        when b.department regexp 'home' then 'homeware'
        when b.department regexp 'beauty' then 'beauty'
        else b.department
    end as department
  FROM baskets a 
  left join (select distinct pid, department, gender from warehouse.product_catalog_history) b
  on itemno = pid
  group by all
),

views_site as(
    SELECT
        AccountNumber_RPID,
        ProductSKU AS itemnumber
    FROM warehouse.bq_views_next_uk
    INNER JOIN warehouse.bq_sessions_next_uk USING (UniqueVisitID, DATE)
    WHERE date between date_add(current_date, -60) and current_date
        AND EventType regexp "pdp_view"
        AND ProductSKU is not null
        AND AccountNumber_RPID is not null
    GROUP BY 1,2
),

views_app as(
    SELECT
        AccountNumber_RPID,
        ProductSKU AS itemnumber
    FROM warehouse.bq_views_next_uk_app
    INNER JOIN warehouse.bq_sessions_next_uk_app USING (UniqueVisitID, DATE)
    WHERE date between date_add(current_date, -60) and current_date
        AND ProductSKU is not null
        AND AccountNumber_RPID is not null
    GROUP BY 1,2
),

views as(
    SELECT *
    FROM views_site
    UNION ALL
    SELECT *
    FROM views_app
),

views_product as (
    select 
        a.AccountNumber_RPID as account_number, a.itemnumber as itemno, 
        case 
            when (
                b.department regexp 'childrenswear' or department regexp 'baby')
                and (b.gender regexp 'newborn') then 'baby'
            when b.department regexp 'childrenswear' and b.gender regexp 'boys' then 'boyswear'
            when b.department regexp 'childrenswear' and b.gender regexp 'girls' then 'girlswear'
            when b.department regexp 'home' then 'homeware'
            when b.department regexp 'beauty' then 'beauty'
            else b.department
        end as department
    FROM views a 
    left join (select distinct pid, department, gender from warehouse.product_catalog_history) b
    on itemnumber = pid
    group by all
),

t11 as (
  SELECT 
    account_number, SUM(womenswear) AS womenswear, SUM(menswear) AS menswear, SUM(baby) AS baby, SUM(boyswear) AS boyswear, SUM(girlswear) AS girlswear, SUM(homeware) AS homeware, SUM(beauty) AS beauty
  FROM
    baskets_product
  PIVOT (
    SUM(purchase_weight)
    FOR department IN ('womenswear', 'menswear', 'baby', 'boyswear', 'girlswear', 'homeware', 'beauty')
  )
  group by 1
),

t12 as (
  SELECT 
    account_number, SUM(womenswear)*0.5 AS womenswear, SUM(menswear)*0.5 AS menswear, SUM(baby)*0.5 AS baby, SUM(boyswear)*0.5 AS boyswear, SUM(girlswear)*0.5 AS girlswear, SUM(homeware)*0.5 AS homeware, SUM(beauty)*0.5 AS beauty
  FROM
    views_product
  PIVOT (
    COUNT(DISTINCT itemno)
    FOR department IN ('womenswear', 'menswear', 'baby', 'boyswear', 'girlswear', 'homeware', 'beauty')
  )
  group by 1
),

t13 as(
    SELECT *
    FROM t11
    UNION ALL
    SELECT *
    FROM t12
),

t1 as(
  select account_number, sum(womenswear) as womenswear, sum(menswear) as menswear, sum(baby) as baby, sum(boyswear) as boyswear, sum(girlswear) as girlswear, sum(homeware) as homeware, sum(beauty) as beauty
  from t13
  group by account_number
),

t2 as (
  select
    account_number, 
    CASE
      WHEN womenswear >0 THEN 1 else 0 end as womens_customer_segment,
    CASE
      WHEN menswear >0 THEN 1 else 0 end as mens_customer_segment,
    CASE
        WHEN baby >0 THEN 1 else 0 end as baby_customer_segment,
    CASE
      WHEN boyswear >0 THEN 1 else 0 end as boys_customer_segment,
    CASE
      WHEN girlswear >0 THEN 1 else 0 end as girls_customer_segment,
    CASE
      WHEN beauty >0 THEN 1 else 0 end as beauty_customer_segment,
    CASE
      WHEN homeware >0 THEN 1 else 0 end as homeware_customer_segment,
    t1.* except(account_number)

  FROM
    t1
)

, t3 as (
SELECT
  account_number, womens_customer_segment, mens_customer_segment, baby_customer_segment, boys_customer_segment, girls_customer_segment, beauty_customer_segment, homeware_customer_segment,
  CASE
    WHEN womens_customer_segment = 1 THEN CAST(womenswear AS DECIMAL) / GREATEST(
        womenswear, menswear, baby, boyswear, girlswear, homeware, beauty)
    ELSE null
  END AS div_womens,
  CASE
    WHEN mens_customer_segment = 1 THEN CAST(menswear AS DECIMAL) / GREATEST(
        womenswear, menswear, baby, boyswear, girlswear, homeware, beauty)
    ELSE null
  END AS div_mens,
  CASE
    WHEN baby_customer_segment = 1 THEN CAST(baby AS DECIMAL) / GREATEST(
        womenswear, menswear, baby, boyswear, girlswear, homeware, beauty)
    ELSE null
  END AS div_baby,
  CASE
    WHEN boys_customer_segment = 1 THEN CAST(boyswear AS DECIMAL) / GREATEST(
        womenswear, menswear, baby, boyswear, girlswear, homeware, beauty)
    ELSE null
  END AS div_boys,
  CASE
    WHEN girls_customer_segment = 1 THEN CAST(girlswear AS DECIMAL) / GREATEST(
        womenswear, menswear, baby, boyswear, girlswear, homeware, beauty)
    ELSE null
  END AS div_girls,
  CASE
    WHEN beauty_customer_segment = 1 THEN CAST(beauty AS DECIMAL) / GREATEST(
        womenswear, menswear, baby, boyswear, girlswear, homeware, beauty)
    ELSE null
  END AS div_beauty,
  CASE
    WHEN homeware_customer_segment = 1 THEN CAST(homeware AS DECIMAL) / GREATEST(
        womenswear, menswear, baby, boyswear, girlswear, homeware, beauty)
    ELSE null
  END AS div_home,
  * except(account_number, womens_customer_segment, mens_customer_segment, baby_customer_segment, boys_customer_segment, girls_customer_segment, beauty_customer_segment, homeware_customer_segment)
FROM t2
)

select
    account_number,
    div_womens,
    div_mens,
    div_baby,
    div_boys,
    div_girls,
    div_beauty,
    div_home,
    current_date() as rundate
from t3 
group by all
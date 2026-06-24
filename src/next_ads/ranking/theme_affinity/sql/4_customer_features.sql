with mult_accounts as(
  select distinct
    account_number,
    email_hash,
    sites,
    orderdate,
    gender_customer,
    Age,
    PostcodeArea_GB,
    PostcodeArea,
    PafPostTownCode,
    app_web,
    GmaName,
    seasons_site,
    itemno
    from marketingdata_prod.digital_marketing.mktg_uk_basket
  where
  orderdate between date_add(date"{reference_date}", -365) and date"{end_date_baskets}"
  group by all
  order by email_hash, account_number, sites
),

null_themes as (
  select distinct a.*, b.theme
  from mult_accounts a
  left join marketingdata_prod.warehouse.next_uk_nextads_item_themes_latest b
  on a.itemno = b.pid
  where b.theme is not null
),

filtered_accounts as (
  select distinct
    account_number,
    email_hash,
    sites,
    orderdate,
    gender_customer,
    Age,
    PostcodeArea_GB,
    PostcodeArea,
    PafPostTownCode,
    app_web,
    GmaName,
    seasons_site
  from null_themes
  group by all
),

customer_flags as(
  select distinct email_hash,
    case when sites = 'NX' then 1 else 0 end as next_cust,
    case when sites = 'VS' then 1 else 0 end as vs_cust,
    case when sites = 'Reiss' then 1 else 0 end as reiss_cust,
    case when sites = 'FatFace' then 1 else 0 end as fatface_cust,
    case when sites = 'JoJo' then 1 else 0 end as jojo_cust,
    case when sites = 'GAP' then 1 else 0 end as gap_cust,
    case when sites = 'Joules' then 1 else 0 end as joules_cust,
    case when sites = 'Childsplay' then 1 else 0 end as childsplay_cust,
    case when sites = 'Made' then 1 else 0 end as made_cust,
    case when sites = 'Aubin' then 1 else 0 end as aubin_cust
  from filtered_accounts
  group by all
),

grouped_emails as (
  select distinct email_hash, max(next_cust) as next_cust, max(vs_cust) as vs_cust, max(reiss_cust) as reiss_cust, max(fatface_cust) as fatface_cust, max(jojo_cust) as jojo_cust, max(gap_cust) as gap_cust, max(joules_cust) as joules_cust, max(childsplay_cust) as childsplay_cust, max(made_cust) as made_cust, max(aubin_cust) as aubin_cust
  from customer_flags
  group by all
),

tp_customers as(
  select distinct a.account_number, b.*
  from filtered_accounts a
  left join grouped_emails b
  on a.email_hash = b.email_hash
  group by all
  order by email_hash
),

mult_email_customers as(
  select distinct account_number, count(distinct email_hash) as count
  from tp_customers
  group by all
),

suspcious_customers as(
  select distinct account_number, case when count > 1 then 1 else 0 end as suspcious
  from mult_email_customers
),

grouped_tp as(
  select distinct account_number, max(email_hash) as email_hash, max(next_cust) as next_cust, max(vs_cust) as vs_cust, max(reiss_cust) as reiss_cust,
  max(fatface_cust) as fatface_cust, max(jojo_cust) as jojo_cust, max(gap_cust) as gap_cust, max(joules_cust) as joules_cust, max(childsplay_cust) as childsplay_cust,
  max(made_cust) as made_cust, max(aubin_cust) as aubin_cust
  from tp_customers
  group by all),

joined_suspicious as(
  select distinct t.*, s.suspcious
  from grouped_tp t
  left join suspcious_customers s
  on t.account_number = s.account_number
  group by all
),

customer_features as(
  select distinct a.* except(email_hash), mode(b.gender_customer) as gender_customer,max(b.Age) as age,
  mode(b.PostcodeArea_GB) as PostcodeArea_GB, mode(b.PostcodeArea) as PostcodeArea, mode(b.PafPostTownCode) as PafPostTownCode, mode(b.app_web) as app_web, mode(b.GmaName) as GmaName, max(b.seasons_site) as seasons_cust
  from joined_suspicious a
  left join mult_accounts b
  on a.account_number = b.account_number
  group by all
)

select
date"{reference_date}" as reference_date,
* ,
current_date() as rundate
from customer_features
group by all

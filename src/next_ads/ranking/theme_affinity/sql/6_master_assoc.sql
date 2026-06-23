WITH
  atbs1_top10 AS (
    SELECT reference_date, account_number, theme_clean2 AS theme_clean, *
    FROM {schema}.{table_prefix}_algo_atbs1
    WHERE reference_date = date"{reference_date}"
  ),
  atbs5_top10 AS (
    SELECT reference_date, account_number, theme_clean2 AS theme_clean, *
    FROM {schema}.{table_prefix}_algo_atbs5
    WHERE reference_date = date"{reference_date}"
  ),
  baskets1_top10 AS (
    SELECT reference_date, account_number, theme_clean2 AS theme_clean, *
    FROM {schema}.{table_prefix}_algo_baskets1
    WHERE reference_date = date"{reference_date}"
  ),
  baskets5_top10 AS (
    SELECT reference_date, account_number, theme_clean2 AS theme_clean, *
    FROM {schema}.{table_prefix}_algo_baskets5
    WHERE reference_date = date"{reference_date}"
  ),
  views1_top10 AS (
    SELECT reference_date, account_number, theme_clean2 AS theme_clean, *
    FROM {schema}.{table_prefix}_algo_views1
    WHERE reference_date = date"{reference_date}"
  ),
  views5_top10 AS (
    SELECT reference_date, account_number, theme_clean2 AS theme_clean, *
    FROM {schema}.{table_prefix}_algo_views5
    WHERE reference_date = date"{reference_date}"
  ),
  views_behavior AS (
    SELECT *
    FROM {schema}.{table_prefix}_views_bytheme
    WHERE reference_date = date"{reference_date}"
  ),
  atbs_behavior AS (
    SELECT *
    FROM {schema}.{table_prefix}_atbs_bytheme
    WHERE reference_date = date"{reference_date}"
  ),
  baskets_behavior AS (
    SELECT *
    FROM {schema}.{table_prefix}_baskets_bytheme
    WHERE reference_date = date"{reference_date}"
  ),
  baskets_target AS (
    SELECT reference_date, account_number, theme_clean, *, 1 AS label
    FROM {schema}.{table_prefix}_baskets_target
    WHERE reference_date = date"{reference_date}"
  ),
  repurchased AS (
    SELECT reference_date, account_number, theme_clean, repurchase_ratio, repurchase_stage
    FROM {schema}.{table_prefix}_repurchase
    WHERE reference_date = date"{reference_date}"
  ),
  advanced_features AS (
    SELECT *
    FROM {schema}.{table_prefix}_advanced_features
    WHERE reference_date = date"{reference_date}"
  ),
  customer_features AS (
    SELECT *
    FROM {schema}.{table_prefix}_customer_features
    -- WHERE reference_date = date"{reference_date}"
  ),
  customer_segments AS (
    SELECT *
    FROM {schema}.{table_prefix}_customer_segments
    WHERE reference_date = date"{reference_date}"
  ),
  popularity_metrics AS (
    SELECT *
    FROM {schema}.{table_prefix}_popularity_metrics
    -- WHERE reference_date = date"{reference_date}"
  )

SELECT
  spine.reference_date,
  spine.account_number,
  spine.theme_clean,

  -- Algo ATBs_top10 1
  -- COALESCE(t1.theme_clean2_atbs_freq12, 0) AS algo_atbs1__freq12_top10,
  -- COALESCE(t1.theme_clean2_atbs_lift, 0.0) AS algo_atbs1__lift_top10,
  -- COALESCE(t1.theme_clean2_atbs_cs, 0.0) AS algo_atbs1__cs_top10,
  -- COALESCE(t1.freq12_norm, 0.0) AS algo_atbs1__freq12_norm_top10,
  -- CASE WHEN t1.account_number IS NOT NULL THEN 1 ELSE 0 END AS algo_atbs1__retrieved_top10,

  -- Algo ATBs_top10 5
  -- COALESCE(t2.theme_clean2_atbs_freq12, 0) AS algo_atbs5__freq12_top10,
  COALESCE(t2.theme_clean2_atbs_lift, 0.0) AS algo_atbs5__lift_top10,
  -- COALESCE(t2.theme_clean2_atbs_cs, 0.0) AS algo_atbs5__cs_top10,
  -- COALESCE(t2.freq12_norm, 0.0) AS algo_atbs5__freq12_norm_top10,
  -- CASE WHEN t2.account_number IS NOT NULL THEN 1 ELSE 0 END AS algo_atbs5__retrieved_top10,

  -- Algo Baskets_top10 1
  -- COALESCE(t3.theme_clean2_baskets_freq12, 0) AS algo_baskets1__freq12_top10,
  -- COALESCE(t3.theme_clean2_baskets_lift, 0.0) AS algo_baskets1__lift_top10,
  COALESCE(t3.theme_clean2_baskets_cs, 0.0) AS algo_baskets1__cs_top10,
  -- COALESCE(t3.freq12_norm, 0.0) AS algo_baskets1__freq12_norm_top10,
  -- CASE WHEN t3.account_number IS NOT NULL THEN 1 ELSE 0 END AS algo_baskets1__retrieved_top10,

  -- Algo Baskets_top10 5
  COALESCE(t4.theme_clean2_baskets_freq12, 0) AS algo_baskets5__freq12_top10,
  COALESCE(t4.theme_clean2_baskets_lift, 0.0) AS algo_baskets5__lift_top10,
  COALESCE(t4.theme_clean2_baskets_cs, 0.0) AS algo_baskets5__cs_top10,
  COALESCE(t4.freq12_norm, 0.0) AS algo_baskets5__freq12_norm_top10,
  -- CASE WHEN t4.account_number IS NOT NULL THEN 1 ELSE 0 END AS algo_baskets5__retrieved_top10,

  -- Algo Views_top10 1
  -- COALESCE(t5.theme_clean2_views_freq12, 0) AS algo_views1__freq12_top10,
  -- COALESCE(t5.theme_clean2_views_lift, 0.0) AS algo_views1__lift_top10,
  -- COALESCE(t5.theme_clean2_views_cs, 0.0) AS algo_views1__cs_top10,
  -- COALESCE(t5.freq12_norm, 0.0) AS algo_views1__freq12_norm_top10,
  -- CASE WHEN t5.account_number IS NOT NULL THEN 1 ELSE 0 END AS algo_views1__retrieved_top10,

  -- Algo Views_top10 5
  -- COALESCE(t6.theme_clean2_views_freq12, 0) AS algo_views5__freq12_top10,
  COALESCE(t6.theme_clean2_views_lift, 0.0) AS algo_views5__lift_top10,
  COALESCE(t6.theme_clean2_views_cs, 0.0) AS algo_views5__cs_top10,
  -- COALESCE(t6.freq12_norm, 0.0) AS algo_views5__freq12_norm_top10,
  -- CASE WHEN t6.account_number IS NOT NULL THEN 1 ELSE 0 END AS algo_views5__retrieved_top10,

  -- Views behavior
  -- COALESCE(t9.recency28, 0) AS views_behavior__recency28,
  -- COALESCE(t9.recency7, 0) AS views_behavior__recency7,
  COALESCE(t9.recency, 9999) AS views_behavior__recency,
  COALESCE(t9.frequency, 0) AS views_behavior__frequency,
  -- COALESCE(t9.most_recent, 0) AS views_behavior__most_recent,
  COALESCE(t9.recency_rank, 999999) AS views_behavior__recency_rank,
  -- CASE WHEN t9.account_number IS NOT NULL THEN 1 ELSE 0 END AS views_behavior__retrieved,

  -- ATBs Behavior
  -- COALESCE(t7.recency28, 0) AS atbs_behavior__recency28,
  -- COALESCE(t7.recency7, 0) AS atbs_behavior__recency7,
  COALESCE(t7.recency, 9999) AS atbs_behavior__recency,
  COALESCE(t7.frequency, 0) AS atbs_behavior__frequency,
  -- COALESCE(t7.most_recent, 0) AS atbs_behavior__most_recent,
  -- COALESCE(t7.recency_rank, 999999) AS atbs_behavior__recency_rank,
  -- CASE WHEN t7.account_number IS NOT NULL THEN 1 ELSE 0 END AS atbs_behavior__retrieved,

  -- Baskets Behavior
  -- COALESCE(t8.recency28, 0) AS baskets_behavior__recency28,
  -- COALESCE(t8.recency7, 0) AS baskets_behavior__recency7,
  -- COALESCE(t8.recency, 9999) AS baskets_behavior__recency,
  COALESCE(t8.frequency, 0) AS baskets_behavior__frequency,
  COALESCE(t8.recency_rank, 999999) AS baskets_behavior__recency_rank,
  -- CASE WHEN t8.account_number IS NOT NULL THEN 1 ELSE 0 END AS baskets_behavior__retrieved,

  -- Aggregates
  (
    CASE WHEN t1.account_number IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN t2.account_number IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN t3.account_number IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN t4.account_number IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN t5.account_number IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN t6.account_number IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN t7.account_number IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN t8.account_number IS NOT NULL THEN 1 ELSE 0 END +
    CASE WHEN t9.account_number IS NOT NULL THEN 1 ELSE 0 END
  ) AS num_retrieval_methods,

  -- Repurchase features
  COALESCE(t10.repurchase_ratio, 0.0) AS repurchase_ratio,
  COALESCE(t10.repurchase_stage, 'unknown') AS repurchase_stage,

  -- Advanced User Features
  -- COALESCE(t11.user_total_interactions, 0) AS user_total_interactions,
  COALESCE(t11.user_total_views, 0) AS user_total_views,
  COALESCE(t11.user_view_to_atb_rate, 0.0) AS user_view_to_atb_rate,
  -- COALESCE(t11.user_app_ratio, 0.0) AS user_app_ratio,
  -- COALESCE(t11.user_platform_segment, 'unknown') AS user_platform_segment,
  -- COALESCE(t11.user_velocity_score, 0.0) AS user_velocity_score,
  -- COALESCE(t11.user_theme_breadth, 0) AS user_theme_breadth,
  -- COALESCE(t11.user_weekend_ratio, 0.0) AS user_weekend_ratio,
  -- COALESCE(t11.user_median_hour, 12) AS user_median_hour,

  -- Customer Features
  -- COALESCE(t12.next_cust, 0) AS next_cust,
  -- COALESCE(t12.vs_cust, 0) AS vs_cust,
  -- COALESCE(t12.reiss_cust, 0) AS reiss_cust,
  -- COALESCE(t12.fatface_cust, 0) AS fatface_cust,
  -- COALESCE(t12.jojo_cust, 0) AS jojo_cust,
  -- COALESCE(t12.gap_cust, 0) AS gap_cust,
  -- COALESCE(t12.joules_cust, 0) AS joules_cust,
  -- COALESCE(t12.childsplay_cust, 0) AS childsplay_cust,
  -- COALESCE(t12.made_cust, 0) AS made_cust,
  -- COALESCE(t12.aubin_cust, 0) AS aubin_cust,
  -- COALESCE(t12.seasons_cust, 0) AS seasons_cust,
  -- COALESCE(t12.suspcious, 0) AS suspcious,
  -- COALESCE(t12.gender_customer, 'unknown') AS gender_customer,
  -- COALESCE(t12.age, 999) AS age,
  -- COALESCE(t12.PostcodeArea_GB, 'unknown') AS PostcodeArea_GB,
  -- COALESCE(t12.PostcodeArea, 'unknown') AS PostcodeArea,
  -- COALESCE(t12.PafPostTownCode, 'unknown') AS PafPostTownCode,
  -- COALESCE(t12.app_web, 'unknown') AS app_web,
  COALESCE(t12.GmaName, 'unknown') AS GmaName,
  COALESCE(t13.Familyconfidence_score, 0.0) AS Familyconfidence_score,
  COALESCE(t13.Coupleconfidence_score, 0.0) AS Coupleconfidence_score,
  COALESCE(t13.Womenswearconfidence_score, 0.0) AS Womenswearconfidence_score,
  COALESCE(t13.Menswearconfidence_score, 0.0) AS Menswearconfidence_score,
  COALESCE(t13.Beautyconfidence_score, 0.0) AS Beautyconfidence_score,
  COALESCE(t13.Homeconfidence_score, 0.0) AS Homeconfidence_score,
  -- COALESCE(t13.total_spend, 0.0) AS total_spend,
  -- COALESCE(t13.spend_bucket, 0) AS spend_bucket,

  -- Popularity Metrics
  COALESCE(t14.views_ly_7,0) as views_ly_7,
  COALESCE(t14.views_ly_30,0) as views_ly_30,
  COALESCE(t14.baskets_ly_7,0) as baskets_ly_7,
  COALESCE(t14.baskets_ly_30,0) as baskets_ly_30,
  COALESCE(t14.trending_7x30,0) as trending_7x30,


  -- Target label
  COALESCE(target.label, 0) AS label,
  "{start_date_views} > {end_date_views}" as views_dates,
  "{start_date_atbs} > {end_date_atbs}" as atbs_dates,
  "{start_date_baskets} > {end_date_baskets}" as baskets_dates,
  "{target_month_start} > {target_month_end}" as target_dates,
  current_date() as rundate

FROM spine
LEFT JOIN atbs1_top10 t1 USING (account_number, reference_date, theme_clean)
LEFT JOIN atbs5_top10 t2 USING (account_number, reference_date, theme_clean)
LEFT JOIN baskets1_top10 t3 USING (account_number, reference_date, theme_clean)
LEFT JOIN baskets5_top10 t4 USING (account_number, reference_date, theme_clean)
LEFT JOIN views1_top10 t5 USING (account_number, reference_date, theme_clean)
LEFT JOIN views5_top10 t6 USING (account_number, reference_date, theme_clean)
LEFT JOIN atbs_behavior t7 USING (account_number, reference_date, theme_clean)
LEFT JOIN baskets_behavior t8 USING (account_number, reference_date, theme_clean)
LEFT JOIN views_behavior t9 USING (account_number, reference_date, theme_clean)
LEFT JOIN repurchased t10 USING (account_number, reference_date, theme_clean)
LEFT JOIN advanced_features t11 USING (account_number, reference_date)
LEFT JOIN customer_features t12 USING(account_number, reference_date)
LEFT JOIN customer_segments t13 USING(account_number, reference_date)
LEFT JOIN popularity_metrics t14 USING(theme_clean, reference_date)
LEFT JOIN baskets_target target USING (account_number, reference_date, theme_clean)
group by all

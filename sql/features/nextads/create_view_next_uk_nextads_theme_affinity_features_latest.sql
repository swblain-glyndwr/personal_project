CREATE OR REPLACE VIEW {catalog}.{schema}.next_uk_nextads_theme_affinity_features_latest AS
SELECT
  account_number,
  theme,
  reference_date,
  month,
  algo_baskets1__cs_top10,
  algo_baskets5__freq12_top10,
  algo_baskets5__cs_top10,
  algo_baskets5__freq12_norm_top10,
  algo_views5__cs_top10,
  views_behavior__recency,
  views_behavior__frequency,
  views_behavior__recency_rank,
  baskets_behavior__frequency,
  num_retrieval_methods,
  repurchase_ratio,
  repurchase_stage,
  user_total_views,
  user_view_to_atb_rate,
  GmaName,
  views_ly_7,
  views_ly_30,
  baskets_ly_7,
  baskets_ly_30,
  trending_7x30,
  simple_rules_rank
FROM {catalog}.{schema}.next_uk_nextads_fs_theme_affinity_model_input


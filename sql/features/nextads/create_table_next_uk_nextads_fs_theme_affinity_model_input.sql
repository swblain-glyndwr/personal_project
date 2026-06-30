CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_theme_affinity_model_input (
  account_number STRING NOT NULL,
  theme STRING NOT NULL,
  reference_date DATE NOT NULL,
  month INT,
  algo_baskets1__cs_top10 DOUBLE,
  algo_baskets5__freq12_top10 DOUBLE,
  algo_baskets5__cs_top10 DOUBLE,
  algo_baskets5__freq12_norm_top10 DOUBLE,
  algo_views5__cs_top10 DOUBLE,
  views_behavior__recency DOUBLE,
  views_behavior__frequency DOUBLE,
  views_behavior__recency_rank DOUBLE,
  baskets_behavior__frequency DOUBLE,
  num_retrieval_methods INT,
  repurchase_ratio DOUBLE,
  repurchase_stage STRING,
  user_total_views DOUBLE,
  user_view_to_atb_rate DOUBLE,
  GmaName STRING,
  views_ly_7 DOUBLE,
  views_ly_30 DOUBLE,
  baskets_ly_7 DOUBLE,
  baskets_ly_30 DOUBLE,
  trending_7x30 DOUBLE,
  simple_rules_rank INT,
  label DOUBLE,
  model_score DOUBLE,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_theme_affinity_model_input PRIMARY KEY (
    account_number,
    theme,
    reference_date
  )
)
USING delta
PARTITIONED BY (reference_date)

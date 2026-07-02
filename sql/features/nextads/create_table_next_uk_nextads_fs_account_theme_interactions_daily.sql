CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_account_theme_interactions_daily (
  account_number STRING NOT NULL,
  theme STRING NOT NULL,
  reference_date DATE NOT NULL,
  views_behavior__recency DOUBLE,
  views_behavior__frequency DOUBLE,
  views_behavior__recency_rank DOUBLE,
  baskets_behavior__frequency DOUBLE,
  baskets_behavior__recency_rank DOUBLE,
  repurchase_ratio DOUBLE,
  repurchase_stage STRING,
  user_total_views DOUBLE,
  user_view_to_atb_rate DOUBLE,
  num_retrieval_methods INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_account_theme_interactions_daily PRIMARY KEY (
    account_number,
    theme,
    reference_date
  )
)
USING delta
PARTITIONED BY (reference_date)

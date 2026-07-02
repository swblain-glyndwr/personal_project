CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_account_theme_affinity_daily (
  account_number STRING NOT NULL,
  theme STRING NOT NULL,
  reference_date DATE NOT NULL,
  month INT,
  theme_affinity_score DOUBLE,
  simple_rules_rank INT,
  model_score DOUBLE,
  adjusted_score DOUBLE,
  rank INT,
  model_name STRING,
  model_version STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_account_theme_affinity_daily PRIMARY KEY (
    account_number,
    theme,
    reference_date
  )
)
USING delta
PARTITIONED BY (reference_date)

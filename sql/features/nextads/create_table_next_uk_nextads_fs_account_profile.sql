CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_account_profile (
  account_number STRING NOT NULL,
  reference_date DATE NOT NULL,
  country_code STRING,
  client_name STRING,
  account_type STRING,
  account_age_days INT,
  postcode_area STRING,
  region STRING,
  gender STRING,
  credit_type STRING,
  latest_known_activity_recency_days INT,
  online_orders_lifetime DOUBLE,
  online_spend_lifetime DOUBLE,
  retail_orders_lifetime DOUBLE,
  retail_spend_lifetime DOUBLE,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_account_profile PRIMARY KEY (
    account_number,
    reference_date
  )
)
USING delta
PARTITIONED BY (reference_date)


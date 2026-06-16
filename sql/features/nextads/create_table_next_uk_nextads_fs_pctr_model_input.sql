CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_pctr_model_input (
  account_number STRING NOT NULL,
  advert_id STRING NOT NULL,
  location STRING NOT NULL,
  session_date DATE NOT NULL,
  reference_date DATE NOT NULL,
  treatment_type STRING,
  click_label INT,
  device_simple STRING,
  channel_simple STRING,
  geocountry_simple STRING,
  session_hour INT,
  session_dayofweek INT,
  session_month INT,
  all_ctr DOUBLE,
  device_ctr DOUBLE,
  channel_ctr DOUBLE,
  geo_ctr DOUBLE,
  viewed_latest_advert_catid_affinity DOUBLE,
  purchased_latest_advert_catid_affinity DOUBLE,
  customer_advert_impressions_30d BIGINT,
  rules_based_pctr DOUBLE,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_pctr_model_input PRIMARY KEY (
    account_number,
    advert_id,
    location,
    session_date,
    reference_date
  )
)
USING delta
PARTITIONED BY (reference_date)

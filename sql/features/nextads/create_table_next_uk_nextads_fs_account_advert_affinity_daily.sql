CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_account_advert_affinity_daily (
  account_number STRING NOT NULL,
  advert_id STRING NOT NULL,
  location STRING NOT NULL,
  reference_date DATE NOT NULL,
  viewed_latest_advert_catid_affinity DOUBLE,
  purchased_latest_advert_catid_affinity DOUBLE,
  customer_advert_impressions_7d BIGINT,
  customer_advert_impressions_30d BIGINT,
  rules_based_pctr DOUBLE,
  advert_algodivision_impressions BIGINT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_account_advert_affinity_daily PRIMARY KEY (
    account_number,
    advert_id,
    location,
    reference_date
  )
)
USING delta
PARTITIONED BY (reference_date)


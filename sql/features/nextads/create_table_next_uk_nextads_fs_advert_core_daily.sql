CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_advert_core_daily (
  advert_id STRING NOT NULL,
  location STRING NOT NULL,
  feature_date DATE NOT NULL,
  campaign_id STRING,
  advert_url STRING,
  product_urls STRING,
  control_sheet_items STRING,
  advert_title STRING,
  headline STRING,
  subtext STRING,
  cta STRING,
  advert_theme STRING,
  advert_category STRING,
  advert_brand_name STRING,
  page_path STRING,
  template_name STRING,
  source_rundate DATE,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_advert_core_daily PRIMARY KEY (
    advert_id,
    location,
    feature_date
  )
)
USING delta
PARTITIONED BY (feature_date)

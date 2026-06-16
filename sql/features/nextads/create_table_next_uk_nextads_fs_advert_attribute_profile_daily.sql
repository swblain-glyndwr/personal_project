CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_advert_attribute_profile_daily (
  advert_id STRING NOT NULL,
  feature_date DATE NOT NULL,
  campaign_id STRING,
  advert_active_location_count BIGINT,
  has_item_attribute_profile BOOLEAN,
  attribute_profile_attribute_count BIGINT,
  attribute_profile_value_count BIGINT,
  advert_item_count BIGINT,
  advert_item_weight_sum DOUBLE,
  top_brand STRING,
  top_use STRING,
  top_colour STRING,
  top_style STRING,
  top_category STRING,
  top_department STRING,
  top_gender STRING,
  brand_profile_map MAP<STRING, DOUBLE>,
  category_profile_map MAP<STRING, DOUBLE>,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_advert_attribute_profile_daily PRIMARY KEY (
    advert_id,
    feature_date
  )
)
USING delta
PARTITIONED BY (feature_date)


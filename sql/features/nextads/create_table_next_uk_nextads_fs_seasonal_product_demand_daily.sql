CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_seasonal_product_demand_daily (
  entity_type STRING NOT NULL,
  entity_id STRING NOT NULL,
  item_id STRING NOT NULL,
  feature_date DATE NOT NULL,
  product_views_7d BIGINT,
  product_views_30d BIGINT,
  product_purchases_7d BIGINT,
  product_purchases_30d BIGINT,
  product_views_ly_same_month BIGINT,
  product_purchases_ly_same_month BIGINT,
  product_trending_7x30 DOUBLE,
  seasonal_product_embedding ARRAY<DOUBLE>,
  seasonal_product_embedding_coverage DOUBLE,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_seasonal_product_demand_daily PRIMARY KEY (
    entity_type,
    entity_id,
    item_id,
    feature_date
  )
)
USING delta
PARTITIONED BY (feature_date)

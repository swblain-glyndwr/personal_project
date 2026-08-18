CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_advert_product_profile_daily (
  advert_id STRING NOT NULL,
  feature_date DATE NOT NULL,
  embedding_model_name STRING NOT NULL,
  embedding_model_version STRING NOT NULL,
  embedding_artifact_sha256 STRING NOT NULL,
  advert_product_item_count BIGINT,
  advert_product_embedded_item_count BIGINT,
  advert_product_embedding_coverage DOUBLE,
  advert_product_embedding ARRAY<DOUBLE>,
  advert_product_embedding_dimension INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_advert_product_profile_daily PRIMARY KEY (
    advert_id,
    feature_date,
    embedding_model_name,
    embedding_model_version
  )
)
USING delta

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_advert_semantic_profile_daily (
  advert_id STRING NOT NULL,
  feature_date DATE NOT NULL,
  embedding_model_name STRING NOT NULL,
  embedding_model_version STRING NOT NULL,
  advert_text_corpus STRING,
  advert_text_hash STRING,
  advert_embedding ARRAY<DOUBLE>,
  advert_embedding_dimension INT,
  advert_semantic_token_count BIGINT,
  advert_semantic_unique_token_count BIGINT,
  advert_has_destination_image BOOLEAN,
  advert_embedding_neighbour_count BIGINT,
  advert_embedding_top_similarity DOUBLE,
  advert_embedding_avg_similarity DOUBLE,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_advert_semantic_profile_daily PRIMARY KEY (
    advert_id,
    feature_date,
    embedding_model_name,
    embedding_model_version
  )
)
USING delta
PARTITIONED BY (feature_date)

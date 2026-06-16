CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_product_embeddings_latest (
  item_id STRING NOT NULL,
  embedding_model_name STRING NOT NULL,
  embedding_model_version STRING NOT NULL,
  embedding ARRAY<DOUBLE>,
  embedding_dimension INT,
  embedding_text_hash STRING,
  embedding_text STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_product_embeddings_latest PRIMARY KEY (
    item_id,
    embedding_model_name,
    embedding_model_version
  )
)
USING delta


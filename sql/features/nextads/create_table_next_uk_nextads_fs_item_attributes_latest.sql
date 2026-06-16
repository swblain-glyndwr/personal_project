CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_item_attributes_latest (
  item_id STRING NOT NULL,
  brand STRING,
  item_use STRING,
  colour STRING,
  style STRING,
  category STRING,
  department STRING,
  gender STRING,
  pattern STRING,
  fit STRING,
  room STRING,
  activity STRING,
  material STRING,
  collaboration STRING,
  item_title STRING,
  item_url STRING,
  item_image_url STRING,
  item_text_corpus STRING,
  attribute_value_map MAP<STRING, STRING>,
  source_updated_at TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_item_attributes_latest PRIMARY KEY (item_id)
)
USING delta


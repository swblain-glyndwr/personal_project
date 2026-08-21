CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_feature_build_sources (
  feature_build_id STRING NOT NULL,
  feature_build_attempt_id STRING NOT NULL,
  reference_date DATE NOT NULL,
  source_name STRING NOT NULL,
  source_table STRING NOT NULL,
  delta_version BIGINT NOT NULL,
  schema_checksum STRING NOT NULL,
  row_count BIGINT,
  source_feature_id STRING,
  source_feature_build_id STRING,
  source_feature_build_attempt_id STRING,
  source_write_receipt_id STRING,
  captured_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_nextads_feature_build_sources PRIMARY KEY (
    feature_build_id,
    feature_build_attempt_id,
    source_name
  )
)
USING DELTA
PARTITIONED BY (reference_date)

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_feature_snapshot_bindings (
  feature_snapshot_id STRING NOT NULL,
  feature_snapshot_attempt_id STRING NOT NULL,
  feature_build_id STRING NOT NULL,
  feature_build_attempt_id STRING NOT NULL,
  reference_date DATE NOT NULL,
  feature_id STRING NOT NULL,
  backing_table STRING NOT NULL,
  delta_version BIGINT NOT NULL,
  row_count BIGINT NOT NULL,
  output_schema_checksum STRING NOT NULL,
  backing_schema_checksum STRING NOT NULL,
  value_checksum STRING NOT NULL,
  write_receipt_id STRING NOT NULL,
  bound_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_nextads_feature_snapshot_bindings PRIMARY KEY (
    feature_snapshot_id,
    feature_snapshot_attempt_id,
    feature_id
  )
)
USING DELTA
PARTITIONED BY (reference_date)

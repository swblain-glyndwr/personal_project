CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_feature_build_outputs (
  feature_build_id STRING NOT NULL,
  feature_build_attempt_id STRING NOT NULL,
  reference_date DATE NOT NULL,
  feature_id STRING NOT NULL,
  backing_table STRING NOT NULL,
  delta_version BIGINT NOT NULL,
  row_count BIGINT NOT NULL,
  contract_schema_checksum STRING NOT NULL,
  output_schema_checksum STRING NOT NULL,
  backing_schema_checksum STRING NOT NULL,
  value_checksum STRING NOT NULL,
  write_receipt_id STRING NOT NULL,
  write_duration_ms BIGINT NOT NULL,
  retry_count INT NOT NULL,
  committed_at TIMESTAMP NOT NULL,
  validated_at TIMESTAMP NOT NULL,
  null_key_count BIGINT NOT NULL,
  duplicate_key_count BIGINT NOT NULL,
  freshness_status STRING NOT NULL,
  row_drift_status STRING NOT NULL,
  validation_status STRING NOT NULL,
  CONSTRAINT pk_nextads_feature_build_outputs PRIMARY KEY (
    feature_build_id,
    feature_build_attempt_id,
    feature_id
  )
)
USING DELTA
PARTITIONED BY (reference_date)

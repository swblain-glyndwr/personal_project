CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_feature_quality_events (
  table_name STRING NOT NULL,
  check_name STRING NOT NULL,
  run_timestamp TIMESTAMP NOT NULL,
  reference_date DATE,
  status STRING,
  row_count BIGINT,
  distinct_key_count BIGINT,
  null_key_count BIGINT,
  duplicate_key_count BIGINT,
  freshness_timestamp TIMESTAMP,
  metric_value DOUBLE,
  details STRING,
  created_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_feature_quality_events PRIMARY KEY (
    table_name,
    check_name,
    run_timestamp
  )
)
USING delta
PARTITIONED BY (reference_date)

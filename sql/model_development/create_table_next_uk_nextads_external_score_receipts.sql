CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_external_score_receipts (
  receipt_id STRING NOT NULL,
  model_name STRING NOT NULL,
  provider_id STRING NOT NULL,
  source_table STRING NOT NULL,
  source_delta_version BIGINT NOT NULL,
  run_date DATE NOT NULL,
  row_count BIGINT NOT NULL,
  schema_checksum STRING NOT NULL,
  producing_run_id STRING NOT NULL,
  components_json STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_nextads_external_score_receipts PRIMARY KEY (receipt_id)
)
USING delta

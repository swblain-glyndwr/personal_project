CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_training_set_receipts (
  receipt_id STRING NOT NULL,
  model_name STRING NOT NULL,
  model_definition_checksum STRING NOT NULL,
  feature_bindings_json STRING NOT NULL,
  observation_start DATE NOT NULL,
  observation_end DATE NOT NULL,
  label_end DATE NOT NULL,
  schema_checksum STRING NOT NULL,
  data_checksum STRING NOT NULL,
  code_sha STRING NOT NULL,
  leakage_status STRING NOT NULL,
  status STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP NOT NULL,
  failure_reason STRING,
  CONSTRAINT pk_nextads_training_set_receipts PRIMARY KEY (receipt_id)
)
USING delta

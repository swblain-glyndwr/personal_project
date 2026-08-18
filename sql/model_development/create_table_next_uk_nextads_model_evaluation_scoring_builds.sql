CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_model_evaluation_scoring_builds (
  scoring_build_id STRING NOT NULL,
  scoring_build_attempt_id STRING NOT NULL,
  model_build_id STRING NOT NULL,
  model_name STRING NOT NULL,
  model_definition_checksum STRING NOT NULL,
  registered_model_name STRING NOT NULL,
  registered_model_version INT NOT NULL,
  model_uri STRING NOT NULL,
  artifact_digest STRING NOT NULL,
  run_date DATE NOT NULL,
  serving_slot STRING NOT NULL,
  account_limit BIGINT NOT NULL,
  input_account_count BIGINT NOT NULL,
  candidate_bindings_json STRING NOT NULL,
  feature_bindings_json STRING NOT NULL,
  input_row_count BIGINT NOT NULL,
  input_schema_checksum STRING NOT NULL,
  input_value_checksum STRING NOT NULL,
  output_table STRING,
  output_delta_version BIGINT,
  output_row_count BIGINT,
  output_schema_checksum STRING,
  output_value_checksum STRING,
  git_commit STRING NOT NULL,
  orchestration_run_id BIGINT NOT NULL,
  task_run_id BIGINT NOT NULL,
  execution_count INT NOT NULL,
  status STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  failure_reason STRING,
  CONSTRAINT pk_nextads_model_evaluation_scoring_builds PRIMARY KEY (
    scoring_build_id,
    scoring_build_attempt_id
  )
)
USING delta
PARTITIONED BY (run_date)

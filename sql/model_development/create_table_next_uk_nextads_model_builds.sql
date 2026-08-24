CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_model_builds (
  model_build_id STRING NOT NULL,
  model_name STRING NOT NULL,
  training_receipt_id STRING NOT NULL,
  model_definition_checksum STRING NOT NULL,
  runtime_profile STRING NOT NULL,
  status STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  mlflow_run_id STRING,
  registered_model_name STRING,
  registered_model_version BIGINT,
  model_uri STRING,
  artifact_digest STRING,
  metrics_json STRING NOT NULL,
  completed_at TIMESTAMP,
  failure_reason STRING,
  research_build_id STRING,
  selection_decision_id STRING,
  selected_candidate_id STRING,
  selected_candidate_evaluation_id STRING,
  registration_code_sha STRING,
  CONSTRAINT pk_nextads_model_builds PRIMARY KEY (model_build_id)
)
USING delta

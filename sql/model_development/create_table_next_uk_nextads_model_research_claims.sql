CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_model_research_claims (
  research_build_id STRING NOT NULL,
  research_attempt_id STRING NOT NULL,
  model_definition_checksum STRING NOT NULL,
  training_receipt_id STRING NOT NULL,
  research_plan_checksum STRING NOT NULL,
  evaluation_schema_version STRING NOT NULL,
  code_sha STRING NOT NULL,
  owner_invocation_id STRING NOT NULL,
  lease_token STRING NOT NULL,
  lease_expires_at TIMESTAMP NOT NULL,
  checkpoint STRING NOT NULL,
  checkpoint_version BIGINT NOT NULL,
  research_frame_binding_json STRING,
  mlflow_experiment_id STRING,
  mlflow_parent_run_id STRING,
  selection_decision_id STRING,
  model_build_id STRING,
  failure_reason STRING,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_nextads_model_research_claims
    PRIMARY KEY (research_build_id)
)
USING delta

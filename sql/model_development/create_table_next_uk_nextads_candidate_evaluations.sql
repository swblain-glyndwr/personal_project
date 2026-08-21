CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_candidate_evaluations (
  candidate_evaluation_id STRING NOT NULL,
  candidate_attempt_id STRING NOT NULL,
  research_build_id STRING NOT NULL,
  research_attempt_id STRING NOT NULL,
  candidate_id STRING NOT NULL,
  candidate_spec_checksum STRING NOT NULL,
  required BOOLEAN NOT NULL,
  status STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  mlflow_run_id STRING,
  model_uri STRING,
  metrics_json STRING NOT NULL,
  artifact_manifest_digest STRING,
  explanation_status STRING,
  failure_reason STRING,
  CONSTRAINT pk_nextads_candidate_evaluations
    PRIMARY KEY (candidate_evaluation_id, candidate_attempt_id)
)
USING delta

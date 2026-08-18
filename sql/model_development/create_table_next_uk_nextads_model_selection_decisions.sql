CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_model_selection_decisions (
  selection_decision_id STRING NOT NULL,
  selection_attempt_id STRING NOT NULL,
  research_build_id STRING NOT NULL,
  research_attempt_id STRING NOT NULL,
  selection_mode STRING NOT NULL,
  recommended_candidate_id STRING NOT NULL,
  selected_candidate_id STRING NOT NULL,
  selected_candidate_evaluation_id STRING NOT NULL,
  reason STRING NOT NULL,
  status STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP NOT NULL,
  reviewed_by STRING,
  model_build_id STRING,
  registered_model_name STRING,
  decision_code_sha STRING,
  failure_reason STRING,
  CONSTRAINT pk_nextads_model_selection_decisions
    PRIMARY KEY (selection_decision_id, selection_attempt_id)
)
USING delta

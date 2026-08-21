CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_model_evaluation_scores (
  scoring_build_id STRING NOT NULL,
  scoring_build_attempt_id STRING NOT NULL,
  run_date DATE NOT NULL,
  model_build_id STRING NOT NULL,
  route STRING NOT NULL,
  scope_type STRING NOT NULL,
  scope_value STRING NOT NULL,
  candidate_build_id STRING NOT NULL,
  candidate_build_attempt_id STRING NOT NULL,
  portfolio_id STRING NOT NULL,
  portfolio_attempt_id STRING NOT NULL,
  candidate_foundation_snapshot_id STRING NOT NULL,
  serving_slot STRING NOT NULL,
  account_number STRING NOT NULL,
  advert_id STRING NOT NULL,
  incumbent_score DOUBLE,
  incumbent_trigger_score DOUBLE,
  incumbent_rank INT NOT NULL,
  predicted_pctr DOUBLE NOT NULL,
  evaluation_rank INT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_nextads_model_evaluation_scores PRIMARY KEY (
    scoring_build_id,
    scoring_build_attempt_id,
    route,
    scope_type,
    scope_value,
    account_number,
    advert_id
  )
)
USING delta
PARTITIONED BY (run_date, route)

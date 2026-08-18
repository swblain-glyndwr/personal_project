CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_model_evaluation_candidates (
  model_build_id STRING NOT NULL,
  training_receipt_id STRING NOT NULL,
  provider_id STRING NOT NULL,
  use_case STRING NOT NULL,
  run_date DATE NOT NULL,
  account_number STRING NOT NULL,
  route STRING NOT NULL,
  location STRING NOT NULL,
  advert_id STRING NOT NULL,
  score DOUBLE NOT NULL,
  provider_rank INT NOT NULL,
  created_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_nextads_model_evaluation_candidates PRIMARY KEY (
    model_build_id,
    account_number,
    route,
    location,
    advert_id
  )
)
USING delta
PARTITIONED BY (run_date)

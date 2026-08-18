CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_feature_builds (
  feature_build_id STRING NOT NULL,
  feature_build_attempt_id STRING NOT NULL,
  reference_date DATE NOT NULL,
  registry_checksum STRING NOT NULL,
  git_commit STRING NOT NULL,
  required_feature_ids_json STRING NOT NULL,
  required_feature_count INT NOT NULL,
  source_count INT NOT NULL,
  output_count INT NOT NULL,
  status STRING NOT NULL,
  job_run_id BIGINT,
  execution_count INT NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  failure_reason STRING,
  CONSTRAINT pk_nextads_feature_builds PRIMARY KEY (
    feature_build_id,
    feature_build_attempt_id
  )
)
USING DELTA
PARTITIONED BY (reference_date)

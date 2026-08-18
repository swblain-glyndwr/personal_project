CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_feature_snapshots (
  feature_snapshot_id STRING NOT NULL,
  feature_snapshot_attempt_id STRING NOT NULL,
  feature_build_id STRING NOT NULL,
  feature_build_attempt_id STRING NOT NULL,
  reference_date DATE NOT NULL,
  registry_checksum STRING NOT NULL,
  git_commit STRING NOT NULL,
  required_feature_ids_json STRING NOT NULL,
  required_feature_count INT NOT NULL,
  binding_count INT NOT NULL,
  status STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  failure_reason STRING,
  CONSTRAINT pk_nextads_feature_snapshots PRIMARY KEY (
    feature_snapshot_id,
    feature_snapshot_attempt_id
  )
)
USING DELTA
PARTITIONED BY (reference_date)

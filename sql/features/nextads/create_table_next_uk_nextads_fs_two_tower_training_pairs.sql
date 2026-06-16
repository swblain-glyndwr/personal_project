CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_two_tower_training_pairs (
  anchor_entity_type STRING NOT NULL,
  anchor_entity_id STRING NOT NULL,
  candidate_entity_type STRING NOT NULL,
  candidate_entity_id STRING NOT NULL,
  label_name STRING NOT NULL,
  reference_date DATE NOT NULL,
  label_value DOUBLE,
  sample_weight DOUBLE,
  negative_sampling_strategy STRING,
  source_event_type STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_two_tower_training_pairs PRIMARY KEY (
    anchor_entity_type,
    anchor_entity_id,
    candidate_entity_type,
    candidate_entity_id,
    label_name,
    reference_date
  )
)
USING delta
PARTITIONED BY (reference_date)


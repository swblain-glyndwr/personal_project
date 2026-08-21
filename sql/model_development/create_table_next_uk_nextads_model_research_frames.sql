CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_model_research_frames (
  research_frame_id STRING NOT NULL,
  research_frame_attempt_id STRING NOT NULL,
  research_build_id STRING NOT NULL,
  research_attempt_id STRING NOT NULL,
  training_receipt_id STRING NOT NULL,
  row_id STRING NOT NULL,
  observation_date DATE NOT NULL,
  split STRING NOT NULL,
  label DOUBLE NOT NULL,
  features_json STRING NOT NULL,
  slices_json STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  CONSTRAINT pk_nextads_model_research_frames
    PRIMARY KEY (research_frame_id, research_frame_attempt_id, row_id)
)
USING delta

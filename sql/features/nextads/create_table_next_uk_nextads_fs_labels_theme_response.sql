CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_labels_theme_response (
  account_number STRING NOT NULL,
  theme STRING NOT NULL,
  reference_date DATE NOT NULL,
  label_name STRING NOT NULL,
  label_value DOUBLE,
  target_event_count BIGINT,
  target_window_days INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_labels_theme_response PRIMARY KEY (
    account_number,
    theme,
    reference_date,
    label_name
  )
)
USING delta
PARTITIONED BY (reference_date)


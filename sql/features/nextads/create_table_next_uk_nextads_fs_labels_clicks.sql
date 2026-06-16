CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_labels_clicks (
  account_number STRING NOT NULL,
  advert_id STRING NOT NULL,
  location STRING NOT NULL,
  session_date DATE NOT NULL,
  label_horizon_days INT NOT NULL,
  impression_count BIGINT,
  click_count BIGINT,
  clicked INT,
  first_click_timestamp TIMESTAMP,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_labels_clicks PRIMARY KEY (
    account_number,
    advert_id,
    location,
    session_date,
    label_horizon_days
  )
)
USING delta
PARTITIONED BY (session_date)


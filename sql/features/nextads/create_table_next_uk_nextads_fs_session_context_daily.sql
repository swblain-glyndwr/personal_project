CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_session_context_daily (
  account_number STRING NOT NULL,
  session_id STRING NOT NULL,
  session_date DATE NOT NULL,
  device_simple STRING,
  channel_simple STRING,
  geocountry_simple STRING,
  session_hour INT,
  session_dayofweek INT,
  session_month INT,
  session_weekofyear INT,
  session_is_weekend INT,
  pages_in_session BIGINT,
  shopping_bag_pages_in_session BIGINT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_session_context_daily PRIMARY KEY (
    account_number,
    session_id,
    session_date
  )
)
USING delta
PARTITIONED BY (session_date)


CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_account_web_activity_90d (
  account_number STRING NOT NULL,
  reference_date DATE NOT NULL,
  browse_sessions_90d BIGINT,
  browse_active_days_90d BIGINT,
  page_events_90d BIGINT,
  shopping_bag_page_events_90d BIGINT,
  avg_pages_per_session_90d DOUBLE,
  action_events_90d BIGINT,
  action_active_days_90d BIGINT,
  add_to_bag_actions_90d BIGINT,
  pdp_action_rows_90d BIGINT,
  browse_session_recency_days INT,
  action_recency_days INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_account_web_activity_90d PRIMARY KEY (
    account_number,
    reference_date
  )
)
USING delta
PARTITIONED BY (reference_date)


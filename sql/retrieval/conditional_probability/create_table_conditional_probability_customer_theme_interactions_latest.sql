CREATE TABLE {catalog}.{schema}.{client}_nextads_conditional_probability_customer_theme_interactions_latest (
  account_number STRING,
  date DATE,
  theme STRING,
  interaction_type STRING,
  interaction_weight DOUBLE,
  array_agg_itemnumber ARRAY<STRING>,
  item_theme_weight DOUBLE,
  inttype_and_time_decay_weight DOUBLE,
  item_count INTEGER,
  rundate DATE NOT NULL
)
PARTITIONED BY (rundate)

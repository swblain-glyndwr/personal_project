CREATE TABLE marketingdata_prod.{schema}.{client}_nextads_conditional_probability_customer_item_interactions_latest (
  account_number STRING,
  itemnumber STRING,
  date DATE,
  interaction_type STRING,
  days_ago INTEGER,
  inttype_and_time_decay_weight DOUBLE,
  rundate DATE NOT NULL
)
PARTITIONED BY (rundate)

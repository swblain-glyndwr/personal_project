CREATE TABLE {catalog}.{schema}.{client}_nextads_conditional_probability_theme_associations_latest (
  theme1 STRING,
  theme2 STRING,
  freq12 DOUBLE,
  theme1_total_weight DOUBLE,
  theme2_total_weight DOUBLE,
  lift DOUBLE,
  lift_adjusted DOUBLE,
  cosine_similarity DOUBLE,
  pct_of_theme2_sequences DOUBLE,
  prob_theme1_precedes_theme2 DOUBLE,
  support_theme1 DOUBLE,
  support_theme2 DOUBLE,
  support_sequence DOUBLE,
  confidence_theme2_given_theme1 DOUBLE,
  rundate DATE NOT NULL
)
PARTITIONED BY (rundate)

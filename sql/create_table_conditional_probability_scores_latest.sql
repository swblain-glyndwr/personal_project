CREATE TABLE marketingdata_prod.{schema}.{client}_nextads_conditional_probability_scores_latest (
  account_number STRING NOT NULL,
  recommended_theme STRING NOT NULL,
  score_freq DOUBLE,
  score_lift DOUBLE,
  score_confidence DOUBLE,
  score_hybrid DOUBLE,
  contributing_seed_themes ARRAY<STRUCT<theme: STRING, weight: DOUBLE>>,
  num_seed_themes INT NOT NULL,
  total_seed_affinity DOUBLE,
  avg_freq12 DOUBLE,
  max_cosine_similarity DOUBLE,
  max_lift_adjusted DOUBLE,
  latest_seed_interaction DATE,
  num_contributing_items INT NOT NULL,
  contributing_seed_items ARRAY<STRUCT<itemno: STRING, weight: DOUBLE>>,
  rundate DATE NOT NULL,
  CONSTRAINT pk_{client}_nextads_conditional_probability_scores_latest PRIMARY KEY (
    account_number,
    recommended_theme
  )
)
PARTITIONED BY (rundate)

CREATE TABLE marketingdata_prod.{schema}.{client}_nextads_conditional_probability_item_themes_latest (
  pid STRING,
  theme STRING,
  item_theme_weight DOUBLE,
  rundate DATE NOT NULL
)
PARTITIONED BY (rundate)

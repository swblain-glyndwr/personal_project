CREATE TABLE IF NOT EXISTS {catalog}.{schema}.next_uk_nextads_fs_theme_popularity_daily (
  theme STRING NOT NULL,
  reference_date DATE NOT NULL,
  views_7d BIGINT,
  views_30d BIGINT,
  baskets_7d BIGINT,
  baskets_30d BIGINT,
  views_ly_7 DOUBLE,
  views_ly_30 DOUBLE,
  baskets_ly_7 DOUBLE,
  baskets_ly_30 DOUBLE,
  trending_7x30 DOUBLE,
  popularity_rank INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  CONSTRAINT pk_nextads_fs_theme_popularity_daily PRIMARY KEY (
    theme,
    reference_date
  )
)
USING delta
PARTITIONED BY (reference_date)

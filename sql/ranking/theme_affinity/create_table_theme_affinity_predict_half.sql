create table {catalog}.{schema}.{client}_nextads_theme_affinity_predict_half (
    account_number string not null,
    theme string not null,
    month int,
    baskets_behavior__recency_rank int,
    prediction float,
  constraint pk_{client}_nextads_theme_affinity_predict_half primary key (
    account_number,
    theme
    )
)

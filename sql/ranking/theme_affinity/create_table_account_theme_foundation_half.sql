create table {catalog}.{schema}.{client}_nextads_account_theme_foundation_half (
    account_number string not null,
    theme string not null,
    month int,
    baskets_behavior__recency_rank int,
    prediction float,
  constraint pk_{client}_nextads_account_theme_foundation_half primary key (
    account_number,
    theme
    )
)

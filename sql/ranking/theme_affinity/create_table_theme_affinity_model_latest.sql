create table {catalog}.{schema}.{client}_nextads_theme_affinity_model_latest (
    AccountNumber string not null,
    NextTheme string not null,
    ProbAggRebased float not null,
    rundate date not null,
  constraint pk_{client}_nextads_theme_affinity_model_latest primary key (
    AccountNumber,
    NextTheme
    )
)

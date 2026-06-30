create table {catalog}.{schema}.{client}_nextads_theme_affinity_model_full (
    AccountNumber string not null,
    NextTheme string not null,
    ProbAggRebased float not null,
    rundate date not null,
  constraint pk_{client}_nextads_theme_affinity_model_full primary key (
    AccountNumber,
    NextTheme,
    rundate
    )
)

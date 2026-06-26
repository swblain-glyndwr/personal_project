create table {catalog}.{schema}.{client}_nextads_next_theme_scores_latest (
    AccountNumber string not null,
    NextTheme string not null,
    ProbAgg float not null,
    ProbBase float not null,
    ProbAggRebased float not null,
    rundate date not null,
  constraint pk_{client}_nextads_next_theme_scores_latest primary key (
    AccountNumber,
    NextTheme
    )
)
partitioned by (NextTheme)
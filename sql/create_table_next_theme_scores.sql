create table marketingdata_prod.{schema}.{client}_nextads_next_theme_scores (
    AccountNumber string not null,
    NextTheme string not null,
    ProbAgg float not null,
    ProbBase float not null,
    ProbAggRebased float not null,
    rundate date not null,
  constraint pk_{client}_nextads_next_theme_scores primary key (
    AccountNumber,
    NextTheme,
    rundate
    )
)
partitioned by (rundate)
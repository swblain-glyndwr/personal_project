create table marketingdata_prod.{schema}.next_uk_nextads_results_aggregates (
    SessionDate date not null,
    AggColumn string not null,
    AggValue not null,
    FallowControl string not null,
    Sessions int,
    Revenue double,
    Conversions int,
    SoftImpressions int,
    SoftClicks int,
    rundate date not null,
  constraint pk_next_uk_nextads_results_aggregates primary key (
    SessionDate,
    AggColumn,
    AggValue,
    FallowControl,
    rundate)
)
partitioned by (rundate)
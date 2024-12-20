create table marketingdata_prod.{schema}.next_uk_nextads_results_aggregates (
    SessionDate date not null,
    Device string not null,
    OS string not null,
    AggColumn string not null,
    AggValue string not null,
    FallowControl string not null,
    Sessions int,
    Revenue double,
    Conversions int,
    SoftImpressions int,
    SoftClicks int,
    rundate date not null,
  constraint pk_next_uk_nextads_results_aggregates primary key (
    SessionDate,
    Device,
    OS,
    AggColumn,
    AggValue,
    FallowControl,
    rundate)
)
partitioned by (SessionDate)
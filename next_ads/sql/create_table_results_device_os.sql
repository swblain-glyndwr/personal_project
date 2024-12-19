create table marketingdata_prod.{schema}.next_uk_nextads_results_device_os (
    SessionDate date not null,
    Device string not null,
    OS string not null,
    FallowControl string not null,
    Sessions int,
    Revenue double,
    Conversions int,
    SoftImpressions int,
    SoftClicks int,
    rundate date not null,
  constraint pk_next_uk_nextads_results_device_os primary key (
    SessionDate,
    Device,
    OS,
    FallowControl,
    rundate)
)
partitioned by (rundate)
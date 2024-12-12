create table marketingdata_prod.{schema}.next_uk_nextads_results_topline (
    SessionDate date not null,
    Device string not null,
    FallowControl string not null,
    Sessions int not null,
    Revenue double not null,
    rundate date not null,
  constraint pk_next_uk_nextads_results_topline primary key (
    SessionDate,
    Device,
    FallowControl,
    rundate)
)
partitioned by (rundate)
create table marketingdata_prod.{schema}.{domain}_nextads_results_topline (
    SessionDate date not null,
    Device string not null,
    OS string not null,
    Sessions int,
    Revenue double,
    Conversions int,
    SoftImpressions int,
    SoftClicks int,
    C_Sessions int,
    C_Revenue double,
    C_Conversions int,
    C_SoftImpressions int,
    C_SoftClicks int,
    rundate date not null,
  constraint pk_{domain}_nextads_results_topline primary key (
    SessionDate,
    Device,
    OS,
    rundate)
)
partitioned by (SessionDate)
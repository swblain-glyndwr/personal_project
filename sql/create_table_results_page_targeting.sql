create table marketingdata_prod.{schema}.{domain}_nextads_results_page_targeting (
    SessionDate date not null,
    Device string not null,
    OS string not null,
    PageGroup string not null,
    Targeting string not null,
    Sessions int,
    Revenue double,
    Conversions int,
    SoftImpressions int,
    SoftClicks int,
    rundate date not null,
  constraint pk_{domain}_nextads_results_page_targeting primary key (
    SessionDate,
    Device,
    OS,
    PageGroup,
    Targeting,
    rundate)
)
partitioned by (SessionDate)
create table marketingdata_prod.{schema}.next_uk_nextads_results_ads_targeting (
    SessionDate date not null,
    Device string not null,
    OS string not null,
    UniqueAdID string not null,
    PageGroup string not null,
    Targeting string not null,
    Sessions int,
    Revenue double,
    Conversions int,
    SoftImpressions int,
    SoftClicks int,
    rundate date not null,
  constraint pk_next_uk_nextads_results_ads_targeting primary key (
    SessionDate,
    Device,
    OS,
    UniqueAdID,
    PageGroup,
    Targeting,
    rundate)
)
partitioned by (SessionDate)
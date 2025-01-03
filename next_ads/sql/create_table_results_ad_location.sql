create table marketingdata_prod.{schema}.next_uk_nextads_results_ad_location (
    SessionDate date not null,
    Device string not null,
    OS string not null,
    TestGroup string not null,
    UniqueAdID string not null,
    LocationSet string not null,
    Sessions int,
    Revenue double,
    Conversions int,
    SoftImpressions int,
    SoftClicks int,
    rundate date not null,
  constraint pk_next_uk_nextads_results_ad_location primary key (
    SessionDate,
    Device,
    OS,
    TestGroup,
    UniqueAdID,
    LocationSet,
    rundate)
)
partitioned by (SessionDate)
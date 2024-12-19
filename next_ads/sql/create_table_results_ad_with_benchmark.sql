create table marketingdata_prod.{schema}.next_uk_nextads_results_ad_with_benchmark (
    SessionDate date not null,
    TestGroup string not null,
    UniqueAdID string not null,
    Sessions int,
    Revenue double,
    Conversions int,
    SoftImpressions int,
    SoftClicks int,
    rundate date not null,
  constraint pk_next_uk_nextads_results_ad_with_benchmark primary key (
    SessionDate,
    TestGroup,
    UniqueAdID,
    rundate)
)
partitioned by (rundate)
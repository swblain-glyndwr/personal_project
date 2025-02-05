create table marketingdata_prod.{schema}.{domain}_nextads_results_ads_location (
    SessionDate date not null,
    Device string not null,
    OS string not null,
    UniqueAdID string not null,
    LocationSet string not null,
    Sessions int,
    Revenue double,
    Conversions int,
    SoftImpressions int,
    SoftClicks int,
    ApportionedRevenue double,
    C_Sessions int,
    C_Revenue double,
    C_Conversions int,
    C_SoftImpressions int,
    C_SoftClicks int,
    C_ApportionedRevenue double,
    SessionOverlapRatio double,
    rundate date not null,
  constraint pk_{domain}_nextads_results_ads_location primary key (
    SessionDate,
    Device,
    OS,
    UniqueAdID,
    LocationSet,
    rundate)
)
partitioned by (SessionDate)
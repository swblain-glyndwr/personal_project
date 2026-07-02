create table {catalog}.{schema}.{client}_nextads_results_ads_page (
    SessionDate date not null,
    Device string not null,
    OS string not null,
    UniqueAdID string not null,
    PageGroupSet string not null,
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
  constraint pk_{client}_nextads_results_ads_page primary key (
    SessionDate,
    Device,
    OS,
    UniqueAdID,
    PageGroupSet,
    rundate)
)
partitioned by (SessionDate)
create table marketingdata_prod.{schema}.next_uk_nextads_results_ad_metadata (
    SessionDate date not null,
    UniqueAdID string not null,
    PotNumber string,
    CampaignNumber string,
    Title string,
    AlgoDivision string,
    TradeDivision string,
    Brand string,
    MASIDToken string,
    Segment string,
    AdDriver string,
    TemplateName string,
    TargetingCriteria string,
    AdCategory string,
    AdMission string,
    AdTrend string,
    AdSubcategory string,
    AdBrandName string,
    AdCampaign string,
    rundate date not null,
  constraint pk_next_uk_nextads_results_ad_metadata primary key (
    SessionDate,
    UniqueAdID,
    rundate)
)
partitioned by (SessionDate)
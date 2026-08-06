CREATE TABLE  {catalog}.{schema}.{client}_nextads_assignments_v2_latest (
    AccountNumber string not null,
    PageType string not null,
    Rank int not null,
    UniqueAdIDBasic string,
    UniqueAdIDBest string,
    UniqueAdIDBestChallenger string,
    UniqueAdIDNextGenAds string,
    Treatment string,
    UniqueAdIDMeasurement string,
    UniqueAdIDAssigned string not null,
    TriggerScore float,
    rundate date not null,
  constraint pk_{client}_nextads_assignments_v2_latest primary key (
    AccountNumber,
    PageType,
    Rank)
  ,constraint nextads_assignment_v2_latest_rank_valid check (Rank >= 1)
  ,constraint nextads_assignment_v2_latest_trigger_finite check (
    TriggerScore is null or TriggerScore between -3.4028235E38 and 3.4028235E38
    )
)
partitioned by (PageType)

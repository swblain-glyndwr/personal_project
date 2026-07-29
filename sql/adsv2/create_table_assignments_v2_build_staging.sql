create table {catalog}.{schema}.{client}_nextads_assignments_v2_build_staging (
    BuildRunID string not null,
    TaskRunID bigint not null,
    ExecutionCount int not null,
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
  constraint pk_{client}_nextads_assignments_v2_build_staging primary key (
    BuildRunID,
    AccountNumber,
    PageType,
    Rank)
)
partitioned by (BuildRunID, PageType)

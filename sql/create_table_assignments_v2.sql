CREATE TABLE  {catalog}.{schema}.{client}_nextads_assignments_v2 (
    AccountNumber string not null,
    PageType string not null,
    Rank int not null,
    UniqueAdIDBasic string,
    UniqueAdIDBest string,
    UniqueAdIDBestChallenger string,
    Treatment string,
    UniqueAdIDMeasurement string,
    UniqueAdIDAssigned string not null,
    rundate date not null,
  constraint pk_{client}_nextads_assignments_v2 primary key (
    AccountNumber,
    PageType,
    Rank,
    rundate)
)
partitioned by (PageType)
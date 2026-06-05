create table {catalog}.{schema}.{client}_nextads_assignments (
    AccountNumber string not null,
    Location string not null,
    UniqueAdIDBasic string,
    UniqueAdIDBest string,
    UniqueAdIDBestChallenger string,
    UniqueAdIDNextGenAds string,
    Treatment string,
    UniqueAdIDMeasurement string,
    UniqueAdIDAssigned string not null,
    MASID string not null,
    rundate date not null,
  constraint pk_{client}_nextads_assignments primary key (
    AccountNumber,
    Location,
    rundate)
)
partitioned by (rundate)
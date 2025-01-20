create table marketingdata_prod.{schema}.{domain}_nextads_assignments (
    AccountNumber string not null,
    Location string not null,
    UniqueAdIDBasic string,
    UniqueAdIDBest string,
    UniqueAdIDBestChallenger string,
    Treatment string,
    UniqueAdIDMeasurement string,
    UniqueAdIDAssigned string not null,
    MASID string not null,
    rundate date not null,
  constraint pk_{domain}_nextads_assignments primary key (
    AccountNumber,
    Location,
    rundate)
)
partitioned by (rundate)
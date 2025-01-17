create table marketingdata_prod.{schema}.next_uk_nextads_assignments_latest (
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
  constraint pk_next_uk_nextads_assignments_latest primary key (
    AccountNumber,
    Location)
)
partitioned by (Location)
create table marketingdata_prod.{schema}.next_uk_nextads_assignments (
    AccountNumber string not null,
    Location string not null,
    UniqueAdIDRandom string,
    UniqueAdIDBest string,
    UniqueAdIDBestChallenger string,
    UniqueAdIDAssigned string not null,
    MASID string not null,
    rundate date not null,
  constraint pk_next_uk_nextads_assignments primary key (
    AccountNumber,
    Location,
    rundate)
)
partitioned by (rundate)
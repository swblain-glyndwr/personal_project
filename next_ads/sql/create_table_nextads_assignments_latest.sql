create table marketingdata_prod.ds_sandbox.next_uk_nextads_assignments_latest (
    AccountNumber string not null,
    UniqueAdID string not null,
    Location string not null,
    Division string not null,
    RandomMASID string,
    BestMASID string,
    MASID string not null,
    rundate date not null,
  constraint pk_ad_assignments_latest primary key (
    AccountNumber,
    UniqueAdID,
    Location,
    Division,
    rundate
    )
)
partitioned by (rundate)
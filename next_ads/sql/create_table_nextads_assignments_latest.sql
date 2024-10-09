create table marketingdata_prod.ds_sandbox.next_uk_nextads_assignments_latest (
    AccountNumber string not null,
    Location string not null,
    RandUniqueAdID string not null,
    RandomMASID string,
    BestUniqueAdID string not null,
    BestMASID string,
    MASID string not null,
    rundate date not null,
  constraint pk_ad_assignments_latest primary key (
    AccountNumber,
    Location,
    rundate
    )
)
partitioned by (rundate)
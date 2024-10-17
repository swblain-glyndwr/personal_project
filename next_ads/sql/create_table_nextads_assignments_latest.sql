create table marketingdata_prod.ds_sandbox.next_uk_nextads_assignments_latest (
    AccountNumber string not null,
    Location string not null,
    RandomUniqueAdID string not null,
    RandomMASID string not null,
    BestUniqueAdID string,
    BestMASID string,
    BestUniqueAdIDChallenger string,
    BestMASIDChallenger string,
    MASID string not null,
    rundate date not null,
  constraint pk_ad_assignments_latest primary key (
    AccountNumber,
    Location
    )
)
partitioned by (Location)
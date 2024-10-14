create table marketingdata_prod.ds_sandbox.next_uk_nextads_assignments (
    AccountNumber string not null,
    Location string not null,
    RandomUniqueAdID string not null,
    RandomMASID string not null,
    BestUniqueAdID string,
    BestMASID string,
    BestUniqueAdIDChall string,
    BestMASIDChall string,
    MASID string not null,
    rundate date not null,
  constraint pk_ad_assignments primary key (
    AccountNumber,
    Location,
    rundate
    )
)
partitioned by (rundate)
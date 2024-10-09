create table marketingdata_prod.ds_sandbox.next_uk_nextads_assignments (
    AccountNumber string not null,
    Location string not null,
    UniqueAdID string not null,
    RandomMASID string,
    BestMASID string,
    MASID string not null,
    rundate date not null,
  constraint pk_ad_assignments primary key (
    AccountNumber,
    UniqueAdID,
    Location,
    rundate
    )
)
partitioned by (rundate)
create table marketingdata_prod.{schema}.{domain}_nextads_ad_items (
  UniqueAdID string not null,
  RepresentativeItems array<string>,
  rundate date not null,
  constraint pk_{domain}_nextads_ad_items primary key (
    UniqueAdID,
    rundate)
)
partitioned by (rundate)
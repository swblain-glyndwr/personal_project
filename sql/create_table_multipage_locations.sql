create table marketingdata_prod.{schema}.{client}_nextads_multipage_locations (
  Location string not null,
  Page string not null,
  Screen string,
  rundate date not null,
  constraint pk_{client}_nextads_multipage_locations primary key (
    Location,
    Page,
    rundate)
)
partitioned by (rundate)
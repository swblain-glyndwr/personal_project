create table {catalog}.{schema}.{client}_nextads_item_attributes_latest (
    pid string not null,
    attribute string not null,
    value string not null,
    rundate date not null,
  constraint pk_{client}_nextads_item_attributes_latest primary key (
    pid,
    attribute,
    value,
    rundate
    )
)
partitioned by (attribute)
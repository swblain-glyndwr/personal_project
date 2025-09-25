create table marketingdata_prod.{schema}.{client}_nextads_attribute_set_latest (
    attribute string not null,
    value string not null,
    rundate date not null,
  constraint pk_{client}_nextads_attribute_set_latest primary key (
    attribute,
    value,
    rundate
    )
)
partitioned by (attribute)
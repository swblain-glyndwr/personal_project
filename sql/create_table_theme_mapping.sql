create table marketingdata_prod.{schema}.{client}_nextads_theme_mapping (
    Theme string not null,
    attribute string not null,
    value string not null,
    rundate date not null,
  constraint pk_{client}_nextads_theme_mapping primary key (
    Theme,
    attribute,
    value,
    rundate
    )
)
partitioned by (rundate)
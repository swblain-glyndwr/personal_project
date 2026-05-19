create table {catalog}.{schema}.{client}_nextads_exclusions_latest (
    PageType string not null,
    Page string not null,
    Exclude_Campaign string not null,
    rundate date not null,
  constraint pk_{client}_nextads_exclusions_latest primary key (
    PageType,
    Page,
    Exclude_Campaign
    )
)
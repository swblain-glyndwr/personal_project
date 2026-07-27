create table {catalog}.{schema}.{client}_nextads_exclusions_latest (
    url string not null,
    masidSlot string not null,
    CMSPageID string not null,
    rundate date not null,
  constraint pk_{client}_nextads_exclusions_latest primary key (
    url,
    masidSlot,
    CMSPageID
    )
)
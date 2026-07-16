CREATE TABLE {catalog}.{schema}.{client}_nextads_cms_content_latest (
  CMSPageID STRING,
  cms_data STRING,
  rundate DATE,
  constraint pk_{client}_nextads_cms_content_latest primary key (CMSPageID)
  )
  partitioned by (rundate)
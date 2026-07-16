CREATE TABLE {catalog}.{schema}.{client}_nextads_cms_content (
  CMSPageID STRING,
  cms_data STRING,
  rundate DATE,
  constraint pk_{client}_nextads_cms_content primary key (CMSPageID, rundate)
  )
  partitioned by (rundate)
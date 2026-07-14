CREATE TABLE {catalog}.{schema}.{client}_nextads_item_fs_catid (
    itemno string not null,
    catid string not null,
    reference_date date not null,
  constraint pk_{client}_nextads_item_catid primary key (
    itemno,
    catid,
    reference_date
    )
)
USING  DELTA
CLUSTER BY (reference_date)

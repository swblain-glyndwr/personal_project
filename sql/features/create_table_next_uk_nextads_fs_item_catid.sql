CREATE TABLE {catalog}.{schema}.next_uk_nextads_item_fs_catid (
    itemno string not null,
    catid string not null,
    reference_date date not null,
  constraint pk_next_uk_nextads_item_catid primary key (
    itemno,
    catid,
    reference_date
    )
)
USING  DELTA
CLUSTER BY (reference_date)
